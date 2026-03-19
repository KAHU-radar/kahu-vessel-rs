#!/usr/bin/env python3
"""
Replay a Navico/Halo radar pcap over UDP multicast.
No third-party dependencies — uses only Python stdlib.

Runs on the Mac and sends packets across the network so mayara on the Pi
picks them up on its real ethernet interface.

Usage:
    python3 tools/replay_pcap.py path/to/halo_and_0183.pcap
    python3 tools/replay_pcap.py path/to/halo_and_0183.pcap --speed 2.0
    python3 tools/replay_pcap.py path/to/halo_and_0183.pcap --loop
"""

import argparse
import socket
import struct
import time
import sys


# ---------------------------------------------------------------------------
# Minimal pcap parser (no dependencies)
# ---------------------------------------------------------------------------

PCAP_MAGIC_LE   = 0xA1B2C3D4
PCAP_MAGIC_NS   = 0xA1B23C4D  # nanosecond-resolution variant

ETH_TYPE_IPV4   = 0x0800
PROTO_UDP       = 17
ETH_HEADER_LEN  = 14


def _read_exact(f, n: int) -> bytes:
    data = f.read(n)
    if len(data) != n:
        raise EOFError
    return data


def load_udp_packets(pcap_path: str) -> list[tuple[float, str, int, bytes]]:
    """
    Parse pcap and return list of (timestamp_s, dst_ip, dst_port, payload)
    for every UDP packet found.
    """
    results = []
    with open(pcap_path, "rb") as f:
        # Global header
        magic = struct.unpack("<I", _read_exact(f, 4))[0]
        if magic == PCAP_MAGIC_LE:
            endian, ns_res = "<", False
        elif magic == PCAP_MAGIC_NS:
            endian, ns_res = "<", True
        elif magic in (0xD4C3B2A1, 0x4D3CB2A1):
            endian, ns_res = ">", magic == 0x4D3CB2A1
        else:
            raise ValueError(f"Not a pcap file (magic={magic:#010x})")

        _version = _read_exact(f, 4)
        _zone    = _read_exact(f, 4)
        _sigfigs = _read_exact(f, 4)
        _snaplen = _read_exact(f, 4)
        link_type = struct.unpack(endian + "I", _read_exact(f, 4))[0]

        if link_type != 1:
            print(f"Warning: link type is {link_type}, expected 1 (Ethernet). Proceeding anyway.")

        # Packet records
        while True:
            try:
                hdr = _read_exact(f, 16)
            except EOFError:
                break
            ts_sec, ts_frac, incl_len, _ = struct.unpack(endian + "IIII", hdr)
            ts = ts_sec + (ts_frac / 1e9 if ns_res else ts_frac / 1e6)
            raw = _read_exact(f, incl_len)

            # Ethernet header
            if len(raw) < ETH_HEADER_LEN:
                continue
            ethertype = struct.unpack("!H", raw[12:14])[0]
            if ethertype != ETH_TYPE_IPV4:
                continue
            ip_raw = raw[ETH_HEADER_LEN:]

            # IPv4 header
            if len(ip_raw) < 20:
                continue
            ihl = (ip_raw[0] & 0x0F) * 4
            proto = ip_raw[9]
            if proto != PROTO_UDP:
                continue
            dst_ip = socket.inet_ntoa(ip_raw[16:20])
            udp_raw = ip_raw[ihl:]

            # UDP header
            if len(udp_raw) < 8:
                continue
            dst_port = struct.unpack("!H", udp_raw[2:4])[0]
            payload = udp_raw[8:]

            results.append((ts, dst_ip, dst_port, payload))

    return results


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------

def replay_once(packets: list, sock: socket.socket, speed: float, loopback: bool) -> int:
    if not packets:
        return 0
    first_ts = packets[0][0]
    wall_start = time.monotonic()
    sent = 0
    for ts, dst_ip, dst_port, payload in packets:
        target = wall_start + (ts - first_ts) / speed
        gap = target - time.monotonic()
        if gap > 0:
            time.sleep(gap)
        if dst_port == 0:
            continue  # port 0 is invalid for UDP sendto
        # On loopback the kernel won't route 255.255.255.255, so redirect to
        # 127.0.0.1.  On a real interface the broadcast reaches the subnet
        # directly, so send as-is.
        send_ip = "127.0.0.1" if (dst_ip.endswith(".255") and loopback) else dst_ip
        try:
            sock.sendto(payload, (send_ip, dst_port))
            sent += 1
        except OSError as e:
            print(f"  send error to {send_ip}:{dst_port}: {e}", file=sys.stderr)
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay Navico radar pcap over UDP (no dependencies)")
    parser.add_argument("pcap", help="Path to .pcap file")
    parser.add_argument("--speed", type=float, default=1.0,
                        help="Replay speed multiplier (default: 1.0 = real-time)")
    parser.add_argument("--loop", action="store_true",
                        help="Loop continuously (Ctrl-C to stop)")
    parser.add_argument("--ttl", type=int, default=4,
                        help="Multicast TTL (default 4; must be >1 to leave the local machine)")
    parser.add_argument("--iface", default="127.0.0.1",
                        help="Local IP address of the outgoing interface (default: 127.0.0.1 for loopback)")
    args = parser.parse_args()

    print(f"Loading {args.pcap} ...")
    packets = load_udp_packets(args.pcap)
    if not packets:
        print("No UDP packets found in pcap.")
        sys.exit(1)
    duration = packets[-1][0] - packets[0][0]
    print(f"Loaded {len(packets)} UDP packets  ({duration:.1f}s of data)")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, args.ttl)
    # Tell the kernel which interface to use for multicast sends.
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF,
                    socket.inet_aton(args.iface))

    loopback = args.iface == "127.0.0.1"
    run = 0
    try:
        while True:
            run += 1
            sent = replay_once(packets, sock, args.speed, loopback)
            print(f"Run {run}: sent {sent} packets")
            if not args.loop:
                break
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
