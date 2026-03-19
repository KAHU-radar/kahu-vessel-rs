#!/usr/bin/env bash
# Enable multicast on loopback for tcpreplay-based testing.
# Only needed on Linux when replaying Navico pcap files via tcpreplay --intf1=lo.
# Not needed in production (eth0 handles multicast automatically).
set -e
sudo ip link set lo multicast on
sudo ip route add 236.0.0.0/8 dev lo 2>/dev/null || true  # ignore if route already exists
echo "Loopback multicast ready."
