#!/usr/bin/env bash
# run_emulator_demo.sh — start mayara emulator + kahu-daemon together for demo/testing.
#
# Usage:
#   ./demo/run_emulator_demo.sh [--dry-run] [--land-filter]
#
# Requirements:
#   - mayara-server built at ~/mayara-server/target/release/mayara-server
#     (or set MAYARA_BIN=/path/to/mayara-server)
#   - kahu-daemon built at ~/kahu-vessel-rs/target/release/kahu-daemon
#     (or set DAEMON_BIN=/path/to/kahu-daemon)
#   - KAHU_API_KEY env var set
#
# Background:
#   mayara's WebSocket spoke stream uses a broadcast channel (capacity 32,
#   ~1.3 s of spokes at 25 Hz).  If mayara has been running for more than
#   ~1 second without any WebSocket client, the buffer fills and new
#   subscribers immediately receive a Lagged error, silently closing the
#   stream.  This script always kills any running mayara first and connects
#   kahu-daemon as quickly as possible after startup to beat the buffer fill.

set -uo pipefail

# ---------------------------------------------------------------------------
# Configurable defaults — prefer locally-built binaries over PATH
# ---------------------------------------------------------------------------
MAYARA_BIN="${MAYARA_BIN:-}"
DAEMON_BIN="${DAEMON_BIN:-}"
UPLOAD_HOST="${UPLOAD_HOST:-crowdsource.kahu.earth}"
UPLOAD_PORT="${UPLOAD_PORT:-9900}"
API_KEY="${KAHU_API_KEY:-}"
SPOKES_PER_REV=2048

# Parse flags forwarded to kahu-daemon
DAEMON_EXTRA_FLAGS=()
for arg in "$@"; do
    DAEMON_EXTRA_FLAGS+=("$arg")
done

# ---------------------------------------------------------------------------
# Resolve binaries — local release build takes priority over PATH
# ---------------------------------------------------------------------------
resolve_bin() {
    local local_path="$1"
    local path_name="$2"
    if [[ -x "$local_path" ]]; then
        echo "$local_path"
    elif command -v "$path_name" &>/dev/null; then
        echo "$path_name"
    else
        echo ""
    fi
}

if [[ -z "$MAYARA_BIN" ]]; then
    MAYARA_BIN="$(resolve_bin "$HOME/mayara-server/target/release/mayara-server" mayara-server)"
fi
if [[ -z "$DAEMON_BIN" ]]; then
    DAEMON_BIN="$(resolve_bin "$HOME/kahu-vessel-rs/target/release/kahu-daemon" kahu-daemon)"
fi

if [[ -z "$MAYARA_BIN" ]]; then
    echo "ERROR: mayara-server not found."
    echo "  Build it: cd ~/mayara-server && cargo build --release"
    echo "  Or set:   MAYARA_BIN=/path/to/mayara-server"
    exit 1
fi
if [[ -z "$DAEMON_BIN" ]]; then
    echo "ERROR: kahu-daemon not found."
    echo "  Build it: cd ~/kahu-vessel-rs && cargo build --release"
    echo "  Or set:   DAEMON_BIN=/path/to/kahu-daemon"
    exit 1
fi

echo "mayara:  $MAYARA_BIN"
echo "daemon:  $DAEMON_BIN"

# ---------------------------------------------------------------------------
# Kill any running mayara / kahu-daemon so we start fresh.
# Stale kahu-daemon instances from previous runs keep retrying against the
# new mayara and show as spurious 500 errors in the mayara log.
# ---------------------------------------------------------------------------
echo "Stopping any existing mayara-server / kahu-daemon instances..."
pkill -x mayara-server 2>/dev/null || true
pkill -x kahu-daemon   2>/dev/null || true
sleep 1

# ---------------------------------------------------------------------------
# Start mayara emulator
# ---------------------------------------------------------------------------
echo "Starting mayara emulator..."
RUST_MIN_STACK=8388608 "$MAYARA_BIN" \
    --emulator \
    --targets none \
    &
MAYARA_PID=$!
echo "mayara PID: $MAYARA_PID"

# Trap to kill mayara when this script exits.
cleanup() {
    echo ""
    echo "Shutting down mayara (PID $MAYARA_PID)..."
    kill "$MAYARA_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Wait for mayara to expose the emulator radar via its HTTP API, then connect
# kahu-daemon immediately — before the broadcast ring buffer fills (~1.3 s).
# Do NOT add an extra sleep here; connect as soon as the radar is visible.
# ---------------------------------------------------------------------------
echo "Waiting for emulator radar to appear..."
RADAR_URL="http://localhost:6502/signalk/v2/api/vessels/self/radars"
POWER_URL="http://localhost:6502/signalk/v2/api/vessels/self/radars/emu0001/controls/power"
SPOKE_URL="ws://localhost:6502/signalk/v2/api/vessels/self/radars/emu0001/spokes"
MAX_WAIT=30
ELAPSED=0
while true; do
    if curl -sf "$RADAR_URL" 2>/dev/null | grep -q "emu0001"; then
        echo "Emulator radar ready."
        break
    fi
    if (( ELAPSED >= MAX_WAIT )); then
        echo "ERROR: mayara did not expose emu0001 within ${MAX_WAIT}s. Check mayara output."
        exit 1
    fi
    sleep 1
    (( ELAPSED++ ))
done

# Set the radar to Transmit (value=2; 1=Standby).
# The emulator starts in Standby; the spokes WebSocket returns 500 until transmitting.
echo "Setting radar to Transmit..."
curl -sf -X PUT "$POWER_URL" \
    -H "Content-Type: application/json" \
    -d '{"value": 2}' >/dev/null \
    || echo "WARNING: failed to set radar to transmit — spoke stream may return 500"
# Brief pause for mayara to start generating spokes before we subscribe.
sleep 1

# ---------------------------------------------------------------------------
# Start kahu-daemon
# ---------------------------------------------------------------------------
DAEMON_ARGS=(
    --ws-url "$SPOKE_URL"
    --upload-host "$UPLOAD_HOST"
    --upload-port "$UPLOAD_PORT"
    --spokes "$SPOKES_PER_REV"
    --startup-delay 0   # mayara is already ready; no extra delay needed
)
if [[ -n "$API_KEY" ]]; then
    DAEMON_ARGS+=(--api-key "$API_KEY")
fi
DAEMON_ARGS+=("${DAEMON_EXTRA_FLAGS[@]}")

echo "Starting kahu-daemon..."
echo "  $DAEMON_BIN ${DAEMON_ARGS[*]}"
RUST_LOG=info RUST_MIN_STACK=8388608 "$DAEMON_BIN" "${DAEMON_ARGS[@]}" || true
