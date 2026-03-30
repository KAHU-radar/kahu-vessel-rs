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
# Startup order (avoids mayara's broadcast-channel buffer-full / Lagged issue):
#   1. Start mayara emulator (radar in Standby — no spokes yet)
#   2. Set radar to Transmit (spokes start flowing)
#   3. Connect kahu-daemon (subscribes to active channel)
#
# Note: mayara has a local aarch64 patch applied at ~/mayara-server that
# handles RecvError::Lagged gracefully (resume instead of closing the stream).
# Without that patch the WebSocket silently disconnects every ~60 s.

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

DAEMON_PID=""

cleanup() {
    echo ""
    echo "Shutting down..."
    [[ -n "$DAEMON_PID" ]] && kill "$DAEMON_PID" 2>/dev/null || true
    kill "$MAYARA_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Wait for mayara to expose the emulator radar.
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
        echo "ERROR: mayara did not expose emu0001 within ${MAX_WAIT}s."
        exit 1
    fi
    sleep 1
    (( ELAPSED++ ))
done

# ---------------------------------------------------------------------------
# Set radar to Transmit before connecting kahu-daemon.
# Wait for "Starting transmission" to confirm spokes are flowing.
# ---------------------------------------------------------------------------
echo "Setting radar to Transmit..."
curl -sf -X PUT "$POWER_URL" \
    -H "Content-Type: application/json" \
    -d '{"value": 2}' >/dev/null \
    || echo "WARNING: failed to set radar to transmit"

# Give the emulator a moment to start generating spokes.
sleep 3

# ---------------------------------------------------------------------------
# Start kahu-daemon — subscribes to an already-running spoke stream.
# With the Lagged fix in mayara, it will resume cleanly if it falls behind.
# ---------------------------------------------------------------------------
DAEMON_ARGS=(
    --ws-url "$SPOKE_URL"
    --upload-host "$UPLOAD_HOST"
    --upload-port "$UPLOAD_PORT"
    --spokes "$SPOKES_PER_REV"
    --startup-delay 0
)
if [[ -n "$API_KEY" ]]; then
    DAEMON_ARGS+=(--api-key "$API_KEY")
fi
DAEMON_ARGS+=("${DAEMON_EXTRA_FLAGS[@]}")

echo "Starting kahu-daemon..."
echo "  $DAEMON_BIN ${DAEMON_ARGS[*]}"
RUST_LOG=info RUST_MIN_STACK=8388608 "$DAEMON_BIN" "${DAEMON_ARGS[@]}" &
DAEMON_PID=$!

echo "Pipeline running. Press Ctrl-C to stop."
wait "$DAEMON_PID"
