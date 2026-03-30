#!/usr/bin/env bash
# run_emulator_demo.sh — start mayara emulator + kahu-daemon together for demo/testing.
#
# Usage:
#   ./demo/run_emulator_demo.sh [--dry-run] [--land-filter]
#
# Requirements:
#   - mayara-server binary in PATH (or at ~/mayara-server/target/release/mayara-server)
#   - kahu-daemon binary in PATH (or at ~/kahu-vessel-rs/target/release/kahu-daemon)
#   - KAHU_API_KEY env var set (or pass --api-key below)
#
# Background:
#   mayara's WebSocket spoke stream uses a broadcast channel with a fixed buffer
#   (capacity 32, ~1.3 s of spokes at 25 Hz).  If mayara has been running for
#   more than ~1 second without any client, the buffer fills and new subscribers
#   immediately receive a Lagged error, causing the stream to close silently.
#   This script kills any running mayara instance and starts both services fresh
#   to guarantee clean delivery from spoke 0.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configurable defaults
# ---------------------------------------------------------------------------
MAYARA_BIN="${MAYARA_BIN:-}"
DAEMON_BIN="${DAEMON_BIN:-}"
UPLOAD_HOST="${UPLOAD_HOST:-crowdsource.kahu.earth}"
UPLOAD_PORT="${UPLOAD_PORT:-9900}"
API_KEY="${KAHU_API_KEY:-}"
SPOKES_PER_REV=2048
STARTUP_DELAY=3    # seconds to wait after mayara starts before connecting

# Parse flags forwarded to kahu-daemon
DAEMON_EXTRA_FLAGS=()
for arg in "$@"; do
    DAEMON_EXTRA_FLAGS+=("$arg")
done

# ---------------------------------------------------------------------------
# Resolve binaries
# ---------------------------------------------------------------------------
find_bin() {
    local name="$1"
    local fallback="$2"
    if command -v "$name" &>/dev/null; then
        echo "$name"
    elif [[ -x "$fallback" ]]; then
        echo "$fallback"
    else
        echo ""
    fi
}

if [[ -z "$MAYARA_BIN" ]]; then
    MAYARA_BIN="$(find_bin mayara-server "$HOME/mayara-server/target/release/mayara-server")"
fi
if [[ -z "$DAEMON_BIN" ]]; then
    DAEMON_BIN="$(find_bin kahu-daemon "$HOME/kahu-vessel-rs/target/release/kahu-daemon")"
fi

if [[ -z "$MAYARA_BIN" ]]; then
    echo "ERROR: mayara-server not found. Install it or set MAYARA_BIN=/path/to/mayara-server"
    exit 1
fi
if [[ -z "$DAEMON_BIN" ]]; then
    echo "ERROR: kahu-daemon not found. Build with 'cargo build --release' or set DAEMON_BIN=/path/to/kahu-daemon"
    exit 1
fi

echo "mayara:  $MAYARA_BIN"
echo "daemon:  $DAEMON_BIN"

# ---------------------------------------------------------------------------
# Kill any running mayara so we start fresh (avoids the broadcast-lag bug).
# ---------------------------------------------------------------------------
echo "Stopping any existing mayara-server instances..."
pkill -x mayara-server 2>/dev/null || true
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
# Wait for mayara to expose the emulator radar via its HTTP API.
# The emulator radar appears as 'emu0001' once the server is ready.
# ---------------------------------------------------------------------------
echo "Waiting for emulator radar to appear..."
RADAR_URL="http://localhost:6502/signalk/v2/api/vessels/self/radars"
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

# Small additional pause so the broadcast channel starts filling before we
# subscribe — this is intentional: we want our subscription to land before
# the buffer wraps for the first time.
echo "Waiting ${STARTUP_DELAY}s for spoke stream to stabilise..."
sleep "$STARTUP_DELAY"

# ---------------------------------------------------------------------------
# Start kahu-daemon
# ---------------------------------------------------------------------------
DAEMON_ARGS=(
    --ws-url "$SPOKE_URL"
    --upload-host "$UPLOAD_HOST"
    --upload-port "$UPLOAD_PORT"
    --spokes "$SPOKES_PER_REV"
    --startup-delay 0   # we handled the delay above
)
if [[ -n "$API_KEY" ]]; then
    DAEMON_ARGS+=(--api-key "$API_KEY")
fi
DAEMON_ARGS+=("${DAEMON_EXTRA_FLAGS[@]}")

echo "Starting kahu-daemon..."
echo "  $DAEMON_BIN ${DAEMON_ARGS[*]}"
RUST_MIN_STACK=8388608 "$DAEMON_BIN" "${DAEMON_ARGS[@]}"
