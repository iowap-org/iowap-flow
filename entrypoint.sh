#!/bin/sh
# entrypoint.sh — iowap-flow (Plan Task 6)
#
# Runs as root and drops to appuser (deploy contract: on first start the
# named volume is root:root → chown needed, then setpriv as an unprivileged user).
#
# 1. Copies the profile to ~/.relay/node.yaml on EVERY start (pitfall #23:
#    the profile lives in the image, not the volume — the volume only holds identity/state).
# 2. Bootstraps the node identity if the volume is empty (pitfall #21):
#    node-cli node register (T-178) is chicken-and-egg-free, creates meta+token
#    and aborts on an existing identity without changing anything — hence
#    the check beforehand instead of --force.
# 3. Starts the daemon in the foreground.
#
# Deployment convention (like the base-image entrypoint, T-148a):
#   1. NODE_NAME — freely selectable (default: container hostname)
#   2. RELAY_URL — required; when unset, mDNS discovery (fallback of the base pattern)
set -eu

APP_HOME=/home/appuser
RELAY_DIR="$APP_HOME/.relay"

if [ "$(id -u)" = "0" ]; then
    mkdir -p "$RELAY_DIR"
    chown -R appuser:appuser "$APP_HOME"
    # No --reset-env: it would also wipe RELAY_URL/NODE_NAME/HOME from the image env.
    # HOME=/home/appuser comes from the Dockerfile ENV.
    exec setpriv --reuid=appuser --regid=appuser --init-groups /entrypoint.sh "$@"
fi

# --- NODE_NAME: explicit env, else container hostname --------------------
NODE_NAME="${NODE_NAME:-$(hostname 2>/dev/null || echo flow-runner)}"
export NODE_NAME

# --- RELAY_URL: explicit env, else mDNS discovery ------------------------
if [ -z "${RELAY_URL:-}" ]; then
    echo "[entrypoint] RELAY_URL not set — attempting mDNS discovery..."
    RELAY_URL=$(python3 -c "
import sys
try:
    from nodes.common.relay_client import _discover_relay_mdns
    url = _discover_relay_mdns(timeout=3.0)
    if url: print(url)
except Exception as e:
    print(f'mDNS discovery failed: {e}', file=sys.stderr)
")
    if [ -z "$RELAY_URL" ]; then
        echo "[entrypoint] ERROR: RELAY_URL required and mDNS discovery found no relay." >&2
        exit 1
    fi
    export RELAY_URL
    echo "[entrypoint] mDNS discovered relay at $RELAY_URL"
fi

mkdir -p "$RELAY_DIR"
cp /app/profiles/node.yaml "$RELAY_DIR/node.yaml"

if [ ! -f "$RELAY_DIR/iowap-agent.json" ] && [ ! -f "$RELAY_DIR/ai-relay-agent.json" ]; then
    node-cli node register "$RELAY_URL" --name "$NODE_NAME"
fi

exec node-daemon --foreground