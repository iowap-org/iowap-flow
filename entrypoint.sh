#!/bin/sh
# entrypoint.sh — iowap-flow (Plan Task 6)
#
# Läuft als root und droppt zu appuser (Deploy-Kontrakt: Named-Volume ist beim
# ersten Start root:root → chown nötig, dann setpriv als unprivilegierter User).
#
# 1. Kopiert das Profil bei JEDEM Start nach ~/.relay/node.yaml (Pitfall #23:
#    Profil liegt im Image, nicht im Volume — Volume hält nur Identität/State).
# 2. Bootstrapt die Node-Identität, falls das Volume leer ist (Pitfall #21):
#    node-cli node register (T-178) ist chicken-and-egg-frei, erstellt meta+token
#    und bricht bei existierender Identität ab, ohne etwas zu ändern — deshalb
#    das Vorher-Checken statt --force.
# 3. Startet den Daemon im Vordergrund.
#
# Deployment-Konvention (wie base-image entrypoint, T-148a):
#   1. NODE_NAME — frei wählbar (Default: Container-Hostname)
#   2. RELAY_URL — Pflicht; wenn unset, mDNS-Discovery (Fallback im Base-Pattern)
set -eu

APP_HOME=/home/appuser
RELAY_DIR="$APP_HOME/.relay"

if [ "$(id -u)" = "0" ]; then
    mkdir -p "$RELAY_DIR"
    chown -R appuser:appuser "$APP_HOME"
    # Kein --reset-env: es würde RELAY_URL/NODE_NAME/HOME aus dem Image-Env
    # mitlöschen. HOME=/home/appuser kommt aus dem Dockerfile-ENV.
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