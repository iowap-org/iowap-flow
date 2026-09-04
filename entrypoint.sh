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

mkdir -p "$RELAY_DIR"
cp /app/profiles/node.yaml "$RELAY_DIR/node.yaml"

if [ ! -f "$RELAY_DIR/iowap-agent.json" ]; then
    node-cli node register "$RELAY_URL" --name "${NODE_NAME:-flow-runner}"
fi

exec node-daemon --foreground