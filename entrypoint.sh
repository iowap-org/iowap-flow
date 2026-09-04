#!/bin/sh
# entrypoint.sh — iowap-flow (Plan Task 6)
# Kopiert das Profil bei JEDEM Start nach ~/.relay/node.yaml (Pitfall #23: Profil liegt
# im Image, nicht im Volume), startet dann den Daemon im Vordergrund.
set -eu

mkdir -p "$HOME/.relay"
cp /app/profiles/node.yaml "$HOME/.relay/node.yaml"

exec node-daemon --foreground