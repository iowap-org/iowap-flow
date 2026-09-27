# Dockerfile — iowap-flow (dedizierter Flow-Runner-Node, T-172; Plan Task 6)
# Base python:3.11-slim, pip install ., COPY profiles/ + handlers/.
FROM python:3.11-slim

WORKDIR /app

# iowap-node wird aus dem public GitHub-Repo installiert (pinned in pyproject) — braucht git zur Build-Zeit.
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY profiles ./profiles
COPY handlers ./handlers

# Dedizierter unprivilegierter User — Deploy-Kontrakt mountet flow-state nach
# /home/appuser/.relay (Plan Task 6, Pitfall #21: Node-Identität überlebt Restarts).
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && useradd --create-home --uid 1000 appuser
ENV HOME=/home/appuser
# Deployment-Konvention (wie base/storage-Images, T-148a):
#   1. NODE_NAME — frei setzbar pro Deployment (Default: Container-Hostname)
#   2. RELAY_URL — Relay-Basis-URL (mDNS-Fallback im entrypoint.sh, falls unset)
# Keine ENV-Defaults hier, damit der Name pro Deployment bewusst gewählt wird.
# Entrypoint startet als root (chown des Volumes beim ersten Start, Pitfall #21)
# und droppt selbst per setpriv auf appuser.
USER root

ENTRYPOINT ["/entrypoint.sh"]