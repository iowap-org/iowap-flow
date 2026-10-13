# Dockerfile — iowap-flow (dedicated flow-runner node, T-172; Plan Task 6)
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

# Dedicated unprivileged user — deploy contract mounts flow-state to
# /home/appuser/.relay (Plan Task 6, Pitfall #21: node identity survives restarts).
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && useradd --create-home --uid 1000 appuser
ENV HOME=/home/appuser
# Deployment convention (same as base/storage images, T-148a):
#   1. NODE_NAME — free to set per deployment (default: container hostname)
#   2. RELAY_URL — relay base URL (mDNS fallback in entrypoint.sh if unset)
# No ENV defaults here — the name is deliberately chosen per deployment.
# Entrypoint starts as root (chown of the volume on first start, Pitfall #21)
# and drops itself to appuser via setpriv.
USER root

ENTRYPOINT ["/entrypoint.sh"]