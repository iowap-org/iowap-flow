# Dockerfile — iowap-flow (dedizierter Flow-Runner-Node, T-172; Plan Task 6)
# Base python:3.11-slim, pip install ., COPY profiles/ + handlers/.
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY profiles ./profiles
COPY handlers ./handlers

# entrypoint.sh kopiert Profil bei JEDEM Start nach ~/.relay/node.yaml (Pitfall #23)
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]