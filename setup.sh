#!/bin/bash
# setup.sh — run once before docker compose up

# Write DOCKER_GID to .env
DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)

if grep -q "^DOCKER_GID=" .env; then
    sed -i "s/^DOCKER_GID=.*/DOCKER_GID=${DOCKER_GID}/" .env
else
    echo "DOCKER_GID=${DOCKER_GID}" >> .env
fi

echo "Docker socket GID: ${DOCKER_GID}"
echo "Run: docker compose up -d"