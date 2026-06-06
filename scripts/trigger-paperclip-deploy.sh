#!/usr/bin/env bash
# Deploy TradeWithCTO on paperclip-vm (Docker twcto stack).
# Pulls twcto_docker from GitHub, refreshes GHCR images, optionally rebuilds app/nginx from trademanthan main.
#
# Usage:
#   ./scripts/trigger-paperclip-deploy.sh
#   REBUILD=1 ./scripts/trigger-paperclip-deploy.sh
#   TRADEMANTHAN_REF=main REBUILD=1 ./scripts/trigger-paperclip-deploy.sh
#
# Requires: SSH access to paperclip-vm (see scripts/paperclip-ssh.sh).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REBUILD="${REBUILD:-1}"
TRADEMANTHAN_REF="${TRADEMANTHAN_REF:-main}"
TWCTO_DIR="${TWCTO_DIR:-/home/ubuntu/twcto}"

echo "Deploying to paperclip-vm (REBUILD=${REBUILD}, TRADEMANTHAN_REF=${TRADEMANTHAN_REF})..."

"${ROOT}/scripts/paperclip-ssh.sh" "REBUILD=${REBUILD} TRADEMANTHAN_REF=${TRADEMANTHAN_REF} TWCTO_DIR=${TWCTO_DIR} bash -s" <<'REMOTE'
set -euo pipefail
cd "${TWCTO_DIR:-/home/ubuntu/twcto}"

echo "[deploy] git pull twcto_docker..."
git fetch origin main
git reset --hard origin/main

echo "[deploy] docker compose pull (best-effort; paperclip-vm is arm64, GHCR may be amd64-only)..."
docker compose pull || true

if [[ "${REBUILD:-1}" == "1" ]]; then
  echo "[deploy] building app + nginx (TRADEMANTHAN_REF=${TRADEMANTHAN_REF})..."
  TRADEMANTHAN_REF="${TRADEMANTHAN_REF:-main}" docker compose build app nginx
fi

echo "[deploy] recreating app + nginx..."
docker compose up -d --force-recreate app nginx

echo "[deploy] waiting for health..."
for _ in $(seq 1 40); do
  if curl -fsS http://127.0.0.1:8080/scan/health >/dev/null 2>&1; then
    echo "[deploy] healthy"
    curl -fsS http://127.0.0.1:8080/scan/health | head -c 200
    echo ""
    exit 0
  fi
  sleep 3
done

echo "[deploy] health check timed out" >&2
docker compose ps
exit 1
REMOTE

echo ""
echo "Verifying public URL..."
curl -fsS --max-time 20 "https://www.tradewithcto.com/scan/health" | head -c 200
echo ""
echo "Deploy complete."
