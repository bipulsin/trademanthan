#!/usr/bin/env bash
# Deploy TradeWithCTO on paperclip-vm (Docker twcto stack).
# Default: pull multi-arch GHCR images (fast). Use REBUILD=1 only as fallback.
#
# Usage:
#   ./scripts/trigger-paperclip-deploy.sh
#   REBUILD=1 ./scripts/trigger-paperclip-deploy.sh
#
# Requires: SSH access to paperclip-vm (see scripts/paperclip-ssh.sh).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REBUILD="${REBUILD:-0}"
NO_CACHE="${NO_CACHE:-0}"
TRADEMANTHAN_REF="${TRADEMANTHAN_REF:-main}"
TWCTO_DIR="${TWCTO_DIR:-/home/ubuntu/twcto}"

echo "Deploying to paperclip-vm (REBUILD=${REBUILD}, NO_CACHE=${NO_CACHE}, TRADEMANTHAN_REF=${TRADEMANTHAN_REF})..."

"${ROOT}/scripts/paperclip-ssh.sh" "REBUILD=${REBUILD} NO_CACHE=${NO_CACHE} TRADEMANTHAN_REF=${TRADEMANTHAN_REF} TWCTO_DIR=${TWCTO_DIR} bash -s" <<'REMOTE'
set -euo pipefail
cd "${TWCTO_DIR:-/home/ubuntu/twcto}"

echo "[deploy] git pull twcto_docker..."
git fetch origin main
git reset --hard origin/main

if [[ "${REBUILD:-0}" == "1" ]]; then
  BUILD_FLAGS=()
  if [[ "${NO_CACHE:-0}" == "1" ]]; then
    BUILD_FLAGS+=(--no-cache)
    echo "[deploy] REBUILD=1 NO_CACHE=1: fresh build (no Docker layer cache)..."
  else
    echo "[deploy] REBUILD=1: building app + nginx locally (TRADEMANTHAN_REF=${TRADEMANTHAN_REF})..."
  fi
  TRADEMANTHAN_REF="${TRADEMANTHAN_REF:-main}" docker compose build "${BUILD_FLAGS[@]}" app nginx
else
  echo "[deploy] pulling app + nginx from GHCR (linux/arm64)..."
  if ! docker compose pull app nginx; then
    echo "[deploy] pull failed — CI may still be building. Run:" >&2
    echo "  ./scripts/wait-twcto-docker-build.sh && REBUILD=0 ./scripts/trigger-paperclip-deploy.sh" >&2
    echo "  or REBUILD=1 ./scripts/trigger-paperclip-deploy.sh" >&2
    exit 1
  fi
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
