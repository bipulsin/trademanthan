#!/usr/bin/env bash
# Roll back paperclip-vm to known-good pre-composite Kavach release (DLF fix only).
#
# Pinned commit: 3b5397c — "Fix checklist refresh for locked symbols that left latest RS top-5."
#
# Usage (from TradeManthan repo root):
#   ./scripts/rollback-paperclip-kavach.sh          # live rollback
#   DRY_RUN=1 ./scripts/rollback-paperclip-kavach.sh  # print remote steps only
#
# Uses the same recreate workaround as trigger-paperclip-deploy.sh:
#   docker compose up -d --force-recreate app nginx

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KNOWN_GOOD_REF="${KNOWN_GOOD_REF:-3b5397c}"
DRY_RUN="${DRY_RUN:-0}"
REBUILD="${REBUILD:-1}"
TWCTO_DIR="${TWCTO_DIR:-/home/ubuntu/twcto}"

echo "Kavach composite rollback → TRADEMANTHAN_REF=${KNOWN_GOOD_REF}"
echo "DRY_RUN=${DRY_RUN} REBUILD=${REBUILD}"

if [[ "${DRY_RUN}" == "1" ]]; then
  echo ""
  echo "Would run:"
  echo "  TRADEMANTHAN_REF=${KNOWN_GOOD_REF} REBUILD=${REBUILD} ${ROOT}/scripts/trigger-paperclip-deploy.sh"
  echo ""
  echo "Remote sequence (same as deploy script):"
  echo "  cd ${TWCTO_DIR}"
  echo "  git fetch origin main && git reset --hard origin/main"
  echo "  docker compose build app nginx   # when REBUILD=1"
  echo "  docker compose up -d --force-recreate app nginx"
  echo "  curl -fsS http://127.0.0.1:8080/scan/health"
  exit 0
fi

TRADEMANTHAN_REF="${KNOWN_GOOD_REF}" REBUILD="${REBUILD}" "${ROOT}/scripts/trigger-paperclip-deploy.sh"

echo ""
echo "Rollback deploy triggered. Verify:"
echo "  curl -s https://www.tradewithcto.com/scan/health"
