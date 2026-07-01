#!/usr/bin/env bash
# Deploy to paperclip-vm when SSH is configured (runtime secret or key file).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

"${ROOT}/scripts/setup-paperclip-ssh.sh"

export REBUILD="${REBUILD:-1}"
export TRADEMANTHAN_REF="${TRADEMANTHAN_REF:-$(git rev-parse HEAD)}"
export APP_SRC_REV="${APP_SRC_REV:-$TRADEMANTHAN_REF}"
export FRONTEND_SRC_REV="${FRONTEND_SRC_REV:-$TRADEMANTHAN_REF}"

echo "Deploying trademanthan ${TRADEMANTHAN_REF:0:12} (REBUILD=${REBUILD})..."
"${ROOT}/scripts/trigger-paperclip-deploy.sh"

echo ""
echo "Running Relative Strength scan..."
timeout 1800 "${ROOT}/scripts/paperclip-ssh.sh" \
  'cd /home/ubuntu/twcto && docker compose exec -T app python3 -c "from backend.services.relative_strength_scanner import run_relative_strength_scan; import json; print(json.dumps(run_relative_strength_scan(scan_trigger=\"manual_deploy\", cache_only=False), default=str))"'

echo ""
echo "Verify:"
curl -fsS --max-time 30 "https://www.tradewithcto.com/scan/health" | head -c 200
echo ""
curl -fsS --max-time 30 "https://www.tradewithcto.com/api/dashboard/relative-strength" | python3 -c "import sys,json; d=json.load(sys.stdin); print('RS last_updated:', d.get('last_updated'))"
