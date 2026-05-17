#!/usr/bin/env bash
# Ask the production server to pull origin/main and restart (no GitHub → EC2 SSH).
# The server runs backend/scripts/deploy_backend.sh via POST /scan/deploy-backend.
#
# Usage:
#   ./scripts/trigger-ec2-deploy.sh
#   DEPLOY_URL=https://www.tradewithcto.com ./scripts/trigger-ec2-deploy.sh

set -euo pipefail

BASE="${DEPLOY_URL:-https://www.tradewithcto.com}"
DEPLOY_ENDPOINT="${BASE%/}/scan/deploy-backend"
STATUS_ENDPOINT="${BASE%/}/scan/deployment-status"

echo "Triggering deploy: $DEPLOY_ENDPOINT"
http_code="$(curl -sS -o /tmp/trademanthan_deploy_resp.json -w "%{http_code}" -X POST "$DEPLOY_ENDPOINT" || true)"
cat /tmp/trademanthan_deploy_resp.json
echo ""

if [[ "$http_code" != "200" && "$http_code" != "409" ]]; then
  echo "Deploy request failed (HTTP $http_code)" >&2
  exit 1
fi

echo "Waiting for deploy to finish..."
for _ in $(seq 1 40); do
  sleep 3
  status_json="$(curl -sS "$STATUS_ENDPOINT" 2>/dev/null || echo '{}')"
  if echo "$status_json" | grep -q '"running":false'; then
    echo "$status_json" | head -c 4000
    echo ""
    if echo "$status_json" | grep -q '"success":true'; then
      if echo "$status_json" | grep -qi 'error\|failed\|✗'; then
        echo "Deploy finished with errors in log — review above." >&2
        exit 1
      fi
      echo "Deploy complete."
      exit 0
    fi
  fi
done

echo "Timed out waiting for deploy — check $STATUS_ENDPOINT" >&2
exit 1
