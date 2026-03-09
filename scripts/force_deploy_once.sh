#!/bin/bash
# One-time manual fix: force server to get latest code when deploy-backend fails.
# Run on EC2: bash scripts/force_deploy_once.sh
# Or: ssh ubuntu@<EC2_IP> "cd /home/ubuntu/trademanthan && bash scripts/force_deploy_once.sh"

set -e
cd /home/ubuntu/trademanthan || exit 1

echo "=== Force deploy: fetch + reset ==="
git fetch origin
git reset --hard origin/main
echo "Commit: $(git rev-parse --short HEAD)"

echo "=== Clearing Python cache ==="
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

echo "=== Restarting backend ==="
pkill -f "uvicorn.*main:app" 2>/dev/null || true
pkill -f "uvicorn.*backend.main:app" 2>/dev/null || true
sleep 2

if systemctl list-unit-files 2>/dev/null | grep -q "trademanthan-backend.service"; then
    sudo systemctl restart trademanthan-backend
    echo "Restarted via systemd"
else
    screen -S trademanthan -X quit 2>/dev/null || true
    sleep 1
    source backend/venv/bin/activate
    screen -dmS trademanthan bash -c 'cd /home/ubuntu/trademanthan && source backend/venv/bin/activate && python3 -u -m uvicorn main:app --host 0.0.0.0 --port 8000'
    echo "Restarted via screen"
fi

echo "=== Done. Verify: curl -s https://tradentical.com/scan/arbitrage/version ==="
