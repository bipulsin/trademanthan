#!/bin/bash
# Simple auxiliary backend restart — binds dev port only (never :8000; production = systemd).

set -e

PROJECT_DIR="/home/ubuntu/trademanthan"
LOG_FILE="/home/ubuntu/trademanthan/logs/trademanthan.log"
# shellcheck disable=SC1091
. "${PROJECT_DIR}/backend/scripts/dev_backend_env.sh" 2>/dev/null || {
    TRADEMANTHAN_DEV_SCREEN=trademanthan-dev
    TRADEMANTHAN_DEV_PORT=9000
}

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Restarting auxiliary backend on :${TRADEMANTHAN_DEV_PORT}..."

pkill -9 -f "uvicorn.*backend.main:app.*--port ${TRADEMANTHAN_DEV_PORT}" 2>/dev/null || true
screen -S "${TRADEMANTHAN_DEV_SCREEN}" -X quit 2>/dev/null || true
sleep 2

screen -wipe 2>/dev/null || true

cd "$PROJECT_DIR" || exit 1
source backend/venv/bin/activate || exit 1

screen -dmS "${TRADEMANTHAN_DEV_SCREEN}" bash -c "cd /home/ubuntu/trademanthan && source backend/venv/bin/activate && python3 -u -m uvicorn backend.main:app --host 0.0.0.0 --port ${TRADEMANTHAN_DEV_PORT}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Auxiliary backend restart initiated (screen ${TRADEMANTHAN_DEV_SCREEN})"
sleep 5

if curl -s "http://localhost:${TRADEMANTHAN_DEV_PORT}/scan/health" --max-time 3 > /dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Auxiliary backend is running and healthy"
    echo "Logs: tail -f $LOG_FILE"
    exit 0
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ Auxiliary backend health check failed"
exit 1
