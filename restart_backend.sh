#!/bin/bash
# Local/auxiliary backend on dev port only — do not use for production (use: sudo systemctl restart trademanthan-backend).

cd /home/ubuntu/trademanthan || exit 1
# shellcheck disable=SC1091
. ./backend/scripts/dev_backend_env.sh 2>/dev/null || {
    TRADEMANTHAN_DEV_PORT=9000
}

echo "Stopping auxiliary uvicorn on :${TRADEMANTHAN_DEV_PORT}..."
pkill -f "uvicorn.*backend.main:app.*--port ${TRADEMANTHAN_DEV_PORT}" 2>/dev/null || true
pkill -f "python.*uvicorn.*backend.main" 2>/dev/null || true
sleep 2

echo "Pulling latest code..."
git pull origin main

echo "Starting auxiliary backend on :${TRADEMANTHAN_DEV_PORT}..."
source backend/venv/bin/activate
nohup python3 -m uvicorn backend.main:app --host 0.0.0.0 --port "${TRADEMANTHAN_DEV_PORT}" > /tmp/uvicorn.log 2>&1 &
BACKEND_PID=$!

echo "Auxiliary backend started with PID: $BACKEND_PID"
sleep 10

if ps -p "$BACKEND_PID" > /dev/null 2>&1; then
    echo "✅ Process running (PID: $BACKEND_PID)"
    echo "Health: curl -s http://localhost:${TRADEMANTHAN_DEV_PORT}/scan/health"
    tail -30 /tmp/uvicorn.log | grep -E 'STARTUP|Scheduler|Monitor|Updater|STARTED|✅|❌|COMPLETE|All Services' || true
else
    echo "❌ Backend failed to start"
    tail -50 /tmp/uvicorn.log
fi
