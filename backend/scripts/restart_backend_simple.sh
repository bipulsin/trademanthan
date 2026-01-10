#!/bin/bash
# Simple Backend Restart Script
# Ensures clean restart with proper logging

set -e

PROJECT_DIR="/home/ubuntu/trademanthan"
LOG_FILE="/home/ubuntu/trademanthan/logs/trademanthan.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Restarting backend..."

# Stop existing backend
pkill -9 -f 'uvicorn.*main:app' 2>/dev/null || true
sleep 2

# Clean up dead screens
screen -wipe 2>/dev/null || true

# Start backend in screen session (without output redirection - logs go to file only)
cd "$PROJECT_DIR" || exit 1
source backend/venv/bin/activate || exit 1

screen -dmS trademanthan bash -c "cd /home/ubuntu/trademanthan && source backend/venv/bin/activate && python3 -u -m uvicorn main:app --host 0.0.0.0 --port 8000"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backend restart initiated"
sleep 5

# Verify backend is running
if curl -s http://localhost:8000/scan/health --max-time 3 > /dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Backend is running and healthy"
    echo "Logs: tail -f $LOG_FILE"
    exit 0
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ Backend health check failed"
    exit 1
fi
