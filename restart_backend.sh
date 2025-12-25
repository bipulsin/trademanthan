#!/bin/bash
# Backend restart script

cd /home/ubuntu/trademanthan

# Kill existing backend
echo "Stopping existing backend..."
pkill -f "uvicorn.*backend.main" 2>/dev/null || true
pkill -f "python.*uvicorn.*backend.main" 2>/dev/null || true
sleep 3

# Pull latest code
echo "Pulling latest code..."
git pull origin main

# Activate venv and start backend
echo "Starting backend..."
source backend/venv/bin/activate
nohup python3 -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 > /tmp/uvicorn.log 2>&1 &
BACKEND_PID=$!

echo "Backend started with PID: $BACKEND_PID"

# Wait for startup
sleep 10

# Check if running
if ps -p $BACKEND_PID > /dev/null 2>&1; then
    echo "✅ Backend is running (PID: $BACKEND_PID)"
    echo "Checking startup logs..."
    tail -30 /tmp/uvicorn.log | grep -E 'STARTUP|Scheduler|Monitor|Updater|STARTED|✅|❌|COMPLETE|All Services'
else
    echo "❌ Backend failed to start"
    echo "Last 50 lines of log:"
    tail -50 /tmp/uvicorn.log
fi

