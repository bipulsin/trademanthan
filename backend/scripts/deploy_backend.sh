#!/bin/bash
# Backend Deployment Script
# This script handles git pull, backend restart, and verification
# Designed to run quickly and return status

set -e

LOG_FILE="/tmp/deploy_backend.log"
TIMEOUT=30

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check if backend is responding
check_backend_health() {
    timeout 5 curl -s -f "http://localhost:8000/scan/health" > /dev/null 2>&1
    return $?
}

log_message "Starting backend deployment..."

# Change to project directory
cd /home/ubuntu/trademanthan || {
    log_message "ERROR: Could not change to project directory"
    exit 1
}

# Pull latest code
log_message "Pulling latest code from git..."
if timeout 10 git pull origin main >> "$LOG_FILE" 2>&1; then
    log_message "✅ Git pull successful"
else
    log_message "⚠️ Git pull had issues (continuing anyway)"
fi

# Kill existing backend process (match both patterns to catch all instances)
log_message "Stopping existing backend..."
pkill -f "uvicorn.*main:app" || true
pkill -f "uvicorn.*backend.main:app" || true
sleep 2

# Verify process is killed
if pgrep -f "uvicorn.*main:app" > /dev/null || pgrep -f "uvicorn.*backend.main:app" > /dev/null; then
    log_message "⚠️ Force killing backend process..."
    pkill -9 -f "uvicorn.*main:app" || true
    pkill -9 -f "uvicorn.*backend.main:app" || true
    sleep 1
fi

# Start backend in screen session (consistent with how it's run manually)
log_message "Starting backend in screen session..."
cd /home/ubuntu/trademanthan
source backend/venv/bin/activate

# Clean up any dead screens
screen -wipe 2>/dev/null || true

# Kill existing screen session if it exists
screen -S trademanthan -X quit 2>/dev/null || true
sleep 1

# Start backend in screen session - Use main:app (root main.py now imports from backend.main)
# Run from project root so imports work correctly
screen -dmS trademanthan bash -c 'cd /home/ubuntu/trademanthan && source backend/venv/bin/activate && python3 -u -m uvicorn main:app --host 0.0.0.0 --port 8000'
sleep 3
BACKEND_PID=$(pgrep -f "uvicorn.*main:app" | head -1)

log_message "Backend started with PID: $BACKEND_PID"

# Wait for backend to start (with timeout)
log_message "Waiting for backend to start..."
for i in {1..20}; do
    sleep 1
    if check_backend_health; then
        log_message "✅ Backend is healthy and responding"
        # Show last few lines of startup log from trademanthan.log (with timeout)
        timeout 2 tail -10 /home/ubuntu/trademanthan/logs/trademanthan.log 2>/dev/null || true
        exit 0
    fi
done

# If we get here, backend didn't start in time
log_message "⚠️ Backend started but health check timed out"
log_message "Check /home/ubuntu/trademanthan/logs/trademanthan.log for details"
# Show last few lines of log (with timeout)
timeout 2 tail -20 /home/ubuntu/trademanthan/logs/trademanthan.log 2>/dev/null || true
exit 1

