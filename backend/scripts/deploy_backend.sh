#!/bin/bash
# Backend Deployment Script
# This script handles git pull, backend restart, and verification
# Designed to run quickly and return status

set -e
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${PATH}"

LOG_FILE="/tmp/deploy_backend.log"
TIMEOUT=30

log_message() {
    local ts
    ts="$(/bin/date '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo 'unknown-time')"
    local line="[$ts] $1"
    if command -v tee >/dev/null 2>&1; then
        echo "$line" | tee -a "$LOG_FILE"
    else
        echo "$line" >> "$LOG_FILE"
        echo "$line"
    fi
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

# Fetch and reset to latest (ensures clean deploy, no local drift)
log_message "Fetching and resetting to origin/main..."
if timeout 15 bash -c 'cd /home/ubuntu/trademanthan && git fetch origin && git reset --hard origin/main' >> "$LOG_FILE" 2>&1; then
    log_message "✅ Git reset successful"
else
    log_message "⚠️ Git fetch/reset had issues (trying pull...)"
    timeout 10 git pull origin main >> "$LOG_FILE" 2>&1 || true
fi
# Clear Python cache to avoid stale bytecode
find /home/ubuntu/trademanthan -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# Log deployed commit for verification
log_message "Deployed commit: $(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"

# Kill existing backend process (match both patterns to catch all instances)
log_message "Stopping existing backend..."
# Manual `screen -S trademanthan` + uvicorn holds :8000 and causes systemd restart loops (errno 98).
log_message "Closing trademanthan screen session if present..."
screen -S trademanthan -X quit 2>/dev/null || true
screen -wipe 2>/dev/null || true
sleep 1
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

# Orphan listeners (e.g. manual python3/uvicorn) may not match pkill patterns but still hold :8000,
# causing systemd to fail with "address already in use" in a restart loop.
if command -v fuser >/dev/null 2>&1; then
    log_message "Ensuring port 8000 is free..."
    sudo fuser -k 8000/tcp >>"$LOG_FILE" 2>&1 || true
    sleep 1
fi

# Prefer systemd restart if service exists (ensures clean reload of new code)
if systemctl list-unit-files 2>/dev/null | grep -q "trademanthan-backend.service"; then
    log_message "Restarting via systemd..."
    sudo systemctl restart trademanthan-backend 2>>"$LOG_FILE" || true
    sleep 3
else
    # Fallback: screen session
    log_message "Starting backend in screen session..."
    cd /home/ubuntu/trademanthan
    source backend/venv/bin/activate

    screen -wipe 2>/dev/null || true
    screen -S trademanthan -X quit 2>/dev/null || true
    sleep 1

    screen -dmS trademanthan bash -c 'cd /home/ubuntu/trademanthan && source backend/venv/bin/activate && python3 -u -m uvicorn main:app --host 0.0.0.0 --port 8000'
    sleep 3
fi
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

