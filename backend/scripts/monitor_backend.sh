#!/bin/bash
# Backend Monitoring Script
# Checks if backend is running and restarts if needed

BACKEND_URL="http://localhost:8000/scan/index-prices"
LOG_FILE="/tmp/backend_monitor.log"
MAX_RESTART_ATTEMPTS=3
RESTART_COOLDOWN=300  # 5 minutes between restart attempts

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

check_backend() {
    # Check if process is running - match both patterns (root main.py and backend.main.py)
    if ! pgrep -f "uvicorn.*main:app" > /dev/null && ! pgrep -f "uvicorn.*backend.main:app" > /dev/null; then
        return 1
    fi
    
    # Check if endpoint responds (with timeout to prevent hanging)
    if timeout 5 curl -s -f "$BACKEND_URL" > /dev/null 2>&1; then
        return 0
    fi
    
    return 1
}

restart_backend() {
    log_message "Attempting to restart backend..."
    
    # Check if backend is actually running before trying to restart
    if check_backend; then
        log_message "✅ Backend is already running and healthy - no restart needed"
        return 0
    fi
    
    # Kill any existing processes - match both patterns
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
    
    # Start backend in screen session - MUST use backend.main:app to load the correct main.py with lifespan
    cd /home/ubuntu/trademanthan || exit 1
    source backend/venv/bin/activate
    screen -dmS trademanthan bash -c 'cd /home/ubuntu/trademanthan && source backend/venv/bin/activate && python3 -u -m uvicorn backend.main:app --host 0.0.0.0 --port 8000'
    
    sleep 5
    
    # Verify it started
    if check_backend; then
        log_message "✅ Backend restarted successfully"
        return 0
    else
        log_message "❌ Backend restart failed"
        return 1
    fi
}

# Main monitoring loop
if check_backend; then
    log_message "✅ Backend is running and healthy"
    exit 0
else
    log_message "⚠️ Backend is not running or not responding"
    
    # Check restart cooldown
    if [ -f /tmp/last_restart_time ]; then
        last_restart=$(cat /tmp/last_restart_time)
        current_time=$(date +%s)
        time_since_restart=$((current_time - last_restart))
        
        if [ $time_since_restart -lt $RESTART_COOLDOWN ]; then
            log_message "⏸️  Restart cooldown active. Waiting..."
            exit 0
        fi
    fi
    
    # Attempt restart
    if restart_backend; then
        date +%s > /tmp/last_restart_time
        exit 0
    else
        log_message "❌ Failed to restart backend after multiple attempts"
        exit 1
    fi
fi

