#!/bin/bash
# Backend Monitoring Script
# Checks if backend is running and restarts if needed

BACKEND_URL="http://localhost:8000/health"
LOG_FILE="/tmp/backend_monitor.log"
MAX_RESTART_ATTEMPTS=3
RESTART_COOLDOWN=300  # 5 minutes between restart attempts

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

check_backend() {
    # Check if process is running
    if ! pgrep -f "uvicorn main:app" > /dev/null; then
        return 1
    fi
    
    # Check if endpoint responds
    if curl -s -f "$BACKEND_URL" > /dev/null 2>&1; then
        return 0
    fi
    
    return 1
}

restart_backend() {
    log_message "Attempting to restart backend..."
    
    # Kill any existing processes
    pkill -f "uvicorn main:app" || true
    sleep 2
    
    # Start backend
    cd /home/ubuntu/trademanthan || exit 1
    nohup python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 > /tmp/uvicorn.log 2>&1 &
    
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

