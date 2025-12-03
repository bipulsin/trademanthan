#!/bin/bash
# Backend Control Script with Timeouts
# Handles backend start/stop/check operations without hanging
# Usage: backend_control.sh [start|stop|restart|status|check]

set -e

ACTION="${1:-status}"
TIMEOUT_SECONDS=10
LOG_FILE="/tmp/uvicorn.log"
PID_FILE="/tmp/uvicorn.pid"
PROJECT_DIR="/home/ubuntu/trademanthan"
BACKEND_DIR="$PROJECT_DIR/backend"

# Function to run command with timeout
run_with_timeout() {
    local timeout=$1
    shift
    local cmd="$@"
    
    # Use timeout if available, otherwise use a background process with sleep
    if command -v timeout >/dev/null 2>&1; then
        timeout "$timeout" bash -c "$cmd"
    else
        # Fallback: run in background and kill after timeout
        eval "$cmd" &
        local pid=$!
        (
            sleep "$timeout"
            kill $pid 2>/dev/null || true
        ) &
        wait $pid 2>/dev/null || true
    fi
}

# Function to check if backend is running
check_backend_running() {
    if pgrep -f "uvicorn.*main:app" > /dev/null 2>&1; then
        return 0
    fi
    return 1
}

# Function to check backend health
check_backend_health() {
    local max_attempts=3
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f --max-time 3 "http://localhost:8000/scan/health" > /dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    return 1
}

# Function to stop backend
stop_backend() {
    echo "Stopping backend..."
    pkill -f "uvicorn.*main:app" 2>/dev/null || true
    sleep 1
    
    # Force kill if still running
    if check_backend_running; then
        pkill -9 -f "uvicorn.*main:app" 2>/dev/null || true
        sleep 1
    fi
    
    if check_backend_running; then
        echo "ERROR: Failed to stop backend"
        return 1
    else
        echo "✅ Backend stopped"
        return 0
    fi
}

# Function to start backend
start_backend() {
    if check_backend_running; then
        echo "⚠️ Backend is already running"
        return 0
    fi
    
    echo "Starting backend..."
    cd "$PROJECT_DIR" || {
        echo "ERROR: Could not change to project directory"
        return 1
    }
    
    source "$BACKEND_DIR/venv/bin/activate" || {
        echo "ERROR: Could not activate virtual environment"
        return 1
    }
    
    # Start backend in background
    nohup python3 -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 > "$LOG_FILE" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_FILE"
    
    # Wait briefly for startup (non-blocking)
    sleep 2
    
    if check_backend_running; then
        echo "✅ Backend started (PID: $pid)"
        echo "Logs: tail -f $LOG_FILE"
        return 0
    else
        echo "ERROR: Backend failed to start"
        return 1
    fi
}

# Function to get backend status
get_status() {
    if check_backend_running; then
        local pid=$(pgrep -f "uvicorn.*main:app" | head -1)
        echo "✅ Backend is running (PID: $pid)"
        
        # Quick health check (non-blocking)
        if check_backend_health; then
            echo "✅ Backend is healthy"
        else
            echo "⚠️ Backend is running but health check failed"
        fi
        return 0
    else
        echo "❌ Backend is not running"
        return 1
    fi
}

# Main action handler
case "$ACTION" in
    start)
        start_backend
        ;;
    stop)
        stop_backend
        ;;
    restart)
        stop_backend
        sleep 1
        start_backend
        ;;
    status)
        get_status
        ;;
    check)
        if check_backend_health; then
            echo "✅ Backend is healthy"
            exit 0
        else
            echo "❌ Backend health check failed"
            exit 1
        fi
        ;;
    *)
        echo "Usage: $0 [start|stop|restart|status|check]"
        exit 1
        ;;
esac

exit $?

