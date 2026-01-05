#!/bin/bash
# Restart Backend Script
# Checks status, restarts backend, and ensures continuous running
# Usage: bash restart_backend.sh [check|restart|setup-systemd]

set -e

ACTION="${1:-restart}"
PROJECT_DIR="/home/ubuntu/trademanthan"
BACKEND_DIR="${PROJECT_DIR}/backend"
LOG_FILE="/tmp/uvicorn.log"
SERVICE_NAME="trademanthan-backend"

check_backend_running() {
    # Check if systemd service is running
    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        return 0
    fi
    
    # Check if process is running (non-systemd)
    if pgrep -f "uvicorn.*main:app" > /dev/null 2>&1; then
        return 0
    fi
    
    return 1
}

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

check_status() {
    echo "📊 Checking backend status..."
    echo ""
    
    # Check systemd service
    if systemctl list-unit-files | grep -q "^${SERVICE_NAME}.service"; then
        echo "Systemd Service: Configured"
        if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
            echo "  Status: ✅ RUNNING"
            systemctl status "$SERVICE_NAME" --no-pager -l | head -10
        else
            echo "  Status: ❌ NOT RUNNING"
        fi
    else
        echo "Systemd Service: ❌ NOT CONFIGURED (using manual process)"
    fi
    
    echo ""
    
    # Check process
    if pgrep -f "uvicorn.*main:app" > /dev/null 2>&1; then
        local pid=$(pgrep -f "uvicorn.*main:app" | head -1)
        echo "Process: ✅ RUNNING (PID: $pid)"
    else
        echo "Process: ❌ NOT RUNNING"
    fi
    
    echo ""
    
    # Check health endpoint
    if check_backend_health; then
        echo "Health Check: ✅ HEALTHY"
    else
        echo "Health Check: ❌ UNHEALTHY"
    fi
}

restart_backend() {
    echo "🔄 Restarting backend..."
    echo ""
    
    # If systemd service exists, use it
    if systemctl list-unit-files | grep -q "^${SERVICE_NAME}.service"; then
        echo "Using systemd service..."
        
        if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
            echo "Stopping service..."
            sudo systemctl stop "$SERVICE_NAME"
            sleep 2
        fi
        
        echo "Starting service..."
        sudo systemctl start "$SERVICE_NAME"
        sleep 3
        
        if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
            echo "✅ Backend restarted via systemd"
        else
            echo "❌ Failed to start via systemd"
            echo "Check logs: sudo journalctl -u $SERVICE_NAME -n 50"
            exit 1
        fi
    else
        echo "Systemd service not found. Using manual restart..."
        
        # Stop existing processes
        echo "Stopping existing backend processes..."
        pkill -f "uvicorn.*main:app" 2>/dev/null || true
        pkill -f "uvicorn backend.main:app" 2>/dev/null || true
        sleep 2
        
        # Force kill if still running
        if pgrep -f "uvicorn.*main:app" > /dev/null 2>&1; then
            pkill -9 -f "uvicorn.*main:app" 2>/dev/null || true
            sleep 1
        fi
        
        # Start backend
        echo "Starting backend..."
        cd "$PROJECT_DIR" || exit 1
        source "$BACKEND_DIR/venv/bin/activate" || exit 1
        nohup python3 -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 > "$LOG_FILE" 2>&1 &
        local pid=$!
        echo "Backend started with PID: $pid"
        sleep 3
        
        if check_backend_running; then
            echo "✅ Backend restarted (PID: $pid)"
        else
            echo "❌ Backend failed to start"
            echo "Check logs: tail -50 $LOG_FILE"
            exit 1
        fi
    fi
    
    # Verify health
    echo ""
    echo "Verifying health..."
    sleep 2
    if check_backend_health; then
        echo "✅ Backend is healthy and responding"
    else
        echo "⚠️ Backend started but health check failed"
        echo "Check logs for errors"
    fi
}

setup_systemd() {
    echo "🔧 Setting up systemd service for continuous running..."
    echo ""
    
    if [ "$EUID" -ne 0 ]; then 
        echo "This operation requires sudo. Running with sudo..."
        sudo bash "$BACKEND_DIR/scripts/setup_systemd_service.sh"
    else
        bash "$BACKEND_DIR/scripts/setup_systemd_service.sh"
    fi
}

# Main action handler
case "$ACTION" in
    check)
        check_status
        ;;
    restart)
        restart_backend
        ;;
    setup-systemd)
        setup_systemd
        ;;
    *)
        echo "Usage: $0 [check|restart|setup-systemd]"
        echo ""
        echo "Commands:"
        echo "  check         - Check backend status"
        echo "  restart       - Restart backend (default)"
        echo "  setup-systemd - Setup systemd service for continuous running"
        exit 1
        ;;
esac

