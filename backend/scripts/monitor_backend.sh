#!/bin/bash
# Backend monitoring — production uses systemd on :8000 only. Never start a parallel uvicorn on 8000.
# If systemd is missing, optional auxiliary server uses dev port (see dev_backend_env.sh).

LOG_FILE="/tmp/backend_monitor.log"
MAX_RESTART_ATTEMPTS=3
RESTART_COOLDOWN=300

# shellcheck disable=SC1091
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/dev_backend_env.sh" 2>/dev/null || {
    TRADEMANTHAN_DEV_SCREEN=trademanthan-dev
    TRADEMANTHAN_DEV_PORT=9000
}

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

systemd_unit_state() {
    systemctl show trademanthan-backend -p ActiveState --value 2>/dev/null || echo "unknown"
}

# If graceful shutdown hangs (common during market hours), force-kill after 3 minutes.
recover_stuck_systemd_stop() {
    local state since now_s elapsed
    state="$(systemd_unit_state)"
    if [ "$state" != "deactivating" ]; then
        rm -f /tmp/trademanthan_deactivating_since 2>/dev/null || true
        return 1
    fi
    now_s=$(date +%s)
    if [ ! -f /tmp/trademanthan_deactivating_since ]; then
        echo "$now_s" > /tmp/trademanthan_deactivating_since
        return 1
    fi
    since=$(cat /tmp/trademanthan_deactivating_since 2>/dev/null || echo "$now_s")
    elapsed=$((now_s - since))
    if [ "$elapsed" -lt 200 ]; then
        log_message "⏸️  systemd deactivating (${elapsed}s) — waiting for stop to finish"
        return 1
    fi
    log_message "⚠️  systemd stuck deactivating ${elapsed}s — sending SIGKILL and starting"
    sudo systemctl kill -s SIGKILL trademanthan-backend 2>>"$LOG_FILE" || true
    sleep 2
    sudo systemctl reset-failed trademanthan-backend 2>>"$LOG_FILE" || true
    sudo systemctl start trademanthan-backend 2>>"$LOG_FILE" || true
    rm -f /tmp/trademanthan_deactivating_since 2>/dev/null || true
    return 0
}

check_backend() {
    local url
    if systemctl list-unit-files 2>/dev/null | grep -q "trademanthan-backend.service"; then
        local state
        state="$(systemd_unit_state)"
        case "$state" in
            activating|reloading)
                log_message "⏸️  systemd state=${state} — waiting"
                return 1
                ;;
            deactivating)
                return 1
                ;;
        esac
        systemctl is-active --quiet trademanthan-backend 2>/dev/null || return 1
        url="http://localhost:8000/scan/health"
    else
        if ! pgrep -f "uvicorn.*backend.main:app.*--port ${TRADEMANTHAN_DEV_PORT}" > /dev/null 2>&1; then
            return 1
        fi
        url="http://localhost:${TRADEMANTHAN_DEV_PORT}/scan/health"
    fi
    # Startup + heavy market jobs can exceed 5s; avoid false-positive restarts.
    if timeout 15 curl -s -f "$url" > /dev/null 2>&1; then
        return 0
    fi
    return 1
}

restart_backend() {
    log_message "Attempting to recover backend..."

    if check_backend; then
        log_message "✅ Backend is already running and healthy - no restart needed"
        return 0
    fi

    if systemctl list-unit-files 2>/dev/null | grep -q "trademanthan-backend.service"; then
        local state
        state="$(systemd_unit_state)"
        case "$state" in
            activating|deactivating|reloading)
                log_message "⏸️  systemd state=${state}; skipping restart"
                return 1
                ;;
        esac
        log_message "Restarting via systemd (production :8000)..."
        sudo systemctl restart trademanthan-backend 2>>"$LOG_FILE" || true
        local i
        for i in $(seq 1 30); do
            sleep 3
            if check_backend; then
                log_message "✅ Backend restarted successfully via systemd (${i} checks)"
                return 0
            fi
        done
        log_message "❌ systemd restart did not pass health check after 90s"
        return 1
    fi

    log_message "No systemd unit — starting auxiliary uvicorn on :${TRADEMANTHAN_DEV_PORT} (screen ${TRADEMANTHAN_DEV_SCREEN})..."
    pkill -f "uvicorn.*backend.main:app.*--port ${TRADEMANTHAN_DEV_PORT}" 2>/dev/null || true
    screen -S "$TRADEMANTHAN_DEV_SCREEN" -X quit 2>/dev/null || true
    sleep 2
    cd /home/ubuntu/trademanthan || exit 1
    # shellcheck disable=SC1091
    source backend/venv/bin/activate
    screen -dmS "$TRADEMANTHAN_DEV_SCREEN" bash -c "cd /home/ubuntu/trademanthan && source backend/venv/bin/activate && python3 -u -m uvicorn backend.main:app --host 0.0.0.0 --port ${TRADEMANTHAN_DEV_PORT}"
    sleep 5
    if timeout 5 curl -s -f "http://localhost:${TRADEMANTHAN_DEV_PORT}/scan/index-prices" > /dev/null 2>&1; then
        log_message "✅ Auxiliary backend up on :${TRADEMANTHAN_DEV_PORT}"
        return 0
    fi
    log_message "❌ Auxiliary backend restart failed"
    return 1
}

if check_backend; then
    log_message "✅ Backend is running and healthy"
    exit 0
fi

if recover_stuck_systemd_stop; then
    for _i in $(seq 1 30); do
        sleep 3
        if check_backend; then
            log_message "✅ Backend recovered from stuck shutdown"
            exit 0
        fi
    done
fi

log_message "⚠️ Backend is not running or not responding"

if [ -f /tmp/last_restart_time ]; then
    last_restart=$(cat /tmp/last_restart_time)
    current_time=$(date +%s)
    time_since_restart=$((current_time - last_restart))
    if [ "$time_since_restart" -lt $RESTART_COOLDOWN ]; then
        log_message "⏸️  Restart cooldown active. Waiting..."
        exit 0
    fi
fi

if restart_backend; then
    date +%s > /tmp/last_restart_time
    exit 0
fi

log_message "❌ Failed to restart backend"
exit 1
