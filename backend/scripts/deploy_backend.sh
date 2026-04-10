#!/bin/bash
# Backend deployment: git sync, then re-exec so the restart phase runs the *latest* script from disk.
# (A single long-running bash process would keep old code in memory after `git reset`.)

set -e
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${PATH}"

LOG_FILE="/tmp/deploy_backend.log"
TIMEOUT=30
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELF="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"

log_message() {
    local ts
    ts="$(/bin/date '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo 'unknown-time')"
    local line="[$ts] $1"
    echo "$line" >> "$LOG_FILE"
}

check_backend_health() {
    timeout 5 curl -s -f "http://localhost:8000/scan/health" > /dev/null 2>&1
    return $?
}

check_dev_backend_health() {
    timeout 5 curl -s -f "http://localhost:${TRADEMANTHAN_DEV_PORT:-9000}/scan/health" > /dev/null 2>&1
    return $?
}

# ─── Phase 1: sync repo only, then re-exec this script so Phase 2 uses updated files from disk ───
if [ "${1:-}" != "--post-pull" ]; then
    log_message "Starting backend deployment (phase: git sync)..."

    cd /home/ubuntu/trademanthan || {
        log_message "ERROR: Could not change to project directory"
        exit 1
    }
    # shellcheck disable=SC1091
    . "/home/ubuntu/trademanthan/backend/scripts/dev_backend_env.sh" 2>/dev/null || {
        TRADEMANTHAN_DEV_SCREEN=trademanthan-dev
        TRADEMANTHAN_DEV_PORT=9000
    }

    log_message "Fetching and resetting to origin/main..."
    if timeout 15 bash -c 'cd /home/ubuntu/trademanthan && git fetch origin && git reset --hard origin/main' >> "$LOG_FILE" 2>&1; then
        log_message "✅ Git reset successful"
    else
        log_message "⚠️ Git fetch/reset had issues (trying pull...)"
        timeout 10 git pull origin main >> "$LOG_FILE" 2>&1 || true
    fi
    find /home/ubuntu/trademanthan -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

    log_message "Deployed commit: $(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
    log_message "Re-execing deploy script (phase: restart with on-disk script)..."
    exec /bin/bash "$SELF" --post-pull
fi

# ─── Phase 2: stop old backend, restart service (always from current script file on disk) ───
# During systemd churn, sleep/pkill may be interrupted; do not abort the whole deploy on non-zero.
set +e

log_message "Starting backend deployment (phase: stop / restart)..."

cd /home/ubuntu/trademanthan || {
    log_message "ERROR: Could not change to project directory (post-pull)"
    exit 1
}
# shellcheck disable=SC1091
. "/home/ubuntu/trademanthan/backend/scripts/dev_backend_env.sh" 2>/dev/null || {
    TRADEMANTHAN_DEV_SCREEN=trademanthan-dev
    TRADEMANTHAN_DEV_PORT=9000
}

log_message "Stopping existing backend..."
log_message "Closing legacy manual screen session (if it held :8000)..."
screen -S trademanthan -X quit 2>/dev/null || true
log_message "Closing dev screen session (auxiliary :${TRADEMANTHAN_DEV_PORT:-9000})..."
screen -S "${TRADEMANTHAN_DEV_SCREEN:-trademanthan-dev}" -X quit 2>/dev/null || true
screen -wipe 2>/dev/null || true
/bin/sleep 1 || true
log_message "Sending SIGTERM to uvicorn (if any)..."
pkill -f "uvicorn.*main:app" 2>/dev/null || true
pkill -f "uvicorn.*backend.main:app" 2>/dev/null || true
/bin/sleep 2 || true

if pgrep -f "uvicorn.*main:app" > /dev/null || pgrep -f "uvicorn.*backend.main:app" > /dev/null; then
    log_message "⚠️ Force killing backend process..."
    pkill -9 -f "uvicorn.*main:app" 2>/dev/null || true
    pkill -9 -f "uvicorn.*backend.main:app" 2>/dev/null || true
    /bin/sleep 1 || true
fi

log_message "Checkpoint: uvicorn stopped; proceeding to port cleanup / service restart..."

if command -v fuser >/dev/null 2>&1; then
    log_message "Ensuring port 8000 is free..."
    sudo -n fuser -k 8000/tcp >>"$LOG_FILE" 2>&1 || log_message "fuser cleanup skipped or failed (non-fatal)"
    /bin/sleep 1 || true
fi

if systemctl list-unit-files 2>/dev/null | grep -q "trademanthan-backend.service"; then
    log_message "Restarting via systemd..."
    sudo systemctl restart trademanthan-backend 2>>"$LOG_FILE" || true
    sleep 3
else
    log_message "Starting auxiliary backend in screen (${TRADEMANTHAN_DEV_SCREEN:-trademanthan-dev} :${TRADEMANTHAN_DEV_PORT:-9000})..."
    cd /home/ubuntu/trademanthan
    # shellcheck disable=SC1091
    source backend/venv/bin/activate

    screen -wipe 2>/dev/null || true
    screen -S trademanthan -X quit 2>/dev/null || true
    screen -S "${TRADEMANTHAN_DEV_SCREEN:-trademanthan-dev}" -X quit 2>/dev/null || true
    sleep 1

    screen -dmS "${TRADEMANTHAN_DEV_SCREEN:-trademanthan-dev}" bash -c "cd /home/ubuntu/trademanthan && source backend/venv/bin/activate && python3 -u -m uvicorn backend.main:app --host 0.0.0.0 --port ${TRADEMANTHAN_DEV_PORT:-9000}"
    /bin/sleep 3 || true
fi
BACKEND_PID=$(pgrep -f "uvicorn.*main:app" | head -1)

log_message "Backend started with PID: $BACKEND_PID"

log_message "Waiting for backend to start..."
for _i in {1..20}; do
    /bin/sleep 1 || true
    if systemctl list-unit-files 2>/dev/null | grep -q "trademanthan-backend.service" && check_backend_health; then
        log_message "✅ Backend is healthy and responding"
        timeout 2 tail -10 /home/ubuntu/trademanthan/logs/trademanthan.log 2>/dev/null || true
        exit 0
    fi
    if ! systemctl list-unit-files 2>/dev/null | grep -q "trademanthan-backend.service" && check_dev_backend_health; then
        log_message "✅ Auxiliary backend is healthy on port ${TRADEMANTHAN_DEV_PORT:-9000}"
        timeout 2 tail -10 /home/ubuntu/trademanthan/logs/trademanthan.log 2>/dev/null || true
        exit 0
    fi
done

log_message "⚠️ Backend started but health check timed out"
log_message "Check /home/ubuntu/trademanthan/logs/trademanthan.log for details"
timeout 2 tail -20 /home/ubuntu/trademanthan/logs/trademanthan.log 2>/dev/null || true
exit 1
