# shellcheck shell=bash
# Source this from manual / screen / nohup helpers only.
# Production (systemd) must keep using port 8000 — never start a second listener there.

export TRADEMANTHAN_DEV_SCREEN="${TRADEMANTHAN_DEV_SCREEN:-trademanthan-dev}"
export TRADEMANTHAN_DEV_PORT="${TRADEMANTHAN_DEV_PORT:-9000}"
