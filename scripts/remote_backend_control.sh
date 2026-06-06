#!/bin/bash
# Remote backend control on paperclip-vm (Docker twcto stack).
# Usage: remote_backend_control.sh [start|stop|restart|status]

set -euo pipefail

ACTION="${1:-status}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_TIMEOUT=25

run_paperclip() {
  "${ROOT}/scripts/paperclip-ssh.sh" "$@"
}

case "$ACTION" in
  start)
    run_paperclip 'cd /home/ubuntu/twcto && docker compose up -d app nginx'
    ;;
  stop)
    run_paperclip 'cd /home/ubuntu/twcto && docker compose stop app nginx'
    ;;
  restart)
    run_paperclip 'cd /home/ubuntu/twcto && docker compose restart app nginx'
    ;;
  status)
    run_paperclip 'cd /home/ubuntu/twcto && docker compose ps && curl -fsS http://127.0.0.1:8080/scan/health | head -c 300; echo'
    ;;
  *)
    echo "Usage: $0 [start|stop|restart|status]" >&2
    exit 1
    ;;
esac
