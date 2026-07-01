#!/usr/bin/env bash
# SSH to production paperclip-vm (TradeWithCTO Docker host).
# Uses ~/.ssh/config host "paperclip" when present, else explicit key + IP.
#
# Usage:
#   ./scripts/paperclip-ssh.sh
#   ./scripts/paperclip-ssh.sh 'docker ps'
#
# Override: PAPERCLIP_HOST, PAPERCLIP_USER, PAPERCLIP_KEY, PAPERCLIP_SSH_HOST (ssh config alias)
# Cloud Agent / CI: set Runtime Secret PAPERCLIP_SSH_PRIVATE_KEY (see setup-paperclip-ssh.sh).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Materialize key from PAPERCLIP_SSH_PRIVATE_KEY when present (no-op if key file exists).
if [[ -x "${ROOT}/scripts/setup-paperclip-ssh.sh" ]]; then
  "${ROOT}/scripts/setup-paperclip-ssh.sh" 2>/dev/null || true
fi

export PAPERCLIP_HOST="${PAPERCLIP_HOST:-140.245.14.17}"
export PAPERCLIP_USER="${PAPERCLIP_USER:-ubuntu}"
export PAPERCLIP_KEY="${PAPERCLIP_KEY:-$HOME/.ssh/paperclip_key}"
export PAPERCLIP_SSH_HOST="${PAPERCLIP_SSH_HOST:-paperclip}"

if [[ -f "$HOME/.ssh/config" ]] && grep -q "^Host ${PAPERCLIP_SSH_HOST}$" "$HOME/.ssh/config" 2>/dev/null; then
  exec ssh -o ConnectTimeout=25 -o ServerAliveInterval=30 "${PAPERCLIP_SSH_HOST}" "$@"
fi

if [[ ! -f "$PAPERCLIP_KEY" ]]; then
  echo "Paperclip SSH key not found: $PAPERCLIP_KEY" >&2
  echo "Run ./scripts/setup-paperclip-ssh.sh after adding PAPERCLIP_SSH_PRIVATE_KEY secret," >&2
  echo "or add Host paperclip to ~/.ssh/config." >&2
  exit 1
fi

exec ssh -i "$PAPERCLIP_KEY" -o ConnectTimeout=25 -o ServerAliveInterval=30 \
  "${PAPERCLIP_USER}@${PAPERCLIP_HOST}" "$@"
