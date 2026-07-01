#!/usr/bin/env bash
# Materialize paperclip-vm SSH credentials for Cloud Agents and CI.
#
# Sources (first match wins for the private key body):
#   PAPERCLIP_SSH_PRIVATE_KEY  — raw PEM or base64-encoded PEM (Cursor / GitHub secret)
#   PAPERCLIP_KEY              — path to an existing key file (default ~/.ssh/paperclip_key)
#
# Also writes ~/.ssh/config Host "paperclip" when missing.
#
# Usage:
#   ./scripts/setup-paperclip-ssh.sh
#   source ./scripts/setup-paperclip-ssh.sh   # export PAPERCLIP_KEY when ready

set -euo pipefail

export PAPERCLIP_HOST="${PAPERCLIP_HOST:-140.245.14.17}"
export PAPERCLIP_USER="${PAPERCLIP_USER:-ubuntu}"
export PAPERCLIP_SSH_HOST="${PAPERCLIP_SSH_HOST:-paperclip}"
export PAPERCLIP_KEY="${PAPERCLIP_KEY:-$HOME/.ssh/paperclip_key}"

mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"

_write_key_from_env() {
  local raw="${PAPERCLIP_SSH_PRIVATE_KEY:-}"
  [[ -n "$raw" ]] || return 1
  if [[ "$raw" == *"BEGIN"* ]]; then
    printf '%s\n' "$raw" >"$PAPERCLIP_KEY"
  else
    echo "$raw" | base64 -d >"$PAPERCLIP_KEY"
  fi
  chmod 600 "$PAPERCLIP_KEY"
  return 0
}

if [[ ! -f "$PAPERCLIP_KEY" ]]; then
  _write_key_from_env || true
fi

if [[ ! -f "$HOME/.ssh/config" ]] || ! grep -q "^Host ${PAPERCLIP_SSH_HOST}$" "$HOME/.ssh/config" 2>/dev/null; then
  cat >>"$HOME/.ssh/config" <<EOF

Host ${PAPERCLIP_SSH_HOST}
  HostName ${PAPERCLIP_HOST}
  User ${PAPERCLIP_USER}
  IdentityFile ${PAPERCLIP_KEY}
  IdentitiesOnly yes
  ConnectTimeout 25
  ServerAliveInterval 30
EOF
  chmod 600 "$HOME/.ssh/config"
fi

if [[ -f "$PAPERCLIP_KEY" ]]; then
  echo "paperclip SSH ready: ${PAPERCLIP_SSH_HOST} (${PAPERCLIP_USER}@${PAPERCLIP_HOST})"
else
  echo "paperclip SSH key not configured." >&2
  echo "Add Runtime Secret PAPERCLIP_SSH_PRIVATE_KEY in Cursor Cloud Agents, or" >&2
  echo "GitHub Actions secret PAPERCLIP_SSH_PRIVATE_KEY, or place a key at ${PAPERCLIP_KEY}." >&2
  exit 1
fi
