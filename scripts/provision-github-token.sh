#!/usr/bin/env bash
# Provision GITHUB_TOKEN for the trusted REBUILD=0 (GHCR pull) deploy path.
#
# Writes a PAT (repo + workflow scopes) to:
#   ~/.config/trademanthan/github_token          (this machine)
#   ubuntu@paperclip:~/.config/trademanthan/github_token  (production host)
#
# Usage:
#   ./scripts/provision-github-token.sh              # read from git credential / prompt
#   ./scripts/provision-github-token.sh --from-git-credential
#   GITHUB_TOKEN=ghp_... ./scripts/provision-github-token.sh
#   ./scripts/provision-github-token.sh --verify-only

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_DIR="${HOME}/.config/trademanthan"
LOCAL_FILE="${LOCAL_DIR}/github_token"
REMOTE_DIR="/home/ubuntu/.config/trademanthan"
REMOTE_FILE="${REMOTE_DIR}/github_token"
FROM_GIT=0
VERIFY_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from-git-credential) FROM_GIT=1; shift ;;
    --verify-only) VERIFY_ONLY=1; shift ;;
    -h|--help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

_token_from_git_credential() {
  printf 'protocol=https\nhost=github.com\n\n' | git credential fill 2>/dev/null \
    | awk -F= '/^password=/{print substr($0,10); exit}'
}

_verify_token() {
  local token="$1"
  local code login
  code="$(curl -sS -o /tmp/tm_gh_user.json -w "%{http_code}" \
    -H "Authorization: Bearer ${token}" \
    -H "Accept: application/vnd.github+json" \
    https://api.github.com/user || true)"
  if [[ "$code" != "200" ]]; then
    echo "Token verification failed (HTTP ${code})." >&2
    return 1
  fi
  login="$(python3 -c 'import json; print(json.load(open("/tmp/tm_gh_user.json")).get("login",""))')"
  rm -f /tmp/tm_gh_user.json
  code="$(curl -sS -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${token}" \
    -H "Accept: application/vnd.github+json" \
    "https://api.github.com/repos/bipulsin/twcto_docker/actions/workflows" || true)"
  if [[ "$code" != "200" ]]; then
    echo "Token cannot read bipulsin/twcto_docker workflows (HTTP ${code}). Need repo scope." >&2
    return 1
  fi
  echo "Token OK (login=${login}, twcto_docker workflows reachable)."
}

if [[ "$VERIFY_ONLY" == "1" ]]; then
  # shellcheck source=scripts/load-github-token.sh
  source "${ROOT}/scripts/load-github-token.sh"
  load_github_token
  _verify_token "$GITHUB_TOKEN"
  exit 0
fi

TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
if [[ -z "$TOKEN" && "$FROM_GIT" == "1" ]]; then
  TOKEN="$(_token_from_git_credential || true)"
fi
if [[ -z "$TOKEN" ]]; then
  # Prefer git credential automatically when available (no prompt needed).
  TOKEN="$(_token_from_git_credential || true)"
fi
if [[ -z "$TOKEN" ]]; then
  if [[ -t 0 ]]; then
    echo -n "Paste GitHub PAT (repo + workflow scopes, not echoed): "
    read -r -s TOKEN
    echo
  else
    echo "No token. Export GITHUB_TOKEN or re-run with --from-git-credential." >&2
    exit 1
  fi
fi
TOKEN="$(printf '%s' "$TOKEN" | tr -d '[:space:]')"
if [[ -z "$TOKEN" ]]; then
  echo "Empty token." >&2
  exit 1
fi

_verify_token "$TOKEN"

mkdir -p "$LOCAL_DIR"
umask 077
printf '%s\n' "$TOKEN" >"$LOCAL_FILE"
chmod 600 "$LOCAL_FILE"
echo "Wrote ${LOCAL_FILE} (mode 600)."

# Install on paperclip-vm for operators who SSH there and run release helpers.
TMP_REMOTE="$(mktemp)"
printf '%s\n' "$TOKEN" >"$TMP_REMOTE"
chmod 600 "$TMP_REMOTE"
"${ROOT}/scripts/paperclip-ssh.sh" "mkdir -p ${REMOTE_DIR} && chmod 700 ${REMOTE_DIR}"
scp -i "${HOME}/.ssh/paperclip_key" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new \
  "$TMP_REMOTE" "ubuntu@140.245.14.17:${REMOTE_FILE}"
"${ROOT}/scripts/paperclip-ssh.sh" "chmod 600 ${REMOTE_FILE} && ls -la ${REMOTE_FILE}"
rm -f "$TMP_REMOTE"
echo "Wrote paperclip:${REMOTE_FILE} (mode 600)."

echo ""
echo "REBUILD=0 path ready. Example:"
echo "  ./scripts/trigger-twcto-docker-build.sh \"\$(git rev-parse HEAD)\""
echo "  ./scripts/wait-twcto-docker-build.sh"
echo "  REBUILD=0 ./scripts/trigger-paperclip-deploy.sh"
