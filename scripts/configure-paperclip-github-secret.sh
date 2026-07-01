#!/usr/bin/env bash
# One-time: store paperclip SSH key in GitHub Actions secrets and trigger deploy.
#
# Run on your laptop (where ~/.ssh/paperclip_key exists), not in Cloud Agent:
#   ./scripts/configure-paperclip-github-secret.sh
#   ./scripts/configure-paperclip-github-secret.sh --deploy
#
# Requires: gh auth login (repo admin on bipulsin/trademanthan)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEY_PATH="${PAPERCLIP_KEY:-$HOME/.ssh/paperclip_key}"
DO_DEPLOY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deploy) DO_DEPLOY=1; shift ;;
    --key) KEY_PATH="${2:-}"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ ! -f "$KEY_PATH" ]]; then
  echo "SSH key not found: $KEY_PATH" >&2
  echo "Set PAPERCLIP_KEY or place paperclip_key at ~/.ssh/paperclip_key" >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "Install GitHub CLI: https://cli.github.com/" >&2
  exit 1
fi

echo "Setting GitHub secret PAPERCLIP_SSH_PRIVATE_KEY from $KEY_PATH ..."
gh secret set PAPERCLIP_SSH_PRIVATE_KEY <"$KEY_PATH" -R bipulsin/trademanthan

# Legacy name used by old EC2 workflow — keep in sync for fallback.
gh secret set EC2_SSH_KEY <"$KEY_PATH" -R bipulsin/trademanthan

echo "Secrets updated."

if [[ "$DO_DEPLOY" == "1" ]]; then
  echo "Triggering deploy-paperclip workflow (REBUILD=1, RS scan)..."
  gh workflow run deploy-paperclip.yml -R bipulsin/trademanthan \
    -f rebuild=true \
    -f run_rs_scan=true \
    -f trademanthan_ref=main
  echo "Watch: gh run watch -R bipulsin/trademanthan \$(gh run list -R bipulsin/trademanthan --workflow deploy-paperclip.yml --limit 1 --json databaseId -q '.[0].databaseId')"
fi
