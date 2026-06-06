#!/usr/bin/env bash
# Release: push TradeManthan app code, then deploy on paperclip-vm via twcto_docker.
#
# Flow:
#   1. git push bipulsin/trademanthan main  (application source)
#   2. SSH paperclip-vm: git pull bipulsin/twcto_docker + docker compose pull/rebuild
#
# Usage:
#   ./scripts/release-push-and-deploy.sh
#   ./scripts/release-push-and-deploy.sh -m "Your commit message"
#   REBUILD=0 ./scripts/release-push-and-deploy.sh   # pull images only (no local docker build)

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMMIT_MSG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--message)
      COMMIT_MSG="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -n "$COMMIT_MSG" ]]; then
  git add -u
  if git diff --cached --quiet; then
    echo "Nothing staged to commit."
  else
    git commit -m "$COMMIT_MSG"
  fi
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Uncommitted changes remain. Commit first or use -m \"message\"." >&2
  git status -sb
  exit 1
fi

echo "Pushing TradeManthan (app) to origin main..."
git push origin main

echo ""
echo "Deploying on paperclip-vm (twcto_docker)..."
export REBUILD="${REBUILD:-1}"
export TRADEMANTHAN_REF="${TRADEMANTHAN_REF:-main}"
exec "$ROOT/scripts/trigger-paperclip-deploy.sh"
