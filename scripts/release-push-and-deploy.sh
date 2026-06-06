#!/usr/bin/env bash
# Release: push TradeManthan, rebuild multi-arch images in CI, deploy on paperclip-vm.
#
# Flow:
#   1. git push bipulsin/trademanthan main
#   2. repository_dispatch → bipulsin/twcto_docker CI (amd64 + arm64 GHCR)
#   3. wait for CI (default), then pull images on paperclip-vm
#
# Usage:
#   GITHUB_TOKEN=ghp_... ./scripts/release-push-and-deploy.sh -m "message"
#   WAIT_CI=0 ./scripts/release-push-and-deploy.sh   # skip wait (deploy may fail if images stale)
#   REBUILD=1 ./scripts/release-push-and-deploy.sh   # fallback: local build on paperclip

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

REF="$(git rev-parse HEAD)"
echo ""
echo "Triggering twcto_docker multi-arch image build for ref ${REF}..."
"$ROOT/scripts/trigger-twcto-docker-build.sh" "$REF"

if [[ "${WAIT_CI:-1}" == "1" ]]; then
  echo ""
  "$ROOT/scripts/wait-twcto-docker-build.sh"
fi

echo ""
echo "Deploying on paperclip-vm (pull from GHCR)..."
export REBUILD="${REBUILD:-0}"
export TRADEMANTHAN_REF="${REF}"
exec "$ROOT/scripts/trigger-paperclip-deploy.sh"
