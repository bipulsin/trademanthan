#!/usr/bin/env bash
# Local release: push TradeManthan to GitHub main, then tell EC2 to pull and deploy.
# GitHub never receives EC2 host/key — only git push; deploy is triggered on the server.
#
# Usage:
#   ./scripts/release-push-and-deploy.sh
#   ./scripts/release-push-and-deploy.sh -m "Your commit message"
#
# Requires: clean commit already staged, or pass -m to commit all tracked changes (not untracked junk).

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

echo "Pushing to origin main..."
git push origin main

echo ""
exec "$ROOT/scripts/trigger-ec2-deploy.sh"
