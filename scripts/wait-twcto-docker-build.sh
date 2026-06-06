#!/usr/bin/env bash
# Wait for the latest twcto_docker publish-images workflow run to finish.
#
# Usage:
#   ./scripts/wait-twcto-docker-build.sh
#   WAIT_TIMEOUT_SEC=1800 ./scripts/wait-twcto-docker-build.sh
#
# Requires GITHUB_TOKEN or GH_TOKEN.

set -euo pipefail

TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-1800}"
POLL_SEC="${POLL_SEC:-30}"
API="https://api.github.com/repos/bipulsin/twcto_docker/actions/workflows/publish-images.yml/runs?per_page=1"

if [[ -z "$TOKEN" ]]; then
  echo "Set GITHUB_TOKEN or GH_TOKEN." >&2
  exit 1
fi

echo "Waiting for twcto_docker CI (timeout ${WAIT_TIMEOUT_SEC}s, poll every ${POLL_SEC}s)..."

deadline=$(( $(date +%s) + WAIT_TIMEOUT_SEC ))
while [[ $(date +%s) -lt $deadline ]]; do
  json="$(curl -fsS -H "Authorization: Bearer ${TOKEN}" -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" "$API")"

  status="$(printf '%s' "$json" | python3 -c "import sys,json; r=json.load(sys.stdin)['workflow_runs'][0]; print(f\"{r['status']}|{r.get('conclusion') or ''}|{r['html_url']}\")")"
  run_status="${status%%|*}"
  rest="${status#*|}"
  conclusion="${rest%%|*}"
  url="${rest##*|}"

  echo "  status=${run_status} conclusion=${conclusion:-pending} ${url}"

  if [[ "$run_status" == "completed" ]]; then
    if [[ "$conclusion" == "success" ]]; then
      echo "CI build succeeded."
      exit 0
    fi
    echo "CI build finished with conclusion: ${conclusion}" >&2
    exit 1
  fi

  sleep "$POLL_SEC"
done

echo "Timed out waiting for twcto_docker CI." >&2
exit 1
