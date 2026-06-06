#!/usr/bin/env bash
# Trigger twcto_docker CI to rebuild multi-arch GHCR images from trademanthan.
#
# Usage:
#   ./scripts/trigger-twcto-docker-build.sh           # build trademanthan main
#   ./scripts/trigger-twcto-docker-build.sh abc1234   # build specific git ref/SHA
#
# Requires GITHUB_TOKEN or GH_TOKEN (classic PAT or fine-grained with access to bipulsin/twcto_docker).

set -euo pipefail

REF="${1:-main}"
TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"

if [[ -z "$TOKEN" ]]; then
  echo "Set GITHUB_TOKEN or GH_TOKEN to trigger bipulsin/twcto_docker CI." >&2
  echo "Create a PAT at https://github.com/settings/tokens with repo scope." >&2
  exit 1
fi

echo "Triggering twcto_docker image build for trademanthan ref: ${REF}"

curl -fsS -X POST \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/bipulsin/twcto_docker/dispatches" \
  -d "{\"event_type\":\"trademanthan-updated\",\"client_payload\":{\"ref\":\"${REF}\"}}"

echo ""
echo "CI started: https://github.com/bipulsin/twcto_docker/actions/workflows/publish-images.yml"
