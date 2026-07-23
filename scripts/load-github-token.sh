#!/usr/bin/env bash
# Load GITHUB_TOKEN / GH_TOKEN for twcto_docker CI dispatch (REBUILD=0 path).
#
# Resolution order (first non-empty wins):
#   1. Already-exported GITHUB_TOKEN or GH_TOKEN
#   2. $TRADEMANTHAN_GITHUB_TOKEN_FILE
#   3. ~/.config/trademanthan/github_token
#   4. /home/ubuntu/.config/trademanthan/github_token  (paperclip)
#
# Usage (from other scripts):
#   # shellcheck source=scripts/load-github-token.sh
#   source "$(dirname "$0")/load-github-token.sh"
#   load_github_token || exit 1
#   # $GITHUB_TOKEN is now set

load_github_token() {
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    export GITHUB_TOKEN
    return 0
  fi
  if [[ -n "${GH_TOKEN:-}" ]]; then
    export GITHUB_TOKEN="$GH_TOKEN"
    return 0
  fi

  local candidates=()
  if [[ -n "${TRADEMANTHAN_GITHUB_TOKEN_FILE:-}" ]]; then
    candidates+=("$TRADEMANTHAN_GITHUB_TOKEN_FILE")
  fi
  candidates+=(
    "${HOME}/.config/trademanthan/github_token"
    "/home/ubuntu/.config/trademanthan/github_token"
  )

  local f token
  for f in "${candidates[@]}"; do
    if [[ -f "$f" && -r "$f" ]]; then
      token="$(tr -d '[:space:]' <"$f")"
      if [[ -n "$token" ]]; then
        export GITHUB_TOKEN="$token"
        export GH_TOKEN="$token"
        return 0
      fi
    fi
  done

  echo "GITHUB_TOKEN not set. Provision with:" >&2
  echo "  ./scripts/provision-github-token.sh" >&2
  echo "Or export GITHUB_TOKEN / place a PAT in ~/.config/trademanthan/github_token" >&2
  return 1
}
