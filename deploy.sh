#!/bin/bash
# Quick deploy to production paperclip-vm (Docker twcto stack).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$ROOT/scripts/trigger-paperclip-deploy.sh"
