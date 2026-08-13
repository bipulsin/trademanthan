#!/usr/bin/env bash
# Backup Sambhav historical source tables (pg_dump custom format).
# Does NOT print or embed database credentials in git.
#
# Usage (on paperclip / compose host):
#   ./scripts/sambhav_backup.sh
#   BACKUP_DIR=/home/ubuntu/backups/sambhav ./scripts/sambhav_backup.sh
#
# Inside Docker app network (preferred in production):
#   docker compose exec -T postgres sh -c '...'  (see docs/SAMBHAV_DATA_BACKUP.md)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
VERSION_TAG="${SAMBHAV_DATASET_VERSION:-sambhav_dataset_v1_20260813}"
BACKUP_DIR="${BACKUP_DIR:-${ROOT}/backups/sambhav}"
mkdir -p "${BACKUP_DIR}"

OUT="${BACKUP_DIR}/${VERSION_TAG}_${STAMP}.dump"
META="${BACKUP_DIR}/${VERSION_TAG}_${STAMP}.meta.json"

# Prefer DATABASE_URL if set (never commit secrets). Else compose postgres service.
if [[ -n "${DATABASE_URL:-}" ]]; then
  pg_dump --format=custom --no-owner --no-acl \
    --table=sambhav_10m_candles \
    --table=sambhav_sessions \
    --table=sambhav_dataset_versions \
    --table=sambhav_import_state \
    --table=sambhav_raw_candles \
    --table=sambhav_features \
    "${DATABASE_URL}" > "${OUT}"
else
  echo "DATABASE_URL not set. On paperclip use:" >&2
  echo "  cd /home/ubuntu/twcto && docker compose exec -T postgres pg_dump -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" --format=custom ..." >&2
  echo "See docs/SAMBHAV_DATA_BACKUP.md" >&2
  exit 2
fi

python3 - <<PY
import json, os
from pathlib import Path
meta = {
  "dataset_version": os.environ.get("SAMBHAV_DATASET_VERSION", "sambhav_dataset_v1_20260813"),
  "backup_file": "${OUT}",
  "created_at_utc": "${STAMP}",
  "tables": [
    "sambhav_10m_candles",
    "sambhav_sessions",
    "sambhav_dataset_versions",
    "sambhav_import_state",
    "sambhav_raw_candles",
    "sambhav_features",
  ],
  "format": "pg_dump custom",
}
Path("${META}").write_text(json.dumps(meta, indent=2) + "\n")
print(json.dumps({"ok": True, "backup": "${OUT}", "meta": "${META}"}))
PY
