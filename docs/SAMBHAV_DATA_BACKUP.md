# Sambhav Historical Data — Backup & Restore

Protects the foundational Sambhav source dataset (`sambhav_10m_candles` and related metadata).

**Never commit database credentials.** Use environment variables or the paperclip Docker secrets already on the host.

---

## What is backed up

| Table | Role |
|-------|------|
| `sambhav_10m_candles` | Source OHLC (immutable for ML) |
| `sambhav_sessions` | Session classification |
| `sambhav_dataset_versions` | Dataset version metadata |
| `sambhav_import_state` | Import cursor / status |
| `sambhav_raw_candles` | Reserved (empty for V1) |
| `sambhav_features` | Feature store (empty until feature phase) |

Preferred format: **PostgreSQL custom** (`pg_dump -Fc`).

---

## Production backup (paperclip / twcto Docker)

```bash
# SSH to paperclip, then:
cd /home/ubuntu/twcto
mkdir -p /home/ubuntu/backups/sambhav
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT=/home/ubuntu/backups/sambhav/sambhav_dataset_v1_20260813_${STAMP}.dump

docker compose exec -T postgres \
  pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom --no-owner --no-acl \
  -t sambhav_10m_candles \
  -t sambhav_sessions \
  -t sambhav_dataset_versions \
  -t sambhav_import_state \
  -t sambhav_raw_candles \
  -t sambhav_features \
  > "$OUT"

ls -lh "$OUT"
```

If `$POSTGRES_USER` / `$POSTGRES_DB` are not exported on the host, use the values from the compose env file **without pasting them into git** (typically user/db `trademanthan`).

Optional helper (local machine with `DATABASE_URL` set):

```bash
chmod +x scripts/sambhav_backup.sh
DATABASE_URL='postgresql://…' ./scripts/sambhav_backup.sh
```

**Backup location (production):** `/home/ubuntu/backups/sambhav/`  
Keep dumps off the application image; do not commit dump files.

---

## Restore procedure

1. Stop writers if needed (optional brief pause of app imports).
2. Restore into the running Postgres (does **not** drop unrelated tables):

```bash
cd /home/ubuntu/twcto
DUMP=/home/ubuntu/backups/sambhav/sambhav_dataset_v1_20260813_YYYYMMDDTHHMMSSZ.dump

# Review: list contents
docker compose exec -T postgres pg_restore -l < "$DUMP" | head

# Restore (tables must already exist via ensure_sambhav_tables, or use --clean carefully)
cat "$DUMP" | docker compose exec -T postgres \
  pg_restore -U trademanthan -d trademanthan --data-only --disable-triggers
```

If a destructive recovery is required (table corrupted), restore schema+data only for Sambhav tables after confirming no other data depends on them. Prefer `--data-only` into empty Sambhav tables created by the app.

3. Verify (inside app container):

```bash
docker compose exec -T app python3 - <<'PY'
from sqlalchemy import text
from backend.database import SessionLocal
from backend.services.sambhav.data_status import compute_data_status
db=SessionLocal()
n=db.execute(text("SELECT COUNT(*) FROM sambhav_10m_candles")).scalar()
q=compute_data_status(db, refresh_sessions=True)
print({"candles": n, "status": q["status"], "regular": q["regular_session_count"], "regular_candles": q["regular_candle_count"]})
db.close()
PY
```

Expect: **43,236** total candles, **1,137** regular sessions, **43,206** regular candles, status **PASS**.

---

## Verification checklist

- [ ] Dump file size > 0 and recent timestamp  
- [ ] `COUNT(*)` on `sambhav_10m_candles` matches pre-backup  
- [ ] Unique `(instrument_key, candle_start)` still enforced  
- [ ] `compute_data_status` → `PASS` for Sambhav V1  
- [ ] Dataset version row `sambhav_dataset_v1_20260813` present / active  

---

## Notes

- Incremental daily imports append via upsert; they must not replace this backup strategy.  
- Feature tables and models are derived; source OHLC is the recovery priority.  
- Never put passwords in this document or in committed scripts.
