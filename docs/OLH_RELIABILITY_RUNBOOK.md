# OLH Reliability Runbook (one page)

**NO production deploy until user reviews.** Never deploy/restart 08:45–09:35 IST on a trading day.  
**2026-11-24** = NSE holiday (Guru Nanak Jayanti) → dress rehearsal day. Jobs for **2026-11-25** stay registered; holiday gate skips alerts/REST.

## Morning force actions (exact commands)

Run inside `twcto-app-1` or against the API with auth as you normally do for `/api/open-low-high/*`.

### 09:21 — Force Phase A

```bash
# API
curl -sS -X POST 'https://www.tradewithcto.com/api/open-low-high/screen' \
  -H 'Content-Type: application/json' \
  -d '{"force":true}'

# Or in-container Python (Phase A only)
./scripts/paperclip-ssh.sh 'docker exec twcto-app-1 /opt/venv/bin/python -c "
from backend.services.open_low_high.phases import run_phase_a
print(run_phase_a(force=True))
"'
```

### 09:26 — Force Phase B (REST-only path)

```bash
./scripts/paperclip-ssh.sh 'docker exec twcto-app-1 /opt/venv/bin/python -c "
from backend.services.open_low_high.phases import run_phase_b
print(run_phase_b(force=True))
"'
```

Phase B uses the dedicated REST budget (`OLH_PHASE_B_RL_*`); it does not share Phase A slots.

### 09:30 — Fallback / repair

```bash
# Full force re-screen (A then B)
curl -sS -X POST 'https://www.tradewithcto.com/api/open-low-high/screen' \
  -H 'Content-Type: application/json' \
  -d '{"force":true}'

# Independent watchdog pulse
python scripts/olh_watchdog.py --label 0930
```

## Dress rehearsal (holiday / offline)

```bash
# From repo root — synthetic 23-Nov fixtures + fault table
python scripts/olh_dress_rehearsal.py --out /tmp/olh_rehearsal.json

# Fault injection only
python scripts/olh_dress_rehearsal.py --faults-only

# Unit tests
python -m pytest backend/test_olh_reliability.py -q
```

## Enable raw recording (staging volume)

```bash
export OLH_RAW_RECORD_ENABLED=1
export OLH_RAW_RECORD_DIR=/home/ubuntu/twcto/data/olh_raw_recordings   # persistent mount
# Retention 14d; ~150 MB WS + ~5 MB REST / day → ~2.1 GB volume mid-case
```

## Config keys

| Key | Default | Purpose |
|-----|---------|---------|
| `OLH_RAW_RECORD_ENABLED` | false | WS/REST capture 09:00–09:35 |
| `OLH_RAW_RECORD_DIR` | `/app/data/olh_raw_recordings` | Container-writable mount (not host-only paths) |
| `OLH_RAW_RECORD_RETENTION_DAYS` | 14 | Prune age |
| `OLH_EQ_STALL_ENABLED` | true | EQ silent-stall watchdog |
| `OLH_EQ_STALL_SEC` | 20 | No EQ ticks → stall |
| `OLH_EQ_STALL_MAX_ATTEMPTS` | 3 | Resub attempt cap / day |
| `OLH_EQ_STALL_ARM` | 09:15:05 | Stall watchdog arm time |
| `OLH_READINESS_ENABLED` | true | 08:50 go/no-go |
| `OLH_DAILY_RECONCILE_ENABLED` | true | 15:45 WS vs REST |
| `OLH_REHEARSAL_MODE` | false | Marker only |
| `OLH_DEPLOY_GUARD_FORCE` | false | Override 08:45–09:35 deploy guard |
| `OLH_ALERTS_ENABLED` | true | Telegram alerts |
| `OLH_PHASE_A_RL_PER_SEC` / `_PER_MIN` / `_SEMAPHORE` | 5 / 120 / 6 | Dedicated Phase A candle budget |
| `OLH_PHASE_B_RL_*` | 4 / 80 / 4 | Dedicated Phase B budget |
| `OLH_CANARY_RL_*` | 1 / 20 / 1 | Dedicated canary budget |
| `OLH_PRIORITY_YIELD_ENABLED` | true | Non-OLH shared candle callers yield 09:10–09:30 |
| `OLH_WS_AUTH_MAX_ATTEMPTS` | 5 | Bounded authorize 401/403 recovery |

**Prod env note (2026-11-25):** set `OLH_RAW_RECORD_DIR=/app/data/olh_raw_recordings` (host `./data` mount). Do **not** use `/home/ubuntu/trademanthan/...` inside the container.

## Independent watchdog (separate process)

```bash
# crontab on host (IST machine or with TZ=Asia/Kolkata)
21 9 * * 1-5 cd /home/ubuntu/TradeManthan && /opt/venv/bin/python scripts/olh_watchdog.py --label 0921
26 9 * * 1-5 cd /home/ubuntu/TradeManthan && /opt/venv/bin/python scripts/olh_watchdog.py --label 0926
30 9 * * 1-5 cd /home/ubuntu/TradeManthan && /opt/venv/bin/python scripts/olh_watchdog.py --label 0930
```

## 23-Nov EQ note

Raw WS frames were **not** retained. INDEX ticks=0 was a parser miss (`indexFF`). EQ silent-stall on 23-Nov is **not verifiable offline**; watchdog + recording land for next session.
