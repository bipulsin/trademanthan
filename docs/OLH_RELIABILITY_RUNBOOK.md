# OLH Reliability Runbook (one page)

Never deploy/restart **08:45–09:35 IST** on a trading day.

## 2026-11-25 forensics (DEGRADED Phase A=0)

### Root causes
1. **WS 401 / junk token** — `upstox_token.json` held `MARK6171|sleep 7...` (last4=`ep 7`, file mtime 2026-11-24T23:57:54). `authorize` returned **401** all morning (`UDAPI100050 Invalid token`). Same token source as REST canary (`token_last4=ep 7`). EQ/INDEX ticks=0 despite 291 subscribed keys.
2. **Shared candle budget starved Phase A** — shared pool ran at **~120 grants/min** from 09:00 (PDC fills + day/intraday warmers). By ~09:13 denials appear; Phase A (still on **shared**) logged REST `no_bars` while dedicated **canary** pool fetched real 09:15 bars (RELIANCE/TCS/INFY/NIFTY).
3. **EQ stall armed too early** — fired at **09:15:00** (now **09:15:05**).

### Shared budget consumers before 09:20 (evidence)
| Minute IST | shared granted≈ | Notes |
|------------|-----------------|-------|
| 09:00–09:12 | 120/min | Cap saturated every minute |
| 09:13 | 47 granted / 3+ denied | First `CandleBudgetDenied` (day + intraday URLs) |
| 09:00+ | PDC `pdc_fill` | Dominant early consumer in OHL_Log |
| 09:10 | Pre-market F&O watchlist | Scheduler job |
| 09:12 | Stock Options contract rollover | Day candles |
| 09:15 | Arbitrage LTP / Index Price / OLH warm | Concurrent with stall |

Going forward: `candle_rl job_usage job=… minute=… granted=… denied=…` tags named jobs.

### Alerts 08:45–09:30 (all `sent` in OHL_Log)
Readiness RED (pdc), DEGRADED canaries 09:05/09:08/09:15, EQ silent-stall ×3, Coverage 09:21/09:26, Phase A degraded, DEGRADED 09:29. **None suppressed** — Telegram send returned success to `@TradeWithCTO`. If not seen by user: check channel mute / bot permissions / wrong chat, not app suppress.

### Upstox isolation (limits)
- REST rate limits: **per API, per user** (not per app). Historical/other: 50/s, 500/min, 2000/30m.
- Market Data Feed V3: **2 WS connections per user** (5 with Upstox Plus); full mode ≤2000 keys/connection.
- A second Upstox **app** under the same login does **not** raise caps. Optional env: `OLH_UPSTOX_API_KEY` / `_SECRET` / `_TOKEN_FILE` (only useful with a **different user** or Plus).

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
| `OLH_UPSTOX_API_KEY` / `_SECRET` / `_TOKEN_FILE` | empty | Optional dedicated OLH Upstox app (limits still per-user) |

**Prod env note (paperclip compose):** `./data` is mounted at `/home/ubuntu/trademanthan/data` inside the app container — set `OLH_RAW_RECORD_DIR=/home/ubuntu/trademanthan/data/olh_raw_recordings`. `/app/data/...` is ephemeral unless separately mounted.

## Independent watchdog (separate process)

```bash
# crontab on host (IST machine or with TZ=Asia/Kolkata)
21 9 * * 1-5 cd /home/ubuntu/TradeManthan && /opt/venv/bin/python scripts/olh_watchdog.py --label 0921
26 9 * * 1-5 cd /home/ubuntu/TradeManthan && /opt/venv/bin/python scripts/olh_watchdog.py --label 0926
30 9 * * 1-5 cd /home/ubuntu/TradeManthan && /opt/venv/bin/python scripts/olh_watchdog.py --label 0930
```

## 23-Nov EQ note

Raw WS frames were **not** retained. INDEX ticks=0 was a parser miss (`indexFF`). EQ silent-stall on 23-Nov is **not verifiable offline**; watchdog + recording land for next session.
