# TWCTO Sambhav — Architecture Assessment & Integration Plan

**Status:** Research module (V1)  
**Scope:** NIFTY 50 index only · 10-minute bars · 30-minute horizon · NSE 09:15–15:30 IST  
**Constraint:** Completely separate from Kavach / RS / Daily Futures trading logic.

---

## 1. Current architecture / Python framework

- **Framework:** FastAPI (`backend/main.py`), lifespan-managed startup/shutdown.
- **ORM / DB access:** SQLAlchemy 2.x via `backend.database.SessionLocal`, `get_db`, `db_session()`.
- **Config / secrets:** `backend.config.settings` + `backend.env_bootstrap` loading project-root `.env`. Never hard-code credentials in Sambhav.
- **Routers:** Mounted in `main.py`; expensive/admin modules often dual-mounted (`/api/...` and bare prefix). Prefer **`/api/sambhav`** so nginx `location ^~ /api/` already proxies without a new static `try_files` conflict.
- **Background work:** APScheduler (`BackgroundScheduler`, `Asia/Kolkata`) for dedicated jobs; heavy ops often run in daemon threads with in-memory job status (see Kavach ignition diagnostics).

## 2. Database architecture

- **Engine:** PostgreSQL via `DATABASE_URL` (default local `postgresql://trademanthan:...`).
- **Schema style:** Mix of SQLAlchemy models and **`CREATE TABLE IF NOT EXISTS` + `ensure_*()`** helpers (Daily Futures, Iron Condor, Kavach shadows). Alembic is listed in requirements but day-to-day tables are mostly ensure-on-first-use.
- **Sambhav approach:** `sambhav_`-prefixed tables only, `ensure_sambhav_tables()` in `backend/services/sambhav/tables.py`. No alterations to Kavach / RS / trade_log tables.
- **Suggested tables:**
  - `sambhav_raw_candles` — 1m OHLCV
  - `sambhav_10m_candles` — unique `(instrument_key, candle_start)`
  - `sambhav_predictions` — immutable audit (`PENDING` / `RESOLVED`)
  - `sambhav_models` — registry (artifact path, status RESEARCH/VALIDATED/LIVE — never auto-VALIDATED)
  - `sambhav_metrics` — walk-forward / live metrics snapshots
  - `sambhav_import_state` — restartable import watermarks

## 3. Existing Upstox integration

- **Client:** `backend.services.upstox_service.UpstoxService`
- **Auth:** API key/secret from settings; token refresh inside `make_api_request`.
- **Historical candles:** V2 + V3 paths; `get_historical_candles_by_instrument_key(...)`, `_fetch_historical_v2_candles`, `_fetch_historical_v3_candles`, intraday merge for today.
- **Interval span caps:** 1m historical ~31 calendar days per request → Sambhav importer must **chunk + resume**.
- **NIFTY key (canonical):** `NSE_INDEX|Nifty 50` (`UpstoxService.NIFTY50_KEY`).
- **Reuse:** Call UpstoxService only; do not fork auth or candle HTTP clients.
- **Note:** Existing `_aggregate_1m_to_n_minute` uses **wall-clock** floor (`minute // n`). Sambhav **must not** reuse that for 10m bars — V1 requires **NSE 09:15-aligned** buckets (see candle convention below).

## 4. Scheduler / background jobs

- Primary trading schedulers live under `smart_future_algo` and a few dedicated APScheduler modules (arbitrage, ChartInk inbox, Iron Condor snapshot, ATR precompute).
- **Sambhav:** Own lightweight `backend/services/sambhav/scheduler.py` — fire shortly after each completed 10m candle during session (prediction-only; no orders). Start/stop from `main.py` lifespan only; do not plug into Kavach/RS jobs.

## 5. Frontend structure

- Static HTML/CSS/JS under `frontend/public/` (nginx `root` / `try_files`).
- Naming: Pascal/camel HTML files (`dailyfutures.html`, `kavachIgnitionDiag.html`, …).
- Patterns: `auth-check.js`, Bearer token in `localStorage` (`trademanthan_token`), left menu via `left-menu.html` + `left-menu.css`, admin gate via `/api/auth/me` + `is_admin == Yes`.
- **Sambhav UI:** `frontend/public/Sambhav.html` (+ optional CSS). Admin-only nav entry. No changes to Kavach pages.

## 6. Authentication for admin / expensive APIs

- JWT via `oauth2_scheme` + `get_user_from_token` (`backend.routers.auth`).
- Admin check pattern (copy from ignition diagnostics):

```python
if (getattr(user, "is_admin", None) or "").strip() != "Yes":
    raise HTTPException(status_code=403, detail="Administrator only")
```

- **GET** status/current/history/performance: authenticated user (or admin-only if we keep research gated).
- **POST** train / import / backtest: **admin only**, background job + job_id poll.

## 7. Potential conflicts with Kavach / RS / etc.

| Risk | Mitigation |
|------|------------|
| Shared Upstox rate budget | Use existing `make_api_request` pacing; chunk imports; avoid overlapping heavy RS scans if possible |
| Shared candle cache | Sambhav persists its own `sambhav_*` tables; importer may call Upstox with explicit date ranges (cache-bypass path via `range_end_date` / direct V2 fetch) |
| Scheduler load | Separate APScheduler; prediction tick is cheap after local DB candles exist |
| Naming / UI confusion | Distinct brand “Sambhav”; disclaimer that this is ML research probability, not Kavach signals |
| Accidental trade wiring | No broker/order imports; prediction-only V1 |
| Wall-clock 10m agg elsewhere | Document 09:15 alignment; keep aggregation inside `sambhav.candles` only |

**Do not modify:** Kavach FSM, scoring, RS scanner, Daily Futures indicator playbook, or existing trade tables.

---

## Candle boundary convention (NSE 09:15 aligned)

Session: **09:15–15:30 IST**.

For a 1m bar timestamp `t` (IST), minutes from session open:

```
m = (t.hour * 60 + t.minute) - (9 * 60 + 15)
candle_start = session_date 09:15 + floor(m / 10) * 10 minutes
```

Examples: 09:15–09:24 → start 09:15; 09:25–09:34 → 09:25; … last full 10m before 15:30 ends at 15:25 (15:25–15:29). Incomplete buckets at session edge are dropped or marked incomplete consistently for hist/live/backtest.

This convention is **identical** for historical import, live scheduler, and backtest.

---

## Package layout (implemented)

```
backend/services/sambhav/     # core library
backend/routers/sambhav.py    # HTTP API
frontend/public/Sambhav.html  # dashboard
scripts/sambhav_*.py          # CLI helpers
backend/test_sambhav_*.py     # unit tests
docs/SAMBHAV_ARCHITECTURE.md # this file
```

## Research honesty rules (product)

- Never present raw model probability as calibrated truth.
- Status labels: `RESEARCH` / `VALIDATED` / `LIVE` — **VALIDATED only via explicit admin action**, never automatic.
- When sample size / ECE is inadequate: show `INSUFFICIENT DATA` / `MODEL NOT VALIDATED` / `CALIBRATION POOR`.
- No BUY/SELL hard rules in V1; outputs are P(UP)/P(DOWN) + diagnostics only.

## Local runbook (no deploy)

```bash
# ML deps (once)
# Python 3.13: use these pins (1.3.2 / 1.7.6 lack 3.13 wheels)
backend/venv/bin/pip install scikit-learn==1.6.1 xgboost==2.1.4 joblib==1.4.2
# macOS: xgboost wheels need OpenMP — `brew install libomp` (or place libomp.dylib at $(brew --prefix)/opt/libomp/lib/)


# Tests (core + API smoke)
PYTHONPATH=. python3 -m pytest backend/test_sambhav_core.py backend/test_sambhav_api.py -q

# Import history → build 10m
PYTHONPATH=. python scripts/sambhav_import.py --from-date 2025-01-01 --to-date 2025-06-30

# Train RESEARCH model + walk-forward
PYTHONPATH=. python scripts/sambhav_train.py

# Walk-forward only
PYTHONPATH=. python scripts/sambhav_backtest.py

# UI (admin): /Sambhav.html — APIs under /api/sambhav/*
```

Do **not** commit/push/deploy unless explicitly requested.
