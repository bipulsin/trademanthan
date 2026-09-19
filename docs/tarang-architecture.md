# Krypto Tarang — Architecture (Phase 0 stub)

**Status:** Phase 0 discovery only. No runtime engine, tables, or UI yet.  
**Product:** IV-gated defined-risk options spreads (iron condor or single credit spread) for MCX Crude Oil / Natural Gas (Upstox) and BTC / ETH (Delta Exchange India).

---

## Discovery summary (repo)

1. **Stack:** FastAPI (`backend/main.py`) + vanilla HTML/JS in `frontend/public/` + SQLAlchemy/Postgres (`backend/database.py`, `backend/models/`). No React SPA for strategy desks.
2. **Strategy module pattern:** package under `backend/services/<feature>/` (schema, scheduler, actions) + `backend/routers/<feature>.py` mounted in `main.py` (often dual `/api/...` and bare prefixes) + `frontend/public/<page>.html` (+ `.js`/`.css`) + nav entry in `frontend/public/left-menu.js`.
3. **Closest analogues:** Commodities Div (`backend/services/commodities_div/`, MCX via Upstox master), Stock Options (`stock_option_*`), Iron Condor advisory (`iron_condor_*` + `upstox_iron_condor.py`) — Tarang should follow CommDiv/Iron Condor desk shape, not invent a new framework.
4. **Auth:** JWT Bearer via `backend/routers/auth.py` / `get_user_from_token`; UI uses same token as other desks. Admin-gated nav via `nav-item-admin` + left-menu visibility API.
5. **DB:** Postgres production; migrations as SQL under `backend/migrations/` with `ensure_*_tables()` idempotent runners (see CommDiv). Prefer that over ORM-only for new Tarang tables.
6. **Jobs:** APScheduler `BackgroundScheduler(timezone="Asia/Kolkata")` started/stopped from FastAPI lifespan in `main.py` (pattern: stock_option, commodities_div, iron_condor).
7. **Upstox:** `UpstoxService` + `token_manager` (`data/upstox_token.json` / `UPSTOX_ACCESS_TOKEN`); instrument master via `backend/services/divtest/instruments.py` (`complete.json.gz`). Option **chain** API is **not** available for MCX — build chain from master + Option Greek `/v3/market-quote/option-greek` (≤50 keys).
8. **Delta:** Existing `backend/delta_api.py` + algo paths; Tarang market data should use public India REST `https://api.india.delta.exchange/v2` (testnet `https://cdn-ind.testnet.deltaex.org`). Do not hard-code keys/lot/expiry.
9. **Secrets:** root `.env` via `backend/env_bootstrap.py` + `backend/config.py` Settings; never ship secrets to the browser.
10. **Defaults for Tarang:** PAPER mode, AUTO off until Phase 5; no VWAP/SuperTrend/EMA/ATR filters — options mechanics / IV gates only.

## Assumptions

- Tarang is a **new** desk package; we will not modify Kavach / CommDiv / Stock Options / Iron Condor behaviour without explicit approval.
- UI is static HTML under `frontend/public/tarang.html` matching existing dark theme + `left-menu.js`.
- Tables are prefixed `tarang_*` / shared names from the product spec (`iv_snapshots`, etc. scoped under Tarang ownership).
- Contract multipliers / lot sizes always come from Upstox master or Delta `contract_value` / product fields at runtime.
- Profiles: CL, NG, BTC, ETH (+ experimental BTC_WEEKEND / ETH_WEEKEND disabled by default).
- Hard exits: MCX ~23:15 IST, Delta ~17:00 IST (config), Delta expiry ~17:30 IST — stored in settings, not code literals preferred.
- Phase 1+ will wire router + scheduler only after user answers shared-infra questions below.

## Spike results

- Upstox MCX: see [`docs/upstox-mcx-findings.md`](./upstox-mcx-findings.md)
- Delta India: same doc (Delta section) + raw [`docs/_spike_delta_india_raw.json`](./_spike_delta_india_raw.json)

## Proposed file map (this repo)

| Spec package | TradeManthan path |
|---|---|
| config / profiles / rule sets | `backend/services/tarang/config.py`, `profiles.py` |
| domain (states, candidates, trades) | `backend/services/tarang/domain/` |
| adapters (`IMarketAdapter`, `IBrokerAdapter`, upstox, delta, paper) | `backend/services/tarang/adapters/` |
| services (ChainBuilder, Greeks, IV, Screener, Sizing, Exit, Execution, EventCalendar, Scheduler, Alerts) | `backend/services/tarang/` (modules or `services/` subpackage) |
| schema / ensure tables | `backend/services/tarang/schema.py` + `backend/migrations/add_tarang.sql` |
| API | `backend/routers/tarang.py` → mount `/api/tarang` (+ bare `/tarang`) in `main.py` **(Phase 1+)** |
| UI | `frontend/public/tarang.html`, `tarang.js`, `tarang.css`; nav **"Tarang"** in `left-menu.js` **(Phase 1+)** |
| tests | `backend/test_tarang_*.py` or `backend/tests/tarang/` |
| Phase 0 spikes | `scripts/tarang_phase0_upstox_mcx_greeks.py`, `scripts/tarang_phase0_delta_india_proof.py` |

Reuse (read-only / wrap, do not fork casually): `divtest.instruments.ensure_instrument_master`, `UpstoxService` / rate limiter, `token_manager`, CommDiv MCX mapping learnings, APScheduler holiday skip helpers.

## Phases (plan only — do not implement until approved)

| Phase | Scope |
|---|---|
| **0** | Discovery, spikes, file plan (this doc) |
| **1** | domain/config/DB/adapters read-only / ChainBuilder / Greeks / IV / data-health UI |
| **2** | Screener + Trade Ticket (paper) |
| **3** | State machine, paper broker, In-Trade, ExitEngine, Report |
| **4** | Live brokers admin-gated |
| **5** | AUTO / kill / hard-exit |
| **6** | Backtest |

## Questions before Phase 1 (blocking / shared infra)

See Phase 0 report; must confirm Upstox re-auth, Delta credentials storage, table naming, nav placement, and whether Tarang may share the process-wide Upstox candle rate limiter.
