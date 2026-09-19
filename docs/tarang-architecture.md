# Kosmic Tarang — Architecture (Phase 0)

**Status:** Phase 0 discovery + follow-up spikes complete. No runtime engine, tables, or UI yet.  
**Product (user-facing):** **Kosmic Tarang** — IV-gated defined-risk options spreads (iron condor or single credit spread) for MCX Crude Oil / Natural Gas (Upstox) and BTC / ETH (Delta Exchange India).  
**Internal code / routes / tables:** short prefix `tarang_` / `/tarang` (e.g. `tarang_iv_snapshots`).

> Older docs may say “Krypto Tarang” / “Krypto-Tarang”; treat those as **Kosmic Tarang**.

---

## Discovery summary (repo)

1. **Stack:** FastAPI (`backend/main.py`) + vanilla HTML/JS in `frontend/public/` + SQLAlchemy/Postgres (`backend/database.py`, `backend/models/`). No React SPA for strategy desks.
2. **Strategy module pattern:** package under `backend/services/<feature>/` (schema, scheduler, actions) + `backend/routers/<feature>.py` mounted in `main.py` (often dual `/api/...` and bare prefixes) + `frontend/public/<page>.html` (+ `.js`/`.css`) + nav entry in `frontend/public/left-menu.js`.
3. **Closest analogues:** Commodities Div (`backend/services/commodities_div/`, MCX via Upstox master), Stock Options (`stock_option_*`), Iron Condor advisory (`iron_condor_*` + `upstox_iron_condor.py`) — Tarang should follow CommDiv/Iron Condor desk shape, not invent a new framework.
4. **Auth:** JWT Bearer via `backend/routers/auth.py` / `get_user_from_token`; UI uses same token as other desks. Admin-gated nav via `nav-item-admin` + left-menu visibility API. **Nav label: “Kosmic Tarang”** next to Iron Condor / CommDiv; **admin-only through Phase 2**.
5. **DB:** Postgres production; migrations as SQL under `backend/migrations/` with `ensure_*_tables()` idempotent runners (see CommDiv). All Tarang tables `tarang_*` via `ensure_*` (IV snapshots → **`tarang_iv_snapshots`**).
6. **Jobs:** APScheduler `BackgroundScheduler(timezone="Asia/Kolkata")` started/stopped from FastAPI lifespan in `main.py` (pattern: stock_option, commodities_div, iron_condor).
7. **Upstox:** `UpstoxService` + `token_manager` (`data/upstox_token.json` / `UPSTOX_ACCESS_TOKEN`); instrument master via `backend/services/divtest/instruments.py` (`complete.json.gz`). Option **chain** API is **not** available for MCX — build chain from master + Option Greek `/v3/market-quote/option-greek` (≤50 keys). Filter full energy via `underlying_symbol` ∈ {`CRUDEOIL`,`NATURALGAS`} on `MCX_FO` (`contractFamily` default `full`).
8. **Delta:** Existing `backend/delta_api.py` + algo paths; Tarang market data should use public India REST `https://api.india.delta.exchange/v2` (testnet `https://cdn-ind.testnet.deltaex.org`, Phase 4 only). PaperBroker for paper. Client order ids prefix `tarang-`. Do not hard-code keys/lot/expiry.
9. **Secrets:** root `.env` via `backend/env_bootstrap.py` + `backend/config.py` Settings; never ship secrets to the browser.
10. **Defaults for Tarang:** PAPER mode, AUTO off until Phase 5; no VWAP/SuperTrend/EMA/ATR filters — options mechanics / IV gates only.

## Assumptions

- Tarang is a **new** desk package; we will not modify Kavach / CommDiv / Stock Options / Iron Condor behaviour without explicit approval.
- **Approved for Phase 1 planning:** share process-wide Upstox rate limiter / WS with CommDiv; Tarang lower priority + capped share — confirm WS subscription limits before dual subscribe. Prefer ask before any CommDiv refactor.
- UI is static HTML under `frontend/public/tarang.html` matching existing dark theme + `left-menu.js`.
- Contract multipliers / lot sizes always come from Upstox master or Delta `contract_value` / product fields at runtime.
- Profiles: CL, NG, BTC, ETH (+ experimental BTC_WEEKEND / ETH_WEEKEND disabled by default).
- Hard exits: MCX ~23:15 IST, Delta ~17:00 IST (config), Delta expiry ~17:30 IST — stored in settings, not code literals preferred.

## Spike / follow-up results

- Upstox MCX + Delta: [`docs/upstox-mcx-findings.md`](./upstox-mcx-findings.md)  
  - Verdict: **`PARTIAL_RESPONSE_BUT_NULLS`** → Black-76 **required as fill** in Phase 1; near-term ATM natives are usable when futures F matches.
  - Delta creds: **`FAIL_IP_NOT_WHITELISTED`** (local + paperclip) — blocking for live Delta auth.
- Min max-loss table: [`docs/tarang-sizing-min-max-loss.md`](./tarang-sizing-min-max-loss.md) — full CL/NG profile widths exceed ₹3k–10k per lot; crypto contracts fit.
- Raw: [`docs/_spike_upstox_mcx_raw.json`](./_spike_upstox_mcx_raw.json), [`docs/_spike_delta_creds_raw.json`](./_spike_delta_creds_raw.json), [`docs/_spike_delta_sizing_raw.json`](./_spike_delta_sizing_raw.json)

## Proposed file map (this repo)

| Spec package | TradeManthan path |
|---|---|
| config / profiles / rule sets | `backend/services/tarang/config.py`, `profiles.py` |
| domain (states, candidates, trades) | `backend/services/tarang/domain/` |
| adapters (`IMarketAdapter`, `IBrokerAdapter`, upstox, delta, paper) | `backend/services/tarang/adapters/` |
| services (ChainBuilder, Greeks, IV, Screener, Sizing, Exit, Execution, EventCalendar, Scheduler, Alerts) | `backend/services/tarang/` (modules or `services/` subpackage) |
| schema / ensure tables | `backend/services/tarang/schema.py` + `backend/migrations/add_tarang.sql` |
| API | `backend/routers/tarang.py` → mount `/api/tarang` (+ bare `/tarang`) in `main.py` **(Phase 1+)** |
| UI | `frontend/public/tarang.html`, `tarang.js`, `tarang.css`; nav **“Kosmic Tarang”** in `left-menu.js` **(Phase 1+)** |
| tests | `backend/test_tarang_*.py` or `backend/tests/tarang/` |
| Phase 0 spikes | `scripts/tarang_phase0_upstox_mcx_greeks.py`, `tarang_phase0_delta_india_proof.py`, `tarang_phase0_delta_creds_check.py`, `tarang_phase0_delta_sizing.py` |

Reuse (read-only / wrap, do not fork casually): `divtest.instruments.ensure_instrument_master`, `UpstoxService` / rate limiter, `token_manager`, CommDiv MCX mapping learnings, APScheduler holiday skip helpers.

## Phases (plan only — do not implement until approved)

| Phase | Scope |
|---|---|
| **0** | Discovery, spikes, file plan, follow-up Greeks/creds/sizing (this doc) |
| **1** | domain/config/DB/adapters read-only / ChainBuilder / Greeks / IV / data-health UI |
| **2** | Screener + Trade Ticket (paper) |
| **3** | State machine, paper broker, In-Trade, ExitEngine, Report |
| **4** | Live brokers admin-gated |
| **5** | AUTO / kill / hard-exit |
| **6** | Backtest |

## Blocking before Phase 1 coding

1. **Delta API keys:** whitelist paperclip `140.245.14.17` and/or provide new India keys with read balances/positions.
2. **MCX risk budget:** raise ₹3k–10k, allow 1-step widths, and/or plan mini contracts later — full profile widths do not fit one lot.
3. Confirm CommDiv share of Upstox WS limits (planning only; no CommDiv code change without ask).
