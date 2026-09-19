# Kosmic Tarang — Architecture

**Status:** Phase 3 complete (state machine, PaperBroker, In-Trade, ExitEngine, alerts, Trade Report). Phase 4 (live brokers) not started.  
**Product (user-facing):** **Kosmic Tarang** — IV-gated defined-risk options spreads (iron condor or single credit spread) for MCX Crude Oil / Natural Gas (Upstox) and BTC / ETH (Delta Exchange India).  
**Internal code / routes / tables:** short prefix `tarang_` / `/tarang` (e.g. `tarang_iv_snapshots`).

> Older docs may say “Krypto Tarang” / “Krypto-Tarang”; treat those as **Kosmic Tarang**.

---

## Discovery summary (repo)

1. **Stack:** FastAPI (`backend/main.py`) + vanilla HTML/JS in `frontend/public/` + SQLAlchemy/Postgres (`backend/database.py`, `backend/models/`). No React SPA for strategy desks.
2. **Strategy module pattern:** package under `backend/services/<feature>/` (schema, scheduler, actions) + `backend/routers/<feature>.py` mounted in `main.py` (often dual `/api/...` and bare prefixes) + `frontend/public/<page>.html` (+ `.js`/`.css`) + nav entry in `frontend/public/left-menu.js`.
3. **Closest analogues:** Commodities Div (`backend/services/commodities_div/`, MCX via Upstox master), Stock Options (`stock_option_*`), Iron Condor advisory (`iron_condor_*` + `upstox_iron_condor.py`) — Tarang follows CommDiv/Iron Condor desk shape.
4. **Auth:** JWT Bearer; UI uses same token as other desks. Admin-gated nav via `nav-item-admin` + left-menu visibility API. **Nav label: “Kosmic Tarang”** next to Iron Condor / CommDiv; **admin-only through Phase 3**.
5. **DB:** Postgres; `backend/migrations/add_tarang.sql` + `ensure_tarang_tables()`. All tables `tarang_*` including **`tarang_iv_snapshots`**, **`tarang_trade_events`**, **`tarang_fills`**.
6. **Jobs:** APScheduler `BackgroundScheduler(timezone="Asia/Kolkata")` — IV snapshots every 30m 09:00–23:30 IST + 08:05 + boot; ExitEngine every minute in session; hard-exit warn/flat from config.
7. **Upstox:** `UpstoxService` + token manager; MCX chain from master + Option Greek. Tarang REST rate budget (`adapters/rate_budget.py`); WS share planned via `set_feed_provider_keys('tarang', …)` capped (max 50), lower priority than CommDiv — CommDiv unchanged in Phase 1.
8. **Delta:** Tarang-specific env `DELTA_INDIA_API_*` (not algo.py hard-coded keys). Public India REST + authenticated read-only when IP-whitelisted. **PaperBroker (Phase 3)** simulates fills; client order ids `tarang-`. Testnet / LIVE Phase 4 only.
9. **Secrets:** root `.env` via `backend/env_bootstrap.py`; paperclip `/home/ubuntu/twcto/.env`. Never ship secrets to the browser.
10. **Defaults:** PAPER mode, AUTO off; ENERGY budget raised for full CL/NG lots (see risk.default.json).

## Phase 1 file map

| Area | Path |
|---|---|
| Config | `backend/services/tarang/config.py`, `config_data/{profiles,risk,events}.default.json` |
| Domain | `backend/services/tarang/domain/` |
| Adapters | `backend/services/tarang/adapters/` (upstox_mcx, delta_india, rate_budget) |
| Chain / Greeks / IV | `chain_builder.py`, `greeks.py`, `greeks_service.py`, `iv_normalize.py`, `iv_snapshots.py` |
| Screener / ticket | `gates.py`, `structures.py`, `screener.py`, `ticket.py` |
| Lifecycle / paper | `state_machine.py`, `paper_broker.py`, `exit_engine.py`, `lifecycle.py`, `events.py`, `report.py` |
| Schema | `schema.py` + `backend/migrations/add_tarang.sql` (`tarang_rejections`, candidates, trades, `tarang_trade_events`, fills) |
| API | `backend/routers/tarang.py` → `/api/tarang` + `/tarang` |
| UI | `frontend/public/tarang.{html,js,css}` — Screener / Ticket / In-Trade / Trade Report / Data health |
| Scheduler | `backend/services/tarang/scheduler.py` — IV + ExitEngine + hard-exit warn/flat IST |
| Tests | `backend/test_tarang_greeks.py`, `backend/test_tarang_phase2.py`, `backend/test_tarang_phase3.py` |
| Docs | `docs/tarang-architecture.md`, `tarang-runbook.md`, `tarang-backtest-data.md`, `tarang-sizing-min-max-loss.md` |

## ENERGY budget (Phase 1)

| Setting | Value | Rationale |
|---|---:|---|
| Per-trade | ₹40,000 | ≥1 full lot at CL/NG profile-min (₹18.75k–25k) and CL profile-max zero-credit (₹40k) |
| Hard cap | ₹50,000 | Headroom |
| Portfolio | ₹80,000 | 2 × per-trade |
| Minis | Off | `contractFamily: full` |

CRYPTO remains ₹3k / ₹10k hard / ₹6k portfolio (2×).

## Spike / follow-up results

- Upstox MCX + Delta: [`docs/upstox-mcx-findings.md`](./upstox-mcx-findings.md)
- Min max-loss: [`docs/tarang-sizing-min-max-loss.md`](./tarang-sizing-min-max-loss.md)
- Historical data: [`docs/tarang-backtest-data.md`](./tarang-backtest-data.md)
- Runbook: [`docs/tarang-runbook.md`](./tarang-runbook.md)

## Phases

| Phase | Scope |
|---|---|
| **0** | Discovery, spikes (done) |
| **1** | domain/config/DB/adapters read-only / ChainBuilder / Greeks / IV / data-health UI (**done**) |
| **2** | Screener + Trade Ticket (paper) (**done**) |
| **3** | State machine, paper broker, In-Trade, ExitEngine, Report (**this**) |
| **4** | Live brokers admin-gated |
| **5** | AUTO / kill / hard-exit |
| **6** | Backtest |

## Assumptions

- Do not modify Kavach / CommDiv / Stock Options / Iron Condor behaviour without need; shared Upstox access OK with Tarang lower priority.
- Contract multipliers / lot sizes from Upstox master or Delta `contract_value` at runtime.
- Hard exits in config (MCX ~23:15 IST, Delta ~17:00 IST), not code literals preferred.
