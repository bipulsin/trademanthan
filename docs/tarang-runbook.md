# Kosmic Tarang — Runbook

**Product:** Kosmic Tarang  
**Internal:** `tarang_*` / `/tarang` / `tarang.html`  
**Defaults:** Forward-test book (internal `mode=PAPER`), auto live orders locked. **No live orders.** `TARANG_LIVE_ENABLED` defaults false. Phase 4/5 are **not enabled**.  
**ENERGY contracts:** minis (`CRUDEOILM` / `NATGASMINI`). Full CL/NG stay behind `contractFamily: full`.  
**Weekend window profiles (`BTC_WEEKEND` / `ETH_WEEKEND`):** **disabled by default.**

## Enabling stages (Phase 4–5, built locked)

1. **Shadow** — payloads logged to `tarang_shadow_orders`, `sent=false`. Current production.
2. **Live with manual confirm** — user records fills via “I placed this at my broker”; system still does not place.
3. **Live auto exit** — requires `TARANG_LIVE_ENABLED`, Phase 4 `approved`, arming phrase, same-day expiry. Not enabled.
4. **Live auto entry** — separate arming switch. Not enabled.

Do not add trade-permission credentials to production. Do not raise ENERGY/CRYPTO budgets.

## Forward-test lifecycle

1. Admin → [Kosmic Tarang](https://www.tradewithcto.com/tarang.html) (Forward test badge).
2. **Screener** → Run screener (or 5-minute scheduler). Every **QUALIFIED** row is recorded as a forward-test trade (no click). Same signal key (underlying+expiry+structure+sorted strikes) is not duplicated while open. After close, cooldown **6 hours** (`risk.forward_test.cooldown_hours`).
3. Optional ticket: **Record forward test** (manual) or **I placed this at my broker** (Live, user-entered fills).
4. In-Trade: MTM + ExitEngine on forward-test rows. Live user rows are not auto-flattened by the broker.
5. **Trade Report** tabs: Forward test / Live / All. All never blends metrics.

## Secrets (never commit)

| Variable | Where | Purpose |
|---|---|---|
| `DELTA_INDIA_API_KEY` | local `.env` (gitignored) + paperclip `/home/ubuntu/twcto/.env` | Tarang Delta India auth |
| `DELTA_INDIA_API_SECRET` | same | Tarang Delta India auth |
| `DELTA_INDIA_API_URL` | same (default `https://api.india.delta.exchange`) | India REST host |
| `TARANG_TELEGRAM_CHAT_ID` | same | Critical Tarang alerts (separate chat or topic) |
| `TARANG_TELEGRAM_THREAD_ID` | same, optional | Forum topic id |
| `TELEGRAM_BOT_TOKEN` | same | Shared bot |

These are **Tarang-specific**. Existing algo Delta keys in `backend/routers/algo.py` are untouched (IP-whitelisted separately).

After editing paperclip `.env`, recreate the app container so compose re-injects env (deploy with `REBUILD=1` or `docker compose up -d --force-recreate app`).

## Forward-test lifecycle (how to try on prod)

1. Admin → [Kosmic Tarang](https://www.tradewithcto.com/tarang.html) (Forward test badge).
2. **Screener** → Run screener → QUALIFIED auto-records a forward-test trade.
3. Pick **INTRADAY** or **POSITIONAL** on the ticket, then **Record forward test** (optional manual) → fills simulated (mid ± fraction of bid-ask + venue fee schedule) → **In-Trade**.
4. In-Trade: MTM, trigger progress, hard-exit countdown, alerts. **Exit** or wait for ExitEngine / hard-exit job. Exits are **not** evaluated on stale quotes; MCX 23:30–09:00 IST is skipped and the overnight gap is logged at the next open.
5. **Trade Report** → Forward test / Live / All. All view shows both counts beside every metric.

## Scheduler (IST)

- **Delta full-chain snapshots:** 24×7 every 30 minutes; every **15 minutes** Friday 18:00 → Sunday 17:00 IST. Boot job on process start.
- **MCX full-chain snapshots:** in-session only (09:00–23:30 IST weekdays), skip weekends and `holiday` table dates, every 30 minutes.
- **ExitEngine:** Delta **every 1 minute, 24×7**. MCX every 1 minute in-session; 5-minute overnight job is a no-op while MCX is closed.
- Hard-exit warning/flatten from `risk.default.json` (`mcx_*` 23:10/23:15, `delta_*` 16:55/17:00)

Manual: `POST /api/tarang/exit-engine/run` or **Run full-chain snapshot** on data-health.

## Hard-exit rules

| Venue | Clock | Applies to | Does **not** flatten |
|---|---|---|---|
| Delta India | **17:00 IST** | (a) **INTRADAY** trades, and (b) **any** position **on its expiry day** | **POSITIONAL** trades on non-expiry days |
| MCX | **23:15 IST** | **INTRADAY** only | **POSITIONAL** energy (holds the 23:30–09:00 IST closed period) |

## Upstox token refresh

Tarang MCX chains use the shared Upstox OAuth token (`data/upstox_token.json` / `UPSTOX_ACCESS_TOKEN`).

1. Open the platform Upstox connect / OAuth callback flow used by other desks.
2. Confirm token file on paperclip: `/home/ubuntu/twcto/data/upstox_token.json` (mounted into the app container).
3. Data-health page (`/tarang.html`) shows a banner when token is missing/expired or MCX master looks empty. Critical alert also goes to the Tarang Telegram chat.
4. Re-check: admin → Kosmic Tarang → Refresh, or `GET /api/tarang/health`.

## Full-chain snapshots (start now)

Compressed rows in `tarang_chain_snapshots` (gzip BYTEA, ~10 strikes either side of ATM, CE+PE, bid/ask/mark/IV/delta/OI/volume/underlying/timestamp). Tagged with IST weekday + hour so weekday vs weekend baselines stay separate. Retention: 400 days (`risk.chain_snapshots.retention_days`). ATM IV is still written to `tarang_iv_snapshots` for the percentile gate.

Data-health **Full-chain coverage** shows days of history per underlying and the earliest date a 6-month backtest could start given the 60-day IV warm-up.

## ENERGY / CRYPTO budgets (operator-set — do not auto-raise)

| Bucket | Per-trade | Hard cap | Portfolio |
|---|---:|---:|---:|
| ENERGY | ₹5,000 | ₹10,000 | ₹10,000 |
| CRYPTO | ₹3,000 | ₹10,000 | ₹6,000 |

If a **mini** width cannot fit the ₹10,000 hard cap, report it — **do not raise the budget**. Full lots remain available only by switching `contractFamily` to `full`.

## Telegram (private ops + optional public signals)

Bot cannot DM by username. **Link my chat:**

1. Open Kosmic Tarang → Data health → **Link my chat** (`https://t.me/Tradewithcto?start=tarang`).
2. Press **Start** on the bot.
3. Click **I've pressed Start — poll** (or configure webhook `POST /api/tarang/telegram/webhook`).
4. Numeric `chat_id` is stored as `tarang_settings.telegram_private_chat_id`.

- **Ops/critical** (token expired, stale feed, kill switch, data gaps, heartbeat, recon, job failure) → **private chat only**.
- Forward-test signals and weekly digest → private by default; `@Tradewithcto` only if `telegram_public_signals=true`.
- If `TARANG_TELEGRAM_CHAT_ID` is unset, the stored private chat id is used (never silent-drop of in-app alerts).

## Scheduler (IST) + locks

Each job takes a Postgres advisory lock (`tarang:<job>`). `coalesce=True` + `misfire_grace_time` so a deploy mid-window still catch-up once.

| Job | When | Alert |
|---|---|---|
| Delta snapshots | 15m (30m weekday :00/:30) | data_gap |
| MCX snapshots | in-session :00/:30 | data_gap / token invalid |
| Screener / FT record | every 5m | — |
| Exit engine | 1m Delta 24x7; MCX in-session | stale_feed private |
| Upstox token | **08:45**, escalate **09:00** and **09:05** IST. **Never 03:30 JWT expiry.** | private Telegram + re-auth link |
| MCX Bhavcopy | 00:30 then hourly to 12:00 weekdays. **No Upstox.** | missing 24h → private |
| Delta 1h candles | 00:45 IST | — |
| Expired option archive | 01:15 IST | — |
| tarang_* backup | 02:00 IST, 30-day retention under `TARANG_BACKUP_DIR` | — |
| Weekly digest | Sunday 18:00 IST private | digest |

**Manual recovery:** Pipeline tab → last error; `POST /api/tarang/bhavcopy/download`, `/delta/candles/run`, `/delta/archive/run`, `/backup/run?dry_run=true`. Drag-drop CSV on Backtest if MCX HTTP is blocked.

**Restore:** `pg_restore -d $DATABASE_URL --data-only <dump>` — **never DELETE** `tarang_trades` forward-test rows. Chain snapshot storage: ~gzip BYTEA per 30m slot × underlyings; 400-day retention (`risk.chain_snapshots.retention_days`).

## Real-order verification (DO NOT RUN until the operator says so)

Phase 4/5 stay locked. Checklist **only** (not executed):

1. Delta **testnet** first.
2. Unfillable far-OTM **MCX** limit (must not fill).
3. Minimum-size supervised spread.

Do **not** enable `TARANG_LIVE_ENABLED`. Do **not** place those orders as part of this follow-up.

## Upstox token refresh

Operator renews the token **manually every day before 09:00 IST**. Tarang never alerts at overnight JWT expiry (~03:30). 08:45 IST check; escalate 09:00 and 09:05 if still invalid. MCX snapshots/screener skipped for invalid token write `tarang_data_gaps`. Bhavcopy downloader does not use Upstox.

## MCX Bhavcopy endpoints

- Daily: `POST https://www.mcxindia.com/backpage.aspx/GetDateWiseBhavCopy` (`Date=YYYYMMDD`, `InstrumentName=OPTFUT|FUTCOM`).
- Date Wise checksums: `POST .../GetHistoricalDataDetails` (date range, max 365d, **aggregated volumes**, not per-expiry OHLC).
- **No single expiry date-range bhavcopy.** Admin backfill loops calendar days (`POST /api/tarang/bhavcopy/backfill`).
- If MCX blocks automation: stop; use drag-and-drop upload.

## Health checks

```bash
curl -s https://www.tradewithcto.com/scan/health
# Admin UI
https://www.tradewithcto.com/tarang.html
```

## Changelog

- **Follow-up (pre-Phase 4):** mini ENERGY, operator risk budgets, full-chain snapshots 24×7 (Delta) / in-session (MCX), coverage counter, ExitEngine cadence split, POSITIONAL overnight paper, real fee schedules, Telegram critical alerts.
- **Phase 3:** state machine + `tarang_trade_events`, PaperBroker, In-Trade + Trade Report tabs, ExitEngine + hard-exit scheduler, in-app alerts.
- **Phase 2:** screener gates, Opportunities + ticket.
- **Phase 1:** domain/config/DB/adapters/IV/data-health.
