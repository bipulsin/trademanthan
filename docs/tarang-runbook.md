# Kosmic Tarang — Runbook (Phase 3)

**Product:** Kosmic Tarang  
**Internal:** `tarang_*` / `/tarang` / `tarang.html`  
**Defaults:** PAPER mode, AUTO off. Phase 3 = paper fills + In-Trade + ExitEngine + Trade Report. **No live orders.**

## Secrets (never commit)

| Variable | Where | Purpose |
|---|---|---|
| `DELTA_INDIA_API_KEY` | local `.env` (gitignored) + paperclip `/home/ubuntu/twcto/.env` | Tarang Delta India auth |
| `DELTA_INDIA_API_SECRET` | same | Tarang Delta India auth |
| `DELTA_INDIA_API_URL` | same (default `https://api.india.delta.exchange`) | India REST host |

These are **Tarang-specific**. Existing algo Delta keys in `backend/routers/algo.py` are untouched (IP-whitelisted separately).

After editing paperclip `.env`, recreate the app container so compose re-injects env (deploy with `REBUILD=1` or `docker compose up -d --force-recreate app`).

## Paper lifecycle (how to try on prod)

1. Admin → [Kosmic Tarang](https://www.tradewithcto.com/tarang.html) (PAPER badge).
2. **Screener** → Run screener → open a **QUALIFIED** row → **Trade Ticket**.
3. **Take trade (PAPER)** → fills simulated (mid ± fraction of bid-ask + fee stub) → jumps to **In-Trade**.
4. In-Trade: MTM, trigger progress, hard-exit countdown, alerts. **Exit** or wait for ExitEngine / hard-exit job.
5. **Trade Report** → closed PAPER trades + metrics; LIVE filter stays empty until Phase 4. CSV via button (auth).

## Scheduler (IST)

- IV snapshots: every 30m 09:00–23:30 + 08:05 + boot
- ExitEngine: every minute 09–23, every 5m overnight
- Hard-exit warning/flatten from `risk.default.json` (`mcx_*` 23:10/23:15, `delta_*` 16:55/17:00)

Manual: `POST /api/tarang/exit-engine/run`

## Upstox token refresh

Tarang MCX chains use the shared Upstox OAuth token (`data/upstox_token.json` / `UPSTOX_ACCESS_TOKEN`).

1. Open the platform Upstox connect / OAuth callback flow used by other desks.
2. Confirm token file on paperclip: `/home/ubuntu/twcto/data/upstox_token.json` (mounted into the app container).
3. Data-health page (`/tarang.html`) shows a banner when token is missing/expired or MCX master looks empty.
4. Re-check: admin → Kosmic Tarang → Refresh, or `GET /api/tarang/health`.

## IV snapshots

APScheduler (Asia/Kolkata): every 30 minutes 09:00–23:30, plus 08:05, plus one boot job.  
Manual: **Run IV snapshot** on the data-health page or `POST /api/tarang/iv-snapshots/run` (admin JWT).

Table: `tarang_iv_snapshots`. Events: `tarang_trade_events` (append-only).

## Delta auth verify (read-only)

From paperclip (keys already in `.env`): balances / margined positions only — never place orders from ops scripts.  
If `ip_not_whitelisted_for_api_key`, whitelist egress **140.245.14.17**.

## ENERGY budget (defaults)

See `backend/services/tarang/config_data/risk.default.json`:

- ENERGY per-trade **₹40,000**, hard cap **₹50,000**, portfolio **₹80,000** (2×)
- CRYPTO per-trade **₹3,000**, hard cap **₹10,000**, portfolio **₹6,000** (2×)
- `contractFamily: full` — minis not enabled

## Upstox WS share

Phase 1–3 uses REST Option Greek (≤50 keys) via Tarang rate budget. Shared feed chunks instruments in batches of **100**. Tarang may later register `set_feed_provider_keys('tarang', keys)` capped at 50 — CommDiv behaviour unchanged.

## Health checks

```bash
curl -s https://www.tradewithcto.com/scan/health
# Admin UI
https://www.tradewithcto.com/tarang.html
```

## Changelog

- **Phase 3:** state machine + `tarang_trade_events`, PaperBroker, In-Trade + Trade Report tabs, ExitEngine + hard-exit scheduler, in-app alerts.
- **Phase 2:** screener gates, Opportunities + PAPER ticket.
- **Phase 1:** domain/config/DB/adapters/IV/data-health.
