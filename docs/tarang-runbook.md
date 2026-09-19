# Kosmic Tarang — Runbook (Phase 1)

**Product:** Kosmic Tarang  
**Internal:** `tarang_*` / `/tarang` / `tarang.html`  
**Defaults:** PAPER mode, AUTO off. Screener / Take trade = Phase 2.

## Secrets (never commit)

| Variable | Where | Purpose |
|---|---|---|
| `DELTA_INDIA_API_KEY` | local `.env` (gitignored) + paperclip `/home/ubuntu/twcto/.env` | Tarang Delta India auth |
| `DELTA_INDIA_API_SECRET` | same | Tarang Delta India auth |
| `DELTA_INDIA_API_URL` | same (default `https://api.india.delta.exchange`) | India REST host |

These are **Tarang-specific**. Existing algo Delta keys in `backend/routers/algo.py` are untouched (IP-whitelisted separately).

After editing paperclip `.env`, recreate the app container so compose re-injects env (deploy with `REBUILD=1` or `docker compose up -d --force-recreate app`).

## Upstox token refresh

Tarang MCX chains use the shared Upstox OAuth token (`data/upstox_token.json` / `UPSTOX_ACCESS_TOKEN`).

1. Open the platform Upstox connect / OAuth callback flow used by other desks.
2. Confirm token file on paperclip: `/home/ubuntu/twcto/data/upstox_token.json` (mounted into the app container).
3. Data-health page (`/tarang.html`) shows a banner when token is missing/expired or MCX master looks empty.
4. Re-check: admin → Kosmic Tarang → Refresh, or `GET /api/tarang/health`.

## IV snapshots

APScheduler (Asia/Kolkata): every 30 minutes 09:00–23:30, plus 08:05, plus one boot job.  
Manual: **Run IV snapshot** on the data-health page or `POST /api/tarang/iv-snapshots/run` (admin JWT).

Table: `tarang_iv_snapshots`.

## Delta auth verify (read-only)

From paperclip (keys already in `.env`):

```bash
# balances + margined positions only — never place orders from ops scripts
```

If `ip_not_whitelisted_for_api_key`, whitelist egress **140.245.14.17**. Local workstation may still fail IP checks; production verify on paperclip.

## ENERGY budget (Phase 1 defaults)

See `backend/services/tarang/config_data/risk.default.json`:

- ENERGY per-trade **₹40,000**, hard cap **₹50,000**, portfolio **₹80,000** (2×)
- CRYPTO per-trade **₹3,000**, hard cap **₹10,000**, portfolio **₹6,000** (2×)
- `contractFamily: full` — minis not enabled

## Upstox WS share

Phase 1 uses REST Option Greek (≤50 keys) via Tarang rate budget. Shared feed chunks instruments in batches of **100** (`upstox_market_feed._CHUNK`). Tarang may later register `set_feed_provider_keys('tarang', keys)` with **max 50** screener-needed strikes, lower priority than CommDiv — CommDiv behaviour unchanged.

## Health checks

```bash
curl -s https://www.tradewithcto.com/scan/health
# Admin UI
https://www.tradewithcto.com/tarang.html
```
