# ATR(14)% nightly precompute — fix for false zeros

**Status:** live fix shipped  
**Related:** WIPRO / systemic `atr14_pct = 0.0` in `rs_scanner_history` (see `README.md`)

## What changed

| Piece | Detail |
|---|---|
| **Table** | `atr_daily_precomputed` — PK `(as_of_date, symbol)`; history retained; NULL + `computation_failed` on failure (**never** sentinel `0.0`) |
| **Run log** | `atr_daily_precompute_runs` — per-run succeeded/failed + failed symbol list |
| **Schedule** | APScheduler **19:00 IST Mon–Fri** (`atr_daily_precompute_scheduler`), same pattern as Iron Condor snapshot |
| **Universe** | Every `arbitrage_master` row with `currmth_future_instrument_key` |
| **Formula** | Reuses `compute_yesterday_range_metrics` / `wilder_atr_14` (Wilder ATR14 / yesterday close × 100) |
| **Intraday** | `enrich_ranked_with_maturity` reads precompute first; live Upstox fallback logged at INFO; failed live fallback does **not** write `0.0` |

## Monitoring

```bash
# Coverage + last run for a session date
curl -s 'https://www.tradewithcto.com/scan/api/dashboard/relative-strength/atr-precompute/status?date=2026-07-24'

# Latest / today for one symbol
curl -s 'https://www.tradewithcto.com/scan/api/dashboard/relative-strength/atr-precompute/latest?symbol=WIPRO'

# Manual run / backfill (on paperclip or via API)
curl -s -X POST 'https://www.tradewithcto.com/scan/api/dashboard/relative-strength/atr-precompute/run'
curl -s -X POST 'https://www.tradewithcto.com/scan/api/dashboard/relative-strength/atr-precompute/backfill?start_date=2026-07-20&end_date=2026-07-24'
```

SQL:

```sql
SELECT * FROM atr_daily_precompute_runs ORDER BY id DESC LIMIT 5;
SELECT as_of_date, COUNT(*) FILTER (WHERE atr14_pct > 0) AS ok,
       COUNT(*) FILTER (WHERE computation_failed) AS failed
FROM atr_daily_precomputed GROUP BY 1 ORDER BY 1 DESC;
```

## Deploy / backfill results

_Filled after paperclip deploy + backfill._

| Field | Value |
|---|---|
| Commit | _(pending)_ |
| Deploy health | _(pending)_ |
| Backfill window | 2026-07-20 → 2026-07-24 |
| Zero/null before (458 known-bad) | _(pending)_ |
| Patched to valid non-zero | _(pending)_ |
| Remaining failures | _(pending)_ |
| Fallback logging | `atr14_pct live fallback for {sym} as_of=...` |
| Sentinel confirmation | failures → NULL / `computation_failed`; history upsert never forces `0.0` |
