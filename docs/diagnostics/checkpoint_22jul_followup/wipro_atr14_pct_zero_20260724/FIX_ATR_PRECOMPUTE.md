# ATR(14)% nightly precompute — fix for false zeros

**Status:** live fix shipped + backfill verified on paperclip  
**Related:** WIPRO / systemic `atr14_pct = 0.0` in `rs_scanner_history` (see `README.md`)  
**Commit:** `149c733b7b42bf6b9aefb1ce1c06453a92136cea`

## What changed

| Piece | Detail |
|---|---|
| **Table** | `atr_daily_precomputed` — PK `(as_of_date, symbol)`; history retained; NULL + `computation_failed` on failure (**never** sentinel `0.0`) |
| **Run log** | `atr_daily_precompute_runs` — per-run succeeded/failed + failed symbol list |
| **Schedule** | APScheduler **19:00 IST Mon–Fri** (`atr_daily_precompute_scheduler`), same pattern as Iron Condor snapshot |
| **Universe** | Every `arbitrage_master` row with non-null `currmth_future_instrument_key` |
| **Formula** | Reuses `compute_yesterday_range_metrics` / `wilder_atr_14` (Wilder ATR14 / yesterday close × 100) |
| **Intraday** | `enrich_ranked_with_maturity` reads precompute first; live Upstox fallback logged at INFO (`atr14_pct live fallback for {sym} as_of=...`); failed live fallback does **not** write `0.0` |

## Monitoring

```bash
# Coverage + last run for a session date
curl -s 'https://www.tradewithcto.com/scan/api/dashboard/relative-strength/atr-precompute/status?date=2026-07-24'

# Latest / today for one symbol
curl -s 'https://www.tradewithcto.com/scan/api/dashboard/relative-strength/atr-precompute/latest?symbol=WIPRO'

# Manual run / backfill
curl -s -X POST 'https://www.tradewithcto.com/scan/api/dashboard/relative-strength/atr-precompute/run'
curl -s -X POST 'https://www.tradewithcto.com/scan/api/dashboard/relative-strength/atr-precompute/backfill?start_date=2026-07-20&end_date=2026-07-24'
```

Or on paperclip:

```bash
docker compose exec -T app python scripts/run_atr_daily_precompute.py --status 2026-07-24
docker compose exec -T app python scripts/run_atr_daily_precompute.py --backfill 2026-07-20 2026-07-24
```

SQL:

```sql
SELECT * FROM atr_daily_precompute_runs ORDER BY id DESC LIMIT 5;
SELECT as_of_date,
       COUNT(*) FILTER (WHERE atr14_pct > 0) AS ok,
       COUNT(*) FILTER (WHERE computation_failed) AS failed
FROM atr_daily_precomputed GROUP BY 1 ORDER BY 1 DESC;
```

## Deploy / backfill results (paperclip 2026-07-25)

| Field | Value |
|---|---|
| Commit | `149c733` |
| Deploy | `REBUILD=1 ./scripts/trigger-paperclip-deploy.sh` — healthy |
| Health | `https://www.tradewithcto.com/scan/health` → healthy |
| Scheduler | Started: `ATR daily precompute scheduler: STARTED (19:00 IST weekdays)` |
| Backfill window | 2026-07-20 → 2026-07-24 |
| Precompute | 200/200 succeeded each day (0 failed) |
| Zero/null before (known-bad) | **458** |
| Patched to valid non-zero | **457** |
| Remaining failures | **1** — `EXIDEIND` @ 2026-07-20 (`currmth_future_instrument_key` is NULL in `arbitrage_master`, excluded from universe) |
| WIPRO `atr14_pct` after | 07-20 **2.2135**, 07-21 **2.1152**, 07-23 **1.9552**, 07-24 **1.9317** (was 0.0) |
| Fallback logging | `atr14_pct live fallback for {sym} as_of=...` (INFO) when precompute miss |
| Sentinel confirmation | failures → NULL + `computation_failed`; history upsert never forces `0.0`; preserves prior positive ATR on conflict |

### Coverage status sample (2026-07-24)

```json
{"ok": true, "as_of_date": "2026-07-24", "arbitrage_master_currmth_n": 200,
 "fresh_atr_n": 200, "failed_atr_n": 0, "coverage_pct": 100.0, "job_ran": true}
```
