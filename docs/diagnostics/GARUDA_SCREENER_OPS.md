# Garuda screener — production ops note

**Status:** shadow + UI only. **No live gating.** Independent of Kavach Grade/Votes / `trade_take_enabled`.

**Module:** `backend/services/garuda_screener/`  
**Job:** `garuda_screener_10m` in `smart_future_algo` (Mon–Fri `:06/:16/:26/:36/:46/:56` IST)  
**API:** `GET /api/dashboard/garuda/latest`  
**UI:** `dailyRSchecklist.html` — Garuda section with permanent non-dismissible testing banner  
**Table:** `garuda_screener_log` (Option A: one row per symbol per `bar_end`)

## Load impact (per 10m bar)

| Step | Approx cost |
|---|---|
| Universe | ~200 FO symbols from `arbitrage_master.currmth_future_instrument_key` (excl. EXIDEIND / NUVAMA / SAMMAANCAP) |
| Candles | **Cache-only** via shared `candle_cache` (`candles_cache_only`) — same source as universe VWAP scan / Kavach. **No Upstox storm.** |
| Compute | Aggregate 5m→10m + EMA5/10 + VWAP + ADX per symbol; evaluate Part1/Part2; rank Top-6 |
| Writes | ~evaluated rows upserted (typically ≤200) with `ON CONFLICT` |

**Competition with other jobs:** Runs +1m after 10m close, overlapping the `:06/:16…` slot of the 5m VWAP research scan cadence family. CPU is local indicator math only; should not contend with nightly ATR precompute (19:00) or RSS Upstox pulls. If cache miss rate is high, log `cache_miss` and skip those symbols for that bar (no backfill).

**First log:** next market open from 09:25 IST bar onward (job starts 09:26). No historical backfill.

## trade_log confluence

Columns (shadow, never gate):

- `garuda_confluence` — `MATCH` / `NO_MATCH` / `NOT_AVAILABLE`
- `garuda_rank` — nullable Top-6 rank at lookup bar
- `garuda_direction` — Garuda side at that bar (even on mismatch)

Populated at:

1. `rule27_trade_log.upsert_trade` / `row_params` auto-lookup when not supplied  
2. `kavach_open_trades.take_trade` → upserts `trade_log` with `source='kavach_checklist'` + confluence

Lookup: nearest `garuda_screener_log.bar_end ≤ entry_time` same session; MATCH iff symbol in Top-6 with same direction.

## Sample SQL — WR / avg R by confluence

```sql
SELECT
  garuda_confluence,
  COUNT(*) AS n,
  ROUND(100.0 * AVG(CASE WHEN r_realized > 0 THEN 1 ELSE 0 END), 1) AS win_rate_pct,
  ROUND(AVG(r_realized)::numeric, 3) AS avg_r,
  ROUND(SUM(r_realized)::numeric, 2) AS total_r
FROM trade_log
WHERE session_date >= CURRENT_DATE - INTERVAL '30 days'
  AND r_realized IS NOT NULL
  AND garuda_confluence IS NOT NULL
GROUP BY garuda_confluence
ORDER BY garuda_confluence;
```

MATCH vs NO_MATCH only:

```sql
SELECT
  CASE WHEN garuda_confluence = 'MATCH' THEN 'TRUE' ELSE 'FALSE' END AS garuda_match,
  COUNT(*) AS n,
  ROUND(100.0 * AVG(CASE WHEN r_realized > 0 THEN 1 ELSE 0 END), 1) AS win_rate_pct,
  ROUND(AVG(r_realized)::numeric, 3) AS avg_r
FROM trade_log
WHERE session_date >= CURRENT_DATE - INTERVAL '30 days'
  AND r_realized IS NOT NULL
  AND garuda_confluence IN ('MATCH', 'NO_MATCH')
GROUP BY 1;
```
