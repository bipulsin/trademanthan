# Pre-market scanner test harness

This repo includes a **standalone** script to replay the pre-market scoring pipeline against **Upstox historical candles**, as if the scanner ran at a fixed IST time (default **09:10**) on a chosen **session date**.

## Prerequisites

- Python environment with project dependencies (`pydantic-settings`, `sqlalchemy`, `requests`, …).
- `DATABASE_URL` reachable if you load symbols from `arbitrage_master` (production DB or local).
- Valid Upstox app credentials in `.env` / environment: `UPSTOX_API_KEY`, `UPSTOX_API_SECRET` (same as the backend).

## Configuration (optional)

In `backend/config.py` (or environment variables):

| Variable | Purpose |
|----------|---------|
| `TEST_SIMULATION_DATE` | Default session date `YYYY-MM-DD` (e.g. `2026-04-13`) |
| `TEST_SIMULATION_TIME` | Label only, default `09:10:00` |
| `TEST_SYMBOL_COUNT` | Max symbols from `arbitrage_master` (default `200`) |
| `TEST_MODE` | Reserved for future use (currently informational) |

## Commands

From the **repository root**:

```bash
export PYTHONPATH=.
```

### 1) Demo: historical daily candles for one symbol

Uses the same endpoint as production:  
`GET .../historical-candle/{instrument_key}/days/1/{to_date}/{from_date}`  
with `range_end_date` anchored to the last completed session before your simulation date.

```bash
python test_premkt_scanner.py --demo-one RELIANCE --date 2026-04-13
```

### 2) Full run (Top N from DB, live Upstox)

```bash
python test_premkt_scanner.py --date 2026-04-13 --limit 50 --top 10
```

Optional same-day sanity check (heuristic):

```bash
python test_premkt_scanner.py --date 2026-04-10 --limit 40 --validate
```

### 3) Offline / no API — `sample_data.json`

```bash
python test_premkt_scanner.py --sample
```

## Scoring model (harness)

Components are **min–max normalized across the tested universe**, then combined with:

| Component | Weight |
|-----------|--------|
| OBV slope (10-day, same helper as Smart Futures) | 30% |
| Gap strength `abs(gap%)` (session open vs prev close) | 25% |
| Range position vs ~52w band from daily history | 25% |
| Momentum: `ema_slope_norm_m5` on **prior session** 5-minute closes | 20% |

Production `premarket_watchlist_job` still uses its own composite (OBV + gap + 20d range); this harness aligns with the **weighted** spec used in your scanner design doc. Compare results as a **regression signal**, not a byte-for-byte match to the job.

## Important caveats

1. **Completed sessions only**: Gap and range need a **fully settled daily bar** for `simulation_date`. You cannot replay a future calendar day; use the **last available trading day** Upstox returns (or `--date` a past Monday with data).
2. **Holidays**: Weekends are skipped for “previous session”; NSE holidays are not modeled—if the API omits a bar, the symbol is skipped.
3. **52-week band**: Implemented as high/low over the long daily window returned (Upstox span), not from a separate instruments “52w” field.
4. **Momentum at 09:10**: Intraday bars for the simulation day do not exist yet; momentum uses the **previous session’s** 5-minute series (matches “pre-open” intent).

## Example output (illustrative)

```
PRE-MARKET SCANNER TEST — 2026-04-10 09:10:00 IST
==========================================================================================
Rank | Symbol       |   Score |   OBV Sl |    Gap% |  RngPos |     Mom
------------------------------------------------------------------------------------------
1    | SBIN         |  0.4410 |  +0.1100 |   +0.55 |  0.8200 |  +0.2500
2    | RELIANCE     |  0.4120 |  +0.1200 |   +0.35 |  0.7800 |  +0.2200
...
------------------------------------------------------------------------------------------
Average score (Top 10): 0.3500
Positive gaps: 6/10
Range position >= 0.85 (near 52w high proxy): 2/10
```

(Run `--sample` to see a table without calling Upstox.)

## Files

| File | Role |
|------|------|
| `test_premkt_scanner.py` | CLI + `PremktTester` |
| `sample_data.json` | Cached metrics for `--sample` |
| `README_TESTING.md` | This document |
