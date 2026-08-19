# HA Momentum 15-minute backtest

Temporary research backtest for current-month NSE futures in `arbitrage_master`.
It is **not** used for live trading.

## What it does

1. Fetches 15-minute OHLCV from Upstox V2 historical candles (cached under `data/candles/`).
2. Computes EMA 5/15/50, MACD histogram (24, 52, 18), ADX(14).
3. Enters on EMA 5/15 cross with trend + MACD + rising ADX>20.
4. Skips trades whose stop would exceed ₹5,000 (1 lot).
5. Writes `ha_backtest_trades` / `ha_skipped_trades` (PostgreSQL) and the public page `frontend/public/hamoment.html`.

Date range: **17 Jul 2026 – 19 Aug 2026**. Page is public (no `auth-check.js`).

The live DB is PostgreSQL (`DATABASE_URL`), not MySQL. Lot size comes from `nse_instruments.json` via `currmth_future_instrument_key` (`arbitrage_master` has no `lot_size` column).

## Run locally

From the repo root, with `.env` containing Upstox token + `DATABASE_URL`:

```bash
chmod +x backtest/run_all.sh
./backtest/run_all.sh
```

Or step by step:

```bash
PYTHONPATH=. python3 backtest/fetch_candles.py
PYTHONPATH=. python3 backtest/run_backtest.py
PYTHONPATH=. python3 backtest/generate_report.py
```

Re-runs skip symbols that already have a non-empty `data/candles/<SYMBOL>_15min.json`.

## Production

After deploy, inside the app container:

```bash
python3 backtest/run_all.py
```

Then copy `frontend/public/hamoment.html` into the nginx image on the next frontend deploy, or re-run `generate_report.py` and commit the HTML.

Public URL: https://www.tradewithcto.com/hamoment.html

## v2 multi-variant run

v2 keeps the same candle cache (`data/candles/<SYMBOL>_15min.json`) and adds Nifty 15m + eight strategy variants (plus `v6b` at 0.3% fixed SL).

```bash
PYTHONPATH=. python3 backtest/run_all_v2.py
```

Steps:

```bash
PYTHONPATH=. python3 backtest/fetch_nifty_candles.py
PYTHONPATH=. python3 backtest/run_backtest_v2.py
PYTHONPATH=. python3 backtest/generate_report_v2.py
```

Nifty cache: `data/candles/NIFTY50_15min.json` (`NSE_INDEX|Nifty 50`). Extra deps: `backtest/requirements_v2.txt` (same stack as v1). The report overwrites `frontend/public/hamoment.html`.
