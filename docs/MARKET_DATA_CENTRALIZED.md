# Centralized Market Data (arbitrage_master)

## Overview

A single backend pipeline refreshes LTP, 5m VWAP, and EMA(5) for all `arbitrage_master` instruments (spot, current-month FUT, next-month FUT). Vajra Futures, Smart Futures, Premium Futures (Daily Futures), and Arbitrage **read shared fields from the database** instead of issuing duplicate Upstox quote batches.

**Unchanged:** signal/scoring logic, execution, UI workflows, and per-algo historical candle fetches for specialized indicators.

## Components

| Module | Role |
|--------|------|
| `backend/services/market_data/engine.py` | Batch LTP + parallel 5m candles → VWAP/EMA → DB write |
| `backend/services/market_data/reads.py` | Algo-facing LTP reads with optional broker fallback |
| `backend/services/market_data/scheduler.py` | 5m job (Smart Future Algo, 9:15–15:35 IST) |
| `backend/services/arbitrage_daily_setup_scheduler.py` | 30m LTP refresh (candles off); morning setup uses full refresh |

## Database columns (`arbitrage_master`)

- Spot: `stock_vwap`, `stock_ema5`, `stock_last_updated` (plus existing `stock_ltp`)
- Current FUT: `currmth_future_vwap`, `currmth_future_ema5`, `currmth_future_last_updated`, optional `currmth_candle_*_5m`
- Next FUT: `nextmth_future_vwap`, `nextmth_future_ema5`, `nextmth_future_last_updated`
- Meta: `market_data_source`, `market_data_refresh_status`, `market_data_refresh_error`, `market_data_last_updated`

## Schedulers

- **Every 5 min** (weekdays, 9:15–15:35 IST): full refresh via `centralized_market_data_5m`
- **Every 30 min** (arbitrage scheduler): LTP-only refresh (`fetch_candles=False`)
- **WebSocket**: `ensure_market_feed_running` on universe keys for live LTP overlay

## API

- `GET /api/market-data/health` — stale counts, last refresh, WS status
- `POST /api/market-data/refresh` — on-demand refresh (authenticated)

## Algo integration

- Vajra `ltp_enrich.py` → `ltp_map_with_fallback`
- Arbitrage selection → `ltp_map_with_fallback`
- Smart Futures gate LTP → `get_ltp_for_instrument_key` with broker fallback
- Daily Futures relative strength → DB LTP first, broker if sparse

## Failsafe

If DB data is stale or missing, `ltp_map_with_fallback(..., allow_broker_fallback=True)` calls Upstox batch quotes so behavior matches pre-refactor when the central job has not run yet.
