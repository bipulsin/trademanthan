# Rocket / Crash live scoring (Upstox websocket)

## What changed

Existing `compute_rocket_score` is unchanged for REST/candle callers that omit
`session_bar_count` (still requires 20 completed bars, OHLCV volume proxy).

Live path: ticks from the **existing** Upstox v3 protobuf websocket
(`backend/services/upstox_market_feed.py`) are aggregated into 5/10/15-minute
candles with signed volume. Default operational TF remains **10m**.

Universe: current-month futures in `arbitrage_master.currmth_future_instrument_key`
only. Dhan is not used.

## Delta

Prefer aggressive buy/sell when best bid/ask is present (trade at/through ask =
buy, at/through bid = sell). Else tick-rule (uptick +, downtick −, unchanged
inherits prior direction). Session cumulative delta resets at 09:15 IST.

This live delta is **not** required to match Pine.

## Session phases (live `session_bar_count` only)

1. Bars 1–3: S3 disabled, max score 3
2. Bars 4–19: lookback = session bar count
3. Bars 20+: lookback 20

## Persistence

- **Rolling:** `rocket_live_state` upsert by `(symbol, timeframe)` — powers
  READY NOW overlay and Future Screener when fresh (≤120s).
- **Research:** `rocket_crash_event_log` append-only when Rocket or Crash ≥ 3
  (forming rising-edge, plus every confirmed bar close at ≥ 3).

Candle REST scoring remains the fallback if the websocket book is cold.

## Feed

`UPSTOX_MARKET_FEED_ENABLED` starts the websocket even when the OI heatmap
(`UPSTOX_OI_ENABLED`) is paused. Reconnects set `data_quality_flag=reconnect_gap`
instead of silently dropping history.

## UI

Rocket badges unchanged. Crash (`💥 n/4`) added beside Rocket on READY NOW cards
and as a Future Screener column, with a “Rocket/Crash ≥ 3” filter.
