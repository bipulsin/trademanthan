# Kosmic Tarang — Historical data availability (Phase 1 spike)

**Product:** Kosmic Tarang  
**Raw probe:** [`docs/_spike_tarang_hist_raw.json`](./_spike_tarang_hist_raw.json) (no secrets)  
**Ran:** 2026-09-19 UTC

## Summary

| Need | Upstox MCX | Delta India |
|---|---|---|
| Live option chain | Build from instrument master + Option Greek (≤50 keys); no MCX option-chain API | Public `/v2/tickers` with `mark_vol` + greeks |
| Historical option IV / chain | **Not available** from Upstox | **No IV history API**; snapshot `mark_vol` yourself |
| Underlying OHLC (RV) | Historical candle APIs for MCX **FUT** keys (token required) | Public `/v2/history/candles` — probe got **90** daily BTCUSD bars (success) |
| Multi-year backtest | Vendor MCX option history **or** accumulate `tarang_iv_snapshots` | 6–12 months own option snapshots + public underlying candles |

## Upstox / MCX

- Option **chain** endpoint is documented as unavailable for MCX — confirmed in Phase 0.
- Option **Greek** is live-only; there is no Upstox series of historical MCX option IV.
- Realized-vol inputs can use Upstox **historical candles on futures** instrument keys once OAuth token is present (local spike skipped candles — no workstation token; production uses paperclip `data/upstox_token.json`).
- **Implication:** Phase 6 energy backtest cannot rely on Upstox alone for option chains. Start IV snapshots **now** (`tarang_iv_snapshots`); for multi-year history, budget for a vendor feed later.

## Delta Exchange India

- Public market data base: `https://api.india.delta.exchange/v2`.
- `/history/candles` works for underlying-style symbols (BTCUSD sample: HTTP 200, 90 daily points).
- Live options: tickers expose `mark_vol` / greeks — suitable for screening and snapshots.
- Option-symbol candle history is opportunistic (symbol must exist / be listed); not a substitute for a full chain history API.
- Authenticated account read (balances / margined positions) is separate from history; see runbook.

## Phase 1 action

IV snapshot scheduler writes ATM IV (and later skew) into **`tarang_iv_snapshots`** on an IST cadence so the percentile gate has data by Phase 2+.

## Phase 6 note

Do not extrapolate. Be explicit in backtests about sample length: own snapshots vs vendor MCX chains vs Delta public candles.
