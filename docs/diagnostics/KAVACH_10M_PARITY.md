# Kavach 10m Parity Note

**Scope:** Locked-symbol live recompute (`daily_checklist_live.recompute_locked_symbol`) now uses `kavach_10m.metrics_from_10m_candles`.

## Aligned with TradingView 10m chart

| Input | Backend |
|-------|---------|
| Bar timeframe | Pairs consecutive **5m** cache bars → **10m** OHLCV (closes 09:25, 09:35, …) |
| Kavach state machine | Same `evaluate_kavach` / `compute_trade_score` as RS scanner |
| Session VWAP | Cumulative from today's 5m H/L/C/V through the 10m bar close index |
| EMA5 / EMA9 | Computed on **10m closes** |
| MACD / SuperTrend / ADX | Standard periods on **10m OHLCV** |
| Volume label | 10m bar volume vs EMA(20) of prior 10m volumes + TOD ratio when available |
| VWAP purity | Native 10m bars (`bar_size=1`, 8 bars) |

## Residual differences (documented in code)

1. **Data source:** Upstox 5m aggregated to 10m; TV may use broker-native 10m feed → minor OHLC drift possible.
2. **VWAP:** Built from 5m constituents, not 10m typical price — matches existing session-VWAP definition in RS scanner.
3. **UI label "EMA10":** Pine display may show EMA10; engine thresholds use **EMA5/EMA9** per `kavach_engine.KavachInput` (unchanged spec).
4. **Warmup:** MACD/ADX need sufficient prior 10m history; first ~30–60 min may differ slightly if cache window is short.
5. **DIR CONFLICT panel parity (2026-07-15):** Conflict overlay uses `include_forming=True` and panel **Trend = price vs VWAP** (not only EMA5 vs VWAP). HOLD/WATCH trading state and opposite Kavach state also suppress READY. SuperTrend still ATR(10)×3 on 10m — if TV Pine uses a different factor, ST votes can still disagree until Pine settings are confirmed.

## Audit trail

Every live recompute writes `rs_live_kavach_audit` with `bar_evaluated_at`, full metrics, and `prev_kavach_state` for edge-flip reconstruction.
