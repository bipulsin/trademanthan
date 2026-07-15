# Kavach 10m Parity Note

**Scope:** Locked-symbol live recompute (`daily_checklist_live.recompute_locked_symbol`) now uses `kavach_10m.metrics_from_10m_candles`.

## Aligned with TradingView Kavach Pine v2.6 (10m chart)

| Input | Backend (`kavach_10m`) | Pine v2.6 default |
|-------|------------------------|-------------------|
| Bar timeframe | Pairs consecutive **5m** → **10m** | Chart 10m |
| SuperTrend | ATR period **10**, multiplier **1.5** | `stPeriod=10`, `stMult=1.5` |
| MACD | **6 / 13 / 5** | `macdFast/Slow/Signal` |
| Panel EMA vs VWAP | EMA(**9**) vs session VWAP | `emaLen=9` (`ema5Raw`) |
| Panel Trend | 2-of-3 of MACD line, ST, EMA vs VWAP | `trendReadBullish/Bearish` |
| READY entry EMA | True **EMA5** (unchanged) | n/a (entry elsewhere) |
| Session VWAP | Cumulative from today's 5m H/L/C/V | `ta.vwap(hlc3)` |
| Volume label | 10m bar vs EMA(20) of prior 10m vols | SMA(20) volume |

## Residual differences

1. **Data source:** Upstox 5m aggregated to 10m; TV may use broker-native 10m feed → minor OHLC drift.
2. **VWAP:** Built from 5m constituents (typical price approx) vs Pine `hlc3` session VWAP.
3. **Kavach FSM:** Backend `evaluate_kavach` is a simplified 10-condition scorer; Pine Layer 3–5 FSM can still differ on BUY vs READY vs HOLD/WATCH even when Trend/ST/MACD labels match.
4. **RS scanner batch:** Still uses classic MACD 12/26/9 and ST×3 for universe ranking (unchanged); live checklist / DIR CONFLICT uses Pine params above.

## Audit trail

Every live recompute writes `rs_live_kavach_audit` with `bar_evaluated_at`, full metrics, and `prev_kavach_state` for edge-flip reconstruction.
