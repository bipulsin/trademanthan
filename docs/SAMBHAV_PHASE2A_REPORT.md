# Sambhav Phase 2A Report — Leakage-Free ML Dataset + Target Research

**Status:** COMPLETE (no model trained)  
**Date:** 2026-08-13  
**Active dataset:** `sambhav_dataset_v1_20260813`  
**Feature version:** `sambhav_features_v1`

This phase builds and inspects the ML observation set. It does **not** select a final classification target and does **not** train models.

---

## 1. Source dataset

| Field | Value |
|-------|--------|
| Instrument | NIFTY 50 (`NSE_INDEX\|Nifty 50`) |
| Interval | 10 minutes |
| Source | Upstox V3 (already imported; **not** re-downloaded) |
| Period | 2022-01-03 → 2026-08-12 |
| Dataset version | `sambhav_dataset_v1_20260813` |
| Filter | `sambhav_sessions.session_type = REGULAR` AND `included_in_sambhav_v1 = true` |

Source OHLC table `sambhav_10m_candles` was **not modified**.

---

## 2–5. Observation counts

| Metric | Count |
|--------|------:|
| Source regular candles | 43,092 |
| Regular sessions | 1,134 |
| Usable same-session +30m target observations | **39,690** |
| Removed (no same-session 30m horizon: 15:05 / 15:15 / 15:25) | **3,402** (= 1,134 × 3) |
| Feature warm-up exclusions (among resolvable) | **47** |
| Final ML-ready rows (resolvable + complete features) | **39,643** |
| Rows persisted to `sambhav_features` | **39,690** |

Prediction timestamp = close of candle T.  
`future_close = close[T+3]` **same session only**. Overnight / next-day closes are never used.

---

## 6–8. Features (`sambhav_features_v1`)

**Feature count:** 43

### Feature list

**Price / candle structure:** `close`, `candle_return`, `oc_return`, `hl_range_pct`, `body_pct`, `upper_wick_pct`, `lower_wick_pct`, `close_loc`

**Returns:** `ret_1`, `ret_2`, `ret_3`, `ret_6`, `ret_9`, `ret_18` (10m…180m)

**Trend:** `ema9`, `ema21`, `ema50`, `ema9_minus_ema21`, `ema21_minus_ema50`, `close_vs_ema9`, `close_vs_ema21`, `close_vs_ema50`, `ema9_slope`, `ema21_slope`

**Momentum:** `rsi9`, `rsi14`, `macd`, `macd_signal`, `macd_hist`, `adx14`

**Volatility:** `atr14`, `atr14_pct`, `realized_vol3`, `realized_vol6`, `realized_vol18`, `range_vs_avg`

**Time / session:** `minutes_since_open`, `hour`, `minute`, `day_of_week`, `sin_time`, `cos_time`, `session_progress`

All rolling / EMA / RSI / MACD / ADX / ATR calculations are **causal** (T and earlier only). No forward-fill.

Warm-up design target ≈ 50 bars (EMA50 / ADX / MACD). Observed warm-up losses among resolvable rows: **47**.

---

## 9. Future-return distribution (`future_return_30m`)

| Stat | Value |
|------|------:|
| count | 39,690 |
| mean | −0.00000130 (−0.00013%) |
| median | +0.0000327 (+0.00327%) |
| std | 0.001732 |
| min | −0.03377 (−3.377%) |
| max | +0.02902 (+2.902%) |
| p1 | −0.00481 |
| p5 | −0.00274 |
| p10 | −0.00189 |
| p25 | −0.00082 |
| p50 | +0.000033 |
| p75 | +0.00085 |
| p90 | +0.00182 |
| p95 | +0.00261 |
| p99 | +0.00459 |
| % > 0 | 51.09% |
| % < 0 | 48.84% |
| % ≈ 0 (\|r\| ≤ 1e−10) | 0.063% |

Near-zero mean with modest 30m dispersion: most moves are small vs ±0.2–0.3% thresholds below.

---

## 10. Target A — binary

Definition: **UP** if `future_return > 0`, else **DOWN** (ties → DOWN).

| Class | Count | % |
|-------|------:|--:|
| UP | 20,279 | 51.09% |
| DOWN | 19,411 | 48.91% |
| n | 39,690 | 100% |

Balance is near even — statistically usable, but includes many tiny moves.

---

## 11. Target B — meaningful move (candidates only)

Thresholds are **not** chosen here. Reported for research:

| ± threshold | UP | NEUTRAL | DOWN | %UP | %NEUTRAL | %DOWN |
|------------:|---:|--------:|-----:|----:|---------:|------:|
| 0.10% | 8,654 | 22,576 | 8,460 | 21.8% | 56.9% | 21.3% |
| 0.15% | 5,398 | 28,780 | 5,512 | 13.6% | 72.5% | 13.9% |
| 0.20% | 3,401 | 32,662 | 3,627 | 8.6% | 82.3% | 9.1% |
| 0.25% | 2,186 | 35,095 | 2,409 | 5.5% | 88.4% | 6.1% |
| 0.30% | 1,426 | 36,659 | 1,605 | 3.6% | 92.4% | 4.0% |

As the band widens, NEUTRAL dominates. Final band selection is deferred to the next reviewed phase (must not use a held-out test period to pick the threshold).

---

## Target C — regression

Continuous target: `future_return_30m` as defined above (n = 39,690).  
Enables future magnitude estimation without committing to a discrete label now.

**Target status:** UNDER RESEARCH (no automatic selection).

---

## 12–13. Volume / VWAP

| Item | Result |
|------|--------|
| Volume available | **false** (100% of regular bars have volume = 0) |
| Volume features | **not built** (would be misleading) |
| VWAP | **excluded** (requires meaningful volume; not invented) |

---

## 14. Look-ahead tests

| Test | Result |
|------|--------|
| Features v1 truncation invariance | **PASS** |
| Same-session targets (no overnight, exclude 15:05/15:15/15:25, 30m horizon) | **PASS** |

Automated unit tests: `backend/test_sambhav_phase2a.py`.

---

## 15. Final dataset row count

| Layer | Rows |
|-------|-----:|
| Persisted feature+target rows (`sambhav_features`, resolvable targets) | 39,690 |
| Complete-feature ML-ready subset | 39,643 |

Each row stores `dataset_version = sambhav_dataset_v1_20260813` and `feature_version = sambhav_features_v1`.

---

## Explicit non-goals (this phase)

- No XGBoost / RF / logistic / LightGBM / NN training  
- No accuracy / probability / calibration metrics  
- No walk-forward model validation  
- No historical re-download  
- No mutation of `sambhav_10m_candles`

---

## Next phase (separate review)

1. Final target definition  
2. Train / validation / test split  
3. Baseline models  
4. XGBoost  
5. Probability calibration  
6. Walk-forward validation  

Artifact: `docs/diagnostics/sambhav_phase2a_20260813.json`
