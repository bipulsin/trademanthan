# Garuda Screener — Logic Reference

**Status:** Shadow / research only. **Does not gate** Kavach READY, Take Trade, Grade, Votes, or live entries.  
**Module:** `backend/services/garuda_screener/`  
**Ops note:** [`docs/diagnostics/GARUDA_SCREENER_OPS.md`](diagnostics/GARUDA_SCREENER_OPS.md)

This document describes **how Garuda ranking is computed**. For production schedule, load impact, and `trade_log` confluence SQL, see the ops note.

---

## 1. Purpose

Garuda is an independent **price-action FO screener** that:

1. Detects **imbalance** (Part 1) on current-month stock futures.
2. Measures **direction / strength / trend / momentum** (Part 2).
3. Ranks imbalance-confirmed names and publishes a **Top-6** list each 10-minute bar.

It is designed for confluence research against checklist entries (MATCH / NO_MATCH), not as a live entry gate.

---

## 2. Cadence & data

| Item | Value |
|------|--------|
| Job id | `garuda_screener_10m` (`smart_future_algo`) |
| Schedule | Mon–Fri IST `:06 / :16 / :26 / :36 / :46 / :56` (≈1 min after 10m bar close) |
| Bar size | 10-minute (aggregated from 5m cache candles) |
| Universe | `arbitrage_master.currmth_future_instrument_key` (~200 FO), excl. `EXIDEIND`, `NUVAMA`, `SAMMAANCAP` |
| Candles | **Cache-only** (`candles_cache_only`) — no Upstox storm on miss |
| Persist | `garuda_screener_log` — one row per `(session_date, bar_end, symbol)` |
| First session bar | From ~09:25 IST bar (job from 09:26) |

Benchmark: **Nifty 50** (`NSE_INDEX|Nifty 50`). Sector/peer RS uses available sector context when present.

---

## 3. Pipeline (per bar)

```
Universe FO keys
    → load 5m cache → aggregate to 10m (+ EMA5/10, session VWAP, ADX)
    → Part 1: imbalance legs → confirm LONG/SHORT (≥2 of 5 required legs)
    → Part 2: direction + strength + trend + momentum
    → cross-section percentiles (day_rs, roc3)
    → rank_score = (strength_pct + momentum_pct) / 2
    → pool = imbalance_confirmed only → sort by rank_score → Top-6
    → upsert all evaluated rows; set top6_rank 1..6 on winners
```

---

## 4. Part 1 — Imbalance

Evaluated on the **current 10m bar** (and lookbacks). Config: `backend/services/garuda_screener/config.py`.

### 4.1 Legs

| # | Leg | LONG pass | SHORT pass | Notes |
|---|-----|-----------|------------|--------|
| 1 | **Range expansion** | Bar range ≥ 1.5× avg ATR(14) range **and** close position ≥ 0.65 | Range expanded **and** close position ≤ 0.35 | Avg range over prior 14 bars |
| 2 | **Close position** | `(close−low)/(high−low) ≥ 0.65` | ≤ 0.35 | Standalone close location |
| 3 | **Volume breakout** | Vol ≥ 1.5× avg vol(14) **and** close > prior 14-bar high | Vol breakout **and** close < prior 14-bar low | |
| 4 | **Consecutive direction** | ≥ 3 consecutive bullish bars | ≥ 3 consecutive bearish bars | Directional candle count |
| 5 | **RS / sector divergence** | Stock window % > Nifty window % **and** > sector/peer window % | Both excesses negative | Window ≈ 6×10m bars (~1h) |
| 6 | **Gap-and-hold** (bonus) | Gap up ≥ `0.75× ATR%` from prior close, unfilled, bar ≤ 10:00 | Symmetric gap down | **Bonus only** — not required for confirmation |

### 4.2 Confirmation rule

Required legs for confirmation:  
`range_expansion`, `close_position`, `volume_breakout`, `consecutive_direction`, `rs_sector_divergence`  
(**gap_and_hold excluded**).

- **LONG confirmed** if ≥ **2** required legs pass LONG.  
- **SHORT confirmed** if ≥ **2** required legs pass SHORT.  
- Else **NEUTRAL** (not imbalance-confirmed).

Only **imbalance-confirmed** symbols enter the Top-6 ranking pool.

---

## 5. Part 2 — Direction, strength, trend, momentum

### 5.1 Direction (3-way agreement)

On the evaluation bar, signs of:

1. Close vs session VWAP  
2. EMA5 − EMA10  
3. Close vs close N bars ago (`DIRECTION_LOOKBACK = 3`)

- **Agreement** = at least two non-zero signs and all agree.  
- Side = LONG / SHORT / NEUTRAL from sum of signs.  
- Published `side` prefers **imbalance side** when confirmed; else direction side.

### 5.2 Strength

- **Day RS** = stock day % change − Nifty day % change (from prior session close).  
- Optional beta-adjusted RS using ~20-day beta (fallback β=1).  
- **strength_percentile** = percentile of `day_rs` vs universe cross-section (higher = stronger).

### 5.3 Trend (diagnostic; not in rank_score)

- ADX(14) and ADX slope over 3 bars  
- Efficiency ratio (ER length 10)

### 5.4 Momentum

- ROC(3) primary, ROC(5) alternate  
- Acceleration = Δ ROC(3)  
- Volume-weighted ROC when avg volume available  
- **momentum_percentile** = percentile of ROC(3) in cross-section  
  - For **SHORT** imbalance: ranks on **−ROC(3)** so strong downside ranks high  
- VWAP slope score (threshold 50 for a pass flag) — **comparison only**, not a gate

---

## 6. Ranking (Top-6)

```
rank_score = (strength_percentile + momentum_percentile) / 2
```

Method label: `avg_strength_momentum_pct`.

1. Filter to `imbalance_confirmed == true`.  
2. Sort descending by `rank_score`, then strength %, then momentum %.  
3. Assign `top6_rank` = 1…6 to the first six.  
4. Long and short compete in **one** combined Top-6 (not separate lists).

Symbols without both percentiles get `rank_score = null` and sort to the bottom of the pool.

---

## 7. Persistence schema (`garuda_screener_log`)

Key columns:

| Column | Meaning |
|--------|---------|
| `session_date` | NSE session date (IST) |
| `bar_end` | 10m bar end timestamp |
| `symbol` | Underlying stock |
| `side` | Screener side (LONG/SHORT/NEUTRAL) |
| `imbalance_confirmed` | Part 1 confirmed |
| `imbalance_side` / `imbalance_hits` / `imbalance_legs` | Part 1 detail |
| `direction_*` | Part 2 direction JSON / flags |
| `day_rs`, `strength_percentile` | Strength |
| `trend_adx`, `trend_adx_slope`, `trend_er` | Trend diagnostics |
| `momentum_percentile`, `momentum` | Momentum JSON |
| `rank_score` | Composite score used for Top-6 |
| `top6_rank` | 1–6 if in Top-6; else null |
| `price`, `components` | Close + packed component blob |
| `logged_at` | Insert/upsert time |

Unique key: `(session_date, bar_end, symbol)`.

---

## 8. Confluence with trades (shadow)

When a `trade_log` row is written without explicit Garuda fields, lookup:

1. Same `session_date`  
2. Nearest `garuda_screener_log.bar_end ≤ entry_time`  
3. **MATCH** if symbol is in that bar’s Top-6 **and** Garuda side matches trade direction  
4. Else **NO_MATCH** if a bar exists; **NOT_AVAILABLE** if no Garuda bar

Stored on `trade_log`: `garuda_confluence`, `garuda_rank`, `garuda_direction` — never used to block Take Trade.

---

## 9. APIs & UI

| Surface | Path / location |
|---------|-----------------|
| Latest Top-6 | `GET /api/dashboard/garuda/latest` |
| Shadow export | `GET /api/export/garuda-shadow` (JWT; includes Top-6 rows + READY NOW promotions + session summaries) |
| Checklist UI | `dailyRSchecklist.html` — Garuda section (testing banner; non-gating) |

---

## 10. Code map

| File | Role |
|------|------|
| `config.py` | Thresholds, Top-N, exclusions |
| `indicators.py` | ATR-style helpers, ROC, ER, percentiles, VWAP slope |
| `screener.py` | Part 1/2 evaluate + `rank_top_n` |
| `job.py` | Live cache scan, DB upsert, Top-6 API, confluence lookup |
| `export.py` | Garuda shadow JSON export |

---

## 11. Design constraints (important)

- **No live gating** — independent of Kavach Grade / Votes / `trade_take_enabled`.  
- **Cache-only candles** — miss → skip symbol for that bar (no Upstox pull in-job).  
- **No historical backfill** at first deploy; series builds from live bars forward.  
- Ranking pool is **imbalance-confirmed only**; a strong `rank_score` without Part 1 confirmation never enters Top-6.

---

*Generated from production code paths under `backend/services/garuda_screener/` (Aug 2026).*
