# 22-Jul checkpoint extraction — 2026-07-08 → 2026-07-22

Read-only pull generated **2026-07-22 IST**. Requested window: **2026-07-08 inclusive → 2026-07-22 inclusive**.  
For every item, **actual coverage** is stated when logging started later than 8-Jul. No live rule/gating changes.

**Artifact folder:** `docs/diagnostics/checkpoint_22jul_extract/`

| File | Item |
|---|---|
| `01_kavach_ready_consistency_log.csv` (+ `_summary.json`) | 1 |
| `02_distance_guard_disagreements.csv` (+ `_summary.json`) | 2 |
| `03_atr_consumed.csv` (+ `_summary.json`) | 3 |
| `04_giveback_2r_matches.csv` / `.json` | 4 |
| `05_expansion_watch_status.json` | 5 |
| `06_r1_plan_exit_*.csv/json` | 6 |
| `07_vwap_touch_reject_status.json` | 7 |
| `08_fast_watch_*.csv/json` | 8 |
| `09_trade_log_enriched.csv` (+ `_summary.json`) | 9 |
| `10_ticket_status.json` | 10 |
| `00_manifest.json` | Index |

---

## Coverage map (do not assume full 8–22 for all tables)

| Source | First data in/near window | Last | Notes |
|---|---|---|---|
| `kavach_ready_consistency_log` | **2026-07-15** | 2026-07-22 | **No rows 08–14 Jul** |
| Dwell / distance shadow in `inputs` | **2026-07-17** ~08:01 UTC | 2026-07-22 | State table from 17-Jul |
| ATR consumed (in consistency `inputs`) | **2026-07-15** | 2026-07-22 | Same floor as consistency log |
| `trade_log` | **2026-07-13** | 2026-07-22 | No DB trades 08–12 Jul |
| `rs_live_kavach_audit` | 2026-07-13 | 2026-07-22 | Retention ~10 sessions |
| `rs_fast_watch` | **2026-07-08** (lifetime 06-Jul) | 2026-07-22 | Dead **13–20 Jul**; recovers 21–22 |
| Expansion Watch DB log | — | — | **No table** |
| `vwap_touch_reject` | — | — | **Not instrumented** |
| `kavach_r1_early_warning_log` | — | — | **Empty (lifetime n=0)** |

Trading sessions present in archive runs in window: 08, 09, 10, 13, 14, 15, 16, 17, 20, 21, 22 (weekends 11–12, 18–19 omitted).

---

## 1. `kavach_ready_consistency_log`

- **Rows:** 2,500  
- **Actual range:** 2026-07-15 → 2026-07-22  
- **First logged_at:** 2026-07-15 03:53:20 UTC  
- **`vwap_gate_enabled` true:** 0 (gate still shadow/off)  
- **`vwap_would_block` true:** 2,491 / 2,500  
- **`quality_pass` true:** 9  

By day: 15=574, 16=149, 17=414, 20=535, 21=346, 22=482.

CSV includes state fields + gate columns + `inputs_json` (full research payload).

---

## 2. Distance-guard Option A / B / C

- **Embedded dwell shadow rows:** 1,409 (from 17-Jul onward inside consistency log)  
- **Disagreement poll rows:** **20** — all pattern **`A=False, B=False, C=True`** (Option C more conservative)  
- **Symbol:** all 20 are **AUBANK on 2026-07-20** (repeated polls)  
- Unique symbol-day block counts in this extract: A=53, B=53, C=53 (overall block tallies equal; disagreements are C-only extras on AUBANK)

**Interpretation:** Live = **B**. Where A/B/C disagreed in this window, **C was stricter**; A and B agreed. Not enough disagreement diversity for a broad A-vs-B ranking beyond “usually identical in this sample.”

---

## 3. ATR consumed

- **Rows with ATR fields:** 2,066  
- **Earliest logged_at:** 2026-07-15 07:05:11 UTC  
- **Sessions:** 15 → 22 Jul  
- Fields: `atr_consumed_pct_from_open`, `atr_consumed_pct_from_opening_range` (+ full dict when present)

Not per-candle market-wide — only when a consistency log row was written (READY / gate-relevant polls).

---

## 4. ≥2R intrabar then give-back (profit-protection scan)

**Universe:** all 25 `trade_log` rows (13–22 Jul).  
**Method:** Upstox 5m→10m; risk = `planned_risk_pts` or nearer \|entry−EMA10\| / \|entry−VWAP\|; MFE ≥ 2R after entry; then close back through entry before exit **or** give-back ≥ 1R with exit_r &lt; 2R.

**Matches: 10**

| Date | Symbol | Dir | Peak R | Exit R | Giveback R | Peak bar (IST) | Hand-known? |
|---|---|---|---|---|---|---|---|
| 13-Jul | ADANIGREEN | LONG | 8.92 | −1.92 | 10.84 | 10:15 | Yes |
| 15-Jul | DIVISLAB | LONG | 3.41 | −1.40 | 4.81 | 10:05 | **New** |
| 15-Jul | CHOLAFIN | LONG | 19.62 | −4.25 | 23.88 | 11:15 | **New** |
| 16-Jul | POLICYBZR | LONG | 2.22 | 0.06 | 2.16 | 12:45 | Yes |
| 16-Jul | UPL | LONG | 2.58 | 0.77 | 1.81 | 15:05 | **New** |
| 17-Jul | KEI | SHORT | 2.73 | −1.65 | 4.38 | 11:35 | **New** |
| 20-Jul | FEDERALBNK | LONG | 4.20 | 0.00 | 4.20 | 13:05 | Yes |
| 21-Jul | HINDZINC | LONG | 3.25 | −3.07 | 6.33 | 14:25 | **New** |
| 22-Jul | DELHIVERY | SHORT | 2.65 | 0.99 | 1.65 | 12:05 | **New** |
| 22-Jul | HAL | LONG | 4.51 | 0.60 | 3.91 | 11:45 | **New** (contrast / partial) |

**Hand-known TATAELXSI:** **not** in match set under this ≥2R definition (20-Jul loss may be &lt;2R MFE vs planned risk, or peak post-exit). Full OHLC-around-peak in `04_giveback_2r_matches.json`.

---

## 5. Expansion Watch

- **`EXPANSION_WATCH_LIVE`:** `0` (off)  
- **Persistent shadow log table:** **none** — DB sample size **0**  
- **Thresholds:** slope ≥ `THRESHOLD_VWAP_SLOPE` (50), EMA align bars = 2, max extension = 1.5 ATR  
- **Go/no-go:** `scripts/analyze_expansion_watch_backtest.py` must clear `credible_positive` before enabling live  

No full-window hit log to export; backtest must be re-run separately if a fresh n is needed.

---

## 6. R1 PLAN EXIT (`kavach_r1_early_warning_log`)

- **Window count:** **0**  
- **Lifetime total:** **0**  
- Still no real-trade R1 early-warning rows.

---

## 7. `vwap_intrabar_touch_reject`

- **Status:** **not live / not added** to shadow tables  
- Research-only script from 21-Jul; earliest usable data = Upstox OHLC replay  
- Prior study: `docs/diagnostics/VWAP_TOUCH_REJECT_CONTINUATION_20260721.json`

---

## 8. Fast Watch flips (`rs_fast_watch`)

| Session | Flips | Reversals | Symbols |
|---|---|---|---|
| 2026-07-08 | 108 | 0 | 96 |
| 2026-07-09 | 27 | 9 | 18 |
| 2026-07-10 | 17 | 6 | 10 |
| 2026-07-13 | **0** | 0 | 0 |
| 2026-07-14 | **0** | 0 | 0 |
| 2026-07-15 | **0** | 0 | 0 |
| 2026-07-16 | **0** | 0 | 0 |
| 2026-07-17 | **0** | 0 | 0 |
| 2026-07-20 | **0** | 0 | 0 |
| 2026-07-21 | 61 | 16 | 43 |
| 2026-07-22 | 61 | 11 | 50 |

**Pattern:** active → collapses to **zero** after 10-Jul through 20-Jul (NameError bug window) → **recovers** 21–22 Jul after PR #5 / `622f0a7` fix. Matches expected silent-failure then restore.

---

## 9. Trade log (checkpoint period)

- **25 trades** in DB, **2026-07-13 → 2026-07-22**  
- **Net realized P&L (where qty present):** **−₹4,062.50**  
- Enriched CSV joins nearest `rs_live_kavach_audit` + consistency row ≤ entry time  
- **Votes:** not stored historically (null + note in CSV)

See `09_trade_log_enriched.csv` for full entry/exit/side/qty/P&L/grades/states.

---

## 10. Ticket status (no data pull)

| Ticket | Status |
|---|---|
| ABB fix coverage | **Unconfirmed** |
| Watching-vs-READY-NOW (Req 5) | **Open** |
| Backend FSM vs Pine | **Open** (v2.7 confidence draft, not live) |
| HCLTECH direction-flip ticket send | **Open** (no evidence sent) |

Source: `docs/CHECKPOINTS_22JUL_8AUG.md`.
