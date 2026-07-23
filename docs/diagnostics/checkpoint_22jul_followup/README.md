# 22-Jul checkpoint follow-up (Items A–F)

Generated **2026-07-22**. Shadow-first — **no live gates**.  
Folder: `docs/diagnostics/checkpoint_22jul_followup/{A,B,C,D,E,F}/`

**Ordering:** A primary → B/E depend on A post-exit / candle grades → C/F independent → D instrumentation (+ backfill).

**B follow-up:** See [`B2_structural_ratchet/`](B2_structural_ratchet/) — EMA5-close-confirm after ≥2R cuts CONTINUATION FP rate from ~7/8 (naive) to **1/8** on the same test set (shadow only; n=10 caveat).

**D follow-up:** See [`D2_vwap_close_confirm/`](D2_vwap_close_confirm/) — sticky READY-episode VWAP **close** confirmation (shadow log only; no live gate).

---

## ITEM A — Pre-entry / post-exit candle context (primary)

**Coverage:** 25 `trade_log` trades, 13→22 Jul.  
**Exception:** **EXIDEIND 20-Jul** — `arbitrage_master.currmth_future_instrument_key` is **NULL** → no OHLC (`A_exideind_note.json`).

**Files:** `A_trade_summaries.csv`, `A_candles_by_trade.json`, `A_correlations.json`

Per trade: up to 6×10m pre-entry + entry + 6×10m post-exit with OHLC, EMA5/10, VWAP, ST, MACD, ADX, reconstructed grade/score/votes, stretch zone, nearest badges.

### Exploratory associations (n=25 — **not significant**)

| Signal | Losers | Winners |
|---|---|---|
| Stretch before entry | 31% | **64%** |
| Avg pre-entry EMA-aligned bars (/6) | 1.9 | **2.8** |
| Avg \|entry−VWAP\| % | 0.90% | 0.97% |

Stretch-before-entry is **more common on winners** in this tiny sample — opposite of a simple “don’t buy extended” rule. Do not promote.

**Post-exit class:** CONTINUATION 13 / REVERSAL 11 / TRUNCATED 1.  
Rule 20 / Rule 23 / Rule 16 exits in this set were all classified CONTINUATION (price kept moving favorably after exit) — small-n curiosity only.

Grade/score use `nifty_pct=0` reconstruction — approximate vs live panel.

---

## ITEM B — ≥2R giveback ratchet (shadow candidate)

**Depends on A** for post-exit CONTINUATION false-positive flag.

**Test population (no Rule 20/23):** ADANIGREEN, DIVISLAB, CHOLAFIN, POLICYBZR, UPL, KEI, FEDERALBNK, HINDZINC (n=8).  
**Already-fast (excluded from test):** DELHIVERY (Rule 20), HAL (Rule 23).

Candidate: after peak ≥2R, exit when close gives back ≥ X R from peak.

| Giveback from peak | Would fire | FP (post-exit CONTINUATION) | Avg R saved vs actual |
|---|---|---|---|
| 0.5R | 8/8 | 7 | ~4.7 |
| 0.75R | 8/8 | 7 | ~4.6 |
| 1.0R | 8/8 | 7 | ~4.3 |
| 1.5R | 8/8 | 7 | ~4.3 |

**Read carefully:** nearly every fire also flags CONTINUATION post-exit under Item A’s classifier — high false-positive risk in this definition/sample. Package for deeper backtest; **do not wire live**. See `B_ratchet_backtest.json`.

---

## ITEM C — TATAELXSI 20-Jul reconciliation

**Root cause of scan miss:** `trade_log` row **id 20** has **null** `ema10_at_entry` / `planned_risk_pts` / `vwap_at_entry` → systematic ≥2R matcher **skipped** the trade (no risk basis), not a silent OHLC bug.

**OHLC replay (futures 10m):** through-exit favorable excursion above 3510 appears **~0** (never traded above entry before SL exit 3500.30). Session/post-exit peak anecdote **3532.50** is **after exit** (~+22.5 pts). With replay EMA10 gap ~71 pts that is only ~0.3R; with a tighter discretionary risk (~5–10 pts) post-exit could look like 2–4R — but that is **post-exit continuation**, not a ≥2R intrabar giveback before EMA10 exit.

**Verdict:** Informational near-miss / post-exit continuation case; matcher exclusion explained by missing risk fields + no pre-exit MFE≥2R.

---

## ITEM D — VWAP touch-and-reject instrumentation

**Status:** **Forward logging added** (shadow-only).

| Piece | Detail |
|---|---|
| Table | `kavach_vwap_touch_reject_log` |
| Definition | LONG: `low≤vwap & close>vwap`; SHORT: `high≥vwap & close<vwap` |
| Live path | `metrics_from_10m` now emits `bar_high/low/open`; `persist_live_kavach_audit` → `persist_vwap_touch_reject` |
| Gate | **None** |
| Backfill | Script `scripts/backfill_vwap_touch_reject.py` (Upstox replay for audit lock symbol-days 13→22 Jul). Summary in `D/D_backfill_summary.json` when job completes. |

Prior research study remains: `docs/diagnostics/VWAP_TOUCH_REJECT_CONTINUATION_20260721.json`.

---

## ITEM E — Grade / Votes at entry

**Depends on A** candle reconstruction (Votes never stored in DB).

- 25 rows in `E_grade_votes_at_entry.csv`
- **Late-entry after grade decay** flag: **KEI 17-Jul** only (EXIDEIND candle context missing due to null ikey)
- Votes = reconstructed ST/MACD/panel-EMA vs VWAP alignment label

---

## ITEM F — `quality_pass` vs `vwap_would_block`

9 passes vs 10 random A-grade blocks:

| Metric | Pass (n=9) | Block sample (n=10) |
|---|---|---|
| `steep_ok` | **100%** | **0%** |
| Avg slope score | **72.6** | **1.4** |
| flip_flop / whipsaw | 0 / 0 | 0 / 0 |

**Verdict:** Filter behaves as an **intentional steep-slope gate**, not a confidence bug. A-grade names still fail when VWAP slope is flat. `would_block≈99.6%` is expected while `READY_VWAP_QUALITY_GATE` stays off and steepening is rare.

---

## Deploy note

Item D code (`kavach_vwap_touch_reject_log.py`, `kavach_10m` OHLC fields, audit hook, `database.py` CREATE) requires push + paperclip deploy for forward logging to persist across restarts.
