# Plan: Full-Universe RS Score/Grade + Dynamic Top-10 (Hysteresis)

**Status:** Design for review — **no live cutover in this pass.**  
**Driver:** TVSMOTOR / OBEROIRLTY grade LOCF gaps (Top-10 persist + lock-only audit).  
**Constraint:** No change to READY NOW promotion rules or Confidence Grade formula — coverage + persistence + Top-10 membership mechanics only.

---

## Cadence confirmation (2026-08-02) — **KEEP 5m**

Verified in code before S0:

1. Live RS (`cache_only=True` in session) reads **`candle_cache.get_recent(..., "minutes/5")` only**. On miss it returns unscored — **no Upstox per-symbol fetch**.
2. Shared warm (`CANDLE_INTERVAL = minutes/5` in `market_data/constants.py`) is what fills that cache on the 10m clock; RS re-runs at :00/:05 are **pure compute** on the same 5m bars (may recompute twice per warm window).
3. NIFTY % prefers `index_prices` DB; Upstox fallback is **one index key**, not the FO universe.

**Hard rule satisfied:** 5m cadence adds **no** additional FO candle API load. Final cadence: **5m**.

§9 locked: bonus **0.20**, neutrals every cycle, grade-first cutover (flag), audit kept, lock/R2 deferred.


| Assumption in prompt | Code today | Plan stance |
|----------------------|------------|-------------|
| Score all ~200 without new API | **Already true** in `run_relative_strength_scan` (`cache_only` market hours) | Keep |
| Persist all 200 each cycle | **False** — `_rank` truncates to `PERSIST_TOP_N=10`/side → `relative_strength_snapshot` | Change (shadow first) |
| Cycle = 10 minutes | **RS job is 5m** (`relative_strength_scanner_5m`, 09:20–15:15); candle warm is **10m** | **Recommend keep RS at 5m** for denser grades (better LOCF). Optional later align to 10m if you want 1:1 with warm. Call out in review. |
| Top-10 = query over full set + hysteresis | Today: truncate-at-persist + **R1/R2 lock** rules (separate from RSS persist) | Shadow: compute hysteresis Top-10 as columns/view; **do not remove R1/R2 until cutover phase** |

**Ranking key today (membership):** primary `relative_strength` (bull high / bear low), tie-break `trade_score` — **not** trade_score alone (`_rank` in `relative_strength_scanner.py`). Hysteresis bonus must apply to **that** membership key unless we explicitly change ranking (out of scope).

---

## 1. Target architecture (after cutover)

```
every RS cycle (recommend remain 5m):
  score full FO universe from candle cache (unchanged)
  persist ALL scored rows → rs_universe_score_snapshot
       raw RS, trade_score, grade, kavach, would_be_rank_raw, …
  compute Top-10/side with incumbent bonus → membership view/columns
       log rs_raw + rs_membership (or score_raw + score_membership)
  cache-miss / unscored → RS-skip ring → candle-warm-deny.html section

live consumers (cutover phase only):
  grade LOCF / gates ← full-universe rows (no Top-10 hole)
  Top-5 lock/promote ← hysteresis Top-10/Top-5 view (not R2 band on sparse persist)
  R1 VWAP trend can remain; R2 “rank outside band” rebuilt on dense ranks
```

**Shadow phase:** write `rs_universe_score_snapshot` + membership columns in parallel; **leave** `relative_strength_snapshot` Top-10 truncate + lock/R1/R2 + UI untouched.

---

## 2. Step 1 — Full-universe persist

### Schema (new)

**Table:** `rs_universe_score_snapshot`  
(Avoid colliding with EOD `rs_universe_kavach_archive`.)

Suggested columns (mirror RSS metrics + membership diagnostics):

| Column | Notes |
|--------|--------|
| `scan_time`, `session_date`, `symbol`, `instrument_key` | |
| All current RSS metric fields | price, RS%, EMAs, VWAP, ADX, vol, grade, trade_score, kavach_*, stretch fields as today |
| `ranking_type` | BULLISH / BEARISH / NEUTRAL |
| `rank_raw` | Rank by **raw** RS sort within side (1 = best), null if NEUTRAL/unscored |
| `rank_membership` | Rank after incumbent bonus (drives dynamic Top-10) |
| `relative_strength_raw` | Unchanged RS% |
| `relative_strength_membership` | RS% ± bonus for sort only (null if NEUTRAL) |
| `in_top10_membership` | bool |
| `in_top5_membership` | bool |
| `incumbent_bonus_applied` | bool |
| `scan_trigger`, `cache_only`, `from_cache` | |
| `exclusion_reason` | null if scored; else `missing_candles_or_min_bars` / `exception` / … |

**Unique:** `(scan_time, symbol)` — one row per symbol per cycle (including unscored-with-reason rows for visibility).

**Volume:** ~200 × ~71 scans/day ≈ **14k rows/day** (~10× current RSS). Fine for Postgres; add retention job (e.g. 30–60 trading days) in same change set.

### Code hook

`backend/services/relative_strength_scanner.py` → `run_relative_strength_scan`:

1. After metrics loop + `_rank` (or refactor `_rank` to return **full** bull/bear lists before truncate).
2. New `persist_universe_scores(...)` → `rs_universe_score_snapshot`.
3. Keep existing `_persist` → `relative_strength_snapshot` **unchanged in shadow**.

Neutrals: persist with `ranking_type=NEUTRAL`, `rank_*=null`, grade/score if computed, or exclusion reason if metrics failed.

### Cadence decision (review checkbox)

- [ ] **A (recommended):** Keep 5m RS job; full-universe write every 5m.  
- [ ] **B:** Move RS to 10m aligned with warm (`:05,:15,…`). Fewer grade samples; simpler ops narrative.

---

## 3. Step 2 — Dynamic Top-10 + incumbent bonus

### Membership algorithm (per side, each cycle)

1. Take all scored BULLISH (resp. BEARISH) rows this scan.  
2. Mark **incumbents** = symbols with `in_top10_membership=true` on **previous** scan (same side), or fallback: previous `relative_strength_snapshot` Top-10 during shadow.  
3. Sort key (unchanged primary metric):
   - Bull: `rs_membership = rs_raw + bonus` if incumbent else `rs_raw`; sort desc, tie-break `trade_score` desc.  
   - Bear: `rs_membership = rs_raw - bonus` if incumbent else `rs_raw`; sort asc (more negative / lower RS wins), tie-break `trade_score` desc.  
4. Assign `rank_membership` 1..N; `in_top10_membership = rank_membership <= 10`; `in_top5_membership = rank_membership <= 5`.  
5. Persist **raw** `relative_strength` / `trade_score` / `confidence_grade` **unmodified** on the symbol row. Bonus never alters grade gate inputs.

### Proposed bonus (explicit — for review, not silent)

**Default proposal: `INCUMBENT_RS_BONUS = 0.20` absolute RS percentage points.**

| Why 0.20 | |
|----------|--|
| Rank today is RS%-primary; bonus must be in RS units | |
| Typical noisy flips in Top-10 are often small RS deltas; 0.20 is a visible but not huge moat | |
| Easy to reason: challenger must beat incumbent by **>0.20 RS%** (bull) | |
| Logged dual ranks enable empirical tune | |

**Alternatives to backtest in same harness (do not ship all):**

| ID | Rule |
|----|------|
| `B0` | No bonus (baseline churn) |
| `B20` | **+0.20 RS pts** (proposed default) |
| `B10` | +0.10 RS pts (lighter) |
| `B35` | +0.35 RS pts (heavier) |
| `T5` | Optional: if membership ever switches to trade_score primary — +5 trade_score pts (out of scope unless ranking change approved) |

### Churn backtest (before live cutover)

Using shadow (or offline replay of full-universe scores once available) on **2026-07-27 … 07-31**:

For each day / side:

- Top-10 set at each scan under `B0` vs `B20` (and optionally B10/B35)
- Metrics: entries/day, exits/day, Jaccard stability vs prior scan, median tenure (scans in Top-10)

Deliverable artifact: `docs/diagnostics/rs_top10_hysteresis_backtest_20260727_31/`  
**Gate:** cutover only after reviewing churn table — if `B20` barely reduces churn, consider `B35`; if it freezes dead names, prefer `B10`.

### What happens to R1/R2?

| Phase | Behavior |
|-------|----------|
| Shadow | **Unchanged** — still driven by truncated `relative_strength_snapshot` + lock audit |
| Cutover | Top-5 lock/promote reads `in_top5_membership` / `rank_membership` from universe table; **R2** reimplemented as “`rank_membership` outside band for M scans” on dense ranks; **R1** (VWAP) can stay. Old “beyond_persist → invisible” pathology goes away |

---

## 4. Step 3 — Audit vs full-universe grade

| | RS scanner (5m cache) | `rs_live_kavach_audit` (locked recompute) |
|--|----------------------|------------------------------------------|
| TF | **5m** indicators | **10m** via `metrics_from_10m_candles` |
| Universe | Full FO (metrics); persist Top-10 today | Lock list only |
| Outputs | RS%, trade_score, confidence_grade, kavach, rank | Same family + `bar_evaluated_at`, prev state, stretch shadows |

**Conclusion:**

- For **grade/score availability / LOCF / “grade is the gate” coverage:** full-universe RS persist **replaces the need** for lock-list audit as the grade source.  
- Audit is **not** identical (10m TF + checklist recompute side effects). Do **not** delete audit in shadow.  
- Cutover options for review:
  1. **Grade consumers → universe snapshot only**; keep audit as optional 10m shadow for locked names.  
  2. Later: run 10m recompute on full universe if checklist truly needs 10m parity (heavier CPU; measure first).

**Lock-list mechanism:** still needed for checklist/READY workflow until a separate design removes “lock” as a product concept. This plan only removes **grade starvation** outside Top-10/lock — not READY NOW logic.

---

## 5. Step 4 — RS scoring skipped → deny monitor

1. Ring buffer sibling to warm cycles (e.g. `rs_score_cycle_log.py`) or fields on warm cycle if timestamps aligned.  
2. Each RS scan: list symbols with `exclusion_reason in (missing_candles_or_min_bars, exception, …)` + counts.  
3. API: extend `GET /market-data/candle-warm-cycles` **or** add `GET /market-data/rs-score-cycles`.  
4. UI: `frontend/public/candle-warm-deny.html` — second section **“RS scoring skipped this cycle”** (distinct from candle-warm deny rows).  
5. Visibility only — no new retry beyond existing cache/warm behavior.

---

## 6. Step 5 — Timing / rate-limit regression

**Expect:** Full-universe **persist** adds DB write (~200 rows) + hysteresis sort; scoring already runs on 200. No new Upstox calls in `cache_only`.

**Verify on paperclip (shadow on):**

| Metric | Before | After (shadow) |
|--------|--------|----------------|
| RS `duration_sec` from existing INFO log | baseline | + persist |
| End-to-end warm 10m job elapsed | baseline | unchanged if RS separate |
| RS skip rate | — | track |

**Pass criteria (proposed):** RS cycle p95 **&lt; 60s** wall clock with margin vs 5m (or 10m) interval; no increase in Upstox candle deny % vs prior week same session shape.

---

## 7. Phased delivery (shadow → cutover)

### Phase S0 — Schema + shadow writer (safe deploy)

- Create `rs_universe_score_snapshot` + indexes + retention.  
- Persist full scored set each RS scan; compute/log hysteresis ranks (`B20`) but **do not** change Top-10 consumers.  
- RS-skip section on deny page.  
- Feature flag: `RS_UNIVERSE_SCORE_SHADOW=1`.

### Phase S1 — Observation (1–3 sessions)

- Confirm row counts ~200/scan, duration, skip section volume.  
- Run hysteresis churn backtest on accumulated shadow (or 07-27–31 replay if backfill script added).  
- Review bonus size.

### Phase C1 — Cutover (separate PR, after review)

- Point grade LOCF / grade-is-the-gate readers to universe table (raw grade).  
- Point lock Top-5 / R2 to `rank_membership` / `in_top5_membership`.  
- Optionally stop truncating `relative_strength_snapshot` **or** make it a view of membership Top-10 for backward compat.  
- Update RSCD / journey copy that assumes “absence = beyond Top-10”.

**Not in scope:** READY NOW promotion criteria, grade formula/thresholds, tip-stale candle math.

---

## 8. Diff sketch (S0 — for review)

```
backend/services/rs_universe_score_snapshot.py   # NEW: ensure table, persist_universe, hysteresis rank
backend/services/relative_strength_scanner.py    # hook after _rank; keep Top-10 _persist
backend/services/rs_score_cycle_log.py           # NEW: ring buffer for skips + duration
backend/services/smart_future_algo.py            # optional: pass RS skip summary
backend/routers/market_data.py                   # expose rs-score-cycles or extend warm cycles
frontend/public/candle-warm-deny.html            # “RS scoring skipped” section
backend/config/settings.py                       # RS_UNIVERSE_SCORE_SHADOW, INCUMBENT_RS_BONUS=0.20
docs/diagnostics/rs_universe_score_shadow/README.md
scripts/backtest_rs_top10_hysteresis_20260727_31.py  # churn B0 vs B20 (needs shadow or replay)
```

No consumer cutover in S0 → **no trade_take / grade-gate behavior change** until C1.

---

## 9. Review decisions needed from you

1. **Cadence:** Keep **5m** RS (recommended) vs force **10m**?  
2. **Bonus:** Accept **`INCUMBENT_RS_BONUS = 0.20` RS pts** as shadow default, subject to churn backtest?  
3. **Neutrals:** Persist NEUTRAL rows every cycle (recommended for “every symbol gets a row”) vs only directional?  
4. **Cutover of lock/R2:** Same PR as grade consumers, or grade-first then lock later?  
5. **Audit:** Keep lock 10m audit as shadow indefinitely, or schedule retirement after grade cutover?

---

## 10. Bottom line

- Scoring all 200 is **already done**; the fix is **persist + membership view + monitoring**, not new candle API load.  
- **Shadow-first** via `rs_universe_score_snapshot` avoids repeating Top-10 entrenchment while R1/R2/UI keep working.  
- Proposed hysteresis: **+0.20 RS percentage points** for incumbents on membership sort only; raw RS/grade untouched; backtest churn before cutover.  
- Audit ≠ RS (10m vs 5m); full-universe RS fixes **grade coverage**; lock audit retirement is optional and separate.  
- Ready to implement **Phase S0** on approval of decisions in §9.
