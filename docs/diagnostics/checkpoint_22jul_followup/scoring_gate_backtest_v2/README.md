# Scoring / gate backtest v2 (READY suppression + DI-only override)

**Status:** complete (offline / read-only). **No live ranking, lock, FSM, gating, or production instrumentation was changed.**

**Data window:** consistency log `2026-07-15` → `2026-07-24` (n=3786). **2026-07-25 has no session rows** (weekend / no log). Part 1 uses `2026-07-20`→`2026-07-24`. Part 2 extends back to `2026-07-15` for sample size.

Script: `scripts/backtest_scoring_gate_v2.py`  
Artifacts:

| File | Contents |
|---|---|
| `00_manifest.json` | Sessions, row counts, auto recommendations |
| `part1_ready_suppression.json` | ATR≥thr + not-progressing → READY→WATCHING variants |
| `part2_direction_imbalance_override.json` | DI-only soft override + EMA-gap filters |
| `recommendations.json` | Machine summary used by this README |

Exit model (unchanged from v1): entry at rendered price; stop / EMA10 before 1R / EMA5 after 1R / 15:15; 1 NSE lot.

---

## Decision table (read this first)

| Part | Verdict | Best params | Why |
|---|---|---|---|
| **1 — ATR READY suppression** | **GO — live display-only at 85% + not progressing, with continued shadow logging alongside** | **85%** + progression not increasing | Suppresses a clearly negative cohort; **0 false-neg** on 07-24 clean trenders. Do **not** touch ranking/lock. Ship display-only NOW; keep logging `atr_ready_suppress_*` for weekly review. |
| **2 — DI-only A/A+ override** | **NO-GO / shadow-only until ~n≥30** | — | n=10 first trades; **median −1.0R**, WR 40%, edge outlier-driven. Need **~8 more trading sessions** (~12 total). Log `would_override_di` only — no live Take Trade override. |

---

## Part 1 — ATR-consumption as READY-family suppression (not ranking)

**Rule:** for checklist/READY path symbols, each 10m-tied render: if `atr_consumed_pct ≥ threshold` **and** progression is **not** increasing → treat as WATCHING (suppress READY pill only). Ranking, morning lock, and promotion untouched.

### Suppression counts (all READY-family renders, 07-20→07-24)

| Threshold | READY family | Suppressed renders | Top symbols (count) |
|---:|---:|---:|---|
| **75%** | 870 | **148** | ICICIGI 44, PGEL 17, AUBANK 15, BAJAJ-AUTO 12, SBILIFE 12 |
| **80%** | 870 | **133** | ICICIGI 32, PGEL 17, AUBANK 15, BAJAJ-AUTO 12, SBILIFE 12 |
| **85%** | 870 | **121** | ICICIGI 32, PGEL 17, AUBANK 15, SBILIFE 12, GODREJCP/DLF 9 |

### Were suppressed READY renders bad trades?

Two views (same exit rules):

| Threshold | All suppressed renders (noisy; many re-renders of same path) | First render per symbol×session (trade-realistic) |
|---|---|---|
| 75% | n=131 sim, WR **42.7%**, avg **−0.18R**, total **−23.3R** | n=12, WR **33.3%**, avg **−0.19R**, median **−0.81R**, **−₹2,379** |
| 80% | n=116, WR **37.9%**, avg **−0.27R**, total **−30.7R** | n=11, WR **27.3%**, avg **−0.25R**, median **−0.73R**, **−₹2,450** |
| **85%** | n=109, WR **38.5%**, avg **−0.30R**, total **−32.9R** | n=10, WR **20.0%**, avg **−0.40R**, median **−0.81R**, **−₹4,996** |

**Separation (all-render sims):** 85% best — 67 negative-R vs 42 positive-R renders; separation_score **32.86** (vs 30.7 @80, 23.3 @75).

### ICICIGI (2026-07-24)

| Threshold | First suppress IST | Of 69 READY | Suppressed-render sim (all) |
|---|---|---:|---|
| 75% | **11:15:25** | **44 / 69** | WR 72.7%, avg +0.29R — **too early**; chops out mid-morning path that still printed small wins to square-off |
| 80% | **12:50:04** | **32 / 69** | WR 62.5%, avg +0.08R (many duplicate square-off paths) |
| **85%** | **12:50:04** | **32 / 69** | same first kick as 80%; first kick trade itself exits **−0.60R** (EMA10) |

Interpretation: 75% over-suppresses ICICIGI. 80/85 kick at the same bar; use **85%** to avoid earlier false chops on other names while keeping the same ICICIGI start.

### False-negative risk (clean trenders 07-24)

Checked: **COFORGE, MPHASIS, KPITTECH, TATAELXSI, ASTRAL**.

| Threshold | Genuine READY renders suppressed |
|---|---:|
| 75 / 80 / **85%** | **0 / 0 / 0** |

Key risk ruled out in this window: progression filter keeps extending trends off the suppression path.

### Part 1 recommendation

**GO — live display-only at 85% + not progressing, with continued shadow logging alongside.**

- Best threshold: **85%** (cleanest separation + same ICICIGI kick as 80% without 75%’s early cut).
- Scope: pill/render only; **never** lock/rank/promotion.
- Live: READY-family → WATCHING when atr ≥85% and progression not increasing (`ATR_READY_SUPPRESS_LIVE`, default on).
- Shadow: keep logging `atr_ready_suppress_fired` / `atr_consumed_pct` / `atr_progression_increasing` into consistency_log.inputs; run `scripts/report_atr_ready_suppress_weekly.py` weekly.

---

## Part 2 — direction_imbalance-only override (warning_stack stays blocked)

**Rule under test:** grade A/A+ READY, hard gates pass, soft blocker is **only** `direction_imbalance` → allow Take Trade. `warning_stack` never overridden (v1 already killed that path).

### Funnel (renders 2026-07-15→24)

| Stage | n |
|---|---:|
| Render rows | 3786 |
| Grade A READY-family | 562 |
| Already take-enabled | 53 |
| Hard-blocked | 125 |
| warning_stack-only (skipped by design) | 244 |
| **direction_imbalance flip candidates** | **43** |
| Other soft / none | 97 |

DI flip **episodes** (contiguous): **11**. First-trade sims: **10** across **4 sessions** (`2026-07-20`…`23`). No DI-only first trades on 07-15…19 or 07-24 in this cut.

### Sample-size projection to n≥30

| Metric | Value |
|---|---|
| First trades | 10 |
| Sessions with DI flips | 4 |
| Trades / session | ~2.5 |
| Sessions needed for n≈30 | **~12** |
| **Additional sessions needed** | **~8** |

### Cohort results (gap filter = 0)

| Metric | Value |
|---|---|
| n | 10 |
| Win rate | **40.0%** |
| Avg R | **+2.66** (outlier-pulled) |
| **Median R** | **−1.0** |
| Total R / ₹ | +26.6R / +₹4,611 |
| Exits | 6× stop (−1R), 4× EMA5 after 1R |

Largest win: SBILIFE LONG **+17.5R** (13:58). Without outliers, median stays at −1R.

### Breakdowns (gap=0)

| Slice | n | WR | Avg R | Median R | Notes |
|---|---:|---:|---:|---:|---|
| LONG | 4 | 25% | +3.62 | **−1.0** | ₹ −2,804 |
| SHORT | 6 | 50% | +2.02 | +0.51 | ₹ +7,415 — better but still small |
| Grade | all A | — | — | — | no A+ in cohort |
| Hour 12 | 4 | 50% | +1.96 | +0.51 | best hour; still thin |
| Hour 10/14 | 1 each | 0% | −1.0 | −1.0 | — |

### EMA5–EMA10 gap filter (structural)

| Min gap % of price | n | WR | Avg R | Median R | ₹ |
|---:|---:|---:|---:|---:|---:|
| 0 | 10 | 40% | 2.66 | **−1.0** | +4,611 |
| 0.05 | 8 | 50% | 3.45 | **0.01** | +8,540 |
| **0.10** | **5** | **60%** | **2.63** | **+2.02** | +7,492 |
| 0.15 | 4 | 75% | 3.54 | +3.66 | +8,553 |

Gap ≥0.10% **looks** better but **n≤5** — cannot green-light live from this.

### Part 2 recommendation

**NO-GO for live / shadow-only until ~n≥30** (with or without gap filter).

- Direction_imbalance-only is still promising vs warning_stack, but **median −1R at n=10** is not an implementable edge.
- **Next step:** keep soft-hold as today; persist **shadow-only** `would_override_di` for **≥8 more trading days** (target n≥30 first trades). Re-run the v2 backtest; only then decide. If after n≥30 median R stays ≤0, **abandon** the override.

---

## Validation / caveats

- No `2026-07-25` consistency-log session (max date `2026-07-24`).
- ATR progression uses production `atr_consumed_pct_from_open` when present; else replay from day open × `rs_scanner_history.atr14_pct` (same approx as v1).
- Part 1 “all renders” over-counts the same forward path; prefer **first per symbol×session** for trade decisions.
- Part 2 DI classification depends on disable-reason / zone_downgrade text — same heuristic as v1.

---

## vs v1

| Idea | v1 | v2 |
|---|---|---|
| ATR decay on ranking/lock | **−38.5R — dead** | Not retested |
| ATR as READY suppress | — | **GO @ 85% live display-only + shadow logging** |
| Blanket A/A+ soft override | WR 33%, median −1R — dead | — |
| warning_stack override | avg −0.18R — dead | Dropped |
| direction_imbalance override | n=9, avg +2.7R | n=10, median −1R — **still NO-GO; need ~8 more days** |
