# Kosmic Tarang — premise test (pre-registered)

**Registered:** 2026-09-20 IST  
**Git commit SHA at registration:** `36d7585` (`Label CRUDEOILM EOD backtests as estimated underlying and split NOT_EVALUABLE days.`)  
**Product:** Kosmic Tarang  
**Status:** Structures and metric locked **before** any P&amp;L. Extra runs of this same list are logged below; structures are not added after seeing results.

Live rules are **not** in scope for this test and must not change: min credit 0.20, ENERGY ₹5,000 / ₹10,000, DTE windows, time stop, Phase 4 locked.

## Metric (locked)

**Mean net P&amp;L per independent expired cycle at 10% per-leg cost** (per-leg fill at mid ± 10% of mid, plus venue fees).

Gross is always shown beside net. Do **not** headline profit factor or max drawdown when a structure has fewer than 10 trades.

## Decision rule (locked)

A structure is **supported** for further work only if all of the following hold on expired cycles:

1. ATM IV (traded quotes only) exceeds RV from entry to expiry **on a majority of entry days**, reported in vol points and in variance terms.
2. Mean net P&amp;L per cycle at **10% per-leg cost** is **positive**.
3. The **narrowest valid** version of the structure has max loss per lot **within** the ₹5,000 default and ₹10,000 hard cap (zero-credit ceiling = width × multiplier). Structures that fail this are **rejected**, not resized by raising budgets.
4. Independent expired-cycle count is stated. Fewer than 10 trades: report numbers, **no** PF/DD headline, **no** go-live claim.

Otherwise the options (without changing live rules) are: change which **research** structure is studied next, **defer MCX**, or **focus Delta**.

## Structures (locked — do not add after results)

All MCX entries use DTE 7–35. Delta (if snapshots exist) uses DTE 3–7. Sizing, fee gate, and risk caps match live. One unit floor is live `floor_units`. Ungated = every DTE-window day with feasible **traded** legs. Gated = live IV / min-credit / fee gates (defaults unchanged).

| ID | Structure | Shorts | Wings | Notes |
|---|---|---|---|---|
| A | Iron condor | 10–16 \|δ\| (CL profile) | Profile `width_steps_min` (CL: 5) | Baseline; live construction |
| B | Iron condor | ~20 \|δ\| (band 0.18–0.22) | ~10 \|δ\| longs | Research only |
| C | Iron condor | ~30 \|δ\| (band 0.28–0.32) | ~15 \|δ\| longs | Research only |
| D | Iron butterfly | ATM short straddle (nearest traded strikes to F) | ~15 \|δ\| longs | Research only |
| E-A | Put credit spread and call credit spread | Same shorts as A | Same wings as A | Single-side of A |
| E-C | Put credit spread and call credit spread | Same shorts as C | Same wings as C | Single-side of C |

No other structures.

## Cost grid (locked)

Per-leg modelled spread as **0 / 5 / 10 / 20% of mid**, plus fees. Headline metric uses **10%**.

## Exits (locked)

- **Hold-to-expiry:** flatten on last session with traded marks before expiry (expiry-day options skipped).
- **Rule-exit (condors / credit spreads):** short-δ stop = `min(2 × \|entry δ\|, 0.45)`; profit target **50%** of max profit and, second level only, **25%** of max profit.
- **Rule-exit (butterflies):** stop if underlying moves from entry by **1.0 × σ** of entry ATM IV (T to expiry, Black-76 DF=1) **or** unrealized loss ≥ **50%** of max loss; profit targets **25%** and **50%** of max profit (two levels only).
- Time stop remains the live profile value (MCX 5 DTE) for rule-exit; hold-to-expiry ignores it.

## IV and RV (locked)

- ATM IV and strike IV from **traded** rows only (volume &gt; 0, non-placeholder close).
- RV from entry to expiry: log returns of the **mapped FUTCOM contract only** (never stitch months). Days with futures volume &lt; 500 lots are flagged and excluded from that contract’s RV window.
- Report (IV − RV) in **vol points** and **variance** (IV² − RV²), per entry day and averaged per cycle.

## Caps

CRUDEOILM multiplier = 10. NATGASMINI multiplier = 250 (from specs if no NG bhavcopy). Reject any structure whose narrowest valid width implies zero-credit max loss &gt; ₹10,000 hard cap.

## Delta premise

Same structures from `tarang_chain_snapshots` + hourly candles when present. State expired snapshot cycle count (may be zero). Weekly update; do not invent cycles.

## Extra runs

| When | SHA | Note |
|---|---|---|
| 2026-09-20 | 36d7585 | Registration only. No P&amp;L yet. |
| 2026-09-20 | f1abdbe | Pre-register commit of this file. Structures unchanged. |
| 2026-09-20 | 2c69185 | First P&amp;L run on production FUTCOM. IV exceeded RV on 62% of 1330 entry days (mean IV−RV +2.3 vol pts). Mean net per cycle at 10% per-leg cost was **negative** for every locked structure. Narrowest max-loss per lot is within ₹5k/₹10k (A–E). Live rules unchanged. |
| 2026-09-20 ~22:00 | 634d617 | Follow-up **pre-register** (calibrated δ schedule, one-entry ~26 DTE, mean-within-cycle, IV−RV quartiles, Δ-F/G/H, kill-criteria draft). No P&amp;L in that commit. |
| 2026-09-20 ~22:15 | a862ee0 / follow-up run | After registration: NATGASMINI and 2025 FUTCOM files **not found** (crude-only, 13 expired cycles). One-entry + 1-lot cap-compliant: uniform 10% still **negative** for A–D; calibrated (Delta-measured half-spreads 0.8–7.7% of mid) still **negative** for A–D. E-C-PE one-entry calibrated mean **+₹907** (n=12; exploratory, not a live change). Prior −₹51k “cycle” was the **sum** of many days + up to 2 lots. Date Wise 20 Sep file compared (257 rows; definition mismatch vs summed expiry volumes). |

---

## Follow-up registration (2026-09-20 ~22:00 IST) — before new P&amp;L

**Prior SHA:** `8843f5b` (premise cache/UI `cc50e8f1`).  
**Live rules still not in scope.** No new MCX UI / jobs / scrapers. Monday 21 Sep 2026 MCX session has **not** happened yet: Delta bid–ask is calibrated from live tickers/snapshots **now**; MCX uses existing snapshots (if any) plus EOD traded rows as a **proxy**. Monday MCX live bid–ask is **pending**.

Locked A–E (and E variants) are unchanged. Extra Delta structures below are **new research IDs**, registered here before any P&amp;L on them.

### Aggregation (locked for this follow-up)

The independent unit remains the **expired cycle**.

1. **One entry per cycle:** the qualifying DTE-window day nearest **26 DTE** (MCX 7–35; Delta 3–7). If none at 26, the first qualifying day in the window.
2. **All entry days:** within a cycle, take the **mean of per-day P&amp;L** (not the sum). The first P&amp;L run summed every qualifying day in the cycle; that is **not** the cycle unit and inflated losses versus caps.

Hold-to-expiry and rule-exit remain as registered above. Headline metric stays **mean net P&amp;L per cycle**. Report **gross (zero cost)** beside **net**. Do **not** headline PF/DD under 10 trades.

### Cost (this follow-up)

- **Uniform robustness (unchanged grid):** per-leg 0 / 5 / 10 / 20% of mid + fees. 10% remains the locked headline for comparison with the first run.
- **Calibrated schedule (new, research only):** per-leg half-spread as a function of \|δ\| bins **ATM / 30 / 15 / 10 / 5**, from measured live bid–ask as % of mid. Delta India: live tickers now. MCX: snapshots if present, else EOD reconstructed mids as proxy until Monday session. Fill: sell at mid×(1 − half-spread), buy at mid×(1 + half-spread). Does **not** change live `paper_fills.spread_fraction`.

### Rule-compliant cap sizing (this follow-up)

Live skip-if-does-not-fit is `floor_units(ENERGY budget ₹5,000, max_loss_per_lot) < 1`. This follow-up **recomputes** with:

- 1 lot only if zero-credit (and credit-adjusted) max loss per lot ≤ ₹5,000 default; skip the day otherwise.
- Also report skip counts vs the ₹10,000 hard cap.
- Record lots used, max loss per lot, skip counts.

This does **not** change live `floor_units` or budgets. It is a research recompute so cycle P&amp;L is comparable to the caps.

### Exploratory: IV−RV quartile-at-entry (PRE-REGISTERED — do not select parameters)

**Label: EXPLORATORY.** Not a live gate. Not a structure filter. Do **not** pick DTE, width, or cost from the result.

- Universe: locked A–E, one-entry-per-cycle **and** all-days (mean-within-cycle), rule-compliant sizing, both uniform 10% and calibrated cost.
- Feature: (ATM IV − mapped-contract RV) in **vol points** at entry, traded quotes only.
- Quartiles: sample quartiles of that feature across all entry days in the run (ties to nearest bin). Each cycle contributes to the quartile of **its chosen entry** (one-entry basis) or to the **mean of its days’ quartile index** only as a diagnostic — primary report is **cycle-clustered**: for each quartile Q1–Q4, the list of cycle P&amp;Ls whose entry fell in that quartile.
- Intervals: bootstrap of the **cycle** means (same seed 36 as the locked harness), not of pooled days.
- Output: n cycles, mean/median/worst net, frac IV&gt;RV inside the bin. No “best quartile → go live” claim.

### Extra Delta structures (registered before P&amp;L)

Delta DTE 3–7. Maker (limit at mid) fees. Same exits as locked. Exploratory relative to live Delta gates (gates **unchanged**).

| ID | Structure | Shorts | Wings | Why registered |
|---|---|---|---|---|
| Δ-F | Iron condor | ~25 \|δ\| (0.22–0.28) | ~10 \|δ\| longs | Higher credit than A |
| Δ-G | Iron condor | ~40 \|δ\| (0.35–0.45) | ~15 \|δ\| longs | Wider / higher credit |
| Δ-H | Iron butterfly | ATM shorts | ~25 \|δ\| longs | Wider fly than D |

### Delta fee view (research)

Model **limit orders at mid** = maker (0.010% of notional, 3.5% premium cap, +GST), vs taker 0.03% as the live paper default. Report fee as **% of structure credit** by ID and width from **current** snapshots. Does not change live `assume_taker_in_paper`.

### Delta counters

Report: expired **forward-test** cycles so far; **first expected expiry date** among open snapshot cycles. Weekly. Do not invent expired cycles.

---

## Kill criteria — **PENDING OPERATOR APPROVAL**

Not in force. Numbers below are **options**, not approved thresholds. Live rules are unchanged until an operator accepts a version of this section.

### Closing MCX research (options)

Any one of these, after operator tick:

1. **Sign:** After the current 13 expired CRUDEOILM cycles **plus** any NG cycles once files exist, mean net per cycle at **calibrated** cost **and** at uniform 10%, on **one-entry / rule-compliant** sizing, is still ≤ 0, **and** the cycle-bootstrap 95% upper bound is ≤ 0. Option: require this on **two** consecutive extra-run SHAs.
2. **Sample ceiling:** Still negative after **20** expired MCX cycles (crude+NG combined) or after **2025 FUTCOM + NG** have been imported and the one-entry book recomputed, whichever comes later.
3. **Data:** Date Wise checksum still **pending** four calendar weeks after 20 Sep 2026 **and** (1) still holds.
4. **Caps:** Even with 1-lot rule-compliant sizing, worst one-entry cycle net &lt; −₹10,000 (hard cap) on a majority of structures.

Suggested operator choice: **(1) AND (2)** — do not close on 13 cycles alone if NG/2025 FUTCOM are still missing.

### Delta go / no-go at ~26 expired weekly cycles (options)

Evaluate on expired **forward-test** cycles only (snapshot+hourly), maker (mid) fees, calibrated Δ-spread, structures A–E plus Δ-F/G/H as registered. Options:

- **Go (research continue / consider paper):** ≥26 expired weekly cycles, mean net per cycle &gt; 0 at maker+calibrated for at least one of A, Δ-F, Δ-G, bootstrap 95% lower bound &gt; 0, worst cycle within ENERGY hard cap at 1 contract.
- **No-go:** ≥26 expired cycles and mean net ≤ 0 on all registered Delta structures, or bootstrap upper bound ≤ 0 on the best of them.
- **Defer:** fewer than 26 expired cycles (current state). First expiry date is reported each week; do not pull this trigger early.

Operator must pick go/no-go numbers; **26 is a suggested count**, not approved.
