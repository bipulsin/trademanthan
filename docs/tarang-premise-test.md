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
