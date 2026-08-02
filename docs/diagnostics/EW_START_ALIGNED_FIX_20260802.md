# EW start_aligned removal — live fix (2026-08-02)

## Bug
`step_ew_v12` awarded `EW=100` via `start_aligned` when EMA5 was already on the qualifying side of VWAP at the first evaluated bar — without an observed crossover, and without requiring `ema_reliable=True`.

Confirmed: BANDHANBNK 2026-07-30 09:35 — EMA5≈VWAP (Δ0.067), `ema_reliable=False`, yet `EW=100` / `start_aligned`.

## Fix
1. Removed `start_aligned` → EW=100 shortcut (live `structural_quality_score` + backtest v1.2 + clean refetch path).
2. EW arm/decay only on **observed** EMA5/VWAP crossover in qualifying direction.
3. Bars with `ema_reliable=False` never arm/decay; only refresh `prev_side`.

## Confirmed case: BANDHANBNK 2026-07-30

| bar | ema_reliable | stored EW / event | fixed EW | Total old → fixed |
|-----|--------------|-------------------|----------|-------------------|
| 09:35 | False | 100 / start_aligned | 0 | **86.33 → 71.33** (below thr 75) |
| 09:45 | False | 80 / bearish | 0 | 49.34 → 37.34 |

Would **not** have crossed 75 under the fix at the inflated morning print.

## Live SQ promotions today (2026-08-02 Sunday)
**None.** `sq_ready_promotion_log` empty; no consistency-log SQ flags. Nothing to revoke live.

## Retroactive week contamination (clean-10m, Top-6+A/B, thr=75)
81 symbol-days first-crossed ≥75 under old EW but never under fixed EW (illustrative). Fix is live before Monday session.
