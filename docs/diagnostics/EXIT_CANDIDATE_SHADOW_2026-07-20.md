# Exit-candidate shadow — high-R give-back (2026-07-20)

Shadow-log / research only. **No live exit gating changed.** `EXIT_CANDIDATE_LIVE` remains off until the 22-Jul checkpoint.

## Constraint

Trading is single-lot, binary exit only — candidates are full-exit rule changes (faster ratchet / spike-reverse / intrabar EMA5 fast-confirm), not partial profit booking.

## Tunables (shadow defaults)

- `HIGH_R_ARM` = 2.0
- `RATCHET_GIVEBACK_R` = 1.0
- `RATCHET_FLOOR_R` = 1.0
- `SPIKE_RETAIN_R` = 1.5

## Rollup (give-back cohort)

| Candidate | Fired | Mean ΔR vs baseline | Mean bars earlier |
|-----------|------:|--------------------:|------------------:|
| `C1_faster_ratchet` | 4/4 | 0.7574 | 1.0 |
| `C2_spike_reverse` | 3/4 | 1.0099 | 1.33 |
| `C3_intrabar_ema5_fast` | 4/4 | 0.3246 | 0.5 |

## Cases

### FEDERALBNK_20260720 — FEDERALBNK LONG

- Entry `350.1`, risk `0.5` pts, peak **3.7R**, session `2026-07-20`
- Notes: 3.7R peak → round-trip scratch; spike closed above EMA5
- Baseline: `EMA5 reverse close after profit protection` @ bar 4 px=350.4 → **0.600R** (give-back 3.1R from peak)
- `C1_faster_ratchet`: bar 3 px=350.8 → **1.400R** (ΔR +0.8, 1 bar(s) earlier) — Faster ratchet close beyond peak−giveback stop
- `C2_spike_reverse`: bar 3 px=350.8 → **1.400R** (ΔR +0.8, 1 bar(s) earlier) — Spike≥arm then close back ≤ retain R (same/next bar)
- `C3_intrabar_ema5_fast`: bar 3 px=350.8 → **1.400R** (ΔR +0.8, 1 bar(s) earlier) — Intrabar EMA5 pierce + weak/beyond close after ≥arm R

### POLICYBZR_giveback — POLICYBZR LONG

- Entry `1580.0`, risk `3.61` pts, peak **3.4626R**, session `2026-07-16`
- Notes: MAE MFE 3.46R → exit ~0R; missed ~1590 book
- Baseline: `EMA10 reverse close` @ bar 4 px=1580.2 → **0.055R** (give-back 3.4072R from peak)
- `C1_faster_ratchet`: bar 2 px=1585.0 → **1.385R** (ΔR +1.3296, 2 bar(s) earlier) — Faster ratchet close beyond peak−giveback stop
- `C2_spike_reverse`: bar 2 px=1585.0 → **1.385R** (ΔR +1.3296, 2 bar(s) earlier) — Spike≥arm then close back ≤ retain R (same/next bar)
- `C3_intrabar_ema5_fast`: bar 3 px=1582.0 → **0.554R** (ΔR +0.4986, 1 bar(s) earlier) — Intrabar EMA5 pierce + weak/beyond close after ≥arm R

### ADANIGREEN_long_giveback — ADANIGREEN LONG

- Entry `1564.5`, risk `5.0` pts, peak **3.5R**, session `2026-07-13`
- Notes: High-R then EMA10 trail round-trip (research narrative)
- Baseline: `EMA5 reverse close after profit protection` @ bar 3 px=1567.0 → **0.500R** (give-back 3.0R from peak)
- `C1_faster_ratchet`: bar 2 px=1571.5 → **1.400R** (ΔR +0.9, 1 bar(s) earlier) — Faster ratchet close beyond peak−giveback stop
- `C2_spike_reverse`: bar 2 px=1571.5 → **1.400R** (ΔR +0.9, 1 bar(s) earlier) — Spike≥arm then close back ≤ retain R (same/next bar)
- `C3_intrabar_ema5_fast`: bar 3 px=1567.0 → **0.500R** (ΔR +0.0, 0 bar(s) earlier) — Intrabar EMA5 pierce + weak/beyond close after ≥arm R

### CHOLAFIN_round_trip — CHOLAFIN LONG

- Entry `1822.1`, risk `0.8` pts, peak **7.375R**, session `2026-07-15`
- Notes: MAE round_trip peak 7.38R → large loss; stress-test candidates
- Baseline: `EMA5 reverse close after profit protection` @ bar 2 px=1823.2 → **1.375R** (give-back 6.0R from peak)
- `C1_faster_ratchet`: bar 2 px=1823.2 → **1.375R** (ΔR +0.0, 0 bar(s) earlier) — Faster ratchet close beyond peak−giveback stop
- `C2_spike_reverse`: did not fire
- `C3_intrabar_ema5_fast`: bar 2 px=1823.2 → **1.375R** (ΔR +0.0, 0 bar(s) earlier) — Intrabar EMA5 pierce + weak/beyond close after ≥arm R

### TATAELXSI_false_stop_20260720 — TATAELXSI LONG

- Entry `3510.0`, risk `8.0` pts, peak **1.0R**, session `2026-07-20`
- Notes: False stop then continuation to 3532.50 post-exit — candidates must stay silent (<2R) on held bars
- Baseline: `EMA10 reverse close` @ bar 2 px=3500.3 → **-1.212R** (give-back 2.2125R from peak)
- `C1_faster_ratchet`: did not fire
- `C2_spike_reverse`: did not fire
- `C3_intrabar_ema5_fast`: did not fire

## 22-Jul checkpoint flags (no code)

1. **FEDERALBNK**: C1/C2/C3 all fire on the spike candle (close 350.80 ≈ 1.4R) vs baseline EMA5 exit one bar later (~0.6R close / ₹0 fill). Spike-reverse (C2) is the most direct narrative match.
2. **TATAELXSI control**: peak &lt; 2R → all candidates silent on the stop-out window (avoids holding-through-pullback bias from the same-day continuation).
3. **CHOLAFIN / C2 miss**: price ground through 2R without a same/next-bar fade to ≤1.5R, so C2 correctly stayed silent; C1/C3 tied baseline on the dump bar. C2 is spike-fade specific, not a slow-trail replacement.
4. **Live wiring**: `kavach_exit_candidate_shadow_log` + snapshot hook in `evaluate_open_trades` log would-fire only; state machine unchanged. `EXIT_CANDIDATE_LIVE` default off.
5. **Before any live flip**: re-run with Upstox-fetched 10m bars for FEDERALBNK/POLICYBZR/ADANIGREEN; confirm C1 ratchet floor does not chop winners that grind &gt;2R without a spike fade.

_Generated 2026-07-20T11:16:14.432682+00:00_
