# EMA `ema_reliable` 6-bar buffer — justify / remove (2026-08-02)

## Verdict

**Remove the buffer** (`EMA_RELIABLE_AFTER_BARS = 0`). With prior-session EMA seeding, EMA5 is mathematically exact from session bar 1. The 6-bar gate was an undocumented conservative guess and was incorrectly suppressing real early EMA5/VWAP crossovers (esp. 09:35–10:35).

## Why was 6 chosen?

**No convergence analysis.** It was introduced with SQ v1.2 as a “convergence window” alongside prior-session seeding — folklore (~EMA5 period + 1, or ~1 hour of 10m bars), not a measured delimiter. Repo docs state the rule without justifying the number.

## Is seeded EMA exact from bar 1?

Yes, for the recursive close EMA:

```
ema_t = k * close_t + (1-k) * ema_{t-1},  k = 2/(period+1)
ema_0 = prior_session_final_EMA5
```

That is the definition of EMA continuation. There is **no numerical warm-up** and no second “stabilization” pass required when the seed is the true prior final EMA.

The gap that shrinks over ~6–10 bars is **cold-start vs seeded**, not “seeded vs truth”:

| session bar | median \|seeded − cold\| (clean-10m sample) |
|------------:|---------------------------------------------:|
| 1 | 4.16 |
| 3 | 1.85 |
| 6 | 0.55 |
| 10 | 0.11 |

Cold-start needs bars to approach the seeded series; **seeded does not need bars to approach anything**. Live path previously cold-started EMA5 in `enrich_session_10m_bars` — that is fixed by seeding from prior-session 10m closes (same idea as the backtest).

Fallback when history is missing: still `ema_reliable=True` from bar 1. `start_aligned` is already gone, so bar 1 only seeds `prev_side` (no free EW=100).

## Confirmed cases (clean native-10m, no `start_aligned`)

### BANDHANBNK 2026-07-30 (LONG)

| buffer | 09:35 EW | early behavior |
|-------:|---------:|----------------|
| 6 | 0 | blocked |
| **0** | **0** | 09:45 bearish cross opposite side — stays 0; first EW=100 at **11:25** |

Does **not** reintroduce the start_aligned false credit.

### M&M / TVSMOTOR 2026-07-31 (LONG)

Genuine bullish cross at **09:45** (2nd bar):

| symbol | buffer=6 | buffer=0 |
|--------|----------|----------|
| M&M | EW stuck 0 all early window (cross during unreliable; first reliable bar only re-seeds) | **EW=100 from 09:45** |
| TVSMOTOR | same miss | **EW=100 from 09:45** |

Buffer=6 does not merely delay credit — because unreliable bars still update `prev_side` while `first_eval` stays true, the real early cross is **lost** when reliability finally turns on.

## Early-window impact (09:35–10:35)

All clean-10m symbol-days 2026-07-27…31 (either direction, EW>0 somewhere in window):

| buffer | early EW>0 rate |
|-------:|----------------:|
| 6 | 9.5% |
| **0** | **63.4%** |

(Top-6+A/B eligible-only rates in the earlier diagnosis were ~0% → ~27% with correct side; same direction of effect.)

Mean Total lift in that earlier eligible cohort: ~61.7–63.7 → ~69 with EW informative early — still below thr=75 alone in many cases, but no longer zero-EW by construction.

## Code change

- `EMA_RELIABLE_AFTER_BARS = 0` in `structural_quality_score.py` and v1.2 backtest script.
- Live `enrich_session_10m_bars`: prior-session 10m EMA5 seed via `prior_session_10m_ema_seed` + `ema_seeded`.
- `step_ew_v12` still honors `ema_reliable=False` if ever passed (tests keep the gate).

## Threshold

Leave `SQ_PROMOTE_THRESHOLD=75`. This fix restores real early EW; it does not revive free start_aligned credit.
