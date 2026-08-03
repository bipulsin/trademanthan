# Garuda/RS Direction Stickiness — Root Cause + 2-Candle Flip Rule

**Session:** 2026-08-03 · FORTIS / CHOLAFIN / BAJAJFINSV / INOXWIND

## Part 1 — Root cause

### Verdict (not an assumption)

| Layer | Sticky at write? | What actually happens |
|-------|------------------|------------------------|
| **Garuda `side`** | **No** — recomputed every closed 10m bar from imbalance (≥2 legs) else VWAP+EMA5/10+ROC vote | FORTIS wrote **SHORT** as early as **10:45 IST** on non-Top-6 rows |
| **RS `ranking_type`** | **No** — recomputed every scan from Kavach bull/bear state | FORTIS did flip **BULLISH→BEARISH** (e.g. ~10:45) but at **grade D!/D** |
| **Consumer LOCF** | **Yes — this is the Fix-3 starvation** | `load_latest_garuda_top6` previously took side from the **last Top-6 row only**. FORTIS last Top-6 was **LONG @ 10:05**; later SHORT bars had `top6_rank=NULL`, so SQ/Fix-3 never saw SHORT |
| **B20 incumbent bonus** | Membership-only | Softens within-side Top-10 churn; **does not** freeze `ranking_type` |
| **Flip-flop gate for Garuda/RS side** | **Did not exist** | SQ VW sub-score is a different path (composite persistence only) |

**Deliberate vs oversight**

- Writer recompute every cycle is **deliberate** (live vote / Kavach state).
- Starving Fix 3 via **Top-6-only side LOCF** is a **consumer gap**, consistent with morning “side inherited from lock/Top-6 entry” diagnostics — not a Garuda freeze, and not B20.
- No existing Garuda/RS flip-flop deterioration rule. Whipsaw control lived elsewhere (SQ VW, READY VWAP quality), not on the directional label itself.

### Evidence (2026-08-03)

- FORTIS Garuda Top-6 sides: **LONG only**. Non-Top-6 SHORT bars: 10:45, 11:35, 11:55, … (7 SHORT bars, 0 Top-6 SHORT).
- CHOLAFIN / INOXWIND: same pattern — morning Top-6 LONG LOCF; later SHORT prints never Top-6.
- BAJAJFINSV: never Top-6; SHORT prints exist but SQ path never had a Top-6 handle.

## Part 2 — 2-candle VWAP confirmation (implemented)

**Rule:** Flip LONG↔SHORT only when candle N closes opposite VWAP vs prior side **and** candle N+1 closes **further** from VWAP on the new side. Confirmed acceleration also **forces** the flip even if the raw vote is still the old side. Unconfirmed raw flips are **rejected** (hold prior).

| Env | Default |
|-----|---------|
| `VWAP_2CANDLE_SIDE_GATE` | ON |

**Wiring**

1. `backend/services/vwap_2candle_side.py` — shared resolve + `directional_side_flip_log`
2. Garuda job — apply before persist; log confirmed/rejected
3. RS universe persist — same gate on `ranking_type` (rebucket bull/bear)
4. `load_latest_garuda_top6` — **rank/score** still LOCF from last Top-6; **side** from **latest any-bar** row (so confirmed SHORT is visible to Fix 3)

### Replay — first SHORT 2-candle confirm (LONG→SHORT)

| Symbol | Flip bar | Confirm bar | vs manual TV |
|--------|----------|-------------|--------------|
| **FORTIS** | 10:45 (966.95 vs 969.25) | **10:55** (966.25, further) | **−20 min** vs 11:15 TV SHORT |
| CHOLAFIN | 09:55 | 10:05 | morning wrong-side LONG; confirm ~20m after promote |
| BAJAJFINSV | 09:45 | 09:55 | same morning dump |
| INOXWIND | 09:55 | 10:05 | demote window after forming tip |

### False-positive / whipsaw (same day, Top-6 universe)

- ~651 two-candle confirms (both directions) across Top-6-touched symbols
- ~46 reversed within the next 2 bars → **~7% whipsaw rate**
- Honest read: 2 candles is **useful** and caught FORTIS before 11:15; not zero-whipsaw. If live noise is painful, next lever is 3-candle or ATR-scaled extension — do not pretend 2 is perfect.

### Before / after for Fix 3

| | Before | After |
|---|--------|-------|
| FORTIS Fix-3 anchor at ~10:55 | Garuda Top-6 LOCF **LONG**; RS BEARISH @ D | Garuda published side can be **SHORT** (confirmed); consumer sees latest side |
| Still required for READY flip | Grade A/B + SQ≥75 + VWAP-side | Unchanged (non-requirements) |

SQ formula / Fix-1 VWAP READY gate untouched.
