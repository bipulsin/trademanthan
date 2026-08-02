# Grade/Score Instability — Fresh-but-Flapping (2026-08-02)

**Status:** diagnosis only — no live change.  
**Distinct from:** LOCF/staleness bug (already fixed). Here `rs_score_stale_minutes≈0` still flaps.

## Verdict

Two layered causes — **not a random race**, and **not intentional noise**:

1. **Bug (dominant in 09:15–~10:55):** live 10m path requires **10 closed session 10m bars** before `ema10` exists. Until then `ema10=None` → stretch % cannot be computed → **stretch penalties silently skipped** → inflated A+/A grades. RSS/scanner path has EMA10 earlier and **does** apply stretch → D!. Observing both (or alternating sources) looks like A+↔D! on a “fresh” symbol.
2. **By design (secondary, all day):** stretch soft/hard cliffs at **0.35% / 0.50%** of price vs nearer of EMA10/VWAP, plus discrete score/purity/volume bands. Small price moves can flip A↔D! even with flat-ish price once stretch is active.

---

## What feeds Confidence Grade / Trade Score

| Layer | Inputs | Role |
|---|---|---|
| **Trade Score** | RS (40), Kavach state (30), vol_ratio (15), ADX (10), VWAP side (5) | Stepped bands — can jump 5–30 pts on threshold crosses |
| **Grade banding** | post-stretch score + `volume_label` + VWAP purity≥60% + regime | Maps to A+/A/B/C/D |
| **Stretch** | `min(\|close−EMA10\|, \|close−VWAP\|)/close×100` | Soft `>0.35` → −20 score + 2 letter steps + `!`; Hard `>0.50` → −50 + force **D!** |

There is **no grade hysteresis**. Recomputed every scan/closed bar.

Canonical formula: `docs/diagnostics/checkpoint_22jul_followup/grade_and_trade_score_formula_reference/README.md`

---

## Root cause #1 — live `ema10` warm-up disables stretch (bug)

```167:173:backend/services/rs_conviction_signals.py
def ema10_10min(candles: List[Dict]) -> Optional[float]:
    """EMA(10) of 10-min closes for passive exit reference."""
    today, _ = _today_slice(candles)
    closes_10m = _aggregate_10m_closes(today)
    if len(closes_10m) < 10:
        return None
    return ema_series(closes_10m, 10)[-1]
```

Session 10m bar ends: 09:25…10:55 = **10th bar**. Until ~10:55 IST, live stretch often cannot run.

**Prod evidence (`kavach_stretch_penalty_log`, live_10m, 07-27…31):**

| Hour IST | rows | ema10 NULL | stretch NULL | penalized |
|---------:|-----:|-----------:|-------------:|----------:|
| 9 | 634 | **100%** | **100%** | 0 |
| 10 | 1151 | **89%** | **89%** | 58 |
| 11+ | … | **0%** | **0%** | normal |

**Confirmed cases 2026-07-30 (live audit vs RSS):**

| Symbol | Live 09:35–10:35 | RSS same window |
|---|---|---|
| HEROMOTOCO | A+ / A, ema10=None, ts=95→85 | D! ts=35, stretch≈0.55–0.93 |
| PERSISTENT | A+ / A, ema10=None, ts=95–98 | A→D!→…→A→D!, stretch crosses 0.35/0.50 |
| WIPRO | C→A (purity 50→80), ema10=None | D / D! with stretch |
| OIL | B (stable-ish live) | D! then C when stretch eases |

Cross-source (RSS matched to live within 7m, morning 09:25–10:55, week):

| Metric | Value |
|---|---:|
| Matched pairs | 592 |
| Disagree by ≥2 letter steps | **59.8%** |
| Live A+/A/B vs RSS D/D! | **54.9%** |
| Live ema10 NULL in match | **96.1%** |

So the “A+→D!→A+” pattern in the early window is largely **two formulas disagreeing**, not one formula randomly oscillating — plus RSS itself cliffing when stretch is on.

---

## Root cause #2 — stretch cliffs (expected sensitivity)

Once EMA10 exists, stretch at 0.35%/0.50% is a **hard discontinuity**. Example: PERSISTENT 10:35 RSS grade **A** (stretch≈0.084) → 10:41 **D!** (stretch≈0.351, just over soft). Price +0.4%.

High+pure+raw≈85–95 → A/A+; soft stretch −20 + 2 letter steps → often **D!** without any “bug” in banding logic.

Purity cliff at 60% and stepped RS/ADX/vol also contribute (e.g. WIPRO live C→A when purity 50→80) but stretch explains most dramatic A↔D! flips.

---

## Prevalence (07-27…31, RSS board — fresh consecutive scans ≤25m apart)

| Metric | Value |
|---|---:|
| Symbol-days with ≥2 scans | 683 |
| Symbol-days with ≥2-letter flip on consecutive fresh scans | **189 (27.7%)** |
| Of those, A+/A ↔ D style | **108 (15.8% of sym-days)** |
| Consecutive pairs | 4395 |
| Pairs with ≥2-letter flip | **350 (8.0%)** |
| Of flip pairs, stretch-threshold attributed | **64%** |
| Flat price (\|Δpx\|&lt;0.2%) + flip2 | 134 pairs; **52%** stretch-threshold cross |

Stretch-log same-bar cliff (pre A/B → post D via stretch): **15.1%** of stretch-log rows.

**Conclusion:** widespread — systemic reliability issue for any consumer that treats instantaneous grade as stable strength, independent of LOCF.

---

## Implication for live SQ (≥75)

SQ gates on `grade_ab_ok` (A+/A/B, no `!`) and uses trade_score in Total (`0.15×… + Grade_Bonus`).

- Morning live path: **missing stretch** can let A+/A through and inflate trade_score → easier Total≥75 on a “good tick” that RSS would mark D!.
- RSS/universe path: stretch can **collapse** grade to D! and cut score −20/−50 → Total drops; next bar under soft threshold → A again.
- Either way, instantaneous grade is a **noisy promotion input** in the first ~1.5h and remains cliff-sensitive all day.

Do **not** trust this week’s early-window SQ promotions as “stable A-grade strength” until stretch is correctly applied on the live path and/or grade is debounced.

---

## Recommendations (no deploy yet)

**P0 — fix the bug (not smoothing):** Seed live `ema10` from prior-session 10m EMA (same idea as EMA5 SQ seed), or lower the cold-start gate, or compute stretch from 5m EMA10 when 10m EMA10 is unavailable. Goal: stretch never silently no-ops because `ema10 is None`.

**Status 2026-08-02 evening:** **P0 shipped** — see `EMA10_COLDSTART_FIX_20260802.md` (exact match vs RSS 24%→88% on confirmed-case window; live-high-vs-RSS-D 76%→0%).

**P1 — stabilize grade for promotion (debounce):** For SQ / READY promotion only, require **2 consecutive fresh** same-side grade bands (e.g. both A+/A/B without `!`, or both failing) before changing the grade used for Grade_Bonus / `grade_ab_ok`. Instantaneous display can stay live. **Held for explicit sign-off — not built.**

**P2 — optional stretch hysteresis:** Small deadband around 0.35/0.50 (e.g. enter soft at 0.38, exit at 0.32) to stop single-tick thrash when price is flat near the cliff. **Held — by-design change, needs separate discussion.**

**Not recommended as first move:** Blaming “ADX/volume noise” alone, or adding LOCF-style staleness logic — this is a different root cause.

---

## Scripts

- `scripts/_diag_grade_flap_20260730.py` — RSS/stretch/prevalence
- `scripts/_diag_grade_flap_part2.py` — live vs RSS morning disagreement + flat flips
