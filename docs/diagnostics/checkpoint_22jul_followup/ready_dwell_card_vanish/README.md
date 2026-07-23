# READY NOW card dwell / vanish (8–22 Jul)

**Closed 2026-07-24.** Live fix shipped same day (no shadow period).

Source: production `kavach_ready_consistency_log` + `kavach_ready_dwell_entry_shadow` on paperclip.
Consistency rows exist for **15–22 Jul only** (no 8–14 Jul rows).

## Card dwell distribution (first visible → disappear)

Visibility = `inputs.card_visible` when present, else `rendered_state ∈ {READY, READY(RECHECK)}`.

| Window | Spells | &lt;1m | 1–5m | 5–10m | ≥10m | Median |
|---|---:|---:|---:|---:|---:|---:|
| 15–22 Jul | 142 | 9 | 36 | 20 | 77 | 10.2m |
| Post-live 20–22 Jul | 71 | 3 | 15 | 13 | 40 | — |

## Causes of vanish under 10m

### Pre-live (17 Jul) — soft stack removed cards

Many &lt;5m spells on 17 Jul ended in `zone_downgrade=warning_stack` (e.g. NATIONALUM 1.0m, BSE 1.7m, CUMMINSIND/KEI/KOTAKBANK ~1.8m). That day predates meaningful soft-hold efficacy in the live path.

### Post-live (20–22 Jul) — soft hold mostly worked; distance did not

Vanished spells with measured end (n=47):

| Cause | n | Under 10m |
|---|---:|---:|
| `distance_guard` (entry too close / beyond EMA10) | 29 | **10** |
| Soft (`warning_stack` / imbalance) after floor | 14 | 0 |
| Hard EMA10 close | 3 | 0 |
| Lock removed | 1 | 0 |

**All 10 post-live under-10m vanishings were distance-guard** — e.g. BANKBARODA 2.2m, PNB 3.0m, MARUTI 4.3m, NHPC 4.4–5.3m, AXISBANK 6.6m, WAAREEENER 7.0m, AMBER/PGEL/GODREJCP ~9.5m.

Soft dwell hold after go-live kept cards through the 10m floor when warning_stack fired mid-episode (ASTRAL/AUBANK polls show `READY · dwell hold (warning_stack) — Take Trade disabled`).

## READY_DWELL_ENTRY_LIVE bypass verdict (pre-fix)

| Path | Prevents soft removal? | Notes |
|---|---|---|
| Soft `warning_stack` / `direction_imbalance` / `vwap_quality` | **Mostly yes** post-18 Jul | Restores `trade_state=READY` so UI shows card |
| Mid-dwell **distance** block | **No — bypass** | Cleared dwell + hid card immediately |
| Grade/score natural leave | **No explicit hold** | Fell through to `card_visible=False` |
| Confirmed EMA10 close / lock remove | Intentional hard end | Correct |
| Frontend | Filters on `trade_state` only | `card_visible` alone never shows a card |

Soft “bypass” polls (n=18): first-observation soft with `WAIT FOR PULLBACK` while shadow said `would_extend_dwell` — live restore missed some first-paint soft cases; floor start on first soft READY is now explicit.

## Take Trade visible-but-disabled (concrete)

Polls where card visible and `trade_take_enabled=false` (15–22 Jul):

| Reason bucket | Polls | Verdict |
|---|---:|---|
| `warning_stack` (incl. dwell soft hold text) | 449 | **Correct** — disable Take, card may stay |
| Explicit `dwell_soft_hold` | 44 | **Correct** |
| Entry window closed | 26 | **Correct** |

Examples (correct disable, card held): ASTRAL / AUBANK 20-Jul — `READY · dwell hold (warning_stack) — Take Trade disabled`.

No evidence of a separate “Take Trade missing while READY with no disable reason” bug in this window; disables tracked above are intentional.

## Fix (2026-07-24, live)

`apply_ready_dwell_entry_live` now enforces a true **10-minute visibility floor** after dwell start:

- Soft / distance / natural leave **inside** floor → keep `trade_state` READY + `card_visible`, Take Trade off
- After floor → normal removal
- **Early hide (judgment):** confirmed EMA10 close reverse, lock removal, EXIT NOW / PLAN EXIT, EXPIRED — READY NOW would be misleading after a confirmed exit-side close

Tests: `backend/test_ready_dwell_entry_shadow.py` (distance mid-dwell hold, soft hold, natural hold, EMA10 early hide, post-floor removal).
