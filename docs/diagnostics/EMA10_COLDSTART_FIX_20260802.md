# Live EMA10 cold-start fix (P0) — 2026-08-02

## Change

`ema10_10min` in `backend/services/rs_conviction_signals.py` now seeds from **prior-session final 10m EMA10** (≥10 prior 10m closes), same pattern as the EMA5 SQ seed. Same-session 10-bar cold-start no longer blocks stretch for the first ~1h40m.

P1 (grade debounce) and P2 (stretch deadband) **not implemented** — held for sign-off.

## Before / after — confirmed cases (2026-07-30 09:25–10:55)

Replay: RSS component inputs → `resolve_score_and_grade` with `ema10=None` (old live) vs seeded 10m EMA10 (new). RSS grade = persisted board grade.

| Metric | Old live (ema10=None) | New live (seeded) |
|---|---:|---:|
| Exact grade match vs RSS | 10/41 (**24%**) | 36/41 (**88%**) |
| Disagree ≥2 letters | 31/41 (**76%**) | 5/41 (**12%**) |
| Live A+/A/B vs RSS D/D! | 31/41 (**76%**) | **0/41 (0%)** |

Examples: HEROMOTOCO 09:45 old **A** → new **D!** (matches RSS D!); PERSISTENT 09:40–10:20 same.

Remaining 5 mismatches are **5m vs 10m EMA10** numeric differences near stretch cliffs (path design), not cold-start no-ops.

## Deploy

Shipped with this commit; verify `ema10_10min` returns non-None before the 10th session bar when prior history is present.
