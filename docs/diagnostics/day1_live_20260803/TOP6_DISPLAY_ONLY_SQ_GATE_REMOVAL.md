# Top-6/Top-10 Display-Only — Gate Removal

**Session validated:** 2026-08-03

## Direct answers

| Question | Answer |
|----------|--------|
| Did SQ require `top6_rank IS NOT NULL`? | **Yes — hard gate** in `evaluate_sq_for_stock` + `apply_sq_ready_promotions` only iterated `load_latest_garuda_top6`. **Removed.** |
| Does Garuda score all 200? | **Yes.** Every cycle evaluates the full prepared universe; `rank_score = (strength_pct + momentum_pct) / 2` is written for all scored rows. Top-6 only sets `top6_rank` on the imbalance-confirmed pool truncate — same pattern as RS full-universe + Top-10 display. |
| Does SQ read full-universe RS? | **Yes** from `rs_universe_score_snapshot` (no Top-10 filter). Previously only requested RS for Top-6 symbols; now loads RS for the full Garuda-scored universe. |
| Garuda model design issue? | **No** — standalone `rank_score` already exists off-Top-6. Persistence was already full-universe; the gap was the SQ consumer gate. |

## Fix (deployed)

- `load_latest_garuda_scores()` — latest row per symbol with `rank_score` (universe)
- `load_latest_garuda_top6()` — display subset only
- SQ eligibility: Garuda score + RS grade A/B + OW/VW/EW + VWAP-side + Total≥75 — **no Top-6 membership**
- Inject `sq_direct` stubs only on actual promote (not all 200)

## Validation (2026-08-03 LOCF / EOD snapshot)

- Garuda scored **200** symbols; **38** ever Top-6; **162** never Top-6
- Never-Top-6 with grade A/B at LOCF: **13**
- Never-Top-6 with SQ≥75 at LOCF: **3** (BAJAJHLDNG 77.1, ONGC 77.2, LAURUSLABS 76.5) — all had strong OW/VW/EW; **VWAP-side failed at EOD** so none would promote on the final bar alone
- Historical SQ promotes that day (Top-6-gated): **9**

These three are the concrete “invisible structure” cases under the old gate. Intraday VWAP-side may have passed earlier; EOD LOCF is conservative.

Env unchanged: `SQ_PROMOTE_*`. Dashboard Top-6/Top-10 meaning unchanged.
