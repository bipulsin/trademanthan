# Kavach 10m Parity Note

**Scope:** Locked-symbol live recompute (`daily_checklist_live.recompute_locked_symbol`) now uses `kavach_10m.metrics_from_10m_candles`.

## Aligned with TradingView Kavach Pine v3.0 (10m chart)

| Input | Backend (`kavach_10m`) | Pine v2.6 default |
|-------|------------------------|-------------------|
| Bar timeframe | Pairs consecutive **5m** → **10m** | Chart 10m |
| SuperTrend | ATR period **10**, multiplier **1.5** | `stPeriod=10`, `stMult=1.5` |
| MACD | **12 / 26 / 9** | `macdFast/Slow/Signal` (v3.0 default 21-Jul) |
| Panel EMA vs VWAP | EMA(**9**) vs session VWAP | `emaLen=9` (`ema5Raw`) |
| Panel Trend | 2-of-3 of MACD line, ST, EMA vs VWAP | `trendReadBullish/Bearish` |
| READY entry EMA | True **EMA5** (unchanged) | n/a (entry elsewhere) |
| Session VWAP | Cumulative from today's 5m H/L/C/V | `ta.vwap(hlc3)` |
| Volume label | 10m bar vs EMA(20) of prior 10m vols | SMA(20) volume |

## Residual differences

1. **Data source:** Upstox 5m aggregated to 10m; TV may use broker-native 10m feed → minor OHLC drift.
2. **VWAP:** Built from 5m constituents (typical price approx) vs Pine `hlc3` session VWAP.
3. **Checklist trade FSM vs Pine TradingState:** READY/WAIT/BLOCKED/EXPIRED (+ dwell) is the runbook; Pine Layer 5 drives chart arrows. Readiness banner / Layer-3 `buyEligible` / stretch `!` / Votes MACD line-vs-signal aligned 23-Jul (Item 8 I8-A/B/C).
4. **RS scanner batch:** Still uses classic MACD 12/26/9 and ST×3 for universe ranking (unchanged); live checklist / DIR CONFLICT uses Pine params above.

## Audit trail

Every live recompute writes `rs_live_kavach_audit` with `bar_evaluated_at`, full metrics, and `prev_kavach_state` for edge-flip reconstruction.

## Observed Dashboard vs TradingView instances (shadow / observation only)

**No live gating.** These rows are human-audited side-by-side snapshots. There is **no** production table with paired dashboard + TV columns (`kavach_ready_consistency_log` / `kavach_badge_input_log` / `rs_live_kavach_audit` / confidence audit are dashboard-only). Dedicated dash↔TV side-by-side instrumentation is warranted once this becomes an Nth recurring thread.

### 2026-07-24 ~10:21–10:22 IST — SRF (user SRFN2026 = SRF futures)

| Side | State |
|------|--------|
| **Dashboard** (user) | READY NOW, Take Trade OK, Confidence **A** (+2.24%), Entry **2657.82**, SL **2653.34**, Risk ₹896, flags **REGIME UNSTABLE** + **CHURN 12**, regime **TRANSITION / ROTATION DAY**, list locked **09:28:54** |
| **TradingView** Kavach v3.0 (same moment) | **NOT READY**, Confidence **C**, Trade Score **85/100**, ADX **32.56**, price **2666.60** (above dash entry 2657.82 since ~09:45) |
| **Prod dashboard tape** (cheap cross-check) | `kavach_ready_consistency_log` **id 3222** @ 10:21:35 IST / **3223** @ 10:22:35 IST: `rendered_state=READY`, `confidence=A`, entry 2657.82 / SL 2653.34, regime TRANSITION, churn 12, **`trade_take_enabled=false`** (`READY · dwell hold (warning_stack)`). Badge rows 19341/19355 same dwell-hold. **`rs_live_kavach_audit`: 0 rows for SRF on 2026-07-24**. |

**Cause hint (cheap, not deep-dived):** Checklist FSM stays READY on lock-snapshot entry/EMA5 (quality_pass=false, steep_ok=false) while TV Pine readiness/grade can disagree on live price (~2666 vs freeze entry 2657.82) and Confidence banding (dash A vs TV C). Missing `rs_live_kavach_audit` for SRF today means no same-bar backend grade/score to compare to TV 85/C. Schema gap: no place to persist the TV half of this pair.

**Action taken:** Logged here only. No rule/gate/code changes.

---

## 2026-07-24 deep-dive — SRF + ICICIGI + SONACOMS (three-symbol thread)

**Investigation only. No live gating / display / Pine logic changes.** Prod evidence pulled from paperclip `postgres` (`kavach_ready_consistency_log` + `rs_live_kavach_audit`). One additive shadow-only instrumentation change (below).

### Same-bar backend vs TradingView (grade parity)

| Symbol | Bar (IST) | Backend ADX | TV ADX | Backend Score | TV Score | Backend Vol / Purity | Backend grade | TV grade | Dominant gap |
|--------|-----------|-------------|--------|---------------|----------|----------------------|---------------|----------|--------------|
| **SRF** | 10:25 | **32.25** | 32.56 ✅ | **85** | 85 ✅ | High / **71.4%** | **A** | **C** | Volume/VWAP-**purity** at the A/C boundary |
| **ICICIGI** | 13:05 | 58.38 | 53.43 (both high) | **85** | **40** | High / 100% | **A** | **D** | **Trade-Score formula** (RS-dominant) |
| **SONACOMS** | 13:05 | **20.43** | **12.01** | **88** | 67 | High / 100% | **A** (RECHECK) | **D** | **ADX** across the 20 gate + score formula |

(Consistency-log rows: SRF id 3222 @10:21:35, ICICIGI id 3684 @13:20:21 entry 1669.21/SL 1667.47, SONACOMS id 3681/3683 @13:18–13:19 entry 726.58/SL 724.89. All three `confidence=A`, regime `TRANSITION — unconfirmed`, and **`trade_take_enabled=false`** with reason `READY · dwell hold (warning_stack)`, `zone_downgrade=warning_stack`, `dwell_soft_hold=true`.)

### Verdict: **(b) genuine logic/data divergence — mixed, three flavors.** Not (a), not (c).

- **NOT (a) stale/frozen grade.** Grades are recomputed every closed 10m bar (`rs_live_kavach_audit` fresh: SRF 10:25, ICICIGI/SONACOMS 13:05). The 13:18–13:20 renders used the 13:05 bar. Entry tracks **live EMA5** and moves each poll (ICICIGI 1662.41 → 1664.32 → 1669.21). SONACOMS was `D!` for 12:25–12:55 and only flipped to `A` at the 13:05 bar (RS spike; price actually *fell* 729.20→724.85) — a live recompute, not a freeze. (Correction to the SRF note above: SRF *did* have audit rows by 10:25 — “0 rows” was only true at ~10:22 before that bar closed.)
- **NOT (c) a COUNTER-REGIME × A bug.** `COUNTER-REGIME` is **visibility-only by design** (`daily_checklist_zones.annotate_regime_context`: “does not change trade_state”). It fired for SONACOMS + ICICIGI (`counter_regime=true`, `regime_lean=BEAR`, LONG cards) but correctly did **not** force A-only or block READY. The trader rule “COUNTER-REGIME needs A” is **not implemented as a code gate** — so there is no bug, only an unenforced discipline. All three were independently graded A by the live engine, so grade discipline (not counter-regime enforcement) is the failing surface.
- **YES (b), three flavors:**
  1. **SRF — purity/volume banding.** Identical ADX (32.2≈32.6) and identical Trade Score (85=85), yet A vs C. Backend `_band_base_grade`: High + purity 71.4% (≥60 → “pure”) + score 85 → `high_pure_score_ge_85` = **A**. TV at score 85 → **C** implies TV computed **not-pure** (`high_not_pure_score_ge_85` → C). Purity 71.4% sits just above the 60% threshold — the Upstox 5m→10m typical-price VWAP vs TV `hlc3` native VWAP disagree right at the purity cutoff. Also TV live price 2666.60 was ~6 pts above the 2657.82 entry → TV banner “NOT READY” on extension.
  2. **ICICIGI — Trade-Score formula.** Similar high ADX (58 vs 53) but score **85 vs 40** → A vs D. Backend Trade Score is **RS-dominant** (`kavach_engine.trade_score_breakdown`: RS 40 / Kavach 30 / Vol 15 / ADX 10 / VWAP 5). A strong-RS but extended (+3.24% from open, TV “Pullback #1”) name scores 85. TV’s composite (40) weighs pullback/extension structure and does not reward RS the same way. Also +3.24% > `MAX_PCT_FROM_OPEN=3.0`, so the backend’s **own** `pine_readiness` would be NOT READY on the pct gate — but the card renders the **FSM** state, which ignores pct-from-open.
  3. **SONACOMS — ADX data divergence.** Backend ADX **20.43** vs TV **12.01** (8.4-pt gap straddling the 20 gate). Backend barely clears its `ADX≥20` FSM gate → `READY(RECHECK)` (20≤adx<25); TV’s 12.01 fails `ADX>20` → D/NOT READY. Score 88 vs 67 compounds it. ADX(14) on Upstox 5m→10m bars vs TV native 10m Wilder ADX.

### Two structural facts behind “dash higher than TV”

1. **Take Trade already agreed with TV.** On all three, the actionable gate (`trade_take_enabled`) was **false** — the warning stack (`REGIME UNSTABLE` + `CHURN 12` + `COUNTER-REGIME`) downgraded to WAIT, and only the `READY_DWELL_ENTRY_LIVE` soft-hold kept the **card pill** visible as READY. The divergence the user sees is a **display/grade** divergence (READY pill + A letter), not a trade-permission divergence.
2. **Two different “READY” semantics.** The card pill = **trade_state FSM** (grade≥B, regime TREND/TRANSITION, ADX≥20, near-EMA5, R:R≥1:2). TV’s banner = **Pine readiness** (`kavach_readiness.classify_kavach_readiness`: grade level + score≥65 + pct<3% + pullback 0–2 + vol). The backend already computes the latter as `pine_readiness` on every enrich but **did not persist it**, so “card READY vs Pine banner NOT READY/WATCHING” was not queryable — see instrumentation.

### Convergence

- **SRF:** converged fast — 10:55 dropped to B (83), 11:25 B (80). TV rose to A ~10 min after 10:21 (user). Divergence was the instantaneous 10:21–10:25 snapshot.
- **ICICIGI:** oscillated A(85)↔B(77) all afternoon (13:25 → B 77); card oscillated READY↔WAIT (warning-stack). TV D at 13:20.
- **SONACOMS:** backend itself was `D!`(25–55) for most of 12:25–12:55 (agreeing with TV D) and only spiked A(88) at the single 13:05 bar. Mostly agreed; the 13:05 RS spike was the outlier.

### Instrumentation done (shadow-only, additive, low-risk)

`daily_checklist_trade_state.enrich_stocks_trade_state` now persists the backend’s own Pine-parity readiness into the existing `kavach_ready_consistency_log.inputs` JSONB (no schema change), so future dash↔TV readiness gaps are directly queryable:

- `inputs.pine_readiness` — backend banner text (READY TO LONG/SHORT / WATCHING / NOT READY).
- `inputs.pine_readiness_detail` — grade_ready_level, pct_from_open + pct_ok, pullback_long/short + ok, vol_enter_ok, dir_long/short, buy/sell eligible, ready_*_practical.
- `inputs.pine_readiness_mismatch` — bool: FSM card READY-family while Pine banner is not READY (helper `pine_readiness_card_mismatch`, unit-tested).

No TV scrape invented (out of scope). The TV half stays human-audited in tables like this; a promoted column can follow if this becomes a standing query. Grade-parity fields (score/volume_label/vwap_purity_pct/adx) are already in `rs_live_kavach_audit`.

**Action taken:** Diagnosis + one additive shadow-only log field. **No live rule / gate / display / Pine changes.** Query going forward:
```sql
SELECT symbol, rendered_state, inputs->>'pine_readiness' AS pine, inputs->>'confidence' AS conf
FROM kavach_ready_consistency_log
WHERE session_date = CURRENT_DATE AND (inputs->>'pine_readiness_mismatch')::bool
ORDER BY symbol, logged_at;
```
