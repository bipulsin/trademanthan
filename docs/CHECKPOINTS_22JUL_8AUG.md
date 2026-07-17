# Kavach checkpoints — 22-Jul & 8-Aug

**Persistent checklist** (single source of truth — do not rely on chat session memory).

| Checkpoint | Window | Contract context |
|---|---|---|
| **22-Jul** | July-series data alone, full review before the roll | July futures only — **nothing skipped** just because 8-Aug re-covers it |
| **8-Aug** | **4-week rolling window**, not pure August-contract data | July futures through ~**23-Jul**, then August futures from the roll (~**24/28-Jul**) through **8-Aug**. Review all 16 items on the **combined** window; where relevant, **flag pre-roll vs post-roll shifts** (liquidity / spread / vol around expiry can move gate thresholds, lock churn, dwell, etc.) |

Last reconciled: **2026-07-18** against owner list (16 items).

---

## Master list (16)

| # | Item | 22-Jul | 8-Aug (+ pre/post-roll note) | Status / notes |
|---|---|---|---|---|
| 1 | VWAP quality gate activation (`vwap_gate_enabled` / `READY_VWAP_QUALITY_GATE`) | Decide | Re-decide on full 4w; note if A/B differ pre vs post roll | Shadow default off; decision pending |
| 2 | Shadow Log Review HTML page (requirements drafted, not sent) | Spec → build decision | UX/data completeness on 4w | Requirements drafted; page not shipped |
| 3 | Expansion Watch backtest clearance (`EXPANSION_WATCH_LIVE`) | Clearance | Confirm still credible across roll | Live gated off until backtest clears |
| 4 | R1 PLAN EXIT — live event validation | Primary | Pre vs post roll event rates / outcomes | Collecting; see `R1_EXIT_NOW_HOLD_VS_EMA10_*` |
| 5 | ATR-consumed logging — instrumentation review | Review | Continuity across roll | Research-only; no READY gate |
| 6 | ABB fix coverage — unconfirmed | Confirm | N/A unless regressions | **Unconfirmed** |
| 7 | Watching-vs-READY-NOW state contradiction (Requirement 5) | Review | Same | Open |
| 8 | Backend FSM vs Pine state machine divergence | Review | Same | Open (Pine v2.7 confidence realign drafted, not live) |
| 9 | HCLTECH direction-flip ticket | Review | Same | Open |
| 10 | BANKINDIA shadow log capture gap | Confirm fix held | Same | Root-caused 15-Jul (`READY_SHADOW_REVIEW_2026-07-15.md`) |
| 11 | 09:25 / 09:45 / 10:15 lock-timing shadow comparison | 2w early read → 22-Jul | Full 4w + pre/post roll | `rs_shadow_selection` + `analyze_rs_shadow_checkpoints.py` |
| 12 | `steep_ok` threshold investigation | Flag | Re-check if roll changes slope distribution | Flagged `STEEP_OK_THRESHOLD_CHECK` |
| 13 | VWAP price-to-VWAP extension metric | Flag / add if missing | Same | Flagged `VWAP_EXTENSION_METRIC_MISSING`; raw log has extension field path |
| 14 | After-hours full-universe Kavach archive | Accumulation check | Discard-rate / coverage on 4w | Job `rs_universe_kavach_archive` ~15:40 IST |
| 15 | Bug 1 + Bug 2 live dwell / entry-guard behavior review | First live sessions (Jul) | Full 4w; **compare Option B (live) vs A & C (shadow)**; **pre-roll vs post-roll** | **LIVE 2026-07-18: Option B** |
| 16 | Option A vs B vs C threshold sensitivity comparison | Early live read | Full 4w; decide stay on B / move to A / month-specific | A+C shadow forever; B live |

---

## Reconciliation (2026-07-18)

### Owner list → ours
All **16 owner items are present** in the table above.

### Extra items we had that were not on the owner list
None as separate checklist rows. Related work tracked **under** existing items (not net-new gates):

- DIR CONFLICT ≥2-of-3 / WHIPSAW VWAP redefine — shipped earlier; feed item **7/8** review, not a 17th checkpoint row.
- Kavach Pine v2.7 confidence realignment draft — under item **8**.
- VWAP+ badge / persist score bump — under items **1/12**, not a separate activation decision.

### Owner items we did **not** previously keep as one persistent doc
Items **2, 6, 7, 8, 9** existed only in chat / scattered diagnostics. They are now **first-class rows** here.

---

## Item 15 / 16 review rule (explicit)

Live behavior review compares:

- **Live:** Option B — `min_gap_pts = max(0.3% × price, 500 / lot)`
- **Shadow:** Option A (`300/lot`) and Option C (`0.25 × ATR`)

Across:

1. **22-Jul** — July-contract segment only  
2. **8-Aug** — combined Jul→Aug window, with **pre-roll vs post-roll** callouts  

Outcome: stay on B, move to A, or allow threshold to differ by contract month.

---

## Go-live record (Bug 1 + Bug 2)

| Field | Value |
|---|---|
| Date | 2026-07-18 (before Mon 20-Jul session) |
| Flag | `READY_DWELL_ENTRY_LIVE=1` |
| Live threshold | **Option B** (`READY_DWELL_ENTRY_OPTION=B`) |
| Shadow forever | Option A, Option C, check2 vs check3, `check3_only` research flag |
| Hard dwell ends | EMA10 confirmed close reverse, R1/R2 lock removal, EXIT NOW / PLAN EXIT |
| Soft (in dwell) | Badges + `trade_take_enabled=false`; **card stays visible** (`card_visible`) |

### Pre-live baseline (2026-07-17 consistency log)

| Metric | Value |
|---|---|
| Consistency rows | 414 |
| READY→non-READY transitions (poll-level) | 275 |
| Of which `zone_downgrade=warning_stack` | **218** |
| Transitions with spell-age &lt; 5 min (poll-level) | 44 / 275 |
| Post-shadow deploy: distance would-block rows | 15 |
| Soft dwell would-extend rows | warning_stack 16 + direction_imbalance 9 |
| READY samples with live price + entry; \|LTP−entry\| &gt; 5 | 1 / 15 (afternoon shadow window only) |

Use Monday+ live sessions for clean before/after spell dwell; 17-Jul afternoon is partial instrumentation.

---

## Maintenance

Update this file when an item is decided, deferred, or scope changes.  
Do **not** treat agent chat transcripts as the checklist.
