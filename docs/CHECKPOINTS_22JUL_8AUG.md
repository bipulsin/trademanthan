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
| Lock removals (day) | **133** |
| Peak removals/hour (IST) | **33** (11:00 hour) — **ATYPICAL high-churn** (≥7/h elevated; ≥20/h atypical) |
| Rendered READY spells | 30 |
| Spells &lt; 5 min | **18 / 30 (60%)** |
| Median spell (min) | **3.09** |
| Soft-kill polls (`warning_stack`) | **218** across 20 symbols |
| Shadow distance would-block rows | 15 |
| Soft dwell would-extend rows | warning_stack 16 + direction_imbalance 9 |
| READY samples with live price + entry; \|LTP−entry\| &gt; 5 | 1 / 15 (afternoon shadow window only) |

**Do not** read Monday vs 17-Jul alone as “improvement vs a normal day.”

### Fairer normal-flow comparator (2026-07-15)

Best recent session with consistency logs + quieter lock churn:

| Metric | 2026-07-15 |
|---|---|
| Lock removals (day) | 64 |
| Peak removals/hour | **13** (elevated vs &lt;7, but not atypical) |
| Rendered READY spells | 26 |
| Spells &lt; 5 min | **3 / 26 (11.5%)** |
| Median spell (min) | **38.37** |
| Soft-kill polls (`warning_stack`) | 0 (pre–dwell instrumentation / different stack logging era) |

2026-07-14 had even quieter removals (peak **10**/h) but **no** consistency-log rows — not usable for READY transition compare.

### Monday report command

```bash
docker compose exec -T app python3 scripts/analyze_ready_dwell_entry_shadow.py \
  --date 2026-07-20 \
  --baseline 2026-07-17 \
  --normal 2026-07-15
```

Report must show: (1) same-metric table vs 17-Jul baseline, (2) atypical high-churn flag on 17-Jul, (3) same-metric table vs 15-Jul normal-flow.

---

## Rule 15 open items (2-candle validation) — 22-Jul session note

Logged from **DELHIVERY SHORT** `trade_log` 2026-07-22 (entry 11:12 @ 474.50 → exit 11:57 @ 473.15). Research annotation only — **no live rule change**.

| Open question | Context from DELHIVERY |
|---|---|
| Does “beyond entry candle” require a **strict intrabar wick** lower/higher than the entry candle extreme? | Entry candle low **474.00**; next candle low **474.10** (not beyond); following candle **closed 474.00** (exact match of entry low, no lower low across 2 candles). Rule 15 initially looked like a **fail**. |
| Should a candle that **closes exactly at** the entry candle low/high count as **pass** or **fail**? | Ambiguous under a strict wick reading; price later made a fresh low to **473.10** before the Rule 20 exit — so the trade still extended, but the 2-candle validator’s formal outcome is unclear. |

**Disposition:** keep as open review item for the **22-Jul** checkpoint pass. Do not tighten or loosen Rule 15 live until this edge case is decided with more examples.

Also noted on that trade (not a Rule 15 change): post-exit grade recovered to **A** within minutes — treated as **noise** (no re-chase of grade flicker after exit).

---

## Profit-protection research thread — 22-Jul contrast case

| Case | Session | Pattern | Outcome class |
|---|---|---|---|
| ADANIGREEN / POLICYBZR / FEDERALBANK (prior) | Earlier Jul | Peak R then give-back toward BE / full round-trip | Give-back / ratchet miss or late |
| **HAL LONG** | **2026-07-22** | Peaked ~**+₹3,150 (~1.84R)**; Rule 23 EMA5 ratchet after 1R fired; exit **+₹915** | **CONTRAST** — ratchet caught reversal **before** full round-trip to breakeven |

Use HAL vs the three give-back cases when comparing **ratchet response time vs give-back size** in the 22-Jul checkpoint review. Source: `trade_log` id for HAL 11:14 entry (see DB). No live gating change.

---

## Entry-to-EMA10 buffer at fill time (open — shadow logging)

**Question:** Does a thin buffer (entry candle closing at/near EMA10) predict outsized losses **independent of Confidence grade**?

| Symbol (22-Jul) | Dir | Entry vs EMA10 | Grade @ entry | Outcome |
|---|---|---|---|---|
| **POLYCAB** | LONG | Fill 9163 vs EMA10 **9139.63** (~23 pts / ~0.26% by fill formula), but **entry candle closed essentially at EMA10** — near-zero usable buffer / SL already underfoot | A (85) | **−₹4,937.50** (Rule 16 blowout; ~1.6–2.1× planned EMA10 risk ₹2,348–3,145) |
| DELHIVERY | SHORT | Fill 474.50 vs EMA10 475.86 (~1.36 pts) — clear side of SL | A (85) | +₹2,801.25 |
| HAL | LONG | Fill 4604 vs EMA10 4593.87 (**~10.1 pts** gap) | A (85) | +₹915 |

**First data point:** POLYCAB 22-Jul — thin/zero usable buffer at fill (candle closed at EMA10), A-grade, outsized loss. Root cause tagged as **ENTRY QUALITY**, not grade/score/ADX. Rule 24 had fired 1–2 candles earlier but was not acted on in time (monitoring-latency miss); still secondary to the thin-buffer entry.

**Instrumentation (shadow-only, no live gate):** `trade_log.entry_to_ema10_buffer_pct = |entry_price − EMA10_at_entry| / entry_price × 100`, auto-filled on every upsert going forward. Compare against DELHIVERY/HAL same-day and later samples; note POLYCAB’s narrative is also about **candle-close vs EMA10**, which may need a companion field later if fill-only % is insufficient.

**If pattern holds over multiple weeks:** consider a checklist addition similar to Rule 2’s ADX 20–25 half-size treatment — thin EMA10 buffer at entry → half-size or skip, regardless of Confidence grade or Trade Score. **Not live until reviewed.**

---

## Maintenance

Update this file when an item is decided, deferred, or scope changes.  
Do **not** treat agent chat transcripts as the checklist.
