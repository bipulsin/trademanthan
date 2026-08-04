# Kavach checkpoints — 22-Jul & 8-Aug

**Persistent checklist** (single source of truth — do not rely on chat session memory).

| Checkpoint | Window | Contract context |
|---|---|---|
| **22-Jul** | July-series data alone, full review before the roll | July futures only — **nothing skipped** just because 8-Aug re-covers it |
| **8-Aug** | **4-week rolling window**, not pure August-contract data | July futures through ~**23-Jul**, then August futures from the roll (~**24/28-Jul**) through **8-Aug**. Review all 16 items on the **combined** window; where relevant, **flag pre-roll vs post-roll shifts** (liquidity / spread / vol around expiry can move gate thresholds, lock churn, dwell, etc.) |

Last reconciled: **2026-07-24** — 22-Jul cycle closed end-to-end (trader offline until next session). Min 10m READY card floor **live**.

---

## Master list (16)

| # | Item | 22-Jul | 8-Aug (+ pre/post-roll note) | Status / notes |
|---|---|---|---|---|
| 1 | VWAP quality gate activation (`vwap_gate_enabled` / `READY_VWAP_QUALITY_GATE`) | Decide | Re-decide on full 4w; note if A/B differ pre vs post roll | Shadow default off; decision pending |
| 2 | Shadow Log Review HTML page | **CLOSED** | UX/data completeness on 4w | **Shipped** `/shadow.html` + APIs (15-Jul); 23-Jul extended sources (touch-reject / close-confirm / expansion) |
| 3 | Expansion Watch (`EXPANSION_WATCH_LIVE`) | **Shadow logging ON** | Confirm still credible across roll | Live alerts still **OFF**; shadow table + :05/:35 scan added 23-Jul so 8-Aug has data |
| 4 | R1 PLAN EXIT — live event validation | Primary | Pre vs post roll event rates / outcomes | Collecting; see `R1_EXIT_NOW_HOLD_VS_EMA10_*` |
| 5 | ATR-consumed logging — instrumentation review | Review | Continuity across roll | Research-only; no READY gate |
| 6 | ABB fix coverage | **CLOSED (code+tests)** | Spot-check if regressions | Fix `1abee67` live; `test_abb_and_any_symbol_use_same_conflict_path` green; optional live visual spot-check |
| 7 | Watching-vs-READY-NOW (Requirement 5) | **CLOSED (UI label)** | Same | Dual surfaces remain; READY NOW shows confirmation note that Take Trade authority is here only — Watching “READY TO …” ≠ entry (23-Jul) |
| 8 | Backend FSM vs Pine confidence | **CLOSED** | Same | Fixed 23-Jul to Pine v3.0 as SoT: **I8-B** `_grade_ok` excludes stretch `!`; **I8-A** readiness dir uses Layer-3 `buyEligible`/`sellEligible`; **I8-C** MACD Votes = line vs signal. Expect fewer READY cards when stretch-marked. |
| 9 | HCLTECH direction-flip ticket | **CLOSED → DIR CONFLICT** | Same | Original HCLTECH SHORT-vs-TV-BUY case covered by DIR CONFLICT live path (`9047d3f`/`1abee67`); no separate unsent ticket body in repo |
| 10 | BANKINDIA shadow log capture gap | Confirm fix held | Same | Root-caused 15-Jul (`READY_SHADOW_REVIEW_2026-07-15.md`) |
| 11 | 09:25 / 09:45 / 10:15 lock-timing shadow comparison | 2w early read → 22-Jul | Full 4w + pre/post roll | `rs_shadow_selection` + `analyze_rs_shadow_checkpoints.py` |
| 12 | `steep_ok` threshold investigation | **CLOSED (intentional)** | Re-check if roll changes slope distribution | Confirmed steep-slope filter (Item F); not a confidence bug |
| 13 | VWAP price-to-VWAP extension metric | **CLOSED (logging)** | Same | `vwap_extension_pct` = \|close−VWAP\|/VWAP×100 on consistency log (+ signed in inputs) from 23-Jul |
| 14 | After-hours full-universe Kavach archive | **Reviewed** | Discard-rate / coverage on 4w | Ran all 8–22 Jul sessions; typical discard 20–50%; **22-Jul partial (13/200) — exclude from coverage**. See `ITEM14_universe_kavach_archive_status.json` |
| 15 | Bug 1 + Bug 2 live dwell / entry-guard behavior review | **CLOSED (floor strengthened 24-Jul)** | Full 4w; **compare Option B (live) vs A & C (shadow)**; **pre-roll vs post-roll** | **LIVE** Option B + **min 10m card visibility** (distance/soft/natural hold; EMA10 close early-hide). See `ready_dwell_card_vanish/` |
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
| 2026-07-24 strengthen | Mid-dwell **distance** + natural leave also hold card ≥10m; Take Trade off. Confirmed EMA10 close / lock remove / EXIT NOW still early-hide (misleading otherwise). **Live, no shadow.** |

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

### Rule 15/22 override case — SRF LONG 24-Jul (journal only)

**SRF** LONG `trade_log` 2026-07-24 (entry ~10:45 @ 2670.00 → exit ~11:34 @ 2670.40, qty 200, +₹80). Rule 15/22 (2-candle validation) **FAILED** (entry high 2681; C1 high 2669.20; C2 close 2671.30 — no new high). Trader **discretionary stay**; should have market-exited ~10:55–11:00. Peak MFE ~+₹1,260 then faded; final exit was confirmed 10m close below EMA5+EMA10 (`exit_trigger_type=rule_compliant` for actual exit). Notes carry `rule_override=true` / `entry_rule_violated=Rule_15_22_2_candle_validation` (no dedicated columns). **No live rule change.**

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

## VWAP touch-and-reject research — CLOSED (NO_GO)

| Field | Value |
|---|---|
| Closed | **2026-07-23** |
| Decision | **NO_GO** — do not promote to shadow rule or live gate; **no further backtest cycles** |
| Evidence | `docs/diagnostics/checkpoint_22jul_followup/D/README_winrate.md` (+ `D_winrate_baseline_summary.json`) |
| Headline | Mean fwd n3 (~+2–3 pts) was **outlier-pulled** (median ~0.3–0.4); LONG edge vs all-bars only ~2.2pp with mean **worse** than baseline; near-VWAP non-reject often beats reject; **PM** win-rates &lt;50%; high-wick buckets fat left tails |
| Forward logging | May **keep running** (`kavach_vwap_touch_reject_log`) — cheap; not an open research thread |
| Do not confuse with | **VWAP close-confirmation** entry filter (`kavach_vwap_close_confirm_shadow` / D2) — separate sticky READY-episode close-above/below VWAP study |

**8-Aug:** treat as closed; no carry-forward open item. Optional glance at live log volume only.

---

## Maintenance

Update this file when an item is decided, deferred, or scope changes.  
Do **not** treat agent chat transcripts as the checklist.

---

## 22-Jul cycle close-out (2026-07-24)

Trader offline after this close. Status snapshot for **8-Aug** handoff:

| Thread | Status |
|---|---|
| Rule 25 | **Live** |
| Item 7 / 8 fixes | **Closed** (UI authority label; Pine v3.0 I8-A/B/C) |
| UI declutter | **Live** |
| READY NOW audio | **Live** |
| Expansion Watch | Shadow logging **fixed/on**; live alerts still OFF |
| VWAP close-confirm shadow | **Collecting** (`kavach_vwap_close_confirm_shadow`) |
| VWAP touch-reject | **Closed NO-GO** |
| +4-candle extension | **Closed, no rule** (see `PLUS4_EXTENSION_OUTLIER_DIAGNOSIS.md`) |
| Appearance-count pattern | **Closed, no rule — retracted** |
| Realism-filter findings | **Documented** (`ready_watching_trade_trace/REALISM_FILTER_SUMMARY.md`) |
| READY card min 10m dwell floor | **Live** (2026-07-24; no shadow). Soft/distance/natural hold inside floor; confirmed EMA10 close / lock remove / EXIT NOW early-hide. See `checkpoint_22jul_followup/ready_dwell_card_vanish/` |

### Prospective logging tables (paperclip, as of close-out)

| Table | Rows | Note |
|---|---:|---|
| `kavach_watching_grade_a_counter` | 0 | Tables exist; empty until next session enrich (market closed / deployed late) — **not broken** |
| `kavach_watching_grade_a_episode` | 0 | Same |
| `kavach_ready_exit_plus4_shadow` | 0 | Same |
| `kavach_vwap_close_confirm_shadow` | 33 | Accumulating |

Shadow modules: `kavach_watching_shadow.py`, `kavach_ready_exit_plus4_shadow.py`.

### Carried forward to 8-Aug (unresolved only)

1. **Item 1** — VWAP quality gate activation decision (still shadow-off).
2. **Item 4** — R1 PLAN EXIT live event validation (collecting).
3. **Item 5** — ATR-consumed logging review (research-only).
4. **Item 11** — Lock-timing 09:25/09:45/10:15 shadow comparison on full 4w + pre/post roll.
5. **Item 16** — Option A vs B vs C stay/move decision on full 4w (B remains live).
6. **Entry-to-EMA10 buffer** — shadow field on `trade_log`; promote only if pattern holds (POLYCAB thin-buffer case).
7. **Rule 15** open edge (exact close at entry extreme) — review only, no live change.
8. **VWAP close-confirm** — keep collecting; decide at 8-Aug (separate from touch-reject NO-GO).
9. **Expansion Watch** — confirm shadow credibility across roll; live still OFF.
10. **Min-10m dwell floor** — spot-check first live session after 24-Jul deploy (expected: zero under-10m distance vanishings).

Do **not** carry: touch-reject, +4 extension rule, appearance-count rule, Item 7/8, UI declutter, READY audio, realism (docs only).

---

## Stale-entry / candle-warm arc (2026-07-31) — carry into 8-Aug

Full arc closed in code on **31-Jul**; first live co-verification is the **next market session**.

| Step | What landed |
|---|---|
| Bug | READY cards blank / tip-regressed entry under candle rate deny |
| Root cause | Shared Upstox candle RL + invalidate-before-refetch emptied last-good tips |
| Phase 1 | Curr-month REST @10m; stock/next LTP via WS @30m; stock/next VWAP/EMA hourly @:20; SF/Vajra live surface removed; OI heatmap disabled (`c115a77`) |
| Health | `/scan/health` Upstox probe fixed (IST window + `check_api_health`) — pre-existing UTC/`nifty` key bug (`7e3aef3`) |
| Tip-stale | Keep last-good series; replace only on fresher tip; tip-stale barred from READY entry (`live_ema5` / `candle_open_fallback` gate unchanged) |
| Monitor | `/candle-warm-deny.html` + `GET /market-data/candle-warm-cycles` — deny % + missing symbols per warm cycle |

**8-Aug review TODO (fill after first live day):**

1. Deny clusters at prior failure windows (~11:32, 13:42, 14:17 IST)? Y/N + notes
2. Any READY/READY-family card with stale tip treated as fresh entry in those windows? (expect **no**)
3. Session deny-rate baseline under Phase 1 + tip-stale together

---

## Candle-warm shared rate-limit bug (infra, `bb9a819`) — resolved via hourly pause

**Category:** Infrastructure (not scoring / RS selection).  
**Full writeup:** `docs/diagnostics/checkpoint_22jul_followup/candle_warm_rate_limit_bb9a819/README.md`

| | |
|---|---|
| **Root cause** | 10m + hourly warms shared one Upstox candle RL bucket with no cross-job mutex; hourly `max_wait=90s` + 10-wide pool → ~95% deny; `:20` hourly spilled into `:25` 10m; alphabetical order starved the same B–S tail |
| **Fix (bb9a819)** | Mutex (block/queue, never skip), hourly `:08`, `max_wait` 300s, symbol rotation, overlap/`lock_wait` log fields |
| **2026-08-04 live** | Mutex worked but queued 10m behind hourly (~400 keys): `scheduled_10m` deny **94–99.5%** in 13:00–15:30 IST overlap windows; RS `10m_warm` skips up to **195/200**; 10m elapsed up to **633s**; `lock_wait` attributable to hourly |
| **Resolution (8-Aug)** | **Pause** `stock_next_vwap_ema_hourly_08` (`STOCK_NEXT_VWAP_EMA_HOURLY_ENABLED=false` default). Code + DB columns retained. See section below. |
| **Status** | Contention root removed by pausing dead-reader job; verify 1–2 sessions vs Aug-4 baseline |

---

## Pause hourly stock/next VWAP·EMA5 job (8-Aug checkpoint)

**Category:** Confirmed-dead-code cleanup (budget remnant post-`c115a77` SF/Vajra removal).  
**Not** a new-signal change — ships without extended shadow per standing rule.  
**Investigation:** code-level usage trace + full dashboard-page audit (conversation thread) — `stock_vwap`, `stock_ema5`, `nextmth_future_vwap`, `nextmth_future_ema5` on `arbitrage_master` have **zero live readers** (no algo, no screen).

| | |
|---|---|
| **Change** | Pause APScheduler `stock_next_vwap_ema_hourly_08` / `scheduled_stock_next_vwap_ema_hourly`. Env: `STOCK_NEXT_VWAP_EMA_HOURLY_ENABLED` (default **false**). Do not delete engine/scheduler functions or drop columns. |
| **Rollback** | Set `STOCK_NEXT_VWAP_EMA_HOURLY_ENABLED=true` and redeploy — pause-only, no data loss. |
| **Health** | `market_data/health.py` freshness uses `market_data_last_updated` / `currmth_future_last_updated` only — **not** the four paused columns. No false-staleness expected. |
| **Mutex / RL** | `_CANDLE_WARM_LOCK` + shared `SlidingWindowRateLimiter` remain for `scheduled_10m` (and any other candle warms). No job assumes the hourly job must run; absence only frees budget. |

### Before / after metrics (verify next 1–2 trading sessions)

Baseline: **2026-08-04** overlap windows (hourly still live).

| Metric | Before (2026-08-04) | After (fill post-deploy) | Expect |
|---|---|---|---|
| `scheduled_10m` deny% (13:00–15:30 IST) | **94–99.5%** during overlap | _TBD_ | Near-zero |
| RS `10m_warm` cycle skips | Up to **195/200** worst cycles | _TBD_ | Near-zero |
| `scheduled_10m` elapsed | Up to **633s** | _TBD_ | ~230–330s (non-overlap baseline) |
| `candle_warm_lock_wait_sec` from hourly | Observed under contention | _TBD_ | **None** attributable to hourly (job paused) |

**Refs:** candle-warm RL writeup (`bb9a819`); SF/Vajra removal `c115a77`; this pause ships with the 8-Aug checkpoint logging above.