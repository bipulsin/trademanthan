# Day-1 Live Analysis — 2026-08-03 (weekend fix bundle)

**Scope:** Observation only — no code changes. First full session after the 2026-08-02 fix/change bundle.  
**Sources:** `kavach_badge_input_log`, `kavach_ready_consistency_log`, `sq_ready_promotion_log`, `rs_universe_score_snapshot`, `logs/trademanthan.log`, `logs/smart_future_algo.log`.  
**Artifacts:** `ready_now_20260803.{csv,json}`, `take_trade_20260803.{csv,json}`, `sq_promotion_tracking_20260803.json`.

---

## 1. Symbol lists

### READY NOW (12 symbols — first badge READY)

| First READY (IST) | Symbol | Side | Promotion | Organic grade/TS | SQ total (if any) | Take enabled? |
|---|---|---|---|---|---:|:---:|
| 09:50 | CHOLAFIN | LONG | organic_FSM | A / 91 | — | **Yes** @ 09:45 |
| 09:50 | INOXWIND | LONG | organic_FSM | A / 85 | — | **Yes** @ 09:45 |
| 09:50 | BAJAJFINSV | LONG | organic_FSM | B / 80 | — | **Yes** @ 09:45 |
| 10:16 | FORTIS | LONG | organic + SQ | A+ / 95 | 80.66 @ 10:35 | **Yes** @ 10:16 |
| 11:10 | DIVISLAB | LONG | **SQ-only** | — | 79.22 | No* |
| 11:16 | PNBHOUSING | LONG | **SQ-only** | — | 82.25 | No* |
| 12:05 | JUBLFOOD | LONG | **SQ-only** | — | 75.64 | No* |
| 12:16 | PAYTM | LONG | **SQ-only** | — | 81.10 | No* |
| 12:25 | ASHOKLEY | LONG | **SQ-only** | — | 81.55 | No* |
| 13:06 | APLAPOLLO | LONG | **SQ-only** | — | 75.39 | No* |
| 13:26 | LTM | LONG | **SQ-only** | — | 75.76 | No* |
| 15:48 | MCX | LONG | badge_READY_only | — | — | No |

\*SQ-promoted names show READY on the badge tape, but `trade_take_enabled=true` never appears in consistency/badge inputs for them today (see §7).

### TAKE TRADE (`trade_take_enabled=true`) — 4 symbols only

| First take (IST) | Symbol | State | Grade | Trade Score | Path |
|---|---|---|---|---:|---|
| 09:45:19 | BAJAJFINSV | READY | B | 80 | Organic FSM |
| 09:45:19 | CHOLAFIN | READY | A | 91 | Organic FSM |
| 09:45:19 | INOXWIND | READY | A | 85 | Organic FSM |
| 10:16:28 | FORTIS | READY | A+ | 95 | Organic FSM |

Gate sequence (organic): consistency stack reached `READY` with `trade_take_enabled=true` inside the entry window — no SQ flag on these rows.

### Overlap flags

| Class | Symbols |
|---|---|
| Both organic READY + SQ promote | **FORTIS** (organic 10:16 → SQ re-stamp 10:35, `also_organic=false` at SQ write — state had moved off organic READY) |
| SQ-only (would not promote without SQ) | DIVISLAB, PNBHOUSING, JUBLFOOD, PAYTM, ASHOKLEY, APLAPOLLO, LTM |
| Organic-only | BAJAJFINSV, CHOLAFIN, INOXWIND |
| Badge READY without organic/SQ log | MCX (late session) |

### SQ breakdowns (promoted today)

| Symbol | Time | Total | RS | Garuda | OW | VW | EW | Bonus | Grade | Rank | Pre-state | Stretch% | EW event |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---|---:|---|
| FORTIS | 10:35 | 80.66 | 85 | 66.14 | 73.3 | 80 | 100 | 20 | A | 6 | WAIT FOR PULLBACK | 1.60 | **bullish** |
| DIVISLAB | 11:10 | 79.22 | 77 | 67.72 | 83.4 | 100 | 100 | 15 | B | 2 | BLOCKED | 1.00 | — |
| PNBHOUSING | 11:16 | 82.25 | 85 | 74.07 | 55.9 | 100 | 100 | 20 | A | 3 | BLOCKED | 2.64 | — |
| JUBLFOOD | 12:05 | 75.64 | 85 | 72.90 | 13.0 | 100 | 100 | 20 | A | 2 | WATCHING | 5.22 | — |
| PAYTM | 12:16 | 81.10 | 100 | 73.98 | 0.0 | 100 | 100 | 25 | A+ | 1 | BLOCKED | 6.85 | — |
| ASHOKLEY | 12:25 | 81.55 | 85 | 73.47 | 51.9 | 100 | 100 | 20 | A | 3 | WAIT FOR PULLBACK | 2.89 | — |
| APLAPOLLO | 13:06 | 75.39 | 85 | 84.28 | 0.0 | 100 | 100 | 20 | A | 1 | WATCHING | 6.54 | — |
| LTM | 13:26 | 75.76 | 85 | 86.75 | 0.0 | 100 | 100 | 20 | A | 3 | BLOCKED | 6.46 | — |

All 8: `also_organic=false`, `ema_reliable=true`, threshold 75.

---

## 2. Full-universe RS scoring — health

### Cycle duration (10m-aligned RS, from `trademanthan.log`)

38 scans logged. Pattern: mostly `cache_only=True` (warm-synced); two late `fetch` EOD scans.

| Metric | Today | Pass criterion |
|---|---:|---|
| p50 | **9.2s** | — |
| p95 | **16.3s** | **&lt; 60s** ✅ |
| max | **23.6s** (EOD fetch) | — |
| avg | 8.8s | — |

Pre-change baseline (weekend validation) targeted p95 &lt; 60s — **met with large margin**.

### Candle deny (pre- vs post-infra fix)

From `smart_future_algo.log` candle-warm summaries (IST via UTC+5:30):

| Job | Today pattern | Notes |
|---|---|---|
| 10m `currmth` | Mix of 0% and 7.5–67.5% spikes; worst dual-overlap still **11:41** (27.5%/967s) | Same starvation pattern through midday |
| Hourly `stock+nextmth` | Still **95.0 / 95.5 / 95.2%** at :20 completions (12:20, 13:20, 14:20) | Pre-`bb9a819` schedule |
| Post-deploy (`~15:17`) | One 10m @ 15:25: **deny 0%**, 200/200, `lock_wait=0` | Hourly `:08` already past — no post-fix hourly sample |

**Verdict:** RS cycle latency is healthy; candle-deny health for the hourly job was **not** fixed for most of today (fix landed after the last `:08`). Carry the deny-monitor close criteria from the `bb9a819` checkpoint writeup.

### `rs_universe_score_snapshot` coverage

| | |
|---|---|
| Scans | **38** |
| Rows/scan | **200** (full universe written every cycle) |
| Avg scored | **158.5 / 200 (79.3%)** |
| Min scored | **32 / 200 (16%)** @ 09:35 |
| Max scored | **200 / 200** |

Exclusion reasons: `missing_candles_or_min_bars` **1555**, `no_prev_close` **21**.

Low-coverage cycles (&lt;150 scored) cluster with candle-deny / cache misses: 09:35, 10:05, 10:25, 10:35, 11:05, 11:35, 11:45, 14:45, 15:15. RS log `skips` track the same (max skips **168** on a 32/200 cycle).

---

## 3. Dynamic Top-10 + B20 hysteresis

| | Today (live membership) | Backtest B20 (07-27–31 reconstituted) |
|---|---:|---:|
| Scans | 38 | 343 / 5d |
| Bull enter+exit events | 148 + 148 | — |
| Bear enter+exit events | 162 + 162 | — |
| Avg bull turnover / transition | **~4 names** | B20 churn events/scan ~17.2 (different definition — full reconstituted board) |
| Incumbent bonus rows | `n_bonus` recorded in snapshot | B20 = −2.8% vs B0 |

**Live read:** membership still turns over hard early when coverage is thin (09:35–09:55 events swap 6–9 names/side). Absolute churn is **not yet comparable** to the July reconstituted board metric; treat today’s number as “noisy under deny/cache gaps,” not a B20 failure.

### Grades outside Top-10 (fresh grade visibility — B20 intent)

Examples with repeated A/B grades while `in_top10_membership=false`:

| Symbol | Side | Cycles outside T10 | Sample grade | Max TS | First seen |
|---|---|---:|---|---:|---|
| PIIND | BULL | 20 | B | 92 | 09:45 |
| HINDZINC | BULL | 16 | B | 95 | 10:45 |
| JIOFIN | BULL | 13 | B | 85 | 11:05 |
| UNOMINDA | BULL | 12 | B | 90 | 10:15 |
| CDSL | BEAR | 11 | B | 97 | 09:25 |

These would previously have been invisible to grade consumers that only saw Top-10/lock boards.

---

## 4. EW / EMA reliability

| Evidence | Detail |
|---|---|
| **Positive** | FORTIS SQ @ 10:35: `ema_reliable=true`, `EW=100`, `ew_event=bullish` on bar **10:25** (not free `start_aligned`) |
| Early bar 2–3 crossover | **Not isolated today** in SQ/consistency EW event tape (no `ew_event` rows in consistency log; only FORTIS bullish event in SQ breakdowns) |
| `start_aligned` false credit | **No sign of free-100 without event** on the SQ promotes that logged `ew_event=None` while EW=100 — those are post-arm persistence, not start-aligned freebies. Worth a dedicated early-bar EW audit tomorrow (bar 09:35/09:45). |

---

## 5. EMA10 cold-start / stretch

FORTIS morning path (universe snapshot) is the clean live proof:

| Time | Grade | \|close−EMA10\|% |
|---|---|---:|
| 09:25 | D | 0.04 |
| **09:45** | **D!** | **1.09** |
| **10:05** | **D!** | **1.07** |
| 10:15 | A | 0.22 |
| 10:25 | A | 0.09 |

Stretch demotion (**D!**) appears from the first stretched morning bar — **not** the old inflated A+/A with silent no-op EMA10. No morning A+/A with ≥0.50% EMA10 distance found in the SQ/early samples checked.

SQ promotes later in the day can still carry high stretch (PAYTM 6.85%, APLAPOLLO 6.54%) with A/A+ — that’s grade-at-promote under current stretch cliffs, separate from the cold-start None bug.

---

## 6. SQ promotion — week-1 day 1 (appendable)

Informational 30m/60m price change after promote (futures LTP from universe snapshot):

| Symbol | Promote | Total | Δ30m% | Δ60m% | Label (30m) |
|---|---|---:|---:|---:|---|
| FORTIS | 10:35 | 80.66 | −0.10 | −0.56 | stall_chop |
| DIVISLAB | 11:10 | 79.22 | **+0.59** | +0.53 | genuine_continuation |
| PNBHOUSING | 11:16 | 82.25 | −0.06 | −0.06 | stall_chop |
| JUBLFOOD | 12:05 | 75.64 | −0.21 | −0.26 | stall_chop / mild fade |
| PAYTM | 12:16 | 81.10 | +0.01 | **+0.46** | stall then continuation |
| ASHOKLEY | 12:25 | 81.55 | −0.27 | −0.50 | reversal_or_fade |
| APLAPOLLO | 13:06 | 75.39 | +0.02 | **+0.54** | stall then continuation |
| LTM | 13:26 | 75.76 | −0.06 | −0.63 | stall → fade |

File: `sq_promotion_tracking_20260803.json` — append one object/day with the same keys.

**Day-1 read:** 8 SQ promotes, 0 also-organic at write time; mix of continuation and fade; several OW=0 / high-stretch promotes (PAYTM, APLAPOLLO, LTM, JUBLFOOD) — watch whether OW=0 + stretch≫0.5% is a quality filter gap.

---

## 7. Unexpected / mismatches vs weekend validation

1. **SQ READY ≠ Take Trade today** — 7 SQ-only names reached badge READY, but **zero** `trade_take_enabled=true` rows for them. Organic take path worked (4 names). Likely SQ promote isn’t sticking take-enabled into the live inputs that consistency/badge sample — **P0 follow-up**.
2. **`promoted_via_structural_score` rarely visible** on subsequent badge/consistency polls (even right after SQ READY). Promotion truth is `sq_ready_promotion_log`; UI/export flag may be one-shot.
3. **Hourly candle deny still ~95%** for most of the day — expected given `:20` schedule until `bb9a819` deploy ~15:17; do not treat as fix failure yet.
4. **Universe score coverage avg ~79%**, with several cycles at 16–48% scored — driven by `missing_candles_or_min_bars` tied to warm deny/cache. RS duration stays fine because scans are largely cache-only.
5. **Top-10 live churn looks high** vs July B20 reconstituted stats — not apples-to-apples; early deny-skewed boards dominate.
6. **MCX** READY at 15:48 on badge only — outside normal entry usefulness; confirm whether dwell/export should ignore post-15:30.
7. **EW bar 2–3 live proof** not captured today — only FORTIS mid-morning bullish event.

---

## Bottom line

| Change | Live day-1 |
|---|---|
| Full-universe RS + cycle latency | **Healthy** (p95 16s) |
| Universe snapshot coverage | **Partial** (avg 79%; deny-linked gaps) |
| B20 / grades outside T10 | **Working** (PIIND, HINDZINC, …) |
| EMA10 cold-start stretch | **Working** (FORTIS D! @ 09:45/10:05) |
| EW reliability / no start_aligned freebie | **Mostly OK**; need early-bar sample |
| SQ ≥75 promote | **8 fires**; mixed PA; **Take Trade not enabled on SQ-only** |
| Candle-warm rate-limit fix | **Too late in session** for hourly proof |

**Highest-priority observation for tomorrow:** why SQ-promoted READY cards don’t set `trade_take_enabled` in the sampled live inputs.
