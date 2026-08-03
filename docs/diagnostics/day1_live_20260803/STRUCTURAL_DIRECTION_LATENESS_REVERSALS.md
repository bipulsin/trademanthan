# Day-1 Structural Review — Direction, Lateness, Missed Reversals (2026-08-03)

**Scope:** Factual diagnostic only. No code changes.  
**Sources:** `rs_live_kavach_audit`, `rs_universe_score_snapshot`, candle cache (session VWAP reconstructed), `rs_lock_membership_audit`, `sq_ready_promotion_log`, `kavach_badge_input_log`, checklist direction.  
**Artifacts:** `direction_verdict_table.json`, `direction_structural_raw.json`, `direction_live_audit_series.txt`, `direction_lock_membership.txt`.

---

## Executive facts (no softener)

1. **Direction is not re-validated against price/VWAP at READY.** It is assigned when the symbol **enters the lock/checklist** (morning RS lock, `intraday_2scan`, or `vwap_adx_promotion`) and then carried forward. READY/SQ evaluate *setup quality for that inherited side* — they do not ask “is price still on the correct side of VWAP for this side?”
2. **There is no hard live gate “LONG only if price > VWAP / SHORT only if price < VWAP.”** `READY_VWAP_QUALITY_GATE` (slope/quality) defaults **off**. Whipsaw / DIR CONFLICT can soft-downgrade; they do not flip direction.
3. **At least one organic READY (BAJAJFINSV) was LONG while live LTP was already below VWAP** in the system’s own audit at the promote second. CHOLAFIN / INOXWIND were above on sticky live-audit LTP but **already below on the forming bar tip** at the same clock time — i.e. the tape had flipped under VWAP while LONG READY fired.
4. **Opposite-direction re-evaluation does not run in parallel.** FORTIS was LONG-only, removed from lock at 10:40, and never evaluated as SHORT when the ~11:15 short developed.
5. **Promotion is systematically late vs early directional expansion** on this session (median ~96 minutes after first +1% from open in the assigned direction; named SQ cases 75–142 minutes behind the manual TV move starts).

These are architectural / gate-coverage findings, not “unlucky candles.”

---

## A. Direction correctness at promotion

### How side is set (code + Day-1 evidence)

| Mechanism | What sets LONG/SHORT | Re-checked at READY? |
|---|---|---|
| `morning_lock` (RS Top-5+5 @ ~09:35) | BULL→LONG / BEAR→SHORT from RS ranking | **No** |
| `intraday_2scan` | Side of the scan that admitted the name | **No** (until remove + re-entry) |
| `vwap_adx_promotion` | Promoted side at entry | **No** |
| SQ promote | Uses **existing** `stock.direction` / Garuda side; does not recompute from price−VWAP | **No** |

Day-1 lock entries for the 12:

| Symbol | Direction at READY | Lock entry (rule @ time) |
|---|---|---|
| CHOLAFIN | LONG | morning_lock BULL @ 09:35 |
| INOXWIND | LONG | morning_lock BULL @ 09:35 |
| BAJAJFINSV | LONG | morning_lock BULL @ 09:35 |
| FORTIS | LONG | vwap_adx_promotion BULL @ 10:10 |
| DIVISLAB | LONG | intraday_2scan BULL @ 11:10 |
| PNBHOUSING | LONG | intraday_2scan BULL @ 11:10 |
| JUBLFOOD | LONG | intraday_2scan BULL @ 11:10 |
| PAYTM | LONG | intraday_2scan BULL @ 12:10 |
| ASHOKLEY | LONG | intraday_2scan BULL @ 12:00 |
| APLAPOLLO | LONG | vwap_adx_promotion BULL @ 12:20 |
| LTM | LONG | **Re-entered BULL** @ 13:00 via `intraday_2scan` (was **BEAR morning_lock @ 09:35**, removed 10:10; BULL via vwap_adx 11:55–12:55) |
| MCX | SHORT | intraday_2scan BEAR @ 10:10 (later re-entry 12:10) |

**Answer to the core question:** side can and does go stale relative to live price/VWAP. It is LOCF from lock admission, not a fresh VWAP-side check at promotion.

### Price vs VWAP at promote (all 12)

Definition used:

- **Live audit:** last `rs_live_kavach_audit` row at/before promote (system LTP + session VWAP).
- **Forming tip:** candle bar open ≤ promote, close vs reconstructed session VWAP (5m cache bars).
- **Last closed:** prior completed bar vs same VWAP.

“Direction-correct” = LONG ⇒ price > VWAP; SHORT ⇒ price < VWAP.

| Symbol | Side | Live audit @ ~promote | Forming tip | Last closed | **Verdict** |
|---|---|---|---|---|---|
| **BAJAJFINSV** | LONG | **09:45:06 px 2072.5 / VWAP 2075.28 → BELOW (−2.78)** | BELOW | ABOVE (09:35) | **WRONG** on live LTP |
| **CHOLAFIN** | LONG | 09:45:05 px 1910.8 / VWAP 1905.61 → ABOVE | **BELOW (−8.0)** | ABOVE (09:35) | **WRONG on forming tip**; audit still sticky above |
| **INOXWIND** | LONG | 09:45:08 px 80.74 / VWAP 80.69 → ABOVE (thin) | **BELOW (−0.57)** | ABOVE (09:35) | **WRONG on forming tip**; audit thin/sticky above |
| FORTIS | LONG | 10:16:20 px 970.15 / VWAP 969.87 → ABOVE | ABOVE | ABOVE | Correct at promote |
| DIVISLAB | LONG | 11:10 px above VWAP (~+1.7%) | ABOVE | ABOVE | Correct at promote |
| PNBHOUSING | LONG | above | ABOVE | ABOVE | Correct at promote |
| JUBLFOOD | LONG | above | ABOVE | ABOVE | Correct at promote |
| PAYTM | LONG | above | ABOVE | ABOVE | Correct at promote |
| ASHOKLEY | LONG | above | ABOVE | ABOVE | Correct at promote |
| APLAPOLLO | LONG | above | ABOVE | ABOVE | Correct at promote |
| LTM | LONG | above (deeply extended) | ABOVE | ABOVE | Correct *side*, late (see B) |
| **MCX** | SHORT | Promote 15:48: closed tip below VWAP → OK for SHORT; earlier live rows often **above** VWAP while still BEAR-locked | below at late tip | below | Late SHORT card; side matched tip, not early structure |

**Manual TV notes for the named morning trio are consistent with the data:** by the 09:45–09:50 window, price was on the wrong side of VWAP for a LONG (BAJAJFINSV unambiguously in live audit; CHOLAFIN/INOXWIND on the forming/break bar).

**Fraction direction-wrong at promote (strict):**

- Live-audit strict: **≥1/12 hard fail (BAJAJFINSV)**; CHOLAFIN/INOXWIND audit still “above” on sticky LTP.
- Forming-tip strict (closer to “what the candle was doing at that second”): **3/12 wrong** (CHOLAFIN, INOXWIND, BAJAJFINSV).
- Combined “any system price feed shows wrong side”: **3/12 organic morning LONGs**.

SQ-only promotes today were **on the correct side of VWAP at fire time** — the SQ problem in these cases is **lateness / extension**, not wrong-side at the bell.

---

## B. Promotion lateness

### Objective lags (minutes)

| Symbol | Promote | Manual TV move start (named) | Lag vs manual (min) | First +1%/−1% from open | Lag vs 1% (min) | % from open @ promote | % of day-range @ promote |
|---|---|---|---:|---|---:|---:|---:|
| CHOLAFIN | 09:45 | (SHORT later ~12:35 — opposite) | — | 09:20-ish long +1% | 25 | +0.4% | 35% |
| INOXWIND | 09:45 | — | — | ~09:15 | 30 | +1.4% | 53% |
| BAJAJFINSV | 09:45 | — | — | ~09:15 | 30 | +0.8% | 37% |
| FORTIS | 10:16 | OK then reverse | — | ~09:35 | 42 | +1.5% | 63% |
| DIVISLAB | 11:10 | **~09:55** | **75.5** | ~10:25 | 46 | +1.2% | **98%** |
| PNBHOUSING | 11:16 | — | — | ~09:40 | 96 | +2.6% | 82% |
| JUBLFOOD | 12:05 | **~10:05** (first leg) | **120.7** | ~09:25 | 161 | +5.1% | 80% |
| PAYTM | 12:16 | — | — | ~09:25 | 172 | +6.0% | 84% |
| ASHOKLEY | 12:25 | — | — | ~10:50 | 96 | +2.5% | 86% |
| APLAPOLLO | 13:06 | — | — | ~09:15 | 232 | +6.4% | 80% |
| LTM | 13:26 | **~11:05** | **141.6** | ~10:10 | 197 | **+6.7%** | **99%** |
| MCX | 15:48 | — | — | early short 1% | 394 | −3.3% | 11% |

**Aggregate (all 12, lag vs first ±1% from open):**

| Stat | Minutes |
|---|---:|
| n | 12 |
| min | 25 |
| median | **96** |
| mean | **127** |
| max | 394 (MCX) |

Named SQ lags vs manual TV: DIVISLAB **+76m**, JUBLFOOD **+121m**, LTM **+142m**.

### Last condition that cleared (why it waited)

| Symbol | Path | Last blocker / unlock |
|---|---|---|
| CHOLAFIN / INOXWIND / BAJAJFINSV | Organic | Entry window opens **09:45**; morning lock already LONG from 09:35. FSM reached READY at window open — **not** waiting on VWAP-side. |
| FORTIS | Organic | Admitted 10:10 via `vwap_adx_promotion`; READY 10:16 (grade A+). |
| DIVISLAB | SQ | Lock entry **same minute** as SQ (11:10). `pre_state=BLOCKED` → SQ bypass (≥75). Move had been underway since ~09:55; **SQ/Top-6+grade only became available after lock admission**. |
| PNBHOUSING | SQ | Same pattern: lock 11:10, SQ 11:16, `pre_state=BLOCKED`. |
| JUBLFOOD | SQ | Lock 11:10, SQ 12:05, `pre_state=WATCHING` — waited on SQ threshold / secondary FSM, **after** first impulse. |
| PAYTM | SQ | Lock 12:10, SQ 12:16, `pre_state=BLOCKED`. |
| ASHOKLEY | SQ | Lock 12:00, SQ 12:25, `pre_state=WAIT FOR PULLBACK`. |
| APLAPOLLO | SQ | vwap_adx @ 12:20, SQ 13:06, `pre_state=WATCHING`. |
| LTM | SQ | BULL re-entry 13:00, SQ 13:26, `pre_state=BLOCKED` — hours after the 11:05 expansion; **already ~7% from open / ~99th percentile of day’s range**. |
| MCX | Other | Late-session READY; take=false; not a meaningful entry. |

**Pattern:** SQ cannot fire before Top-6 + lock membership + grade A/B + score ≥75. That stack often completes **after** the readable impulse. Organic 09:45 READY fires at window open on morning lock side **without** requiring current VWAP-side confirmation — which is how wrong-side LONGs appear.

---

## C. Missed reversals (FORTIS)

### What happened

| Time | Event |
|---|---|
| 10:10 | FORTIS enters lock **BULL** (`vwap_adx_promotion`) |
| 10:16 | READY LONG (direction-correct vs VWAP) |
| 10:35 | SQ re-stamp READY LONG; badge shows SQ |
| **10:40** | **Removed from lock** (`vwap_adx_slope_expiry`) |
| ~10:35+ | Price begins opposite-side VWAP closes (data: first opposite 2-bar @ 10:35) |
| ~11:15 | Manual TV: strong SHORT develops |
| After 10:40 | **No badge / checklist SHORT evaluation** — FORTIS direction in all badge rows = LONG only; checklist row remains LONG |

### Architecture answer

**Deliberate design of the lock board, not a one-off bug:** the checklist evaluates each symbol on **one inherited direction**. There is no parallel “also score SHORT” path for a LONG-locked name. Once removed from lock, the symbol is not actively promoted for the opposite side unless it **re-enters** lock on that side (LTM did BEAR→BULL across the day via separate entry events; FORTIS never re-entered as BEAR).

What it would take to catch the 11:15 SHORT (design options only — not implementing):

1. Continuously score both sides for lock members, or  
2. On hard thesis break (e.g. sustained opposite VWAP closes / EMA10 breakdown), allow opposite-side candidate state, or  
3. Keep removed names in a “watch opposite” shadow until re-lock.

None of these exist today.

---

## D. Aggregate severity

| Question | Day-1 answer |
|---|---|
| Direction-correct at promote? | **8/12** clearly correct on VWAP side; **3/12 morning organics wrong or flipping under VWAP at fire**; MCX mixed/late |
| Wrong-side caused by stale side? | **Yes — side from lock entry, not re-checked** |
| Hard VWAP-side READY gate? | **No (off / absent)** |
| Avg / median lag (1% from open → READY) | **mean ~127m / median ~96m** |
| Named SQ vs manual move | **+76 to +142 minutes**; LTM/DIVISLAB near top of day’s range at fire |
| Opposite-side after failed LONG? | **Not evaluated** (FORTIS) |

### Classification

| Issue | Organic FSM | SQ path |
|---|---|---|
| Wrong direction vs live VWAP | **Yes (morning trio)** | Not the main failure today at fire time |
| Late vs early impulse | Mild for 09:45 window opens; **severe for missed early shorts** | **Systemic** (Top-6/lock/SQ stack) |
| No reverse re-eval | **Yes** | **Yes** (inherits same one-direction board) |

**This is not well described as “an unlucky day.”** The wrong-side organics follow directly from “side = morning RS lock” + “no VWAP-side hard gate at READY.” The SQ lateness follows directly from admission/score timing after the move. The FORTIS miss follows directly from one-direction lock membership.

---

## Implications for “continue SQ this week?” (facts only — decision is yours)

Evidence that would support **pause / immediate gate** before more live risk:

- Proven path to READY LONG with price already below VWAP (BAJAJFINSV live audit).
- Proven absence of opposite-side watching after thesis break (FORTIS).
- SQ fires routinely after 1–2+ hours and deep into the day’s range.

Evidence that would support **continue with scrutiny** (not a clean bill of health):

- Most SQ fires today were VWAP-side-correct at the bell (wrong problem is timing/extension, not side).
- Organic take path and SQ take-enable on the live object are working as previously corrected.

No recommendation is asserted here — only the data required to choose.

---

## Appendix — raw pointers

- Live BAJAJFINSV @ 09:45:06: `price=2072.5`, `vwap=2075.28`, `delta=−2.78` (`direction_live_audit_series.txt`)
- Forming tips CHOLAFIN/INOXWIND/BAJAJFINSV @ 09:45 bar: all below recon VWAP (`direction_structural_raw.json`)
- FORTIS membership remove 10:40; badge directions all LONG (`direction_lock_membership.txt`)
- SQ `pre_state` column: BLOCKED / WAIT / WATCHING at promote (`sq_ready_promotion_log`)
