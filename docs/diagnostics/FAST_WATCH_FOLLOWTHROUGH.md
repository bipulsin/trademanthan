# Fast Watch follow-through vs 4–5× movers (Part B)

Generated: 2026-07-20 (paperclip read-only run against production `rs_fast_watch`).

**Sample:** 269 Fast Watch flags, 2026-07-06 → 2026-07-10 (all available history — recording stopped after Jul-10 due to Part A bug). Futures 5m candles via Upstox (`arbitrage_master` currmth key — same instrument Kavach uses).

---

## Definitions (confirmed before measuring)

| Term | Definition |
|---|---|
| **ENTRY_REF (primary)** | Session extreme from 09:15 IST through the flag bar: **low** for LONG, **high** for SHORT. This is the conservative “already covered” distance for an intraday leg. |
| **ENTRY_REF (sensitivity)** | Session **open** (first 5m bar open). |
| **P_flag** | `flip_price` when present, else 5m close at/just before `first_flip_at`. |
| **PRE_FLAG** | \|P_flag − ENTRY_REF\| (floor 5 bps of P_flag). Near-extreme flags excluded from “core” rates. |
| **Follow-through window** | Flag bar → **15:30 IST same session** (Fast Watch is a same-day checklist tool; multi-day out of scope). |
| **REMAINING** | \|session peak after flag − P_flag\| (peak = max high LONG / min low SHORT). |
| **4× / 5× hit** | REMAINING ≥ N × PRE_FLAG (ticket wording: subsequent move ≥ N× distance already covered). |
| **1×-delayed entry** | Close of the **next completed 10m bar** after `first_flip_at` (matches Kavach’s closed-10m cadence). |
| **CAPTURED** | \|peak after delayed entry − P_delayed\|. |
| **False positive (fizzle)** | REMAINING < 1× PRE_FLAG. |

Ambiguity called out: session-extreme ENTRY_REF is harsh on late-day flags (most of the range is already behind you). Open-ref sensitivity and absolute %-of-price metrics are reported so the multiple definition isn’t the only lens.

VWAP-slope-steepening is a **different** signal (Jul-7 research, +6–8pp). It is **not** what Fast Watch stores. `kavach_vwap_raw_log` only starts ~Jul-15 and has no price path — not re-run here. Part B below is **Fast Watch / Kavach-flip only**.

---

## Verdict: **NO-GO as standalone early-entry trigger for 3–4×+ capture after 1× delay**

Fast Watch alone does **not** historically deliver the 4–5× “still ahead” profile Bipul needs, and with a one-bar delay the ≥3× capture rate is ~8–12% while fizzles are ~66–72%.

It still surfaces some absolute same-session continuation (~1/3 of flags leave ≥1% of price on the table; ~1/2 leave ≥0.5%). Useful as a **discretionary early alert**, especially **before 11:00 IST**, not as a replacement for Votes/Grade without further filters.

**Part C (remove Votes/Grade / collapse to Fast Watch-only) should stay deferred.**

---

## Headline rates (core = exclude near-extreme floor cases)

### Primary — ENTRY_REF = session extreme (n=262)

| Metric | Rate | Wilson 95% |
|---|---|---|
| Remaining ≥4× PRE_FLAG | **6.1%** (16) | 3.8–9.7% |
| Remaining ≥5× PRE_FLAG | **4.6%** (12) | 2.6–7.8% |
| Fizzle (remaining <1×) | **72.1%** (189) | 66.4–77.2% |
| 1×-delayed captured ≥3× | **8.4%** (22) | 5.6–12.4% |
| 1×-delayed captured ≥4× | **5.3%** (14) | 3.2–8.8% |

Multiples distribution (remaining / PRE_FLAG): p25=0.14, **p50=0.46**, p75=1.29, p90=3.47.  
Delayed captured multiples: p25=0.11, **p50=0.41**, p75=1.26, p90=3.03.

### Sensitivity — ENTRY_REF = session open (n=263)

| Metric | Rate |
|---|---|
| Remaining ≥4× | 7.6% |
| Remaining ≥5× | 5.7% |
| Fizzle <1× | 66.2% |
| Delayed ≥3× | **11.8%** |
| Delayed ≥4× | 8.4% |
| Median remaining mult | 0.56 |
| Median captured mult | 0.48 |

Same conclusion under the alternate reference.

### Absolute same-session remaining (not a multiple)

| Threshold | Primary core |
|---|---|
| Remaining ≥0.5% of P_flag | 54.2% |
| Remaining ≥1.0% of P_flag | 34.4% |
| Captured (delayed) ≥0.5% | ~52% |
| Captured (delayed) ≥1.0% | ~33% |
| Remaining ≤0 (no favorable peak) | 9 / 269 |
| Captured ≤0 after delay | 14 / 269 |

---

## Cohorts (primary core)

| Cohort | n | Delayed ≥3× | Fizzle | Remaining ≥1% px |
|---|---|---|---|---|
| LONG | 127 | 6.3% | 79.5% | — |
| SHORT | 135 | 10.4% | 65.2% | — |
| Confirmed BUY/SELL | 203 | 9.9% | 70.0% | 37.9% |
| READY / READY SHORT | 59 | 3.4% | 79.7% | 22.0% |
| Flag before 11:00 IST | 74 | **21.6%** | **47.3%** | 47.3% |
| 11:00–13:00 | 88 | 6.8% | 70.5% | 51.1% |
| After 13:00 | 100 | **0.0%** | **92.0%** | 10.0% |
| Edge-triggered era Jul 9–10 | 42 | 2.4% | 61.9% | 54.8% |
| Earlier Jul 6–8 (mostly level-era rows) | 220 | 9.5% | 74.1% | 30.5% |

Morning flags are the only cohort approaching “usable” on the delayed ≥3× bar — still far from a majority, and 4–5× remaining remains rare.

---

## Cost of misses (why “worked X%” alone is insufficient)

- **~7 in 10** flags are fizzles under the multiple definition (less still ahead than already done).
- Even in absolute terms, **~2 in 3** leave <1% of price after the flag in-session.
- Late-day flags (≥13:00) are nearly all noise for this thesis (0% delayed ≥3×, 92% fizzle).
- READY flips underperform confirmed BUY/SELL.

---

## Artifacts

- `docs/diagnostics/fast_watch_followthrough.json` — per-flag rows + primary summary  
- `docs/diagnostics/fast_watch_followthrough_sensitivity.json` — open-ref sensitivity  
- `scripts/analyze_fast_watch_followthrough.py` — reproducible runner  

---

## Part A cross-link

Recording died after 2026-07-10 (`NameError: latest_audit_pair` in `rs_fast_watch.record_fast_watch_flips`, swallowed at debug). UI flag was on; audit kept writing. Fix is in this PR — merge + deploy restores live Fast Watch; it does **not** change the Part B no-go on standalone 4–5× entry.
