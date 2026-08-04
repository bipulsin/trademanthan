# Candle-warm shared rate-limit bug (infra fix, `bb9a819`)

**Category:** Infrastructure — separate from scoring-architecture and RS-candidate-selection threads.  
**Checkpoint:** 8-Aug (carry from 2026-08-03 live session)  
**Commit:** `bb9a819` (deployed paperclip ~2026-08-03 15:17 IST)  
**Related arc:** `docs/CHECKPOINTS_22JUL_8AUG.md` → *Stale-entry / candle-warm arc (2026-07-31)*  
**Monitor:** `/candle-warm-deny.html` · `GET /api/market-data/candle-warm-cycles`

---

## Root cause

10m (`currmth`) and hourly (`stock+nextmth`) candle-warm jobs shared one process-wide `SlidingWindowRateLimiter` (5/s, 120/min, 1500/30min) and the same Upstox API key/token, with **no mutual exclusion** between the two job ids. `max_instances=1` was **per-job-id** and did not prevent cross-job overlap.

Hourly job dispatched ~400 symbols via `ThreadPoolExecutor(max_workers=10)` with `max_wait=90s`, causing mass instant-deny under budget pressure (~28s to reject 95% of requests).

Legacy hourly cron at `:20` was too close to the 10m job's `:25` slot once hourly runtime exceeded ~5 minutes, causing bucket collisions — worst observed at **completion** timestamp **11:41** (10m: 27.5% deny / 967s elapsed; hourly: 27.8% deny / 1269s elapsed, both overlapping). Log timestamps were end-of-run (`updated_at_ist`), not start.

Symbol iteration was alphabetical (`ORDER BY stock`), so the same B–S tail symbols starved whenever budget ran dry mid-batch each cycle.

---

## Fix shipped (`bb9a819`, deployed)

| Change | Detail |
|---|---|
| Hourly `max_wait` | Raised **90s → 300s** (process-wide override during stock+nextmth warm) |
| Mutual exclusion | Process mutex `_CANDLE_WARM_LOCK` (blocking, **no timeout**) serializes candle warms across both job ids |
| Schedule | Hourly cron moved **`:20` → `:08`**; legacy `:20` job id removed |
| Fairness | Per-cycle symbol rotation offset so the same alphabetical tail doesn't always starve |
| Observability | New log/UI fields: `concurrent_job_overlap_detected`, `overlapping_with`, `candle_warm_lock_wait_sec`, `started_at_ist` |

---

## Mutex behavior confirmed (code inspection, 2026-08-03)

| Question | Answer |
|---|---|
| Contended trigger | **Queues** — second job **blocks and waits** for the mutex |
| Skip? | **Never** skips due to lock |
| Bypass? | **Never** proceeds without the lock |
| Max wait | **None** on the mutex (indefinite). Slot-level `max_wait` (90s/300s) is separate |
| Visibility | Queued job completes late with `candle_warm_lock_wait_sec` > 0 (and usually `concurrent_job_overlap_detected=True`); no distinct “skipped due to lock” cycle type |

APScheduler `max_instances=1` remains per job id and does **not** replace the mutex for cross-job serialization.

---

## Before metrics (pre-fix, 2026-08-03 log)

| Job | Typical deny% | Notes |
|---|---:|---|
| Hourly `stock+nextmth` | **95.0–95.5%** | Often ~28s elapsed (mass instant deny) |
| 10m `currmth` | **0%** baseline | Spikes **15–19%** every ~30–40 min (same alphabetical tail) |
| Worst overlap | Both @ completion **11:41** | 10m 27.5%/967s; hourly 27.8%/1269s |

---

## After metrics (post-deploy, as of filing 2026-08-03)

| Field | Value |
|---|---|
| Samples with new fields | **n=1** |
| Cycle | `2026-08-03 15:25:00` · `scheduled_10m` |
| `candle_warm_lock_wait_sec` | **0.0** |
| `concurrent_job_overlap_detected` | **False** |
| `candle_deny_pct` | **0.0** (200/200 indicators ok) |

Contended wait **not yet observed** — deploy timing (~15:17 IST) fell between the `:08` hourly and `:25` 10m runs, so no real collision has occurred yet to exercise the mutex under contention. In-process deny-monitor ring buffer resets on deploy; use `logs/smart_future_algo.log` (or post-restart cycles) for continuity.

---

## Status

**Design and logging confirmed correct via code inspection.** Live 2026-08-04 showed mutex serialization still left `scheduled_10m` starved behind the ~400-key hourly warm (deny 94–99.5%, RS skips up to 195/200, elapsed up to 633s).

---

## Resolution (8-Aug): pause hourly job

Usage audit confirmed `stock_vwap` / `stock_ema5` / `nextmth_future_vwap` / `nextmth_future_ema5` have **zero live readers**. Hourly schedule paused via `STOCK_NEXT_VWAP_EMA_HOURLY_ENABLED=false` (default); code + columns retained for rollback.

See `docs/CHECKPOINTS_22JUL_8AUG.md` → *Pause hourly stock/next VWAP·EMA5 job* for before/after verification table vs 2026-08-04 baseline.

**Close criteria (updated):** fill after metrics on that checkpoint after 1–2 sessions with hourly paused (expect near-zero 10m deny / RS skips; elapsed ~230–330s; no hourly `lock_wait`).
