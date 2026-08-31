# Breakfast dry-run — 2026-08-31

**Commit:** `92a8628` (HEAD at audit time)  
**Generated:** 2026-08-31 ~15:05 IST  
**Mode:** Read-only reconstruction; no DB writes

---

## Executive summary

| Item | Result |
|------|--------|
| Live freeze at 9:20:30 | **FAILED** — `lock_status=failed`, `failure_reason=no_sectors_at_freeze` |
| `breakfast_live_signals` rows | 6 rows exist but are **retrospective backfill** (`manual_note=backfilled_retrospective`), not live capture |
| 1m tick reconstruction (live_tick path) | **Impossible for 2026-08-31** — Upstox V2 `minutes/1` historical returns no bars for today |
| 5m off-cycle reconstruction | **Matches backfill** — SHORT, Healthcare + Metal, 6 stocks |
| Root cause (high confidence) | `live_tick` uses `minutes/1` without V3 intraday merge; same-day 1m bars unavailable at freeze time |

---

## 2026-08-31 — 1m tick reconstruction (FAILED)

Ran `live_tick` logic on prod (`docker compose exec app python3`) with `fetch_1m_parallel` for session `2026-08-31`.

### Upstox 1m data check

| Instrument | Total 1m bars returned | Bars on 2026-08-31 |
|------------|------------------------|---------------------|
| NIFTY50 | 1125 | **0** |
| TORNTPHARM FUT | 1155 | **0** |

Latest 1m bar in API response: `2026-08-28T15:29:00+05:30` (prior session).

Disk cache (`/home/ubuntu/trademanthan/data/breakfast_strategy_candle_cache/*_1m.json`) also has **0** bars for 2026-08-31.

### Tick output (all empty)

| Tick (job fires) | `upto_hhmm` | NIFTY bias | Sector standings | Picks |
|------------------|-------------|------------|------------------|-------|
| 9:16:05 | 09:17 | — | — | — |
| 9:17:05 | 09:18 | — | — | — |
| 9:18:05 | 09:19 | — | — | — |
| 9:19:05 | 09:20 | — | — | — |
| 9:20:05 | 09:21 | — | — | — |

**Conclusion:** This is consistent with prod freeze failure. The scheduler likely ran, fetched stale 1m history (through Fri 28-Aug), built empty forming bars, and froze with zero sectors.

### Why 5m backfill works but 1m live does not

`upstox_service.py` merges V3 intraday only for:

```python
_INTRADAY_MERGE_INTERVALS = {"minutes/5", "minutes/15", "minutes/30", "hours/1"}
```

`minutes/1` is **excluded**. The 5m path (used by `build_off_cycle_preview_state` / backfill) gets today's session via V3 intraday merge. The live tick path (`fetch_1m_parallel` → `minutes/1`) does not.

---

## 2026-08-31 — 5m off-cycle reconstruction (SUCCESS)

Command: `scripts/backfill_breakfast_live_snapshot.py --session-date 2026-08-31` (dry-run, no `--apply`)

| Field | Value |
|-------|-------|
| NIFTY bias | negative |
| NIFTY bias % | -0.002% |
| Direction | SHORT |

### Sector picks at 9:20

| Rank | Sector | Move % | Stocks (rank 1–3) |
|------|--------|--------|-------------------|
| 1 | Nifty Healthcare | -0.281% | TORNTPHARM (-1.161%), ZYDUSLIFE (-0.983%), GLENMARK (-0.894%) |
| 2 | Nifty Metal | -0.233% | NATIONALUM (-3.221%), HINDALCO (-2.612%), TATASTEEL (-2.267%) |

### Comparison to existing DB backfill

| Symbol | DB sector_move | DB stock_move | Recon stock_move | Match |
|--------|----------------|---------------|------------------|-------|
| TORNTPHARM | -0.281 | -1.161 | -1.161 | ✓ |
| ZYDUSLIFE | -0.281 | -0.983 | -0.983 | ✓ |
| GLENMARK | -0.281 | -0.894 | -0.894 | ✓ |
| NATIONALUM | -0.233 | -3.221 | -3.221 | ✓ |
| HINDALCO | -0.233 | -2.612 | -2.612 | ✓ |
| TATASTEEL | -0.233 | -2.267 | -2.267 | ✓ |

**All 6 backfill rows match 5m reconstruction exactly.**

### Session lock row (prod, read-only)

```
lock_status: failed
failure_reason: no_sectors_at_freeze
signal_count: 0
capture_source: live_scheduler
```

---

## Reference: 2026-08-28 — 1m tick reconstruction (pipeline validation)

When 1m data exists, the tick pipeline produces evolving standings. Note **sector universe frozen at 9:16** (`picked_at_916`: IT + Telecom) while NIFTY bias flips SHORT at 9:18 — picks then reference frozen sectors with empty stock lists.

| Minute | upto | NIFTY % | Dir | Top sectors | Picks |
|--------|------|---------|-----|-------------|-------|
| 16 | 09:17 | +0.003 | LONG | IT +0.875, Telecom +0.608 | IT: COFORGE, PERSISTENT, NAUKRI; Telecom: INDUSTOWER |
| 17 | 09:18 | +0.011 | LONG | IT +1.244, Telecom +0.676 | IT: COFORGE, TECHM, TCS; Telecom: INDUSTOWER |
| 18 | 09:19 | -0.022 | SHORT | Chemicals -0.463, FMCG -0.424 | PSU Bank, Private Bank — **no stocks** |
| 19 | 09:20 | -0.043 | SHORT | Chemicals -0.528, FMCG -0.449 | PSU Bank, Private Bank — **no stocks** |
| 20 | 09:21 | -0.044 | SHORT | FMCG -0.509, Realty -0.466 | PSU Bank, Private Bank — **no stocks** |

This demonstrates both that the reconstruction script works when 1m data exists, and that **freezing picked sectors at 9:16 can desync from NIFTY bias flips** later in the window.

---

## Gaps / cannot verify

1. **Minute-by-minute 2026-08-31** — blocked by missing same-day 1m API data (even at 15:00 IST).
2. **Prod scheduler logs** — no `breakfast minute tick` / `breakfast freeze lock` lines in current docker log buffer (container may have rotated).
3. **Whether Upstox serves intraday 1m during 9:16–9:20 live** — not testable retrospectively; if it does, today's failure may be timing-specific. If it doesn't, live path is structurally broken on Mondays / after weekends.
