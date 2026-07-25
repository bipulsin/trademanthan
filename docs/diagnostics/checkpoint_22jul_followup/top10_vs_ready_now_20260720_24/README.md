# Top-10 vs READY NOW — 2026-07-20 → 2026-07-24

**Status:** complete (read-only). **No live ranking, FSM, gating, or production changes.**

**Sessions:** 2026-07-20 → 2026-07-24 (5 NSE sessions).  
**Script:** `scripts/top10_vs_ready_now_20260720_24.py` (ran on paperclip app container).

**Public page:** https://www.tradewithcto.com/top10-vs-ready-now.html  
**Public API:** `GET /scan/diagnostics/top10-vs-ready-now?start=&end=` (also `/api/diagnostics/…`)  
**Seed JSON:** `frontend/public/data/top10-vs-ready-now.json`

---

## Daily feed (how to add a day)

**Preferred (automatic):** once the session exists in `kavach_ready_consistency_log` + `relative_strength_snapshot`, open the page with **Prefer live DB** checked, or call:

```bash
curl -s 'https://www.tradewithcto.com/scan/diagnostics/top10-vs-ready-now?start=2026-07-25&end=2026-07-25'
```

No HTML rebuild. Response is cached ~5 minutes (`refresh=1` bypasses).

**Optional seed refresh** (static `/data/…` fallback):

```bash
START=2026-07-25 END=2026-07-25 WRITE_PUBLIC_SEED=1 \
  PYTHONPATH=/app /opt/venv/bin/python scripts/top10_vs_ready_now_20260720_24.py
# commit frontend/public/data/top10-vs-ready-now.json if you want the seed updated in git
```

---

## Artifacts

| File | Contents |
|---|---|
| `top10_vs_ready_now_20260720_24.csv` | One row per (date, symbol) — primary deliverable |
| `00_manifest.json` | Counts, matching definition, caveats |
| `README.md` | This file |

---

## Columns (CSV)

| Column | Meaning |
|---|---|
| `date` | Session date (IST calendar day) |
| `symbol` | Uppercase symbol |
| `n_ready_now` | Count of READY-family renders in `kavach_ready_consistency_log` (`rendered_state` starts with `READY`, including `READY(RECHECK)`) |
| `ready_now_times` | Comma-separated distinct `HH:MM` (IST) of those renders — **minute-deduped** |
| `top10_not_ready_times` | Comma-separated distinct `HH:MM` for Top-10 scans **not** covered by READY (see matching) |
| `n_top10_not_ready` | Length of `top10_not_ready_times` |
| `zero_ready_top10` | `Y` if Top-10 appeared that day but `n_ready_now=0` |
| `n_top10_scans` | Distinct `scan_time` count with rank 1–10 (either side) |

**Sort:** `date` ascending, then `n_ready_now` descending, then `symbol`. Zero-READY Top-10 rows sit at the bottom of each day (`zero_ready_top10=Y`).

---

## Matching definition (“at/near”)

A `relative_strength_snapshot` Top-10 scan at time **T** is **covered** by READY NOW if any READY-family render for that symbol-day has:

> **|logged_at − T| ≤ 5 minutes** (inclusive)

Uncovered scans contribute their distinct `HH:MM` to `top10_not_ready_times`.

**Why ±5m (not same 10m bar):** RS scans and consistency-log renders are not locked to identical bar boundaries; ±5m catches near-miss timing without requiring exact minute equality. Same-10m-bar flooring would differ at bar edges (e.g. scan 10:14 vs READY 10:16).

---

## Scope

Union of:
- ≥1 Top-10 appearance that day (`rank_position` 1–10, either `ranking_type`), **or**
- ≥1 READY-family render that day

Includes Top-10-never-READY rows (`n_ready_now=0`, blank `ready_now_times`, `zero_ready_top10=Y`).

---

## Row counts

| Day | Rows | Zero-READY Top-10 |
|---|---:|---:|
| 2026-07-20 | 167 | 152 |
| 2026-07-21 | 145 | 132 |
| 2026-07-22 | 164 | 150 |
| 2026-07-23 | 154 | 141 |
| 2026-07-24 | 162 | 149 |
| **Total** | **792** | **724** |

Source footprint: 2649 consistency_log rows → **870** READY-family; **6380** RS Top-10 rows; **68** symbol-days with ≥1 READY.

---

## Caveats

1. **`relative_strength_snapshot` persists Top-10 per side only** (~20 rows/scan). Absence from the table means outside Top-10 that scan, **not** “not scored.”
2. READY count is **render count**; times are **minute-deduped** — `n_ready_now` can exceed the number of listed `HH:MM` values.
3. Top-10 uniqueness is by `scan_time` (both sides at the same instant count once).
4. Fully covered Top-10 symbol-days have blank `top10_not_ready_times` / `n_top10_not_ready=0`.
5. On this window, every READY symbol-day also had ≥1 Top-10 scan (no READY-only rows).
6. No sessions after 2026-07-24 in these tables (07-25 Saturday).

---

## Reproduction

```bash
scp scripts/top10_vs_ready_now_20260720_24.py paperclip:/tmp/
./scripts/paperclip-ssh.sh 'docker cp /tmp/top10_vs_ready_now_20260720_24.py twcto-app-1:/tmp/ && \
  docker compose -f /home/ubuntu/twcto/docker-compose.yml exec -T app bash -lc \
  "OUT_DIR=/tmp/top10_vs_ready_now_20260720_24 START=2026-07-20 END=2026-07-24 \
   PYTHONPATH=/app /opt/venv/bin/python -u /tmp/top10_vs_ready_now_20260720_24.py"'
```
