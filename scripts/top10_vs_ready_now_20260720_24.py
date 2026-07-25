#!/usr/bin/env python3
"""Read-only: Top-10 vs READY NOW per (date, symbol).

Offline only — no live gating / ranking / production changes.

Primary consumer of this logic in production is:
  GET /scan/diagnostics/top10-vs-ready-now  (backend.services.top10_vs_ready_now)

This script is for offline artifact dumps and refreshing the public seed JSON.

Run on paperclip app container:
  PYTHONPATH=/app /opt/venv/bin/python /tmp/top10_vs_ready_now_20260720_24.py

Env:
  OUT_DIR   output dir (default /tmp/top10_vs_ready_now_20260720_24)
  START / END  inclusive session dates (default 2026-07-20 .. 2026-07-24)
  WRITE_PUBLIC_SEED=1  also merge/write frontend/public/data/top10-vs-ready-now.json
  PUBLIC_SEED_PATH     override path for the public seed JSON

Matching ("at/near"):
  A Top-10 RS scan at T is covered by READY NOW if any READY-family render
  for that symbol-day has |logged_at − T| ≤ 5 minutes (inclusive).

Daily feed (production):
  Prefer the live API — once session rows exist in kavach_ready_consistency_log
  and relative_strength_snapshot, open /top10-vs-ready-now.html (Prefer live DB)
  or GET /scan/diagnostics/top10-vs-ready-now?start=YYYY-MM-DD&end=YYYY-MM-DD.
  No page rebuild needed.

  Optional seed refresh (static fallback / offline):
    START=2026-07-25 END=2026-07-25 WRITE_PUBLIC_SEED=1 \\
      PYTHONPATH=/app /opt/venv/bin/python scripts/top10_vs_ready_now_20260720_24.py
    Then commit the updated frontend/public/data/top10-vs-ready-now.json if desired.
"""
from __future__ import annotations

import csv
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

IST = pytz.timezone("Asia/Kolkata")
OUT = Path(os.environ.get("OUT_DIR", "/tmp/top10_vs_ready_now_20260720_24"))
START = os.environ.get("START", "2026-07-20")
END = os.environ.get("END", "2026-07-24")
NEAR_WINDOW = timedelta(minutes=5)
_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PUBLIC_SEED = _ROOT / "frontend" / "public" / "data" / "top10-vs-ready-now.json"
WRITE_PUBLIC_SEED = os.environ.get("WRITE_PUBLIC_SEED", "").strip() in ("1", "true", "yes")
PUBLIC_SEED_PATH = Path(os.environ.get("PUBLIC_SEED_PATH", str(DEFAULT_PUBLIC_SEED)))


def q(sql: str, **params) -> List[Dict[str, Any]]:
    with SessionLocal() as s:
        return [dict(r) for r in s.execute(text(sql), params).mappings().all()]


def ist(dt) -> Optional[datetime]:
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M")


def is_ready_family(state: Any) -> bool:
    return (state or "").upper().startswith("READY")


def _merge_public_seed(new_rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> None:
    """Merge rows for START..END into the public seed JSON (replace overlapping days)."""
    PUBLIC_SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing: Dict[str, Any] = {}
    if PUBLIC_SEED_PATH.is_file():
        try:
            existing = json.loads(PUBLIC_SEED_PATH.read_text(encoding="utf-8"))
        except Exception:
            existing = {}

    keep = [
        r
        for r in (existing.get("rows") or [])
        if not (START <= str(r.get("date") or "") <= END)
    ]
    merged_raw = keep + new_rows
    merged: List[Dict[str, Any]] = []
    per_day: Dict[str, int] = defaultdict(int)
    per_day_zero: Dict[str, int] = defaultdict(int)
    for r0 in merged_raw:
        r = dict(r0)
        z = r.get("zero_ready_top10")
        r["zero_ready_top10"] = z is True or str(z).upper() in ("Y", "TRUE", "1")
        for k in ("n_ready_now", "n_top10_not_ready", "n_top10_scans"):
            try:
                r[k] = int(r.get(k) or 0)
            except (TypeError, ValueError):
                r[k] = 0
        per_day[r["date"]] += 1
        if r["zero_ready_top10"]:
            per_day_zero[r["date"]] += 1
        merged.append(r)

    merged.sort(key=lambda r: (r["date"], -r["n_ready_now"], r["symbol"]))
    sessions = sorted(per_day.keys())
    out = {
        "ok": True,
        "source": "seed",
        "generated_at_ist": datetime.now(IST).isoformat(),
        "purpose": meta.get("purpose"),
        "live_changes": "none",
        "matching_definition": meta.get("matching_definition"),
        "sessions": sessions,
        "date_range": [sessions[0], sessions[-1]] if sessions else [START, END],
        "row_counts": {
            "total": len(merged),
            "per_day": dict(sorted(per_day.items())),
            "zero_ready_top10_total": sum(per_day_zero.values()),
            "zero_ready_top10_per_day": dict(sorted(per_day_zero.items())),
        },
        "source_counts": meta.get("source_counts"),
        "caveats": meta.get("caveats"),
        "rows": merged,
    }
    PUBLIC_SEED_PATH.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    print(f"       wrote public seed {PUBLIC_SEED_PATH} rows={len(merged)}")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    started = datetime.now(IST).isoformat()
    print(f"[0] Top-10 vs READY NOW  START={START} END={END}  near=±{NEAR_WINDOW}")

    renders = q(
        """
        SELECT session_date::text AS session_date,
               UPPER(symbol) AS symbol,
               rendered_state,
               logged_at
        FROM kavach_ready_consistency_log
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
        ORDER BY session_date, symbol, logged_at
        """,
        a=START,
        b=END,
    )
    print(f"    consistency_log rows: {len(renders)}")

    rs_rows = q(
        """
        SELECT scan_time,
               UPPER(symbol) AS symbol,
               ranking_type,
               rank_position
        FROM relative_strength_snapshot
        WHERE scan_time >= CAST(:a AS timestamp)
          AND scan_time < CAST(:b AS timestamp) + INTERVAL '1 day'
          AND rank_position IS NOT NULL
          AND rank_position BETWEEN 1 AND 10
        ORDER BY scan_time, ranking_type, rank_position
        """,
        a=START,
        b=END,
    )
    print(f"    RS Top-10 rows: {len(rs_rows)}")

    ready_times: Dict[Tuple[str, str], List[datetime]] = defaultdict(list)
    ready_count: Dict[Tuple[str, str], int] = defaultdict(int)
    for r in renders:
        if not is_ready_family(r["rendered_state"]):
            continue
        t = ist(r["logged_at"])
        if not t:
            continue
        day = r["session_date"]
        key = (day, r["symbol"])
        ready_count[key] += 1
        ready_times[key].append(t)

    top10_scans: Dict[Tuple[str, str], List[datetime]] = defaultdict(list)
    seen_scan: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for r in rs_rows:
        t = ist(r["scan_time"])
        if not t:
            continue
        day = t.strftime("%Y-%m-%d")
        if day < START or day > END:
            continue
        key = (day, r["symbol"])
        iso = t.isoformat()
        if iso in seen_scan[key]:
            continue
        seen_scan[key].add(iso)
        top10_scans[key].append(t)

    for key in top10_scans:
        top10_scans[key].sort()

    all_keys = sorted(set(ready_count.keys()) | set(top10_scans.keys()))
    print(f"    symbol-days in scope: {len(all_keys)}")

    rows_out: List[Dict[str, Any]] = []
    zero_ready_top10 = 0
    per_day: Dict[str, int] = defaultdict(int)
    per_day_zero: Dict[str, int] = defaultdict(int)

    for day, sym in all_keys:
        key = (day, sym)
        r_times = ready_times.get(key, [])
        n_ready = ready_count.get(key, 0)
        ready_hhmm = sorted({hhmm(t) for t in r_times})

        uncovered_hhmm: List[str] = []
        seen_mm: Set[str] = set()
        for st in top10_scans.get(key, []):
            covered = any(abs((rt - st).total_seconds()) <= NEAR_WINDOW.total_seconds() for rt in r_times)
            if covered:
                continue
            mm = hhmm(st)
            if mm in seen_mm:
                continue
            seen_mm.add(mm)
            uncovered_hhmm.append(mm)

        zero_flag = n_ready == 0 and len(top10_scans.get(key, [])) > 0
        if zero_flag:
            zero_ready_top10 += 1
            per_day_zero[day] += 1

        per_day[day] += 1
        rows_out.append(
            {
                "date": day,
                "symbol": sym,
                "n_ready_now": n_ready,
                "ready_now_times": ",".join(ready_hhmm),
                "top10_not_ready_times": ",".join(uncovered_hhmm),
                "n_top10_not_ready": len(uncovered_hhmm),
                "zero_ready_top10": "Y" if zero_flag else "",
                "n_top10_scans": len(top10_scans.get(key, [])),
            }
        )

    rows_out.sort(key=lambda r: (r["date"], -r["n_ready_now"], r["symbol"]))

    csv_path = OUT / f"top10_vs_ready_now_{START.replace('-', '')}_{END.replace('-', '')}.csv"
    if START == "2026-07-20" and END == "2026-07-24":
        csv_path = OUT / "top10_vs_ready_now_20260720_24.csv"
    fields = [
        "date",
        "symbol",
        "n_ready_now",
        "ready_now_times",
        "top10_not_ready_times",
        "n_top10_not_ready",
        "zero_ready_top10",
        "n_top10_scans",
    ]
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows_out:
            w.writerow({k: row[k] for k in fields})

    manifest = {
        "generated_at_ist": started,
        "purpose": "Top-10 vs READY NOW per (date, symbol) — read-only",
        "live_changes": "none",
        "sessions": sorted(per_day.keys()),
        "date_range": [START, END],
        "matching_definition": (
            "A relative_strength_snapshot Top-10 scan at T is covered by READY NOW "
            "if any READY-family render (rendered_state starts with READY, including "
            "READY(RECHECK)) for that symbol-day has |logged_at − T| ≤ 5 minutes "
            "(inclusive). Uncovered scan times are listed as distinct HH:MM (IST)."
        ),
        "ready_count_def": (
            "n_ready_now = count of READY-family consistency_log rows "
            "(each render counted). ready_now_times = distinct HH:MM of those renders."
        ),
        "top10_def": (
            "rank_position 1–10 on either ranking_type (bull/bear). "
            "Multiple rows at the same scan_time (e.g. both sides) count once. "
            "top10_not_ready_times / n_top10_not_ready use distinct HH:MM of uncovered scans."
        ),
        "row_counts": {
            "total": len(rows_out),
            "per_day": dict(sorted(per_day.items())),
            "zero_ready_top10_total": zero_ready_top10,
            "zero_ready_top10_per_day": dict(sorted(per_day_zero.items())),
        },
        "source_counts": {
            "consistency_log_rows": len(renders),
            "ready_family_renders": sum(ready_count.values()),
            "rs_top10_rows": len(rs_rows),
            "symbol_days": len(all_keys),
        },
        "caveats": [
            "relative_strength_snapshot only persists Top-10 per side; absence ≠ not scored.",
            "READY-family = rendered_state.upper().startswith('READY') (READY / READY(RECHECK)).",
            "Matching window ±5m may differ from same-10m-bar flooring near bar edges.",
            "n_ready_now counts renders; ready_now_times dedupe by minute — count can exceed listed times.",
            "Top-10 times listed only when uncovered; fully covered Top-10 symbols have blank top10_not_ready_times.",
            "Scope = union of (≥1 Top-10 scan) OR (≥1 READY-family render) that day.",
            "zero_ready_top10=Y flags Top-10-never-READY symbol-days (n_ready_now=0).",
        ],
        "artifacts": {
            "csv": str(csv_path.name),
            "manifest": "00_manifest.json",
        },
        "public_page": "/top10-vs-ready-now.html",
        "public_api": "/scan/diagnostics/top10-vs-ready-now",
    }
    with (OUT / "00_manifest.json").open("w") as f:
        json.dump(manifest, f, indent=2)

    if WRITE_PUBLIC_SEED:
        seed_rows = []
        for row in rows_out:
            seed_rows.append({**row, "zero_ready_top10": row["zero_ready_top10"] == "Y"})
        _merge_public_seed(seed_rows, manifest)

    print(f"[done] rows={len(rows_out)} zero_ready_top10={zero_ready_top10}")
    print(f"       per_day={dict(sorted(per_day.items()))}")
    print(f"       wrote {csv_path}")


if __name__ == "__main__":
    main()
