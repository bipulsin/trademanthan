"""Read-only Top-10 vs READY NOW diagnostics.

Computes per (date, symbol) coverage of relative_strength_snapshot Top-10
scans by READY-family renders in kavach_ready_consistency_log (±5m match).

No live gating / ranking / production changes.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
NEAR_WINDOW = timedelta(minutes=5)
CACHE_TTL_SEC = 300  # 5 minutes

_ROOT = Path(__file__).resolve().parents[2]
SEED_JSON = _ROOT / "frontend" / "public" / "data" / "top10-vs-ready-now.json"

_cache_lock = threading.Lock()
_cache: Dict[str, Any] = {"key": None, "expires": 0.0, "payload": None}


def _q(sql: str, **params) -> List[Dict[str, Any]]:
    with SessionLocal() as s:
        return [dict(r) for r in s.execute(text(sql), params).mappings().all()]


def _ist(dt) -> Optional[datetime]:
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M")


def _is_ready_family(state: Any) -> bool:
    return (state or "").upper().startswith("READY")


def _default_date_range() -> Tuple[str, str]:
    """Default: prefer seeded range; else last 5 calendar days ending today IST."""
    seed = load_seed()
    if seed and seed.get("date_range"):
        dr = seed["date_range"]
        if len(dr) == 2 and dr[0] and dr[1]:
            return str(dr[0]), str(dr[1])
    today = datetime.now(IST).date()
    end = today.isoformat()
    start = (today - timedelta(days=4)).isoformat()
    return start, end


def load_seed() -> Optional[Dict[str, Any]]:
    try:
        if not SEED_JSON.is_file():
            return None
        return json.loads(SEED_JSON.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("top10_vs_ready_now seed load failed: %s", exc)
        return None


def compute_rows(start: str, end: str) -> Dict[str, Any]:
    """Build rows from DB for inclusive [start, end] session dates."""
    started = datetime.now(IST).isoformat()
    renders = _q(
        """
        SELECT session_date::text AS session_date,
               UPPER(symbol) AS symbol,
               rendered_state,
               logged_at
        FROM kavach_ready_consistency_log
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
        ORDER BY session_date, symbol, logged_at
        """,
        a=start,
        b=end,
    )
    rs_rows = _q(
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
        a=start,
        b=end,
    )

    ready_times: Dict[Tuple[str, str], List[datetime]] = defaultdict(list)
    ready_count: Dict[Tuple[str, str], int] = defaultdict(int)
    for r in renders:
        if not _is_ready_family(r["rendered_state"]):
            continue
        t = _ist(r["logged_at"])
        if not t:
            continue
        key = (r["session_date"], r["symbol"])
        ready_count[key] += 1
        ready_times[key].append(t)

    top10_scans: Dict[Tuple[str, str], List[datetime]] = defaultdict(list)
    seen_scan: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for r in rs_rows:
        t = _ist(r["scan_time"])
        if not t:
            continue
        day = t.strftime("%Y-%m-%d")
        if day < start or day > end:
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
    rows_out: List[Dict[str, Any]] = []
    zero_ready_top10 = 0
    per_day: Dict[str, int] = defaultdict(int)
    per_day_zero: Dict[str, int] = defaultdict(int)

    for day, sym in all_keys:
        key = (day, sym)
        r_times = ready_times.get(key, [])
        n_ready = ready_count.get(key, 0)
        ready_hhmm = sorted({_hhmm(t) for t in r_times})

        uncovered_hhmm: List[str] = []
        seen_mm: Set[str] = set()
        for st in top10_scans.get(key, []):
            covered = any(
                abs((rt - st).total_seconds()) <= NEAR_WINDOW.total_seconds()
                for rt in r_times
            )
            if covered:
                continue
            mm = _hhmm(st)
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
                "zero_ready_top10": zero_flag,
                "n_top10_scans": len(top10_scans.get(key, [])),
            }
        )

    rows_out.sort(key=lambda r: (r["date"], -r["n_ready_now"], r["symbol"]))
    sessions = sorted(per_day.keys())
    return {
        "ok": True,
        "source": "db",
        "generated_at_ist": started,
        "purpose": "Top-10 vs READY NOW per (date, symbol) — read-only",
        "live_changes": "none",
        "matching_definition": (
            "A relative_strength_snapshot Top-10 scan at T is covered by READY NOW "
            "if any READY-family render (rendered_state starts with READY, including "
            "READY(RECHECK)) for that symbol-day has |logged_at − T| ≤ 5 minutes "
            "(inclusive). Uncovered scan times are listed as distinct HH:MM (IST)."
        ),
        "sessions": sessions,
        "date_range": [start, end] if sessions else [start, end],
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
        "rows": rows_out,
    }


def _filter_seed(seed: Dict[str, Any], start: str, end: str) -> Dict[str, Any]:
    norm_rows: List[Dict[str, Any]] = []
    per_day: Dict[str, int] = defaultdict(int)
    per_day_zero: Dict[str, int] = defaultdict(int)
    for r0 in seed.get("rows") or []:
        d = str(r0.get("date") or "")
        if not (start <= d <= end):
            continue
        r = dict(r0)
        z = r.get("zero_ready_top10")
        r["zero_ready_top10"] = z is True or str(z).upper() in ("Y", "TRUE", "1")
        for k in ("n_ready_now", "n_top10_not_ready", "n_top10_scans"):
            try:
                r[k] = int(r.get(k) or 0)
            except (TypeError, ValueError):
                r[k] = 0
        per_day[d] += 1
        if r["zero_ready_top10"]:
            per_day_zero[d] += 1
        norm_rows.append(r)
    sessions = sorted(per_day.keys())
    return {
        "ok": True,
        "source": "seed",
        "generated_at_ist": seed.get("generated_at_ist"),
        "purpose": seed.get("purpose") or "Top-10 vs READY NOW per (date, symbol) — read-only",
        "live_changes": "none",
        "matching_definition": seed.get("matching_definition"),
        "sessions": sessions,
        "date_range": [start, end],
        "row_counts": {
            "total": len(norm_rows),
            "per_day": dict(sorted(per_day.items())),
            "zero_ready_top10_total": sum(per_day_zero.values()),
            "zero_ready_top10_per_day": dict(sorted(per_day_zero.items())),
        },
        "source_counts": seed.get("source_counts"),
        "rows": norm_rows,
        "seed_path": str(SEED_JSON.relative_to(_ROOT)) if SEED_JSON.is_file() else None,
    }


def get_payload(
    start: Optional[str] = None,
    end: Optional[str] = None,
    *,
    prefer: str = "db",
    refresh: bool = False,
) -> Dict[str, Any]:
    """Return diagnostic payload.

    prefer:
      - "db": compute from DB; fall back to seed on empty/error
      - "seed": serve seeded JSON only (filtered)
    """
    d0, d1 = _default_date_range()
    start = (start or d0).strip()
    end = (end or d1).strip()
    if start > end:
        start, end = end, start

    cache_key = f"{prefer}:{start}:{end}"
    now = time.time()
    if not refresh:
        with _cache_lock:
            if (
                _cache.get("key") == cache_key
                and _cache.get("expires", 0) > now
                and _cache.get("payload") is not None
            ):
                payload = dict(_cache["payload"])
                payload["cached"] = True
                return payload

    payload: Dict[str, Any]
    if prefer == "seed":
        seed = load_seed()
        if not seed:
            return {"ok": False, "error": "seed JSON not found", "date_range": [start, end]}
        payload = _filter_seed(seed, start, end)
    else:
        try:
            payload = compute_rows(start, end)
            if not payload.get("rows"):
                seed = load_seed()
                if seed:
                    payload = _filter_seed(seed, start, end)
                    payload["note"] = "DB returned 0 rows; served seed fallback"
        except Exception as exc:
            logger.exception("top10_vs_ready_now DB compute failed")
            seed = load_seed()
            if seed:
                payload = _filter_seed(seed, start, end)
                payload["note"] = f"DB error; served seed fallback: {exc}"
            else:
                return {
                    "ok": False,
                    "error": str(exc),
                    "date_range": [start, end],
                }

    payload["cached"] = False
    with _cache_lock:
        _cache["key"] = cache_key
        _cache["expires"] = now + CACHE_TTL_SEC
        _cache["payload"] = payload
    return payload
