"""GET /api/big-move — analysis, and paged rows, both read from the study CSV."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from backend.services.big_move.job import find_result, find_rows, find_skips
from backend.services.big_move.store import read_csv

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/big-move", tags=["big-move"])

_CACHE: Dict[str, Any] = {"rows_sig": None, "rows": [], "skips_sig": None, "skips": []}


def _load(path, slot: str) -> List[Dict[str, Any]]:
    if path is None:
        return []
    try:
        sig = (str(path), path.stat().st_mtime, path.stat().st_size)
    except OSError:
        return []
    if _CACHE.get(f"{slot}_sig") == sig:
        return _CACHE[slot]
    rows = read_csv(path)
    _CACHE[f"{slot}_sig"] = sig
    _CACHE[slot] = rows
    return rows


def _page(rows: List[Dict[str, Any]], offset: int, limit: int) -> Dict[str, Any]:
    offset = max(0, offset)
    limit = min(200, max(1, limit))
    return {"total": len(rows), "offset": offset, "rows": rows[offset : offset + limit]}


@router.get("")
def get_big_move() -> Dict[str, Any]:
    path = find_result()
    if path is None:
        return {"ok": False, "detail": "Big Move Study is not on this server yet.", "rankings": {}, "rules": {}, "screens": {}}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("big_move read %s: %s", path, exc)
        raise HTTPException(status_code=500, detail="Could not read the Big Move Study") from exc
    if isinstance(doc, dict):
        doc["ok"] = True
    return doc


@router.get("/rows")
def get_rows(
    offset: int = 0,
    limit: int = 50,
    symbol: str = "",
    date: str = "",
    feature: str = "",
    feature_value: str = "",
    sort: str = "date",
    direction: str = "desc",
) -> Dict[str, Any]:
    rows = _filter(_load(find_rows(), "rows"), symbol, date, feature, feature_value)
    rows = _sort(rows, sort, direction)
    return _page(rows, offset, limit)


@router.get("/events")
def get_events(
    threshold: int = Query(2, ge=2, le=4),
    side: str = Query("up"),
    offset: int = 0,
    limit: int = 50,
    symbol: str = "",
    date: str = "",
    feature: str = "",
    feature_value: str = "",
    sort: str = "date",
    direction: str = "desc",
) -> Dict[str, Any]:
    key = f"big_{side if side in ('up', 'down', 'either') else 'up'}_{threshold}"
    rows = [row for row in _load(find_rows(), "rows") if float(row.get(key) or 0) == 1]
    rows = _filter(rows, symbol, date, feature, feature_value)
    rows = _sort(rows, sort, direction)
    return _page(rows, offset, limit)


@router.get("/skips")
def get_skips(offset: int = 0, limit: int = 100, symbol: str = "") -> Dict[str, Any]:
    rows = _load(find_skips(), "skips")
    wanted = symbol.strip().upper()
    if wanted:
        rows = [row for row in rows if wanted in str(row.get("symbol") or "").upper()]
    return _page(rows, offset, limit)


def _filter(rows: List[Dict[str, Any]], symbol: str, date: str, feature: str, feature_value: str) -> List[Dict[str, Any]]:
    wanted = symbol.strip().upper()
    day = date.strip()
    column = feature.strip()
    raw_value = feature_value.strip()
    number: Optional[float] = None
    if raw_value:
        try:
            number = float(raw_value)
        except ValueError:
            number = None
    out = []
    for row in rows:
        if wanted and wanted not in str(row.get("symbol") or "").upper():
            continue
        if day and not str(row.get("date") or "").startswith(day):
            continue
        if column:
            cell = row.get(column)
            if cell in (None, ""):
                continue
            if raw_value:
                if number is not None:
                    try:
                        if float(cell) < number:
                            continue
                    except (TypeError, ValueError):
                        if raw_value.lower() not in str(cell).lower():
                            continue
                elif raw_value.lower() not in str(cell).lower():
                    continue
        out.append(row)
    return out


def _sort(rows: List[Dict[str, Any]], sort: str, direction: str) -> List[Dict[str, Any]]:
    reverse = str(direction).lower() != "asc"

    def key(row: Dict[str, Any]):
        value = row.get(sort)
        if value in (None, ""):
            return (1, "")
        try:
            return (0, float(value))
        except (TypeError, ValueError):
            return (0, str(value))

    return sorted(rows, key=key, reverse=reverse)
