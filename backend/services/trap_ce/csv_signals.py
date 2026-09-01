"""Parse Trap-CE Chartink-style CSV (Date + Symbol; optional direction)."""
from __future__ import annotations

import csv
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.services.trap_ce.config import SKIP_SHORT


def parse_trigger_datetime(raw: str) -> datetime:
    s = (raw or "").strip().strip('"')
    for fmt in (
        "%d-%m-%Y %I:%M %p",
        "%d-%m-%Y %I:%M%p",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"unrecognized trigger datetime: {raw!r}")


def trigger_bar_start(dt: datetime) -> time:
    """10m bar start: '9:15 am' → 09:15, '11:15 am' → 11:15."""
    return time(dt.hour, dt.minute)


def _norm_header(h: str) -> str:
    return (h or "").strip().lower().replace(" ", "_")


def _direction_from_row(row: Dict[str, str]) -> str:
    for key in ("direction", "side", "option_type", "ce_pe", "type"):
        v = (row.get(key) or "").strip().upper()
        if not v:
            continue
        if v in ("PE", "PUT", "SHORT", "SELL"):
            return "SHORT"
        if v in ("CE", "CALL", "LONG", "BUY"):
            return "LONG"
    return "LONG"


def load_trap_ce_csv(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    out: List[Dict[str, Any]] = []
    for i, raw in enumerate(reader, start=2):
        row = {_norm_header(k): (v or "").strip() for k, v in raw.items() if k}
        date_s = row.get("date") or row.get("trigger_date") or ""
        symbol = (row.get("symbol") or row.get("stock") or "").strip().upper()
        if not date_s or not symbol:
            continue
        dt = parse_trigger_datetime(date_s)
        direction = _direction_from_row(row)
        skip: Optional[str] = SKIP_SHORT if direction == "SHORT" else None
        out.append(
            {
                "csv_line": i,
                "symbol": symbol,
                "session_date": dt.date(),
                "trigger_time": trigger_bar_start(dt),
                "trigger_dt": dt,
                "direction": direction,
                "marketcapname": row.get("marketcapname") or "",
                "sector": row.get("sector") or "",
                "skip_reason": skip,
            }
        )
    return out


def trading_dates(signals: List[Dict[str, Any]]) -> List[date]:
    return sorted({s["session_date"] for s in signals})
