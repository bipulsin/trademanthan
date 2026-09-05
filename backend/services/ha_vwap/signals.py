"""CSV signal rows → session + 10m bar_start (IST)."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytz

from backend.services.ha_vwap.config import BAR_MINUTES, MARKET_OPEN

IST = pytz.timezone("Asia/Kolkata")
CSV_TIME_FMT = "%d-%m-%Y %I:%M %p"


@dataclass(frozen=True)
class CsvSignal:
    session: datetime  # date part
    when: datetime  # IST naive wall time as datetime
    symbol: str
    bar_start: time
    raw_time: str


def default_signals_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    for p in (
        root / "data" / "ha_vwap" / "signals.csv",
        Path("/home/ubuntu/twcto/data/ha_vwap/signals.csv"),
        Path("/home/ubuntu/trademanthan/data/ha_vwap/signals.csv"),
    ):
        if p.is_file():
            return p
    return root / "data" / "ha_vwap" / "signals.csv"


def parse_csv_datetime(raw: str) -> datetime:
    s = (raw or "").strip().replace("\ufeff", "")
    return datetime.strptime(s, CSV_TIME_FMT)


def bar_start_containing(dt: datetime) -> time:
    """Map wall time to session-aligned 10m bar_start.

    Bars run [09:15, 09:25), [09:25, 09:35), … timestamp = bar start.
    A CSV time on the grid (09:45) uses that bar. Mid-bar (09:48) uses 09:45.
    """
    minutes = dt.hour * 60 + dt.minute
    open_m = MARKET_OPEN.hour * 60 + MARKET_OPEN.minute
    if minutes < open_m:
        return MARKET_OPEN
    delta = minutes - open_m
    floored = open_m + (delta // BAR_MINUTES) * BAR_MINUTES
    return time(floored // 60, floored % 60)


def load_csv_signals(path: Optional[Path] = None) -> List[CsvSignal]:
    path = path or default_signals_path()
    text = path.read_bytes()
    if text.startswith(b"\xef\xbb\xbf"):
        text = text[3:]
    rows: List[CsvSignal] = []
    reader = csv.DictReader(text.decode("utf-8").splitlines())
    for row in reader:
        raw = (row.get("Date") or row.get("date") or "").strip()
        sym = (row.get("Symbol") or row.get("symbol") or "").strip().upper()
        if not raw or not sym:
            continue
        dt = parse_csv_datetime(raw)
        rows.append(
            CsvSignal(
                session=datetime(dt.year, dt.month, dt.day),
                when=dt,
                symbol=sym,
                bar_start=bar_start_containing(dt),
                raw_time=raw,
            )
        )
    return rows


def signals_by_session(rows: List[CsvSignal]) -> Dict[str, List[CsvSignal]]:
    out: Dict[str, List[CsvSignal]] = {}
    for r in rows:
        out.setdefault(r.session.date().isoformat(), []).append(r)
    return out


def date_span(rows: List[CsvSignal]) -> Tuple[Optional[datetime], Optional[datetime]]:
    if not rows:
        return None, None
    ds = [r.session for r in rows]
    return min(ds), max(ds)
