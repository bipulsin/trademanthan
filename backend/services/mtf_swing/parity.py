"""v1 ↔ v1_equivalent parity helpers.

v1's historical CSVs were produced under partial Upstox fetches (many
``no candles`` months). A later continuous ``v1_equivalent`` run with full
history therefore takes screens that v1 never saw — e.g. ADANIENSOL SHORT
screened on 2026-04-30 (Thu, because 2026-05-01 Labour Day is an NSE holiday)
and filled 2026-05-04. v1 had no April candles for that symbol, so the screen
never ran; May refill could not recover it because bars before ``active_from``
are warm-up only (no new screens).

``v1_fetch_holes.json`` records the final per-(timeframe, symbol, YYYY-MM)
fetch outcome from v1 run logs. ``v1_equivalent`` strips those hole months from
every bar window (including warm-up) so continuous carry replays the same
candle gaps v1 effectively had.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from backend.services.mtf_swing.data import data_dir

logger = logging.getLogger(__name__)

HoleKey = Tuple[str, str, str]  # timeframe, symbol, YYYY-MM

_SUCCESS_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
    r"mtf_swing (?P<tf>\d+m) (?P<month>\d{4}-\d{2}) (?P<sym>\S+) bars=(?P<bars>\d+)"
)
_FAIL_TYPED_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
    r"mtf_swing (?P<tf>\d+m) (?P<sym>\S+) (?P<month>\d{4}-\d{2}) no candles"
)
_FAIL_UNTYPED_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
    r"mtf_swing (?P<sym>\S+) (?P<month>\d{4}-\d{2}) no candles"
)


def holes_path() -> Path:
    return data_dir() / "v1_fetch_holes.json"


def month_label(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def load_holes(path: Optional[Path] = None) -> Set[HoleKey]:
    p = path or holes_path()
    if not p.is_file():
        return set()
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("mtf_swing parity could not read %s: %s", p, exc)
        return set()
    out: Set[HoleKey] = set()
    for item in payload.get("holes") or []:
        if isinstance(item, (list, tuple)) and len(item) >= 3:
            out.add((str(item[0]), str(item[1]).upper(), str(item[2])))
        elif isinstance(item, str):
            parts = item.split("|")
            if len(parts) >= 3:
                out.add((parts[0], parts[1].upper(), parts[2]))
    return out


def save_holes(holes: Iterable[HoleKey], path: Optional[Path] = None, *, meta: Optional[Dict] = None) -> Path:
    p = path or holes_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted({(tf, sym.upper(), month) for tf, sym, month in holes})
    payload = {
        "holes": [[tf, sym, month] for tf, sym, month in ordered],
        "count": len(ordered),
        **(meta or {}),
    }
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp.replace(p)
    return p


def is_hole(holes: Set[HoleKey], timeframe: str, symbol: str, month: str) -> bool:
    return (timeframe, symbol.upper(), month) in holes


def filter_bars_for_holes(bars: Sequence, holes: Set[HoleKey], *, timeframe: str, symbol: str) -> List:
    """Drop bars whose session month is a recorded v1 fetch hole for this symbol/tf."""
    if not holes or not bars:
        return list(bars)
    sym = symbol.upper()
    out = []
    for bar in bars:
        session = getattr(bar, "session_date", None)
        if session is None:
            out.append(bar)
            continue
        if is_hole(holes, timeframe, sym, month_label(session)):
            continue
        out.append(bar)
    return out


def parse_log_events(text: str, *, default_tf: Optional[str] = None) -> List[Tuple[str, str, str, str, str]]:
    """Return (ts, timeframe, symbol, month, status) events in file order."""
    events: List[Tuple[str, str, str, str, str]] = []
    for ln in text.splitlines():
        m = _SUCCESS_RE.search(ln)
        if m:
            events.append(
                (m.group("ts"), m.group("tf"), m.group("sym").upper(), m.group("month"), "ok")
            )
            continue
        m = _FAIL_TYPED_RE.search(ln)
        if m:
            events.append(
                (m.group("ts"), m.group("tf"), m.group("sym").upper(), m.group("month"), "no_candles")
            )
            continue
        m = _FAIL_UNTYPED_RE.search(ln)
        if m and default_tf:
            events.append(
                (m.group("ts"), default_tf, m.group("sym").upper(), m.group("month"), "no_candles")
            )
    return events


def holes_from_events(events: Sequence[Tuple[str, str, str, str, str]]) -> Set[HoleKey]:
    last: Dict[HoleKey, str] = {}
    for _ts, tf, sym, month, status in events:
        last[(tf, sym, month)] = status
    return {k for k, status in last.items() if status == "no_candles"}


def build_holes_from_v1_logs(
    log_specs: Sequence[Tuple[Path, Optional[str]]],
) -> Set[HoleKey]:
    """Merge v1 run logs in order; last status per (tf, symbol, month) wins."""
    events: List[Tuple[str, str, str, str, str]] = []
    for path, default_tf in log_specs:
        if not path.is_file():
            logger.warning("mtf_swing parity missing log %s", path)
            continue
        chunk = parse_log_events(path.read_text(encoding="utf-8", errors="ignore"), default_tf=default_tf)
        logger.info("mtf_swing parity parsed %s events=%s default_tf=%s", path.name, len(chunk), default_tf)
        events.extend(chunk)
    return holes_from_events(events)


def default_v1_log_specs() -> List[Tuple[Path, Optional[str]]]:
    root = data_dir()
    return [
        (root / "full_run.log", "60m"),
        (root / "full_run_60m_refill.log", "60m"),
        (root / "full_run_15m.log", "15m"),
    ]
