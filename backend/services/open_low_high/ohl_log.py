"""Dated OHL call logs (``OHL_Log_19Nov2026``) + 3-day cleanup."""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pytz

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_LOG_DIR_CANDIDATES = (
    Path("/app/logs"),
    Path(__file__).resolve().parents[3] / "logs",
)
_FILENAME_RE = re.compile(r"^OHL_Log_(\d{1,2}[A-Za-z]{3}\d{4})(?:\.txt)?$")
_MONTHS = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)


def ohl_logs_dir() -> Path:
    for p in _LOG_DIR_CANDIDATES:
        try:
            if p.exists() or str(p).startswith("/app/"):
                p.mkdir(parents=True, exist_ok=True)
                return p
        except OSError:
            continue
    fallback = Path(__file__).resolve().parents[3] / "logs"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def ohl_log_filename(d: Optional[date] = None) -> str:
    day = d or datetime.now(IST).date()
    return f"OHL_Log_{day.day:02d}{_MONTHS[day.month - 1]}{day.year}"


def ohl_log_path(d: Optional[date] = None) -> Path:
    return ohl_logs_dir() / ohl_log_filename(d)


def _parse_log_date(name: str) -> Optional[date]:
    m = _FILENAME_RE.match(name)
    if not m:
        return None
    token = m.group(1)
    try:
        return datetime.strptime(token, "%d%b%Y").date()
    except ValueError:
        return None


def ohl_log(
    outcome: str,
    *,
    action: str,
    symbol: str = "",
    source: str = "",
    detail: str = "",
    when: Optional[datetime] = None,
) -> None:
    """Append one OHL-tagged line: success / failed / skipped.

    Format example:
    ``2026-11-19 09:20:15 IST | OHL | success | phase_a_ws | RELIANCE | ws | got 09:15``
    """
    t = when or datetime.now(IST)
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    outcome_n = str(outcome or "unknown").strip().lower() or "unknown"
    parts = [
        t.strftime("%Y-%m-%d %H:%M:%S IST"),
        "OHL",
        outcome_n,
        str(action or "").strip() or "-",
        str(symbol or "").strip().upper() or "-",
        str(source or "").strip() or "-",
        str(detail or "").strip() or "-",
    ]
    line = " | ".join(parts)
    path = ohl_log_path(t.date())
    try:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        logger.warning("OHL log write failed path=%s", path, exc_info=True)
    # Mirror to app logger with consistent tag.
    logger.info("OHL %s action=%s symbol=%s source=%s detail=%s", outcome_n, action, symbol, source, detail)


def cleanup_ohl_logs(*, keep_days: int = 3, now: Optional[datetime] = None) -> dict[str, Any]:
    """Delete ``OHL_Log_*`` files older than ``keep_days`` (IST calendar days)."""
    base = ohl_logs_dir()
    t = now or datetime.now(IST)
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    cutoff = t.date() - timedelta(days=max(1, int(keep_days)))
    deleted: list[str] = []
    kept = 0
    for path in sorted(base.glob("OHL_Log_*")):
        parsed = _parse_log_date(path.name)
        if parsed is None:
            continue
        if parsed < cutoff:
            try:
                path.unlink()
                deleted.append(path.name)
            except OSError:
                logger.warning("OHL log delete failed %s", path, exc_info=True)
        else:
            kept += 1
    payload = {
        "ok": True,
        "dir": str(base),
        "cutoff": cutoff.isoformat(),
        "deleted": deleted,
        "kept": kept,
    }
    logger.info("OHL log cleanup deleted=%s kept=%s cutoff=%s", len(deleted), kept, cutoff)
    return payload
