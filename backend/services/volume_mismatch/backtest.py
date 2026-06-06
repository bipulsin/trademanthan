"""Volume Mismatch Futures historical backtest — May 2026 onward."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

import pytz

from backend.config import settings
from backend.services.upstox_service import UpstoxService
from backend.services.volume_mismatch.backtest_universe import (
    load_volume_mismatch_universe_for_session,
)
from backend.services.volume_mismatch.constants import DEFAULT_GAP_THRESHOLD_PCT
from backend.services.volume_mismatch.scanner import collect_volume_mismatch_signals_for_date

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

BACKTEST_DEFAULT_FROM = date(2026, 5, 1)


def _iter_weekdays(d0: date, d1: date) -> List[date]:
    out: List[date] = []
    d = d0
    while d <= d1:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def run_volume_mismatch_backtest(
    from_date: date,
    to_date: date,
    *,
    gap_threshold: float = DEFAULT_GAP_THRESHOLD_PCT,
    day_pause_sec: float = 1.0,
    max_workers: int = 4,
) -> Dict[str, Any]:
    """Replay first-15m volume mismatch scan for each session in range."""
    if from_date > to_date:
        return {"error": "from_date must be <= to_date", "rows": []}

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if not getattr(upstox, "access_token", None):
        return {"error": "Upstox token unavailable", "rows": []}

    session_days = _iter_weekdays(from_date, to_date)
    all_rows: List[Dict[str, Any]] = []
    by_date: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    for sd in session_days:
        try:
            universe = load_volume_mismatch_universe_for_session(sd)
            if not universe:
                by_date.append(
                    {
                        "trade_date": sd.isoformat(),
                        "signal_count": 0,
                        "long_count": 0,
                        "short_count": 0,
                        "universe_count": 0,
                        "signals": [],
                        "skipped": "empty_universe",
                    }
                )
                continue
            signals = collect_volume_mismatch_signals_for_date(
                upstox,
                universe,
                sd,
                gap_threshold=gap_threshold,
                max_workers=max_workers,
            )
            for s in signals:
                row = dict(s)
                row["trade_date"] = sd.isoformat()
                all_rows.append(row)
            long_n = sum(1 for s in signals if s.get("direction") == "LONG")
            short_n = sum(1 for s in signals if s.get("direction") == "SHORT")
            by_date.append(
                {
                    "trade_date": sd.isoformat(),
                    "signal_count": len(signals),
                    "long_count": long_n,
                    "short_count": short_n,
                    "universe_count": len(universe),
                    "signals": signals,
                }
            )
            logger.info(
                "VM backtest %s: %s signals (L=%s S=%s) universe=%s",
                sd,
                len(signals),
                long_n,
                short_n,
                len(universe),
            )
            if day_pause_sec > 0:
                import time

                time.sleep(day_pause_sec)
        except Exception as e:
            logger.exception("VM backtest day %s failed: %s", sd, e)
            errors.append({"trade_date": sd.isoformat(), "error": str(e)})

    all_rows.sort(
        key=lambda r: (
            str(r.get("trade_date") or ""),
            -(float(r.get("score") or 0)),
            str(r.get("symbol") or ""),
        ),
        reverse=True,
    )
    by_date.sort(key=lambda g: str(g.get("trade_date") or ""), reverse=True)

    symbols = {str(r.get("symbol") or "").upper() for r in all_rows if r.get("symbol")}
    long_total = sum(1 for r in all_rows if r.get("direction") == "LONG")
    short_total = sum(1 for r in all_rows if r.get("direction") == "SHORT")

    return {
        "algo": "volume_mismatch_futures",
        "generated_at": datetime.now(IST).isoformat(),
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "gap_threshold_pct": gap_threshold,
        "scan_time_ist": "09:30:30",
        "summary": {
            "trading_days_scanned": len(by_date),
            "total_signals": len(all_rows),
            "long_count": long_total,
            "short_count": short_total,
            "unique_symbols": len(symbols),
            "errors": len(errors),
        },
        "by_date": by_date,
        "rows": all_rows,
        "errors": errors,
    }


def build_output_document(result: Dict[str, Any]) -> Dict[str, Any]:
    """Stable public JSON shape for the backtest page."""
    return {
        "algo": result.get("algo"),
        "generated_at": result.get("generated_at"),
        "from_date": result.get("from_date"),
        "to_date": result.get("to_date"),
        "gap_threshold_pct": result.get("gap_threshold_pct"),
        "scan_time_ist": result.get("scan_time_ist"),
        "summary": result.get("summary") or {},
        "by_date": result.get("by_date") or [],
        "rows": result.get("rows") or [],
        "errors": result.get("errors") or [],
        "error": result.get("error"),
    }


def default_out_path() -> "Path":
    from pathlib import Path

    name = "volume_mismatch_backtest.json"
    ec2 = Path("/home/ubuntu/trademanthan/data") / name
    if ec2.parent.is_dir():
        return ec2
    root = Path(__file__).resolve().parents[3]
    for rel in ("backend/data", "data"):
        p = root / rel / name
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    return root / "backend" / "data" / name
