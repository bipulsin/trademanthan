"""Gap + Bollinger Band Futures backtest — May 2026 onward (backtest path only)."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz

from backend.config import settings
from backend.services.market_holiday import refresh_holiday_dates_from_db
from backend.services.upstox_service import UpstoxService
from backend.services.volume_mismatch.backtest_signals import collect_gap_bb_signals_for_date
from backend.services.volume_mismatch.backtest_universe import (
    load_volume_mismatch_universe_for_session,
)
from backend.services.volume_mismatch.candles import BacktestDailyCache

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

BACKTEST_DEFAULT_FROM = date(2026, 5, 1)
SIGNAL_CRITERIA = (
    "LONG: gap down (open < prev close) + first 15m close below lower BB (20,2 daily). "
    "SHORT: gap up (open > prev close) + first 15m close above upper BB."
)


def _load_holiday_dates(
    upstox: UpstoxService,
    from_date: date,
    to_date: date,
) -> set[date]:
    """NSE holidays for the backtest range — DB first, Upstox API fallback."""
    holidays = refresh_holiday_dates_from_db()
    if holidays:
        return holidays
    out: set[date] = set()
    for year in range(from_date.year, to_date.year + 1):
        for dstr in upstox.get_market_holidays(year) or []:
            try:
                out.add(date.fromisoformat(str(dstr)[:10]))
            except ValueError:
                continue
    return out


def _iter_trading_days(d0: date, d1: date, holiday_dates: set[date]) -> List[date]:
    out: List[date] = []
    d = d0
    while d <= d1:
        if d.weekday() < 5 and d not in holiday_dates:
            out.append(d)
        d += timedelta(days=1)
    return out


def _write_incremental_artifact(
    path: Optional[Path],
    *,
    from_date: date,
    to_date: date,
    all_rows: List[Dict[str, Any]],
    by_date: List[Dict[str, Any]],
    errors: List[Dict[str, str]],
) -> None:
    if path is None:
        return
    symbols = {str(r.get("symbol") or "").upper() for r in all_rows if r.get("symbol")}
    long_total = sum(1 for r in all_rows if r.get("direction") == "LONG")
    short_total = sum(1 for r in all_rows if r.get("direction") == "SHORT")
    partial = {
        "algo": "gap_bb_futures_backtest",
        "signal_criteria": SIGNAL_CRITERIA,
        "generated_at": datetime.now(IST).isoformat(),
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "scan_time_ist": "09:30:30",
        "partial": True,
        "summary": {
            "trading_days_scanned": len(by_date),
            "total_signals": len(all_rows),
            "long_count": long_total,
            "short_count": short_total,
            "unique_symbols": len(symbols),
            "errors": len(errors),
        },
        "by_date": list(by_date),
        "rows": list(all_rows),
        "errors": list(errors),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(partial, f, indent=2, default=str)
    tmp.replace(path)


def run_volume_mismatch_backtest(
    from_date: date,
    to_date: date,
    *,
    day_pause_sec: float = 0.5,
    max_workers: int = 4,
    out_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Replay gap + BB first-15m scan for each session in range."""
    if from_date > to_date:
        return {"error": "from_date must be <= to_date", "rows": []}

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if not getattr(upstox, "access_token", None):
        return {"error": "Upstox token unavailable", "rows": []}

    holiday_dates = _load_holiday_dates(upstox, from_date, to_date)
    session_days = _iter_trading_days(from_date, to_date, holiday_dates)
    holidays_in_range = sum(
        1
        for d in (from_date + timedelta(days=i) for i in range((to_date - from_date).days + 1))
        if d.weekday() < 5 and d in holiday_dates
    )
    logger.info(
        "Gap+BB backtest range %s..%s: %s trading days (%s NSE holidays skipped)",
        from_date,
        to_date,
        len(session_days),
        holidays_in_range,
    )

    daily_cache = BacktestDailyCache()
    all_rows: List[Dict[str, Any]] = []
    by_date: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    for sd in session_days:
        try:
            import time as _time

            day_t0 = _time.monotonic()
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
            signals, day_stats = collect_gap_bb_signals_for_date(
                upstox,
                universe,
                sd,
                max_workers=max_workers,
                daily_cache=daily_cache,
            )
            for s in signals:
                row = dict(s)
                row["trade_date"] = sd.isoformat()
                all_rows.append(row)
            long_n = sum(1 for s in signals if s.get("direction") == "LONG")
            short_n = sum(1 for s in signals if s.get("direction") == "SHORT")
            day_elapsed = round(_time.monotonic() - day_t0, 2)
            by_date.append(
                {
                    "trade_date": sd.isoformat(),
                    "signal_count": len(signals),
                    "long_count": long_n,
                    "short_count": short_n,
                    "universe_count": len(universe),
                    "signals": signals,
                    "timing": {**day_stats, "total_sec": day_elapsed},
                }
            )
            logger.info(
                "Gap+BB backtest %s: %s signals (L=%s S=%s) universe=%s "
                "in %.1fs (m15=%s daily=%s gaps=%s bb=%s)",
                sd,
                len(signals),
                long_n,
                short_n,
                len(universe),
                day_elapsed,
                day_stats.get("m15_api"),
                day_stats.get("daily_api"),
                day_stats.get("gaps"),
                day_stats.get("bb_evals"),
            )
            _write_incremental_artifact(
                out_path,
                from_date=from_date,
                to_date=to_date,
                all_rows=all_rows,
                by_date=by_date,
                errors=errors,
            )
            if day_pause_sec > 0:
                import time

                time.sleep(day_pause_sec)
        except Exception as e:
            logger.exception("Gap+BB backtest day %s failed: %s", sd, e)
            errors.append({"trade_date": sd.isoformat(), "error": str(e)})
            _write_incremental_artifact(
                out_path,
                from_date=from_date,
                to_date=to_date,
                all_rows=all_rows,
                by_date=by_date,
                errors=errors,
            )

    all_rows.sort(
        key=lambda r: (
            str(r.get("trade_date") or ""),
            str(r.get("symbol") or ""),
        ),
        reverse=True,
    )
    by_date.sort(key=lambda g: str(g.get("trade_date") or ""), reverse=True)

    symbols = {str(r.get("symbol") or "").upper() for r in all_rows if r.get("symbol")}
    long_total = sum(1 for r in all_rows if r.get("direction") == "LONG")
    short_total = sum(1 for r in all_rows if r.get("direction") == "SHORT")

    result = {
        "algo": "gap_bb_futures_backtest",
        "signal_criteria": SIGNAL_CRITERIA,
        "generated_at": datetime.now(IST).isoformat(),
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "scan_time_ist": "09:30:30",
        "partial": False,
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
    if out_path is not None:
        doc = build_output_document(result)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2, default=str)
    return result


def build_output_document(result: Dict[str, Any]) -> Dict[str, Any]:
    """Stable public JSON shape for the backtest page."""
    return {
        "algo": result.get("algo"),
        "signal_criteria": result.get("signal_criteria"),
        "generated_at": result.get("generated_at"),
        "from_date": result.get("from_date"),
        "to_date": result.get("to_date"),
        "scan_time_ist": result.get("scan_time_ist"),
        "partial": result.get("partial"),
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
