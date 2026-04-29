from __future__ import annotations

from datetime import date, datetime, time as dt_time
from typing import Dict, List

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import engine
from backend.services.daily_futures_service import (
    _evaluate_indicator_exit_signal,
    _last_completed_15m_candles_for_indicator,
)
from backend.services.upstox_service import UpstoxService

IST = pytz.timezone("Asia/Kolkata")


def _dt(d: date, hhmm: str) -> datetime:
    h, m = [int(x) for x in hhmm.split(":")]
    return IST.localize(datetime.combine(d, dt_time(h, m)))


def _trade_info(td: date) -> Dict[str, Dict]:
    q = text(
        """
        SELECT t.underlying, t.direction_type, t.instrument_key, t.entry_time, t.exit_time
        FROM daily_futures_user_trade t
        JOIN daily_futures_screening s ON s.id = t.screening_id
        WHERE s.trade_date = :td
        """
    )
    out: Dict[str, Dict] = {}
    with engine.begin() as conn:
        rows = conn.execute(q, {"td": td}).mappings().all()
    for r in rows:
        out[str(r["underlying"]).upper()] = dict(r)
    return out


def _counts_in_window(candles: List[Dict], direction: str, start_dt: datetime, end_dt: datetime) -> List[int]:
    arr: List[int] = []
    for i, c in enumerate(candles):
        ts = c["timestamp"]
        if ts < start_dt or ts > end_dt:
            continue
        ev = _evaluate_indicator_exit_signal("replay", direction, candles[: i + 1])
        arr.append(int(ev.get("count") or 0))
    return arr


def main() -> int:
    td = date(2026, 4, 29)
    winners = {
        "COCHINSHIP": ("09:50", "14:48"),
        "EXIDEIND": ("10:34", "11:45"),
        "RBLBANK": ("11:19", "13:04"),
    }
    losers = {
        "ADANIENT": ("10:54", "12:24"),
        "ADANIPORTS": ("10:52", "12:22"),
        "PNBHOUSING": ("13:35", "15:05"),
    }
    trades = _trade_info(td)
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    now_ist = _dt(td, "15:30")
    failures: List[str] = []

    for sym, (en, ex) in {**winners, **losers}.items():
        row = trades.get(sym)
        if not row or not row.get("instrument_key"):
            failures.append(f"{sym}: missing trade/instrument_key for replay date")
            continue
        cands = _last_completed_15m_candles_for_indicator(
            ux, str(row["instrument_key"]), td, now_ist, limit=120
        )
        counts = _counts_in_window(cands, str(row.get("direction_type") or "LONG"), _dt(td, en), _dt(td, ex))
        if not counts:
            failures.append(f"{sym}: no 15m candles in hold window")
            continue
        if sym in winners and max(counts) > 1:
            failures.append(f"{sym}: expected max<=1, got max={max(counts)}")
        if sym in losers and max(counts) < 2:
            failures.append(f"{sym}: expected >=2 within 90 min, got max={max(counts)}")

    if failures:
        print("VALIDATION_FAILED")
        for f in failures:
            print("-", f)
        return 1
    print("VALIDATION_PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

