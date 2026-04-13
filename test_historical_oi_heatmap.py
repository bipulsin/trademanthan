#!/usr/bin/env python3
"""
Historical OI heatmap tester (Smart Futures / dashboard-style OI regime).

Replays the OI buildup interpretation at a past IST session time using Upstox
``historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}`` data.

Usage:
  PYTHONPATH=. python test_historical_oi_heatmap.py \\
    --session-date 2026-04-13 --target-time 09:45 --open-time 09:15 --top-n 50

Requires ``UPSTOX_API_KEY`` / ``UPSTOX_API_SECRET`` (or backend ``.env`` via ``backend.config``).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytz

from backend.config import settings
from backend.services.oi_heatmap import (
    filter_stock_futures_rows,
    load_nse_instruments_json,
    pick_nearest_expiry_future_per_underlying,
)
from backend.services.oi_integration import interpret_oi_signal
from backend.services.premarket_scoring import parse_candle_date_ist
from backend.services.upstox_service import UpstoxService

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("hist_oi_heatmap")

IST = pytz.timezone("Asia/Kolkata")


def _parse_dt_ist(ts: Any) -> Optional[datetime]:
    """Parse Upstox candle timestamp to timezone-aware IST datetime."""
    if ts is None:
        return None
    s = str(ts).strip()
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = IST.localize(dt)
            else:
                dt = dt.astimezone(IST)
            return dt
        return None
    except ValueError:
        return None


def _sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    if not candles:
        return []
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def _find_candle_at_minute(
    candles: List[dict], session_d: date, hour: int, minute: int
) -> Optional[dict]:
    """1m (or aligned) bar whose candle *start* in IST matches session_d and HH:MM."""
    for c in candles:
        dt = _parse_dt_ist(c.get("timestamp"))
        if not dt:
            continue
        if dt.date() != session_d:
            continue
        if dt.hour == hour and dt.minute == minute:
            return c
    return None


def _fetch_candles_for_day(
    ux: UpstoxService,
    instrument_key: str,
    session_d: date,
    interval: str,
) -> Optional[List[Dict[str, Any]]]:
    """
    Single-day window anchored on ``session_d`` (``range_end_date`` = that day).
    Uses ``days_back=0`` so ``from_date == to_date`` when supported.
    """
    raw = ux.get_historical_candles_by_instrument_key(
        instrument_key,
        interval=interval,
        days_back=0,
        range_end_date=session_d,
    )
    if raw:
        return raw
    raw = ux.get_historical_candles_by_instrument_key(
        instrument_key,
        interval=interval,
        days_back=1,
        range_end_date=session_d,
    )
    return raw


def _candles_for_session_date(candles: List[dict], session_d: date) -> List[dict]:
    out = []
    for c in candles:
        if parse_candle_date_ist(c.get("timestamp")) == session_d:
            out.append(c)
    return _sort_candles(out)


def _signal_display(code: str) -> str:
    m = {
        "LONG_BUILDUP": "LONG BUILDUP",
        "SHORT_BUILDUP": "SHORT BUILDUP",
        "LONG_UNWINDING": "LONG UNWINDING",
        "SHORT_COVERING": "SHORT COVERING",
        "NEUTRAL": "NEUTRAL",
    }
    return m.get((code or "").upper(), code or "NEUTRAL")


def _status_emoji(code: str) -> str:
    return {
        "LONG_BUILDUP": "🟢",
        "SHORT_BUILDUP": "🔴",
        "LONG_UNWINDING": "🟠",
        "SHORT_COVERING": "🔵",
        "NEUTRAL": "⚪",
    }.get((code or "").upper(), "⚪")


@dataclass
class RowOut:
    symbol: str = ""
    price_chg_pct: float = 0.0
    oi_chg_pct: float = 0.0
    signal: str = "NEUTRAL"
    score: float = 0.0


def _compute_row(
    open_c: dict,
    target_c: dict,
) -> Optional[RowOut]:
    o_open = float(open_c.get("open") or 0)
    c_close = float(target_c.get("close") or 0)
    oi_s = open_c.get("oi")
    oi_e = target_c.get("oi")
    if oi_s is None or oi_e is None:
        return None
    start_oi = float(oi_s)
    end_oi = float(oi_e)
    if o_open <= 0 or start_oi <= 0:
        return None

    price_dp = c_close - o_open
    oi_dp = end_oi - start_oi
    sig = interpret_oi_signal(price_dp, oi_dp)
    price_pct = (c_close - o_open) / o_open * 100.0
    oi_pct = (end_oi - start_oi) / start_oi * 100.0
    score = abs(oi_dp) + abs(price_pct) * 0.01
    return RowOut(
        price_chg_pct=round(price_pct, 2),
        oi_chg_pct=round(oi_pct, 2),
        signal=sig,
        score=round(score, 4),
    )


def build_universe_near_month(top_n: int, full_200: bool) -> List[Tuple[str, str]]:
    """(instrument_key, underlying_symbol) — deterministic alphabetical cap."""
    n = 200 if full_200 else top_n
    raw = load_nse_instruments_json()
    fut = filter_stock_futures_rows(raw)
    per_u = pick_nearest_expiry_future_per_underlying(fut)
    per_u.sort(key=lambda r: (r.get("underlying_symbol") or "").upper())
    out: List[Tuple[str, str]] = []
    for r in per_u:
        ik = (r.get("instrument_key") or "").strip()
        u = (r.get("underlying_symbol") or "").strip().upper()
        if ik and u:
            out.append((ik, u))
        if len(out) >= n:
            break
    return out


def build_universe_by_liquidity(top_n: int) -> List[Tuple[str, str]]:
    """Same ranking as live OI heatmap (Upstox batch volume); requires live batch quote API."""
    from backend.services.oi_heatmap import build_liquidity_universe_instrument_keys

    keys = build_liquidity_universe_instrument_keys(top_n)
    raw = load_nse_instruments_json()
    ik_to_u: Dict[str, str] = {}
    for r in raw:
        if not isinstance(r, dict):
            continue
        ik = (r.get("instrument_key") or "").strip()
        u = (r.get("underlying_symbol") or "").strip().upper()
        if ik:
            ik_to_u[ik] = u
    out: List[Tuple[str, str]] = []
    for ik in keys:
        out.append((ik, ik_to_u.get(ik, ik.split("|")[-1] if "|" in ik else ik)))
    return out


def run_snapshot(
    session_d: date,
    open_h: int,
    open_m: int,
    target_h: int,
    target_m: int,
    top_n: int,
    full_200: bool,
    liquidity_rank: bool,
    sleep_s: float,
) -> List[RowOut]:
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if liquidity_rank:
        universe = build_universe_by_liquidity(200 if full_200 else top_n)
    else:
        universe = build_universe_near_month(top_n, full_200)
    rows: List[RowOut] = []

    for ik, sym in universe:
        time.sleep(sleep_s)
        raw_1m = _fetch_candles_for_day(ux, ik, session_d, "minutes/1")
        day_1m = _candles_for_session_date(_sort_candles(raw_1m or []), session_d)
        interval_used = "1m"
        open_c = _find_candle_at_minute(day_1m, session_d, open_h, open_m)
        target_c = _find_candle_at_minute(day_1m, session_d, target_h, target_m)

        if not open_c or not target_c:
            raw_5m = _fetch_candles_for_day(ux, ik, session_d, "minutes/5")
            day_5m = _candles_for_session_date(_sort_candles(raw_5m or []), session_d)
            interval_used = "5m"
            open_c = _find_candle_at_minute(day_5m, session_d, open_h, open_m)
            target_c = _find_candle_at_minute(day_5m, session_d, target_h, target_m)

        if not open_c or not target_c:
            logger.warning("%s: missing %02d:%02d or %02d:%02d bar (%s)", sym, open_h, open_m, target_h, target_m, interval_used)
            continue

        ro = _compute_row(open_c, target_c)
        if ro is None:
            logger.warning("%s: missing OI on candles (need oi field in historical response)", sym)
            continue
        ro.symbol = sym
        rows.append(ro)
        logger.debug("%s OK (%s)", sym, interval_used)

    rows.sort(key=lambda r: r.score, reverse=True)
    return rows


def _fmt_clock_ampm(h: int, m: int) -> str:
    suf = "AM" if h < 12 else "PM"
    h12 = h % 12
    if h12 == 0:
        h12 = 12
    return f"{h12}:{m:02d} {suf}"


def print_table(
    session_d: date,
    target_h: int,
    target_m: int,
    rows: List[RowOut],
) -> None:
    title = session_d.strftime("%Y-%m-%d") + f" @ {_fmt_clock_ampm(target_h, target_m)}"
    print()
    print("OI HEATMAP SNAPSHOT:", title)
    print("=" * 59)
    print(f"{'Symbol':<12} | {'Price Chg%':>10} | {'OI Chg%':>8} | {'Signal':<16} | Status")
    print("-" * 59)
    for r in rows:
        pfx = "+" if r.price_chg_pct > 0 else ""
        oi_pfx = "+" if r.oi_chg_pct > 0 else ""
        print(
            f"{r.symbol:<12} | {pfx}{r.price_chg_pct:>8.2f}% | {oi_pfx}{r.oi_chg_pct:>6.2f}% | "
            f"{_signal_display(r.signal):<16} | {_status_emoji(r.signal)}"
        )
    if not rows:
        print("(no rows — check session date is a trading day, times, and Upstox OI in candles)")
    print()


def main() -> None:
    p = argparse.ArgumentParser(description="Historical OI heatmap (Upstox historical candles)")
    p.add_argument("--session-date", type=str, default="2026-04-13", help="YYYY-MM-DD (IST session)")
    p.add_argument("--open-time", type=str, default="09:15", help="HH:MM — open candle for start price/OI")
    p.add_argument("--target-time", type=str, default="09:45", help="HH:MM — snapshot candle (close/OI)")
    p.add_argument("--top-n", type=int, default=50, help="Number of near-month stock FUT underlyings (alphabetical)")
    p.add_argument("--full-200", action="store_true", help="Use ~200 names (same cap as live heatmap universe shape)")
    p.add_argument(
        "--liquidity-rank",
        action="store_true",
        help="Rank universe by live Upstox volume (like dashboard); slower, extra API batch calls up front",
    )
    p.add_argument("--sleep", type=float, default=0.15, help="Delay between API calls (rate limit)")
    args = p.parse_args()

    session_d = date.fromisoformat(args.session_date)
    oh, om = [int(x) for x in args.open_time.split(":")]
    th, tm = [int(x) for x in args.target_time.split(":")]

    rows = run_snapshot(
        session_d, oh, om, th, tm, args.top_n, args.full_200, args.liquidity_rank, args.sleep
    )
    print_table(session_d, th, tm, rows)


if __name__ == "__main__":
    main()
