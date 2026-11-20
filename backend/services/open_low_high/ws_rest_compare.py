"""Compare WS-built 09:15 5m vs REST for a sample of equities + Nifty.

Scheduled/report hook — callable any time after the 09:15 bar is closed.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

import pytz

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def compare_ws_vs_rest_0915_sample(
    *,
    session: Optional[date] = None,
    n_equities: Optional[int] = None,
    tolerance_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """Compare WS-aggregated 09:15 vs REST for ``n_equities`` + Nifty.

    Returns a report dict; does not raise on individual symbol failures.
    """
    from backend.services.open_high_low.bars import bar_at
    from backend.services.open_high_low.config import NIFTY_INDEX_KEY
    from backend.services.open_high_low.data import load_universe
    from backend.services.open_low_high.candle_builder import compare_ws_vs_rest_0915
    from backend.services.open_low_high.config import (
        OFFICIAL_OPEN_TOLERANCE_PCT,
        WS_REST_0915_COMPARE_N,
    )
    from backend.services.open_low_high.ohl_log import ohl_log
    from backend.services.open_low_high.screener import (
        FIRST_CANDLE,
        _fetch_rest_5m,
        _fetch_ws_5m_bars,
        _resolve_equity_key,
        _upstox,
    )

    trade_date = session or datetime.now(IST).date()
    n = int(n_equities if n_equities is not None else WS_REST_0915_COMPARE_N)
    tol = float(tolerance_pct if tolerance_pct is not None else OFFICIAL_OPEN_TOLERANCE_PCT)
    upstox = _upstox()
    universe = load_universe()[: max(n * 3, n)]  # oversample in case of key miss

    targets: List[Dict[str, str]] = []
    for row in universe:
        if len(targets) >= n:
            break
        sym = str(row.get("symbol") or "").upper()
        if not sym:
            continue
        ik = _resolve_equity_key(upstox, row, sym)
        if ik and ik.upper().startswith("NSE_EQ|"):
            targets.append({"symbol": sym, "instrument_key": ik})
    targets.append({"symbol": "NIFTY", "instrument_key": NIFTY_INDEX_KEY})

    results: List[Dict[str, Any]] = []
    mismatches = 0
    for t in targets:
        sym = t["symbol"]
        ik = t["instrument_key"]
        ws_bars, trust = _fetch_ws_5m_bars(ik, trade_date)
        rest_bars = _fetch_rest_5m(upstox, ik, trade_date)
        ws_b = bar_at(ws_bars, trade_date, FIRST_CANDLE) if ws_bars else None
        rest_b = bar_at(rest_bars, trade_date, FIRST_CANDLE) if rest_bars else None
        row: Dict[str, Any] = {
            "symbol": sym,
            "instrument_key": ik,
            "ws_trust": trust,
            "ws_present": ws_b is not None,
            "rest_present": rest_b is not None,
        }
        if ws_b is None or rest_b is None:
            row["ok"] = False
            row["reason"] = "missing_bar"
            results.append(row)
            continue
        cmp = compare_ws_vs_rest_0915(
            {"open": ws_b.open, "high": ws_b.high, "low": ws_b.low, "close": ws_b.close},
            {"open": rest_b.open, "high": rest_b.high, "low": rest_b.low, "close": rest_b.close},
            symbol=sym,
            tolerance_pct=tol,
        )
        row.update(cmp)
        if not cmp.get("ok"):
            mismatches += 1
        results.append(row)
        ohl_log(
            "success" if cmp.get("ok") else "failed",
            action="ws_rest_0915_compare",
            symbol=sym,
            source="ws+rest",
            detail=f"ok={cmp.get('ok')} mismatches={cmp.get('mismatches')}",
        )

    report = {
        "ok": True,
        "session": trade_date.isoformat(),
        "tolerance_pct": tol,
        "n": len(results),
        "mismatches": mismatches,
        "results": results,
    }
    logger.info(
        "olh ws_rest_0915_compare session=%s n=%s mismatches=%s",
        trade_date.isoformat(),
        len(results),
        mismatches,
    )
    return report
