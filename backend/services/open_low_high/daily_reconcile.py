"""15:45 IST daily WS vs REST reconciliation for 145+Nifty (09:15/09:20/09:25).

Trading days only. Reports mismatch count + worst offenders.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.open_low_high.clock import olh_now
from backend.services.open_low_high.config import OFFICIAL_OPEN_TOLERANCE_PCT
from backend.services.open_low_high.ohl_log import ohl_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

RECONCILE_CLOCK = time(15, 45, 0)
BUCKETS = (time(9, 15), time(9, 20), time(9, 25))


def _ohlc_tuple(bar: Any) -> Optional[Tuple[float, float, float, float]]:
    if bar is None:
        return None
    try:
        return (
            float(bar.open),
            float(bar.high),
            float(bar.low),
            float(bar.close),
        )
    except Exception:
        if isinstance(bar, dict):
            return (
                float(bar["open"]),
                float(bar["high"]),
                float(bar["low"]),
                float(bar["close"]),
            )
        return None


def _mismatch_score(
    ws: Tuple[float, float, float, float],
    rest: Tuple[float, float, float, float],
    *,
    tol_pct: float,
) -> float:
    """Max relative abs diff across OHLC (percent of REST price)."""
    worst = 0.0
    for a, b in zip(ws, rest):
        if b == 0:
            continue
        worst = max(worst, abs(a - b) / abs(b) * 100.0)
    return worst if worst > tol_pct else 0.0


def run_daily_reconcile(
    *,
    session: Optional[date] = None,
    now: Optional[datetime] = None,
    tolerance_pct: Optional[float] = None,
    send_alert: bool = True,
) -> Dict[str, Any]:
    """Compare WS-built 5m vs REST for full universe + Nifty at 09:15/20/25."""
    from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist
    from backend.services.open_high_low.bars import bar_at
    from backend.services.open_high_low.config import NIFTY_INDEX_KEY
    from backend.services.open_high_low.data import load_universe
    from backend.services.open_low_high.screener import (
        _fetch_rest_5m,
        _fetch_ws_5m_bars,
        _resolve_equity_key,
        _upstox,
    )

    t = now or olh_now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    trade_date = session or t.date()
    tol = float(tolerance_pct if tolerance_pct is not None else OFFICIAL_OPEN_TOLERANCE_PCT)

    report: Dict[str, Any] = {
        "ok": True,
        "session": trade_date.isoformat(),
        "at": t.isoformat(),
        "tolerance_pct": tol,
        "buckets": [b.strftime("%H:%M") for b in BUCKETS],
        "mismatch_count": 0,
        "checked": 0,
        "worst_offenders": [],
        "by_bucket": {},
    }

    if should_skip_scheduled_market_jobs_ist(
        IST.localize(datetime.combine(trade_date, time(12, 0)))
    ):
        report["skipped"] = "holiday_or_weekend"
        ohl_log("skipped", action="daily_reconcile", detail="holiday_or_weekend")
        return report

    upstox = _upstox()
    universe = load_universe()
    targets: List[Dict[str, str]] = []
    for row in universe:
        sym = str(row.get("symbol") or "").upper()
        if not sym or sym in ("NIFTY", "BANKNIFTY"):
            continue
        ik = _resolve_equity_key(upstox, row, sym)
        if ik and str(ik).upper().startswith("NSE_EQ|"):
            targets.append({"symbol": sym, "instrument_key": ik})
    targets.append({"symbol": "NIFTY", "instrument_key": NIFTY_INDEX_KEY})

    offenders: List[Dict[str, Any]] = []
    by_bucket: Dict[str, Dict[str, int]] = {
        b.strftime("%H:%M"): {"checked": 0, "mismatches": 0, "missing": 0} for b in BUCKETS
    }

    for trow in targets:
        sym = trow["symbol"]
        ik = trow["instrument_key"]
        try:
            ws_bars, _trust = _fetch_ws_5m_bars(ik, trade_date)
        except Exception:
            ws_bars = []
        try:
            rest_bars = _fetch_rest_5m(upstox, ik, trade_date)
        except Exception:
            rest_bars = []

        for bucket in BUCKETS:
            bkey = bucket.strftime("%H:%M")
            ws_b = bar_at(ws_bars, trade_date, bucket) if ws_bars else None
            rest_b = bar_at(rest_bars, trade_date, bucket) if rest_bars else None
            ws_t = _ohlc_tuple(ws_b)
            rest_t = _ohlc_tuple(rest_b)
            report["checked"] += 1
            by_bucket[bkey]["checked"] += 1
            if ws_t is None or rest_t is None:
                by_bucket[bkey]["missing"] += 1
                report["mismatch_count"] += 1
                offenders.append(
                    {
                        "symbol": sym,
                        "bucket": bkey,
                        "score": 999.0,
                        "reason": "missing_bar",
                        "ws": ws_t,
                        "rest": rest_t,
                    }
                )
                continue
            score = _mismatch_score(ws_t, rest_t, tol_pct=tol)
            if score > 0:
                by_bucket[bkey]["mismatches"] += 1
                report["mismatch_count"] += 1
                offenders.append(
                    {
                        "symbol": sym,
                        "bucket": bkey,
                        "score": round(score, 4),
                        "reason": "ohlc_diff",
                        "ws": ws_t,
                        "rest": rest_t,
                    }
                )

    offenders.sort(key=lambda r: float(r.get("score") or 0), reverse=True)
    report["worst_offenders"] = offenders[:25]
    report["by_bucket"] = by_bucket
    report["ok"] = report["mismatch_count"] == 0

    ohl_log(
        "success" if report["ok"] else "failed",
        action="daily_reconcile",
        detail=(
            f"checked={report['checked']} mismatches={report['mismatch_count']} "
            f"worst={report['worst_offenders'][0]['symbol'] if offenders else '-'}"
        ),
    )
    if send_alert and report["mismatch_count"] > 0:
        try:
            from backend.services.open_low_high.alerts import send_olh_alert

            top = ", ".join(
                f"{o['symbol']}@{o['bucket']}({o['score']})" for o in offenders[:5]
            )
            send_olh_alert(
                f"Daily reconcile mismatches {trade_date.isoformat()}",
                f"count={report['mismatch_count']} top={top}",
            )
        except Exception:
            logger.debug("reconcile alert failed", exc_info=True)

    logger.info(
        "olh daily_reconcile session=%s checked=%s mismatches=%s",
        trade_date.isoformat(),
        report["checked"],
        report["mismatch_count"],
    )
    return report
