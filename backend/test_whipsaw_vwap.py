"""Unit tests for VWAP-based WHIPSAW event detection."""
from datetime import datetime, timedelta

import pytz

from backend.services.daily_checklist_chop_gates import (
    count_whipsaw_reversals,
    list_whipsaw_reversal_events,
    list_whipsaw_reversal_events_ema5,
)

IST = pytz.timezone("Asia/Kolkata")


def _bars_5m(session_date: str, closes_high_low_vol):
    """Build 5m candles for a session so pairs form confirmed 10m bars.

    Each tuple is (close, high, low, volume) for one 5m bar starting 09:15.
    """
    d = datetime.strptime(session_date, "%Y-%m-%d")
    out = []
    t0 = IST.localize(datetime(d.year, d.month, d.day, 9, 15))
    for i, (c, h, lo, v) in enumerate(closes_high_low_vol):
        ts = t0 + timedelta(minutes=5 * i)
        out.append(
            {
                "timestamp": ts.isoformat(),
                "open": c,
                "high": h,
                "low": lo,
                "close": c,
                "volume": v,
            }
        )
    return out


def test_vwap_whipsaw_long_flip_counts():
    # Tiny volume on early bars so later VWAP is dominated by recent prices.
    seq = []
    for _ in range(2):  # event 1: above then below
        seq += [(110, 111, 109, 10), (110, 111, 109, 10)]
        seq += [(90, 91, 89, 10), (90, 91, 89, 10)]
    for _ in range(2):  # event 2
        seq += [(110, 111, 109, 10), (110, 111, 109, 10)]
        seq += [(90, 91, 89, 10), (90, 91, 89, 10)]
    candles = _bars_5m("2026-07-16", seq)
    events = list_whipsaw_reversal_events(
        candles, session_date="2026-07-16", is_long=True, near_atr=0.35, atr=2.0
    )
    assert len(events) >= 2
    assert events[0]["basis"].startswith("vwap_")
    assert "touch_vwap" in events[0]
    assert count_whipsaw_reversals(
        candles, session_date="2026-07-16", is_long=True, near_atr=0.35, atr=2.0
    ) == len(events)


def test_ema5_helper_still_callable():
    seq = [(100, 101, 99, 1000)] * 20
    candles = _bars_5m("2026-07-16", seq)
    ev = list_whipsaw_reversal_events_ema5(
        candles, session_date="2026-07-16", is_long=True, near_atr=0.35, atr=2.0
    )
    assert isinstance(ev, list)
