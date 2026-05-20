"""Vajra opening-session candle guards (09:20 screening readiness)."""
from datetime import datetime

import pytz

from backend.services.vajra.candles import (
    drop_incomplete_last_bar,
    is_bar_complete,
    is_opening_session_ist,
    is_vajra_screening_ready_ist,
    min_bars_for_tf,
    opening_session_skip_5m_validation,
    prepare_vajra_candles,
)

IST = pytz.timezone("Asia/Kolkata")


def _at(h: int, m: int) -> datetime:
    return IST.localize(datetime(2026, 5, 21, h, m, 0))


def test_screening_ready_from_0920():
    assert is_vajra_screening_ready_ist(_at(9, 19)) is False
    assert is_vajra_screening_ready_ist(_at(9, 20)) is True
    assert is_vajra_screening_ready_ist(_at(10, 0)) is True


def test_opening_session_window():
    assert is_opening_session_ist(_at(9, 15)) is True
    assert is_opening_session_ist(_at(9, 40)) is True
    assert is_opening_session_ist(_at(9, 41)) is False


def test_drop_incomplete_30m_at_0920():
    now = _at(9, 20)
    candles = [
        {"timestamp": "2026-05-20T14:45:00+05:30", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10},
        {"timestamp": "2026-05-21T09:15:00+05:30", "open": 2, "high": 3, "low": 2, "close": 3, "volume": 5},
    ]
    assert is_bar_complete(candles[-1], 30, now) is False
    out = prepare_vajra_candles(candles, "30m", now=now)
    assert len(out) == 1


def test_5m_bar_complete_at_0920():
    candle = {"timestamp": "2026-05-21T09:15:00+05:30"}
    assert is_bar_complete(candle, 5, _at(9, 20)) is True


def test_opening_min_bars_lower_than_regular():
    assert min_bars_for_tf("30m", opening_session=True) < min_bars_for_tf("30m", opening_session=False)


def test_skip_5m_validation_early_open():
    assert opening_session_skip_5m_validation(_at(9, 25)) is True
    assert opening_session_skip_5m_validation(_at(9, 40)) is False
