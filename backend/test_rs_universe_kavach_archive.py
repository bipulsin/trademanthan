"""Tests for after-hours full-universe Kavach archive."""
from datetime import datetime

import pytz

from backend.services.rs_universe_kavach_archive import (
    _after_archive_window,
    _contract_month_hint,
    _score_directional,
)

IST = pytz.timezone("Asia/Kolkata")


def test_after_archive_window_post_close():
    dt = IST.localize(datetime(2026, 7, 7, 15, 35))
    assert _after_archive_window(dt) is True


def test_after_archive_window_mid_session_false():
    dt = IST.localize(datetime(2026, 7, 7, 11, 0))
    assert _after_archive_window(dt) is False


def test_contract_month_hint_parses_fut_symbol():
    assert _contract_month_hint("NIFTY26JULFUT") == "26JUL"


def test_score_directional_assigns_ranks():
    rows = [
        {
            "symbol": "A",
            "kavach_state": "BUY",
            "relative_strength": 3.0,
            "volume_ratio": 1.2,
            "adx": 25,
            "current_price": 100,
            "vwap": 99,
            "volume_label": "High",
            "vwap_purity_pct": 70,
            "market_regime": "TREND",
        },
        {
            "symbol": "B",
            "kavach_state": "BUY",
            "relative_strength": 1.0,
            "volume_ratio": 1.0,
            "adx": 20,
            "current_price": 50,
            "vwap": 49,
            "volume_label": "Average",
            "vwap_purity_pct": 50,
            "market_regime": "TREND",
        },
    ]
    bull, bear = _score_directional(rows)
    assert len(bull) == 2
    assert bull[0]["symbol"] == "A"
    assert bull[0].get("would_be_rank_bull") == 1
