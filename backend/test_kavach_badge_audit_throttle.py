"""Badge audit throttle: always write on trade_state change."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytz

from backend.services.kavach_badge_audit import (
    _LOG_MIN_INTERVAL,
    should_log_badge_audit,
)

IST = pytz.timezone("Asia/Kolkata")


def _prev(**kwargs):
    base = dict(
        logged_at=IST.localize(datetime(2026, 8, 3, 9, 44, 33)),
        trade_state="SCANNING",
        whipsaw_active=False,
        dir_conflict_active=False,
        regime_unstable_active=True,
        churn_active=False,
        persistence={},
        gate_badges=[],
    )
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_badge_writes_immediately_on_trade_state_change():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = _prev(trade_state="SCANNING")
    now = IST.localize(datetime(2026, 8, 3, 9, 45, 19))  # < 4 min later
    assert should_log_badge_audit(
        db,
        "2026-08-03",
        "CHOLAFIN",
        {"REGIME UNSTABLE"},
        trade_state="READY",
        now=now,
    ) is True


def test_badge_throttle_still_applies_when_state_and_badges_unchanged():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = _prev(trade_state="READY")
    now = IST.localize(datetime(2026, 8, 3, 9, 45, 19))  # < 4 min
    assert should_log_badge_audit(
        db,
        "2026-08-03",
        "CHOLAFIN",
        {"REGIME UNSTABLE"},
        trade_state="READY",
        now=now,
    ) is False
    later = now + _LOG_MIN_INTERVAL
    assert should_log_badge_audit(
        db,
        "2026-08-03",
        "CHOLAFIN",
        {"REGIME UNSTABLE"},
        trade_state="READY",
        now=later,
    ) is True


def test_badge_writes_on_badge_set_change_even_within_interval():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = _prev(
        trade_state="READY", regime_unstable_active=True
    )
    now = IST.localize(datetime(2026, 8, 3, 9, 45, 19))
    assert should_log_badge_audit(
        db,
        "2026-08-03",
        "CHOLAFIN",
        {"REGIME UNSTABLE", "WHIPSAW"},
        trade_state="READY",
        now=now,
    ) is True
