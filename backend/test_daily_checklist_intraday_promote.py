"""Tests for intraday daily_snapshot promotion (2 consecutive Top-5 scans)."""
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytz

from backend.services.daily_checklist_snapshot import (
    PROMOTION_CUTOFF_MIN,
    PROMOTION_SCANS_REQUIRED,
    _eligible_consecutive_top5,
    promote_intraday_from_rs,
)

IST = pytz.timezone("Asia/Kolkata")


def _scan_row(sym, side, rank, rs=1.0):
    return SimpleNamespace(
        symbol=sym,
        ranking_type="BEARISH" if side == "BEAR" else "BULLISH",
        rank_position=rank,
        relative_strength=rs,
    )


def test_promotion_constants():
    assert PROMOTION_SCANS_REQUIRED == 2
    assert PROMOTION_CUTOFF_MIN == 14 * 60 + 30


def test_eligible_requires_two_consecutive_same_side():
    t1 = IST.localize(datetime(2026, 7, 10, 12, 31, 0))
    t2 = IST.localize(datetime(2026, 7, 10, 12, 35, 0))
    t3 = IST.localize(datetime(2026, 7, 10, 12, 40, 0))

    times = MagicMock()
    times.fetchall.return_value = [
        SimpleNamespace(scan_time=t1),
        SimpleNamespace(scan_time=t2),
        SimpleNamespace(scan_time=t3),
    ]

    def top5(st):
        m = MagicMock()
        if st == t1:
            m.fetchall.return_value = [_scan_row("GODREJPROP", "BULL", 4)]
        elif st == t2:
            m.fetchall.return_value = [_scan_row("GODREJPROP", "BULL", 5)]
        else:
            m.fetchall.return_value = [_scan_row("OTHER", "BULL", 1)]
        return m

    db = MagicMock()

    def execute(sql, params=None):
        q = str(sql)
        if "DISTINCT scan_time" in q:
            return times
        return top5(params["st"])

    db.execute.side_effect = execute
    now = IST.localize(datetime(2026, 7, 10, 12, 45, 0))
    elig = _eligible_consecutive_top5(db, "2026-07-10", now=now)
    assert ("GODREJPROP", "BULL") in elig
    assert elig[("GODREJPROP", "BULL")]["rank"] == 5


def test_eligible_ignores_single_scan_blip():
    t1 = IST.localize(datetime(2026, 7, 10, 10, 45, 0))
    t2 = IST.localize(datetime(2026, 7, 10, 10, 50, 0))

    times = MagicMock()
    times.fetchall.return_value = [
        SimpleNamespace(scan_time=t1),
        SimpleNamespace(scan_time=t2),
    ]

    def execute(sql, params=None):
        q = str(sql)
        if "DISTINCT scan_time" in q:
            return times
        m = MagicMock()
        if params["st"] == t1:
            m.fetchall.return_value = [_scan_row("GODREJPROP", "BULL", 2)]
        else:
            m.fetchall.return_value = [_scan_row("OTHER", "BULL", 1)]
        return m

    db = MagicMock()
    db.execute.side_effect = execute
    now = IST.localize(datetime(2026, 7, 10, 11, 0, 0))
    elig = _eligible_consecutive_top5(db, "2026-07-10", now=now)
    assert ("GODREJPROP", "BULL") not in elig


def test_promote_adds_new_and_flips_existing():
    t1 = IST.localize(datetime(2026, 7, 10, 12, 31, 0))
    t2 = IST.localize(datetime(2026, 7, 10, 12, 35, 0))
    now = IST.localize(datetime(2026, 7, 10, 12, 40, 0))

    lock_check = MagicMock()
    lock_check.fetchone.return_value = (1,)

    times = MagicMock()
    times.fetchall.return_value = [
        SimpleNamespace(scan_time=t1),
        SimpleNamespace(scan_time=t2),
    ]

    existing = MagicMock()
    existing.fetchall.return_value = [
        SimpleNamespace(symbol="MANAPPURAM", direction="BEAR", rank=5, rs_score=-1.0),
    ]

    calls = []

    def execute(sql, params=None):
        q = str(sql)
        calls.append(q)
        if "FROM snapshot_lock" in q:
            return lock_check
        if "DISTINCT scan_time" in q:
            return times
        if "FROM daily_snapshot" in q and "ORDER BY" in q:
            return existing
        if "FROM relative_strength_snapshot" in q and "rank_position" in q:
            m = MagicMock()
            m.fetchall.return_value = [
                _scan_row("GODREJPROP", "BULL", 4),
                _scan_row("MANAPPURAM", "BULL", 2),
            ]
            return m
        return MagicMock()

    db = MagicMock()
    db.execute.side_effect = execute
    out = promote_intraday_from_rs(db, "2026-07-10", now=now)
    assert any(p["symbol"] == "GODREJPROP" and p["direction"] == "BULL" for p in out["promoted"])
    assert any(f["symbol"] == "MANAPPURAM" and f["to_direction"] == "BULL" for f in out["flipped"])
    assert any("DELETE FROM daily_snapshot" in c for c in calls)


def test_promote_skips_when_not_locked():
    db = MagicMock()
    lock_check = MagicMock()
    lock_check.fetchone.return_value = None
    db.execute.return_value = lock_check
    now = IST.localize(datetime(2026, 7, 10, 12, 0, 0))
    out = promote_intraday_from_rs(db, "2026-07-10", now=now)
    assert out["reason"] == "not_locked"
    assert out["promoted"] == []


def test_promote_skips_past_cutoff():
    db = MagicMock()
    lock_check = MagicMock()
    lock_check.fetchone.return_value = (1,)
    db.execute.return_value = lock_check
    now = IST.localize(datetime(2026, 7, 10, 14, 45, 0))
    out = promote_intraday_from_rs(db, "2026-07-10", now=now)
    assert out["reason"] == "past_cutoff"
