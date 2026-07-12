"""Tests for lock retention / removal (R1/R2) and no-swap promote."""
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytz

from backend.services.daily_checklist_snapshot import (
    PROMOTION_CUTOFF_MIN,
    PROMOTION_SCANS_REQUIRED,
    REMOVAL_RANK_BAND,
    REMOVAL_RANK_SCANS,
    _eligible_consecutive_top5,
    _r2_rank_gone,
    promote_intraday_from_rs,
)
from backend.services.kavach_confidence import (
    VWAP_CONSISTENCY_BARS,
    vwap_opposite_side_consecutive,
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
    assert REMOVAL_RANK_BAND == 10
    assert REMOVAL_RANK_SCANS == 3
    assert VWAP_CONSISTENCY_BARS == 8


def test_vwap_opposite_consecutive_long_lock():
    closes = [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0]
    vwaps = [10.5] * 8
    assert vwap_opposite_side_consecutive(
        closes, vwaps, lock_direction="LONG", num_bars=8, bar_size=1
    )
    closes2 = list(closes)
    closes2[-1] = 11.0
    assert not vwap_opposite_side_consecutive(
        closes2, vwaps, lock_direction="LONG", num_bars=8, bar_size=1
    )


def test_vwap_opposite_consecutive_short_lock():
    closes = [11.0] * 8
    vwaps = [10.0] * 8
    assert vwap_opposite_side_consecutive(
        closes, vwaps, lock_direction="SHORT", num_bars=8, bar_size=1
    )


def test_eligible_uses_latest_pair_only():
    t1 = IST.localize(datetime(2026, 7, 10, 12, 31, 0))
    t2 = IST.localize(datetime(2026, 7, 10, 12, 35, 0))
    t3 = IST.localize(datetime(2026, 7, 10, 12, 40, 0))
    times = MagicMock()
    times.fetchall.return_value = [SimpleNamespace(scan_time=t) for t in (t1, t2, t3)]

    def top5(st):
        m = MagicMock()
        if st in (t2, t3):
            m.fetchall.return_value = [_scan_row("GODREJPROP", "BULL", 4 if st == t2 else 5)]
        else:
            m.fetchall.return_value = [_scan_row("OTHER", "BULL", 1)]
        return m

    db = MagicMock()
    db.execute.side_effect = lambda sql, params=None: (
        times if "DISTINCT scan_time" in str(sql) else top5(params["st"])
    )
    now = IST.localize(datetime(2026, 7, 10, 12, 45, 0))
    elig = _eligible_consecutive_top5(db, "2026-07-10", now=now)
    assert ("GODREJPROP", "BULL") in elig
    assert elig[("GODREJPROP", "BULL")]["rank"] == 5


def test_eligible_ignores_stale_historical_pair():
    t1 = IST.localize(datetime(2026, 7, 10, 9, 25, 0))
    t2 = IST.localize(datetime(2026, 7, 10, 9, 30, 0))
    t3 = IST.localize(datetime(2026, 7, 10, 10, 45, 0))
    t4 = IST.localize(datetime(2026, 7, 10, 10, 50, 0))
    times = MagicMock()
    times.fetchall.return_value = [SimpleNamespace(scan_time=t) for t in (t1, t2, t3, t4)]

    def top5(st):
        m = MagicMock()
        if st in (t1, t2):
            m.fetchall.return_value = [_scan_row("GODREJPROP", "BEAR", 5)]
        else:
            m.fetchall.return_value = [_scan_row("OTHER", "BULL", 1)]
        return m

    db = MagicMock()
    db.execute.side_effect = lambda sql, params=None: (
        times if "DISTINCT scan_time" in str(sql) else top5(params["st"])
    )
    now = IST.localize(datetime(2026, 7, 10, 11, 0, 0))
    elig = _eligible_consecutive_top5(db, "2026-07-10", now=now)
    assert ("GODREJPROP", "BEAR") not in elig


def test_r2_requires_outside_band_for_m_scans():
    t1 = IST.localize(datetime(2026, 7, 10, 12, 0, 0))
    t2 = IST.localize(datetime(2026, 7, 10, 12, 5, 0))
    t3 = IST.localize(datetime(2026, 7, 10, 12, 10, 0))
    times = MagicMock()
    times.fetchall.return_value = [SimpleNamespace(scan_time=t) for t in (t1, t2, t3)]

    def execute(sql, params=None):
        if "DISTINCT scan_time" in str(sql):
            return times
        m = MagicMock()
        m.fetchall.return_value = [_scan_row("OTHER", "BULL", 1)]
        return m

    db = MagicMock()
    db.execute.side_effect = execute
    now = IST.localize(datetime(2026, 7, 10, 12, 15, 0))
    assert _r2_rank_gone(db, "2026-07-10", "GODREJPROP", "BULL", now=now)


def test_promote_adds_new_but_does_not_swap_flip():
    t1 = IST.localize(datetime(2026, 7, 10, 12, 31, 0))
    t2 = IST.localize(datetime(2026, 7, 10, 12, 35, 0))
    now = IST.localize(datetime(2026, 7, 10, 12, 40, 0))

    lock_check = MagicMock()
    lock_check.fetchone.return_value = (1,)
    times = MagicMock()
    times.fetchall.return_value = [SimpleNamespace(scan_time=t1), SimpleNamespace(scan_time=t2)]
    existing = MagicMock()
    existing.fetchall.return_value = [
        SimpleNamespace(
            symbol="MANAPPURAM", direction="BEAR", rank=5, rs_score=-1.0, locked_at=t1
        ),
    ]

    def execute(sql, params=None):
        q = str(sql)
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
    with patch(
        "backend.services.daily_checklist_snapshot._r1_vwap_trend_broken",
        return_value=False,
    ), patch(
        "backend.services.daily_checklist_snapshot._r2_rank_gone",
        return_value=False,
    ):
        out = promote_intraday_from_rs(db, "2026-07-10", now=now)

    assert any(p["symbol"] == "GODREJPROP" and p["direction"] == "BULL" for p in out["promoted"])
    assert out["flipped"] == []
    assert not any(r.get("symbol") == "MANAPPURAM" for r in out.get("removed") or [])


def test_promote_skips_when_not_locked():
    db = MagicMock()
    lock_check = MagicMock()
    lock_check.fetchone.return_value = None
    db.execute.return_value = lock_check
    now = IST.localize(datetime(2026, 7, 10, 12, 0, 0))
    out = promote_intraday_from_rs(db, "2026-07-10", now=now)
    assert out["reason"] == "not_locked"


def test_promote_skips_past_cutoff():
    db = MagicMock()
    lock_check = MagicMock()
    lock_check.fetchone.return_value = (1,)
    db.execute.return_value = lock_check
    now = IST.localize(datetime(2026, 7, 10, 14, 45, 0))
    out = promote_intraday_from_rs(db, "2026-07-10", now=now)
    assert out["reason"] == "past_cutoff"
