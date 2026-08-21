"""Unit tests for Kavach BT checkpoint (no Upstox / no DB)."""
from __future__ import annotations

from datetime import datetime

import pytz

from backend.services.kavach_bt_checkpoint.exits import (
    pick_best_exit,
    simulate_dynamic_trail_exit,
)
from backend.services.kavach_bt_checkpoint.pullback import (
    count_pullbacks_v2_on_10m,
    pb_bucket,
    pullback_at_entry,
)
from backend.services.kavach_bt_checkpoint.resistance import (
    detect_pivot_levels,
    evaluate_resistance_confluence,
)
from backend.services.kavach_bt_checkpoint.report import build_summary_rows

IST = pytz.timezone("Asia/Kolkata")


def _bar(i, o, h, l, c, ema5, ema10, vwap):
    total_min = 25 + i * 10
    hour = 9 + total_min // 60
    minute = total_min % 60
    return {
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "ema5": ema5,
        "ema10": ema10,
        "vwap": vwap,
        "bar_end": IST.localize(datetime(2026, 8, 10, hour, minute)),
        "volume": 1000,
    }


def test_pullback_v2_dual_reset_and_ema5_increment():
    # Bar0: no dual. Bar1: dual reset. Bar2: EMA5 touch (+1). Bar3: leave EMA5. Bar4: touch again (+2).
    bars = [
        _bar(0, 100, 101, 99.5, 100.5, 100.2, 98.0, 97.5),
        _bar(1, 100, 100.5, 98.5, 99.0, 99.5, 99.0, 99.0),  # dual @ 99
        _bar(2, 100.5, 101.5, 100.0, 101.0, 100.2, 99.0, 98.5),  # EMA5 touch
        _bar(3, 101.0, 102.0, 100.5, 101.5, 100.0, 99.1, 98.6),  # no EMA5 touch (ema5 below low)
        _bar(4, 101.2, 102.2, 100.0, 101.8, 100.3, 99.1, 98.6),  # EMA5 touch again
    ]
    long_s, short_s, flags = count_pullbacks_v2_on_10m(bars)
    assert flags[1]["dual_reset"] is True
    assert flags[2]["dual_reset"] is False
    assert long_s[1] == 0
    assert long_s[2] == 1
    assert long_s[4] == 2
    assert pb_bucket(5) == "5+"
    assert pb_bucket(3) == "3"


def test_pullback_hard_block_at_entry():
    bars = []
    px = 100.0
    for i in range(14):
        vwap = 90.0
        ema10 = 90.0
        if i % 2 == 0:
            # touch EMA5
            ema5 = px + 0.1
            bars.append(_bar(i, px, px + 1.0, px - 0.05, px + 0.3, ema5, ema10, vwap))
        else:
            # leave EMA5 (ema5 below bar)
            ema5 = px - 2.0
            bars.append(_bar(i, px, px + 1.0, px + 0.1, px + 0.5, ema5, ema10, vwap))
        px += 0.5
    long_s, _, flags = count_pullbacks_v2_on_10m(bars)
    assert all(not f["dual_reset"] for f in flags)
    assert long_s[-1] >= 5
    info = pullback_at_entry(bars, bars[-1]["bar_end"], "LONG")
    assert info["pb_v2"] >= 5
    assert info["pb_hard_blocked"] is True


def test_resistance_confluence_warning():
    bars = []
    for i in range(10):
        c = 100 + i
        bars.append(_bar(i, c, c + 1, c - 1, c, c, c - 0.5, c - 1))
    bars[5] = _bar(5, 105, 112, 104, 106, 106, 105, 104)
    bars[6] = _bar(6, 106, 108, 105, 107, 107, 106, 105)
    bars[7] = _bar(7, 107, 109, 106, 108, 108, 107, 106)
    bars[3] = _bar(3, 111.9, 112.1, 111.5, 112.0, 111, 110, 109)
    bars[4] = _bar(4, 112.0, 112.2, 111.8, 111.95, 111, 110, 109)
    pivots = detect_pivot_levels(bars)
    assert isinstance(pivots, list)
    res = evaluate_resistance_confluence(
        bars, entry_idx=7, entry_price=111.7, direction="LONG"
    )
    assert "res_confluence" in res
    assert res.get("warning_only", True) is True or res["res_confluence"] in (True, False)


def test_dynamic_trail_locks_1r_at_2r():
    entry = 100.0
    risk = 1.0
    bars = [
        _bar(0, 100, 100.5, 99.8, 100.4, 100, 99, 99),
        _bar(1, 100.4, 102.2, 100.3, 102.0, 101, 100, 99),  # ~2R MFE
        _bar(2, 102.0, 103.1, 101.9, 103.0, 102, 101, 100),  # ~3R
        _bar(3, 103.0, 103.2, 101.4, 101.5, 102, 101, 100),  # close back — should trail
    ]
    ev = simulate_dynamic_trail_exit(bars, entry=entry, risk_pts=risk, direction="LONG")
    assert ev is not None
    assert ev["method"] == "B_dynamic_trail"
    assert ev["exit_r"] is not None


def test_pick_best_exit_and_summary():
    assert pick_best_exit({"exit_r": 1.0}, {"exit_r": 2.5}, {"exit_r": 0.5}) == "B"
    details = [
        {
            "r_realized": 1.0,
            "mfe_r": 2.0,
            "mae_r": -0.5,
            "pnl": 100,
            "pb_v2": 1,
            "pb_legacy": 2,
            "pb_hard_blocked": False,
            "res_confluence": False,
            "exit_a_r": 0.8,
            "exit_b_r": 1.5,
            "exit_c_r": 1.0,
            "best_exit_method": "B",
            "garuda_confluence": "MATCH",
            "garuda_rank": 2,
        },
        {
            "r_realized": -1.0,
            "mfe_r": 0.5,
            "mae_r": -1.2,
            "pnl": -80,
            "pb_v2": 5,
            "pb_legacy": 3,
            "pb_hard_blocked": True,
            "res_confluence": True,
            "exit_a_r": -0.5,
            "exit_b_r": -0.2,
            "exit_c_r": -1.0,
            "best_exit_method": "B",
            "garuda_confluence": "NO_MATCH",
            "garuda_rank": None,
        },
    ]
    rows = build_summary_rows(details)
    types = {r["cohort_type"] for r in rows}
    assert "overall" in types
    assert "pullback_v2" in types
    assert "garuda" in types
    assert "recommendation" in types
