"""Stock Options webhook gate, duplicate ignore, EMA arm/invalidate (no DB)."""
from datetime import datetime

import pytz

from backend.services.stock_option_signals import (
    SIDE_BEAR,
    SIDE_BULL,
    STATUS_ACTIVE,
    STATUS_EXECUTED,
    STATUS_RADAR,
    STATUS_REJECTED,
    invalidate_outcome,
    aggregate_intraday_to_2h,
    combined_pnl,
    ema_condition_holds,
    hard_stop_price,
    next_ema_action,
    parse_chartink_symbols,
    pick_nearest_delta,
    should_insert_new_signal,
    side_from_williamsr,
    spread_lines,
)

IST = pytz.timezone("Asia/Kolkata")


def test_williamsr_gate_bounds_excluded():
    assert side_from_williamsr(-2.9) == SIDE_BEAR
    assert side_from_williamsr(0) == SIDE_BEAR
    assert side_from_williamsr(-3) is None
    assert side_from_williamsr(-3.0) is None
    assert side_from_williamsr(-50) is None
    assert side_from_williamsr(-97) is None
    assert side_from_williamsr(-97.1) == SIDE_BULL
    assert side_from_williamsr(-100) == SIDE_BULL
    assert side_from_williamsr("nope") is None


def test_parse_columns_williamsr_and_alias():
    parsed = {
        "stocks": "AAA, BBB, CCC",
        "trigger_prices": "1,2,3",
        "triggered_at": "2:14 pm",
        "scan_name": "HA-stock option",
        "scan_url": "ha-stock-option",
        "alert_name": "Alert for HA-stock option",
        "webhook_url": "https://www.tradewithcto.com/webhook/stockOption",
        "columns": [
            {"symbol": "RELIANCE", "williamsr": -1.2},
            {"symbol": "TCS", "williams_r": -98.4},
            {"symbol": "INFY", "williamsr": -50},
        ],
    }
    status, rows, meta = parse_chartink_symbols(parsed)
    assert status == "partial"
    assert [r["symbol"] for r in rows] == ["RELIANCE", "TCS", "INFY"]
    assert side_from_williamsr(rows[0]["williamsr"]) == SIDE_BEAR
    assert side_from_williamsr(rows[1]["williamsr"]) == SIDE_BULL
    assert side_from_williamsr(rows[2]["williamsr"]) is None
    assert meta["scan_name"] == "HA-stock option"


def test_ignore_if_radar_or_active_allow_if_only_executed():
    assert should_insert_new_signal([]) is True
    assert should_insert_new_signal([STATUS_RADAR]) is False
    assert should_insert_new_signal([STATUS_ACTIVE]) is False
    assert should_insert_new_signal([STATUS_EXECUTED, STATUS_RADAR]) is False
    assert should_insert_new_signal([STATUS_EXECUTED]) is True
    assert should_insert_new_signal([STATUS_EXECUTED, STATUS_EXECUTED]) is True
    assert should_insert_new_signal([STATUS_REJECTED]) is True


def test_invalidate_keeps_executed_only_with_arm_and_strikes():
    armed = datetime(2026, 9, 8, 11, 15)
    assert invalidate_outcome(armed, 1400, 1350) == STATUS_EXECUTED
    assert invalidate_outcome(None, 1400, 1350) == STATUS_REJECTED
    assert invalidate_outcome(armed, None, 1350) == STATUS_REJECTED
    assert invalidate_outcome(armed, 1400, None) == STATUS_REJECTED


def test_ema_arm_and_invalidate():
    assert ema_condition_holds(SIDE_BEAR, 90, 100, 110) is True
    assert ema_condition_holds(SIDE_BEAR, 100, 100, 90) is False
    assert ema_condition_holds(SIDE_BULL, 120, 100, 90) is True
    assert ema_condition_holds(SIDE_BULL, 90, 100, 80) is False

    assert next_ema_action(STATUS_RADAR, SIDE_BEAR, 90, 100, 110, False) == "arm"
    assert next_ema_action(STATUS_RADAR, SIDE_BEAR, 120, 100, 110, False) == "hold"
    assert next_ema_action(STATUS_RADAR, SIDE_BEAR, None, 100, 110, False) == "hold"
    assert next_ema_action(STATUS_ACTIVE, SIDE_BEAR, 120, 100, 110, False) == "invalidate"
    assert next_ema_action(STATUS_ACTIVE, SIDE_BEAR, 120, 100, 110, True) == "hold"
    assert next_ema_action(STATUS_ACTIVE, SIDE_BULL, 80, 100, 110, False) == "invalidate"
    assert next_ema_action(STATUS_ACTIVE, SIDE_BULL, 130, 100, 110, False) == "hold"


def test_spread_labels_and_delta_volume_tiebreak():
    sell, buy = spread_lines(SIDE_BEAR, 2500, 2600)
    assert sell == "Sell CE:~28 Δ - 2500"
    assert buy == "Buy CE~18 Δ 2600"
    sell_pe, buy_pe = spread_lines(SIDE_BULL, 1400.5, 1350)
    assert sell_pe == "Sell PE:~28 Δ - 1400.5"
    assert buy_pe == "Buy PE~18 Δ 1350"

    legs = [
        {"strike": 100, "delta_pts": 28.2, "volume": 10},
        {"strike": 110, "delta_pts": 28.4, "volume": 500},
        {"strike": 120, "delta_pts": 40, "volume": 9999},
    ]
    picked = pick_nearest_delta(legs, 28)
    assert picked["strike"] == 110


def test_aggregate_2h_from_1h_aligned_0915():
    candles = [
        {"timestamp": "2026-09-08T09:15:00+05:30", "open": 10, "high": 12, "low": 9, "close": 11, "volume": 1},
        {"timestamp": "2026-09-08T10:15:00+05:30", "open": 11, "high": 13, "low": 10, "close": 12, "volume": 2},
        {"timestamp": "2026-09-08T11:15:00+05:30", "open": 12, "high": 14, "low": 11, "close": 13, "volume": 3},
    ]
    now = IST.localize(datetime(2026, 9, 8, 11, 15))
    bars = aggregate_intraday_to_2h(candles, now=now)
    assert len(bars) == 1
    assert bars[0]["close"] == 12
    assert bars[0]["open"] == 10
    assert bars[0]["high"] == 13


def test_pnl_and_hard_stop():
    assert combined_pnl(10, 4, 6, 3) == (10 - 4) - (6 - 3)
    assert combined_pnl(10, 4, None, 3) is None
    assert hard_stop_price(12.5) == 37.5
