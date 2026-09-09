"""Stock Options webhook gate, duplicate ignore, EMA arm/invalidate (no DB)."""
from datetime import datetime, timedelta

import pytz

from backend.services.stock_option_signals import (
    EXPIRY_REMARKS,
    SIDE_BEAR,
    SIDE_BULL,
    STATUS_ACTIVE,
    STATUS_COMPLETED,
    STATUS_EXECUTED,
    STATUS_RADAR,
    STATUS_REJECTED,
    WR_PERIOD,
    active_past_max_age,
    expiry_remarks,
    invalidate_outcome,
    aggregate_intraday_to_2h,
    combined_pnl,
    completed_2h_ohlc,
    ema_condition_holds,
    hard_stop_price,
    realized_credit_pnl,
    _row_public,
    next_ema_action,
    parse_chartink_symbols,
    pick_nearest_delta,
    should_insert_new_signal,
    side_from_williamsr,
    spread_lines,
    williams_r_at,
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


def test_parse_columns_ignores_webhook_williamsr():
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
    assert status == "success"
    assert rows == [{"symbol": "RELIANCE"}, {"symbol": "TCS"}, {"symbol": "INFY"}]
    assert all("williamsr" not in r for r in rows)
    assert meta["scan_name"] == "HA-stock option"


def _bars(n: int, close: float, start: datetime) -> list:
    out = []
    for i in range(n):
        out.append({"end": start + timedelta(hours=2 * i), "high": 110.0, "low": 90.0, "close": close})
    return out


def test_williams_r_280_gate_uses_last_completed_bar():
    start = datetime(2026, 1, 1, 11, 15)
    asof = start + timedelta(hours=2 * (WR_PERIOD - 1))
    later = asof + timedelta(hours=2)

    bear = _bars(WR_PERIOD, 109.5, start)
    bear.append({"end": later, "high": 110.0, "low": 90.0, "close": 90.0})
    wr_bear = williams_r_at(bear, asof)
    assert wr_bear == (110.0 - 109.5) / (110.0 - 90.0) * -100.0
    assert side_from_williamsr(wr_bear) == SIDE_BEAR
    assert williams_r_at(bear, asof) != williams_r_at(bear, later)

    bull = _bars(WR_PERIOD, 90.2, start)
    assert side_from_williamsr(williams_r_at(bull, asof)) == SIDE_BULL

    mid = _bars(WR_PERIOD, 100.0, start)
    assert side_from_williamsr(williams_r_at(mid, asof)) is None
    assert williams_r_at(bear[: WR_PERIOD - 1], asof) is None

    candles = [
        {"timestamp": "2026-09-08T09:15:00+05:30", "open": 10, "high": 12, "low": 9, "close": 11},
        {"timestamp": "2026-09-08T10:15:00+05:30", "open": 11, "high": 13, "low": 10, "close": 12},
    ]
    ohlc = completed_2h_ohlc(aggregate_intraday_to_2h(candles, now=IST.localize(datetime(2026, 9, 8, 11, 15))))
    assert ohlc[0]["end"] == datetime(2026, 9, 8, 11, 15)
    assert ohlc[0]["high"] == 13
    assert ohlc[0]["low"] == 9


def test_ignore_if_radar_or_active_allow_if_only_executed():
    assert should_insert_new_signal([]) is True
    assert should_insert_new_signal([STATUS_RADAR]) is False
    assert should_insert_new_signal([STATUS_ACTIVE]) is False
    assert should_insert_new_signal([STATUS_EXECUTED, STATUS_RADAR]) is False
    assert should_insert_new_signal([STATUS_EXECUTED]) is True
    assert should_insert_new_signal([STATUS_EXECUTED, STATUS_EXECUTED]) is True
    assert should_insert_new_signal([STATUS_REJECTED]) is True
    assert should_insert_new_signal([STATUS_COMPLETED]) is True


def test_invalidate_keeps_executed_only_with_arm_and_strikes():
    armed = datetime(2026, 9, 8, 11, 15)
    assert invalidate_outcome(armed, 1400, 1350) == STATUS_EXECUTED
    assert invalidate_outcome(None, 1400, 1350) == STATUS_REJECTED
    assert invalidate_outcome(armed, None, 1350) == STATUS_REJECTED
    assert invalidate_outcome(armed, 1400, None) == STATUS_REJECTED


def test_active_expires_after_72h():
    armed = datetime(2026, 9, 6, 11, 15)
    assert active_past_max_age(armed, datetime(2026, 9, 9, 11, 14)) is False
    assert active_past_max_age(armed, datetime(2026, 9, 9, 11, 15)) is True
    assert active_past_max_age(None, datetime(2026, 9, 9, 11, 15)) is False
    assert expiry_remarks(None) == EXPIRY_REMARKS
    assert expiry_remarks("keep strikes") == f"keep strikes | {EXPIRY_REMARKS}"
    assert expiry_remarks(EXPIRY_REMARKS) == EXPIRY_REMARKS


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
    # Credit spread: net credit at entry minus cost to close.
    # pnl = sell_entry - buy_entry - (sell_exit - buy_exit)
    assert realized_credit_pnl(10, 4, 6, 3) == (10 - 4) - (6 - 3)
    assert realized_credit_pnl(10, 4, 6, 3) == 3
    assert realized_credit_pnl(10, 4, None, 3) is None
    assert hard_stop_price(10) == 30.0
    done = _row_public({
        "id": 1,
        "status": STATUS_COMPLETED,
        "sell_cost": 10,
        "buy_cost": 4,
        "sell_ltp": 99,
        "buy_ltp": 1,
        "sell_exit_price": 6,
        "buy_exit_price": 3,
    })
    assert done["combined_pnl"] == 3
    assert done["hard_stop"] == 30.0
