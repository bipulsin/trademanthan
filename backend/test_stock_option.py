"""Stock Options webhook gate, duplicate ignore, EMA arm/invalidate (no DB)."""
from datetime import date, datetime, timedelta

import pytz

from backend.services.stock_option_signals import (
    DELTA_BUY_INDEX,
    DELTA_SELL_INDEX,
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
    delta_targets_for_symbol,
    detect_ema_cross_side,
    expiry_remarks,
    format_contract_mmm_yyyy,
    invalidate_outcome,
    aggregate_intraday_to_2h,
    attach_rupee_pnl,
    combined_pnl,
    pnl_rupees,
    completed_2h_ohlc,
    ema_condition_holds,
    hard_stop_price,
    is_index_symbol,
    realized_credit_pnl,
    _optional_exit_bundle,
    _row_public,
    next_ema_action,
    next_index_ema_action,
    parse_chartink_symbols,
    parse_fut_trading_symbol_expiry,
    pick_nearest_delta,
    resolve_contract_mmm_yyyy,
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


def test_index_symbols_and_delta_targets():
    assert is_index_symbol("NIFTY") is True
    assert is_index_symbol("banknifty") is True
    assert is_index_symbol("RELIANCE") is False
    assert delta_targets_for_symbol("NIFTY") == (DELTA_SELL_INDEX, DELTA_BUY_INDEX)
    assert delta_targets_for_symbol("BANKNIFTY") == (15.0, 2.0)
    assert delta_targets_for_symbol("TCS") == (28.0, 18.0)
    sell, buy = spread_lines(SIDE_BEAR, 25000, 25500, symbol="NIFTY")
    assert sell == "Sell CE:~15 Δ - 25000"
    assert buy == "Buy CE~2 Δ 25500"
    pub = _row_public({"symbol": "NIFTY", "status": "Radar", "side": None})
    assert pub["is_index"] is True
    assert pub["index_fut"] is True
    pub2 = _row_public({"symbol": "RELIANCE", "status": "Radar", "side": None})
    assert pub2["is_index"] is False
    assert pub2["index_fut"] is False


def test_index_contract_as_of_roll_day20():
    """Armed after day 20 rolls contract month; early-month stays same month."""
    assert resolve_contract_mmm_yyyy("NIFTY", datetime(2026, 8, 27, 13, 15)) == "SEP-2026"
    assert resolve_contract_mmm_yyyy("BANKNIFTY", datetime(2026, 9, 7, 11, 15)) == "SEP-2026"


def test_index_ema_cross_vs_hold_bull_put():
    """BULL PUT: arm only on cross above both — not when already holding above."""
    # Cross: prev not above both (e9 below e30), curr strictly above both
    assert detect_ema_cross_side(95, 100, 90, 120, 100, 90) == SIDE_BULL
    # Hold already true on prev (e9 > both) — not a cross
    assert detect_ema_cross_side(120, 100, 90, 130, 100, 90) is None
    # Curr equal to one EMA — not strictly above both
    assert detect_ema_cross_side(95, 100, 90, 100, 100, 90) is None
    # Curr above one but not both
    assert detect_ema_cross_side(95, 100, 110, 105, 100, 110) is None

    action, side = next_index_ema_action(
        STATUS_RADAR, None, 95, 100, 90, 120, 100, 90, False
    )
    assert action == "arm" and side == SIDE_BULL
    # Same levels that would arm a stock on hold alone — index stays hold (already above)
    action, side = next_index_ema_action(
        STATUS_RADAR, None, 120, 100, 90, 130, 100, 90, False
    )
    assert action == "hold" and side is None
    # Stock path WOULD arm when condition merely holds
    assert next_ema_action(STATUS_RADAR, SIDE_BULL, 130, 100, 90, False) == "arm"


def test_index_ema_cross_vs_hold_bear_call():
    """BEAR CALL: arm only on cross below both — not when already holding below."""
    assert detect_ema_cross_side(105, 100, 110, 90, 100, 110) == SIDE_BEAR
    assert detect_ema_cross_side(90, 100, 110, 85, 100, 110) is None
    assert detect_ema_cross_side(105, 100, 110, 100, 100, 110) is None

    action, side = next_index_ema_action(
        STATUS_RADAR, None, 105, 100, 110, 90, 100, 110, False
    )
    assert action == "arm" and side == SIDE_BEAR
    action, side = next_index_ema_action(
        STATUS_RADAR, None, 90, 100, 110, 85, 100, 110, False
    )
    assert action == "hold" and side is None
    assert next_ema_action(STATUS_RADAR, SIDE_BEAR, 85, 100, 110, False) == "arm"


def test_index_active_invalidates_when_hold_fails():
    """After arm, indices use hold-fail invalidate (same as stocks), not reverse-cross."""
    action, side = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BULL, 95, 100, 90, 120, 100, 90, False
    )
    assert action == "hold" and side is None
    action, side = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BULL, 120, 100, 90, 80, 100, 90, False
    )
    assert action == "invalidate"
    action, side = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BEAR, 105, 100, 110, 90, 100, 110, False
    )
    assert action == "hold"
    action, side = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BEAR, 90, 100, 110, 120, 100, 110, False
    )
    assert action == "invalidate"
    # Trade submitted → never invalidate
    action, _ = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BULL, 120, 100, 90, 80, 100, 90, True
    )
    assert action == "hold"
    # Incomplete EMAs → hold
    action, _ = next_index_ema_action(
        STATUS_RADAR, None, None, 100, 90, 120, 100, 90, False
    )
    assert action == "hold"


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

    near15 = [
        {"strike": 200, "delta_pts": 14.5, "volume": 10},
        {"strike": 210, "delta_pts": 15.2, "volume": 900},
        {"strike": 220, "delta_pts": 2.1, "volume": 50},
    ]
    assert pick_nearest_delta(near15, DELTA_SELL_INDEX)["strike"] == 210
    assert pick_nearest_delta(near15, DELTA_BUY_INDEX)["strike"] == 220

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


def test_optional_exit_bundle():
    assert _optional_exit_bundle(None, None, None) is None
    assert _optional_exit_bundle("", "", "") is None
    try:
        _optional_exit_bundle("2026-09-10", None, 1.0)
        assert False, "partial exit should raise"
    except ValueError:
        pass
    exited, sell_x, buy_x = _optional_exit_bundle("2026-09-10", 6.5, 2.0)
    assert exited == date(2026, 9, 10)
    assert sell_x == 6.5
    assert buy_x == 2.0


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
    assert pnl_rupees(3, 125) == 375.0
    assert pnl_rupees(3, None) is None
    assert pnl_rupees(3, 0) is None
    assert pnl_rupees(None, 125) is None
    attach_rupee_pnl(done, 125)
    assert done["lot_size"] == 125
    assert done["combined_pnl_inr"] == 375.0
    assert done["combined_pnl"] == 3
    missing = {"combined_pnl": 3}
    attach_rupee_pnl(missing, None)
    assert missing["lot_size"] is None
    assert missing["combined_pnl_inr"] is None


def test_contract_mmm_yyyy_from_fut_symbol():
    assert format_contract_mmm_yyyy(date(2026, 9, 29)) == "SEP-2026"
    assert parse_fut_trading_symbol_expiry("BAJAJFINSV FUT 29 SEP 26") == date(2026, 9, 29)
    assert parse_fut_trading_symbol_expiry("BAJAJ-AUTO FUT 29 SEP 26") == date(2026, 9, 29)
    assert parse_fut_trading_symbol_expiry("") is None
    label = resolve_contract_mmm_yyyy("BAJAJFINSV", datetime(2026, 9, 7, 13, 15))
    assert label in (None, "SEP-2026")
    # Historical month missing from instruments → map armed_at (day ≤ 20 → same month)
    assert resolve_contract_mmm_yyyy("NIFTY", datetime(2026, 8, 19, 11, 15)) == "AUG-2026"
    # Roll window after day 20 → next month
    assert resolve_contract_mmm_yyyy("NIFTY", datetime(2026, 8, 26, 13, 15)) == "SEP-2026"


def test_row_public_includes_contract():
    row = _row_public({
        "id": 1,
        "symbol": "BAJAJFINSV",
        "status": "Completed",
        "side": "BEAR CALL",
        "contract_mmm_yyyy": "SEP-2026",
        "sell_cost": 10,
        "buy_cost": 4,
        "sell_exit_price": 6,
        "buy_exit_price": 3,
        "hard_stop_placed": False,
    })
    assert row["contract_mmm_yyyy"] == "SEP-2026"


def test_ema_closes_ready_and_index_fut_ohlc_fallback(monkeypatch):
    """Index OHLC falls back to currmth FUT when NSE_INDEX history is thin."""
    from backend.services import stock_option_signals as sos

    assert not sos._ema_closes_ready([1.0] * 99)
    assert sos._ema_closes_ready([1.0] * 100)

    calls: list[str] = []

    def fake_fetch(ik, now=None):
        calls.append(ik)
        if ik.startswith("NSE_INDEX"):
            return [1.0] * 10  # too thin for EMA100
        return [float(i) for i in range(120)]

    monkeypatch.setattr(sos, "_fetch_2h_closes", fake_fetch)
    monkeypatch.setattr(
        sos,
        "resolve_currmth_fut_instrument_key",
        lambda symbol, db: "NSE_FO|68407",
    )
    closes = sos._fetch_2h_closes_for_ema(
        "NIFTY",
        "NSE_INDEX|Nifty 50",
        db=object(),
    )
    assert calls == ["NSE_INDEX|Nifty 50", "NSE_FO|68407"]
    assert len(closes) == 120

    calls.clear()
    stock_closes = sos._fetch_2h_closes_for_ema(
        "RELIANCE",
        "NSE_EQ|RELIANCE",
        db=object(),
    )
    assert calls == ["NSE_EQ|RELIANCE"]
    assert len(stock_closes) == 120  # stock path: no FUT fallback call

