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
    build_selling_report,
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
    next_stock_ema_action,
    parse_chartink_symbols,
    parse_fut_trading_symbol_expiry,
    pick_nearest_delta,
    resolve_contract_mmm_yyyy,
    selling_day_key,
    should_insert_new_signal,
    side_from_williamsr,
    spread_lines,
    williams_r_at,
)

IST = pytz.timezone("Asia/Kolkata")


def test_williamsr_gate_bounds_excluded():
    assert side_from_williamsr(-0.9) == SIDE_BEAR
    assert side_from_williamsr(0) == SIDE_BEAR
    assert side_from_williamsr(-1) is None
    assert side_from_williamsr(-1.0) is None
    assert side_from_williamsr(-50) is None
    assert side_from_williamsr(-99) is None
    assert side_from_williamsr(-99.1) == SIDE_BULL
    assert side_from_williamsr(-100) == SIDE_BULL
    assert side_from_williamsr("nope") is None
    # Former -3/-97 gates no longer qualify
    assert side_from_williamsr(-2.9) is None
    assert side_from_williamsr(-97.1) is None


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

    bear = _bars(WR_PERIOD, 109.9, start)
    bear.append({"end": later, "high": 110.0, "low": 90.0, "close": 90.0})
    wr_bear = williams_r_at(bear, asof)
    assert wr_bear == (110.0 - 109.9) / (110.0 - 90.0) * -100.0
    assert side_from_williamsr(wr_bear) == SIDE_BEAR
    assert williams_r_at(bear, asof) != williams_r_at(bear, later)

    bull = _bars(WR_PERIOD, 90.05, start)
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


def test_ignore_if_open_lifecycle_allow_completed_rejected():
    assert should_insert_new_signal([]) is True
    assert should_insert_new_signal([STATUS_RADAR]) is False
    assert should_insert_new_signal([STATUS_ACTIVE]) is False
    assert should_insert_new_signal([STATUS_EXECUTED, STATUS_RADAR]) is False
    # Executed is open lifecycle — block until Completed / Trade Report
    assert should_insert_new_signal([STATUS_EXECUTED]) is False
    assert should_insert_new_signal([STATUS_EXECUTED, STATUS_EXECUTED]) is False
    assert should_insert_new_signal([STATUS_REJECTED]) is True
    assert should_insert_new_signal([STATUS_COMPLETED]) is True
    assert should_insert_new_signal([STATUS_COMPLETED, STATUS_REJECTED]) is True
    # Index re-seed may ignore Executed
    assert should_insert_new_signal([STATUS_EXECUTED], block_executed=False) is True
    assert should_insert_new_signal([STATUS_RADAR], block_executed=False) is False


def test_should_remove_radar_eod_gates():
    from backend.services.stock_option_signals import should_remove_radar_eod

    assert should_remove_radar_eod(SIDE_BULL, -4.9) is True
    assert should_remove_radar_eod(SIDE_BULL, -5.0) is False
    assert should_remove_radar_eod(SIDE_BULL, -50) is False
    assert should_remove_radar_eod(SIDE_BEAR, -95.1) is True
    assert should_remove_radar_eod(SIDE_BEAR, -95.0) is False
    assert should_remove_radar_eod(SIDE_BEAR, -50) is False
    assert should_remove_radar_eod(SIDE_BEAR, None) is False
    assert should_remove_radar_eod(None, -0.5) is False


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


def test_ema_arm_requires_fresh_cross_matching_side():
    assert ema_condition_holds(SIDE_BEAR, 90, 100, 110) is True
    assert ema_condition_holds(SIDE_BEAR, 100, 100, 90) is False
    assert ema_condition_holds(SIDE_BULL, 120, 100, 90) is True
    assert ema_condition_holds(SIDE_BULL, 90, 100, 80) is False

    # Fresh BEAR cross + hold → arm
    assert (
        next_stock_ema_action(
            STATUS_RADAR, SIDE_BEAR, 105, 100, 110, 90, 100, 110, False
        )
        == "arm"
    )
    # Hold already true on prev (no fresh cross) → hold
    assert (
        next_stock_ema_action(
            STATUS_RADAR, SIDE_BEAR, 90, 100, 110, 85, 100, 110, False
        )
        == "hold"
    )
    # Hold alone without prev (compat wrapper) never arms
    assert next_ema_action(STATUS_RADAR, SIDE_BEAR, 90, 100, 110, False) == "hold"
    assert next_ema_action(STATUS_RADAR, SIDE_BEAR, None, 100, 110, False) == "hold"
    # Cross opposite to WR side → no arm / no flip
    assert (
        next_stock_ema_action(
            STATUS_RADAR, SIDE_BULL, 105, 100, 110, 90, 100, 110, False
        )
        == "hold"
    )
    # Active hold fail → demote
    assert (
        next_stock_ema_action(
            STATUS_ACTIVE, SIDE_BEAR, 90, 100, 110, 120, 100, 110, False
        )
        == "demote"
    )
    assert (
        next_stock_ema_action(
            STATUS_ACTIVE, SIDE_BEAR, 90, 100, 110, 120, 100, 110, True
        )
        == "hold"
    )
    assert (
        next_stock_ema_action(
            STATUS_ACTIVE, SIDE_BULL, 120, 100, 90, 80, 100, 90, False
        )
        == "demote"
    )
    assert (
        next_stock_ema_action(
            STATUS_ACTIVE, SIDE_BULL, 95, 100, 90, 120, 100, 90, False
        )
        == "hold"
    )


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
    # Stock path also requires fresh cross (not mere hold)
    assert (
        next_stock_ema_action(
            STATUS_RADAR, SIDE_BULL, 120, 100, 90, 130, 100, 90, False
        )
        == "hold"
    )
    assert (
        next_stock_ema_action(
            STATUS_RADAR, SIDE_BULL, 95, 100, 90, 120, 100, 90, False
        )
        == "arm"
    )


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
    assert (
        next_stock_ema_action(
            STATUS_RADAR, SIDE_BEAR, 90, 100, 110, 85, 100, 110, False
        )
        == "hold"
    )
    assert (
        next_stock_ema_action(
            STATUS_RADAR, SIDE_BEAR, 105, 100, 110, 90, 100, 110, False
        )
        == "arm"
    )


def test_index_active_demotes_when_hold_fails():
    """After arm, indices demote to Radar when hold fails (not reverse-cross)."""
    action, side = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BULL, 95, 100, 90, 120, 100, 90, False
    )
    assert action == "hold" and side is None
    action, side = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BULL, 120, 100, 90, 80, 100, 90, False
    )
    assert action == "demote"
    action, side = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BEAR, 105, 100, 110, 90, 100, 110, False
    )
    assert action == "hold"
    action, side = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BEAR, 90, 100, 110, 120, 100, 110, False
    )
    assert action == "demote"
    # Trade submitted → never demote
    action, _ = next_index_ema_action(
        STATUS_ACTIVE, SIDE_BULL, 120, 100, 90, 80, 100, 90, True
    )
    assert action == "hold"
    # Incomplete EMAs → hold
    action, _ = next_index_ema_action(
        STATUS_RADAR, None, None, 100, 90, 120, 100, 90, False
    )
    assert action == "hold"


def test_row_public_exposes_arm_caution():
    pub = _row_public({"symbol": "PIIND", "status": "Executed", "arm_caution": True})
    assert pub["arm_caution"] is True
    pub2 = _row_public({"symbol": "TCS", "status": "Radar"})
    assert pub2["arm_caution"] is False


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


def test_session_includes_1515_final_bar():
    from backend.services.stock_option_signals import (
        completed_2h_bars,
        session_2h_bucket_end,
        session_2h_bucket_start,
    )

    ts = IST.localize(datetime(2026, 9, 11, 15, 15))
    bucket = session_2h_bucket_start(ts)
    assert bucket is not None
    assert (bucket.hour, bucket.minute) == (15, 15)
    assert session_2h_bucket_end(bucket) == IST.localize(datetime(2026, 9, 11, 15, 30))

    candles = [
        {"timestamp": "2026-09-11T13:15:00+05:30", "open": 1, "high": 2, "low": 1, "close": 100.0, "volume": 1},
        {"timestamp": "2026-09-11T15:15:00+05:30", "open": 1, "high": 2, "low": 1, "close": 105.0, "volume": 1},
    ]
    before_close = completed_2h_bars(candles, now=IST.localize(datetime(2026, 9, 11, 15, 15)))
    assert [b["close"] for b in before_close] == [100.0]
    after_close = completed_2h_bars(candles, now=IST.localize(datetime(2026, 9, 11, 15, 30)))
    assert [b["close"] for b in after_close] == [100.0, 105.0]


def test_fetch_2h_closes_overlays_hours1_when_hours2_stale(monkeypatch):
    """hours/2 with ≥100 bars must still pick up today's hours/1 buckets."""
    from backend.services import stock_option_signals as sos

    class _FakeUpstox:
        def __init__(self, *a, **k):
            pass

    monkeypatch.setattr("backend.services.upstox_service.UpstoxService", _FakeUpstox)
    sos.clear_tick_wr_bars_cache()

    h2 = []
    d0 = date(2026, 7, 1)
    for i in range(120):
        day = d0 + timedelta(days=i // 4)
        hh = [9, 11, 13, 15][i % 4]
        h2.append(
            {
                "timestamp": f"{day.isoformat()}T{hh:02d}:15:00+05:30",
                "open": 1,
                "high": 2,
                "low": 1,
                "close": float(1000 + i),
                "volume": 1,
            }
        )

    calls = []

    def fake_retry(u, ik, *, interval, days_back, range_end_date=None):
        calls.append((interval, days_back))
        if interval == "hours/2":
            return h2
        if interval == "hours/1":
            return [
                {"timestamp": "2026-09-15T09:15:00+05:30", "open": 1, "high": 2, "low": 1, "close": 4893.0, "volume": 1},
                {"timestamp": "2026-09-15T10:15:00+05:30", "open": 1, "high": 2, "low": 1, "close": 4893.0, "volume": 1},
                {"timestamp": "2026-09-15T11:15:00+05:30", "open": 1, "high": 2, "low": 1, "close": 4886.0, "volume": 1},
                {"timestamp": "2026-09-15T12:15:00+05:30", "open": 1, "high": 2, "low": 1, "close": 4891.5, "volume": 1},
            ]
        return []

    monkeypatch.setattr(sos, "_upstox_candles_with_retry", fake_retry)
    monkeypatch.setattr(sos, "_fetch_2h_closes_from_10m_fallback", lambda *a, **k: [])
    now = IST.localize(datetime(2026, 9, 15, 13, 15))
    closes = sos._fetch_2h_closes("NSE_EQ|TITAN", now=now)
    assert len(closes) >= 100
    assert closes[-1] == 4891.5
    assert closes[-2] == 4893.0
    # Healthy path: hours/2 + short hours/1 overlay only (no full 60d / 15m).
    assert ("hours/2", 120) in calls
    assert any(iv == "hours/1" and db <= sos.EMA_OVERLAY_DAYS_WHEN_READY for iv, db in calls)
    assert not any(iv == "minutes/15" for iv, _ in calls)


def test_normalize_2h_bars_newest_first_to_ascending():
    """Upstox-style newest-first input must not reverse EMA closes."""
    from backend.services.stock_option_signals import (
        _closes_from_2h_bar_dicts,
        _normalize_2h_bars_chronological,
        ema_plausible_vs_last_close,
        ema_snapshot,
    )

    asc = _normalize_2h_bars_chronological(
        [
            {
                "timestamp": (
                    f"2026-08-{(i // 4) + 1:02d}T{[9, 11, 13, 15][i % 4]:02d}:15:00+05:30"
                ),
                "close": 2200.0 + i * 0.5,
            }
            for i in range(110)
        ]
    )
    desc = list(reversed(asc))
    closes_fixed = _closes_from_2h_bar_dicts(desc)
    assert closes_fixed[0] == float(asc[0]["close"])
    assert closes_fixed[-1] == float(asc[-1]["close"])
    snap = ema_snapshot(closes_fixed)
    assert snap["ema9"] is not None
    assert ema_plausible_vs_last_close(snap["ema9"], closes_fixed[-1])
    # Reversed closes without normalize sit near the series tip (old highs).
    snap_bad = ema_snapshot([float(b["close"]) for b in desc])
    assert snap_bad["ema9"] is not None
    assert abs(snap_bad["ema9"] - closes_fixed[-1]) > abs(snap["ema9"] - closes_fixed[-1])


def test_ema_plausible_vs_last_close():
    from backend.services.stock_option_signals import ema_plausible_vs_last_close

    assert ema_plausible_vs_last_close(2278.0, 2282.0) is True
    assert ema_plausible_vs_last_close(2692.0, 2282.0) is False
    assert ema_plausible_vs_last_close(None, 2282.0) is False


def test_merge_2h_prefers_overlay_on_conflict():
    from backend.services.stock_option_signals import _merge_2h_bar_dicts

    primary = [
        {"timestamp": "2026-09-15T09:15:00+05:30", "close": 100.0},
        {"timestamp": "2026-09-15T11:15:00+05:30", "close": 101.0},
    ]
    overlay = [
        {"timestamp": "2026-09-15T11:15:00+05:30", "close": 999.0},
        {"timestamp": "2026-09-15T13:15:00+05:30", "close": 102.0},
    ]
    merged = _merge_2h_bar_dicts(primary, overlay)
    assert [b["close"] for b in merged] == [100.0, 999.0, 102.0]


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
    # Leg-wise: buy=(LTP−entry), sell=(entry−LTP); sum == credit − close cost.
    assert combined_pnl(10, 4, 6, 3) == (3 - 4) + (10 - 6)
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


def test_selling_report_groups_by_exit_date_with_cumulative():
    assert selling_day_key({"exit_date": "2026-09-16 14:30:00", "date_traded": "2026-09-10"}) == "2026-09-16"
    assert selling_day_key({"date_traded": "2026-09-14"}) == "2026-09-14"
    out = build_selling_report(
        [
            {"exit_date": "2026-09-15 14:00:00", "combined_pnl_inr": 1000, "symbol": "A"},
            {"exit_date": "2026-09-16 11:00:00", "combined_pnl_inr": 500, "symbol": "B"},
            {"exit_date": "2026-09-16 15:00:00", "combined_pnl_inr": -200, "symbol": "C"},
            {"date_traded": "2026-09-14", "combined_pnl_inr": 300, "symbol": "D"},
        ]
    )
    assert out["success"] is True
    assert out["summary"]["total_days"] == 3
    assert out["summary"]["total_trades"] == 4
    assert out["summary"]["overall_pnl"] == 1600
    assert [d["date"] for d in out["data"]] == ["2026-09-16", "2026-09-15", "2026-09-14"]
    assert out["data"][0]["total_pnl"] == 300
    assert out["data"][0]["cumulative_pnl"] == 1600
    assert out["data"][0]["total_trades"] == 2
    assert out["data"][1]["total_pnl"] == 1000
    assert out["data"][1]["cumulative_pnl"] == 1300
    assert out["data"][2]["total_pnl"] == 300
    assert out["data"][2]["cumulative_pnl"] == 300


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
    assert row["ema_stale"] is False
    assert row["ema_fetch_ok"] is None


def test_row_public_hides_stale_emas():
    row = _row_public({
        "id": 2,
        "symbol": "RELIANCE",
        "status": "Radar",
        "ema9": 100.5,
        "ema30": 99.0,
        "ema100": 98.0,
        "ema_fetch_ok": False,
        "ema_updated_at": datetime(2026, 9, 11, 11, 15),
        "hard_stop_placed": False,
    })
    assert row["ema_stale"] is True
    assert row["ema_fetch_ok"] is False
    assert row["ema9"] is None
    assert row["ema30"] is None
    assert row["ema100"] is None
    assert row["ema_updated_at"] == "2026-09-11 11:15:00"


def test_closes_from_5m_via_10m_aggregates(monkeypatch):
    from backend.services import stock_option_signals as sos

    # Two hours of 5m bars inside 09:15–11:15 session bucket.
    base = IST.localize(datetime(2026, 9, 11, 9, 15))
    candles = []
    for i in range(24):
        ts = base + timedelta(minutes=5 * i)
        candles.append({
            "timestamp": ts.isoformat(),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0 + i * 0.1,
            "volume": 10,
        })
    # Force completed bucket relative to after 11:15.
    now = IST.localize(datetime(2026, 9, 11, 11, 20))
    closes = sos._closes_from_5m_via_10m(candles, now=now)
    assert len(closes) >= 1
    assert closes[-1] == candles[-1]["close"]


def test_fetch_2h_closes_tries_10m_when_hours1_only_mid_length(monkeypatch):
    """Mid-length hours/1 must not skip Kavach 10m (need EMA100 history)."""
    from backend.services import stock_option_signals as sos

    class _FakeUpstox:
        def __init__(self, *a, **k):
            pass

    monkeypatch.setattr(
        "backend.services.upstox_service.UpstoxService",
        _FakeUpstox,
    )
    monkeypatch.setattr(
        sos,
        "_upstox_candles_with_retry",
        lambda *a, **k: [],
    )
    monkeypatch.setattr(sos, "completed_2h_bars", lambda candles, now=None: [])
    monkeypatch.setattr(
        sos,
        "aggregate_intraday_to_2h",
        lambda candles, now=None: [{"close": float(i)} for i in range(40)],
    )
    called = {"n": 0}

    def fake_10m(ikey, now=None):
        called["n"] += 1
        return [float(i) for i in range(120)]

    monkeypatch.setattr(sos, "_fetch_2h_closes_from_10m_fallback", fake_10m)
    out = sos._fetch_2h_closes("NSE_EQ|X", now=IST.localize(datetime(2026, 9, 11, 15, 45)))
    assert called["n"] == 1
    assert len(out) == 120


def test_mark_ema_fetch_failed_preserves_prior_emas(monkeypatch):
    from backend.services import stock_option_signals as sos

    executed: list[str] = []

    class _FakeResult:
        pass

    class _FakeDb:
        def execute(self, stmt, params=None):
            executed.append(str(stmt))
            return _FakeResult()

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(sos, "SessionLocal", lambda: _FakeDb())
    sos._mark_ema_fetch_failed(
        42,
        datetime(2026, 9, 11, 15, 15),
        prior_ema9=100.0,
        prior_ema30=99.0,
        prior_ema100=98.0,
    )
    assert len(executed) == 1
    sql = executed[0].lower()
    assert "ema_fetch_ok" in sql
    assert "false" in sql
    assert "ema9" not in sql

    executed.clear()
    sos._mark_ema_fetch_failed(43, datetime(2026, 9, 11, 15, 15))
    assert len(executed) == 1
    sql = executed[0].lower()
    assert "ema_fetch_ok" in sql
    assert "ema9" in sql


def test_schedule_ema_fetch_retry_adds_date_job(monkeypatch):
    from backend.services import stock_option_scheduler as sch

    added = {}

    class _FakeSched:
        def add_job(self, fn, trigger, **kwargs):
            added["fn"] = fn
            added["trigger"] = trigger
            added["kwargs"] = kwargs

    monkeypatch.setattr(sch, "_scheduler", _FakeSched())
    monkeypatch.setattr(sch, "_within_ema_retry_window", lambda now=None: True)
    sch._ema_retry_chain = 0
    sch._schedule_ema_fetch_retry()
    assert added["fn"] is sch._retry_tick
    assert added["kwargs"]["id"] == sch.EMA_RETRY_JOB_ID
    assert added["kwargs"]["replace_existing"] is True


def test_tick_schedules_retry_when_fetch_failed(monkeypatch):
    from backend.services import stock_option_scheduler as sch

    scheduled = {"n": 0}
    monkeypatch.setattr(sch, "should_skip_scheduled_market_jobs_ist", lambda: False)
    monkeypatch.setattr(sch, "run_wr_radar_scan", lambda: {"ok": True, "inserted": 0})
    monkeypatch.setattr(sch, "run_ema_tick", lambda: {"ok": True, "ema_fetch_failed": 3})
    monkeypatch.setattr(sch, "_schedule_ema_fetch_retry", lambda: scheduled.__setitem__("n", scheduled["n"] + 1))
    sch._tick()
    assert scheduled["n"] == 1

    monkeypatch.setattr(sch, "run_ema_tick", lambda: {"ok": True, "ema_fetch_failed": 0})
    sch._tick()
    assert scheduled["n"] == 1


def test_post_market_runs_ema_then_eod_cleanup(monkeypatch):
    from backend.services import stock_option_scheduler as sch

    order = []
    monkeypatch.setattr(sch, "should_skip_scheduled_market_jobs_ist", lambda: False)
    monkeypatch.setattr(
        sch,
        "run_ema_tick",
        lambda: order.append("ema") or {"ok": True, "ema_fetch_failed": 0},
    )
    monkeypatch.setattr(
        sch,
        "run_radar_eod_cleanup",
        lambda: order.append("eod") or {"ok": True, "removed": 0},
    )
    sch._post_market_tick()
    assert order == ["ema", "eod"]


def test_webhook_insert_disabled_returns_zero(monkeypatch):
    """ChartInk path logs only — no Radar inserts."""
    from backend.services import stock_option_signals as sos

    class _FakeDb:
        def execute(self, *a, **k):
            return None

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(sos, "ensure_stock_option_tables", lambda: None)
    monkeypatch.setattr(sos, "SessionLocal", lambda: _FakeDb())
    out = sos.insert_webhook_and_signals(
        received_at=datetime(2026, 9, 15, 11, 15),
        source_ip="127.0.0.1",
        parsed={"stocks": "RELIANCE, TCS", "scan_name": "HA-stock option"},
        raw_payload={"stocks": "RELIANCE, TCS"},
    )
    assert out["inserted"] == 0
    assert out["chartink_inserts"] is False
    assert out["candidates"] == 2
    assert out["ignored"] == 2


def test_side_from_williamsr_still_used_by_scan_gates():
    """Scan insert gates reuse side_from_williamsr (> -1 / < -99)."""
    assert side_from_williamsr(-0.5) == SIDE_BEAR
    assert side_from_williamsr(-99.5) == SIDE_BULL
    assert side_from_williamsr(-50) is None


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

