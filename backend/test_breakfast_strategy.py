"""Breakfast Strategy unit tests (no Upstox)."""
from datetime import date

from backend.services.breakfast_strategy.candles import (
    _synthetic_5m_bar,
    bar_move_pct,
    first_5m_bar,
)
from backend.services.breakfast_strategy.engine import _nifty_bias, _simulate_exit_long, _simulate_exit_short


def _bar(ts: str, o, h, l, c, v=1000):
    return {"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_flat_nifty_is_positive_bias():
    bar = _bar("2026-08-01T09:20:00+05:30", 100, 100, 99, 100)
    bias, pct = _nifty_bias(bar)
    assert bias == "positive"
    assert pct == 0.0


def test_long_tp_before_sl_same_candle():
    sd = date(2026, 8, 1)
    candles = [
        _bar("2026-08-01T09:25:00+05:30", 100, 102, 98, 97),
    ]
    entry = 100.0
    sl = 99.0
    tp = 101.0
    _t, px, kind, extreme = _simulate_exit_long(
        candles, sd, entry_price=entry, sl_price=sl, tp_price=tp, lot_size=100
    )
    assert kind == "target_hit"
    assert px == tp
    assert extreme == 102


def test_hindalco_style_anchor_tp_on_920_bar():
    """9:15 anchor close; 9:20 bar low should hit short TP before VWAP."""
    sd = date(2026, 8, 20)
    candles = [
        _bar("2026-08-20T09:15:00+05:30", 1049, 1053.4, 1043.75, 1044.0, v=151200),
        _bar("2026-08-20T09:20:00+05:30", 1043.65, 1043.65, 1025.3, 1025.3, v=478800),
        _bar("2026-08-20T09:25:00+05:30", 1026.05, 1030.84, 1025.15, 1030.0, v=182700),
    ]
    anchor = 1044.0
    entry = anchor * (1.0 - 0.0003)
    tp = anchor * (1.0 - 0.01)
    sl = anchor * (1.0 + 0.01)
    _t, px, kind, _ext = _simulate_exit_short(
        candles, sd, entry_price=entry, sl_price=sl, tp_price=tp, lot_size=700, monitor_from=(9, 20)
    )
    assert kind == "target_hit"
    assert abs(px - tp) < 0.01


    sd = date(2026, 8, 1)
    # Session bars from 9:15 for VWAP; monitor from 9:25
    candles = [
        _bar("2026-08-01T09:15:00+05:30", 100, 101, 99, 100, v=1000),
        _bar("2026-08-01T09:20:00+05:30", 100, 101, 99, 100, v=1000),
        _bar("2026-08-01T09:25:00+05:30", 100, 100.5, 98, 98.5, v=1000),
    ]
    _t, px, kind, _ext = _simulate_exit_long(
        candles, sd, entry_price=100.0, sl_price=90.0, tp_price=200.0, lot_size=100
    )
    assert kind == "vwap_breach"
    assert px == 98.5


def test_long_pnl_cap_exit():
    sd = date(2026, 8, 1)
    candles = [
        _bar("2026-08-01T09:15:00+05:30", 100, 101, 99, 100, v=1000),
        _bar("2026-08-01T09:20:00+05:30", 100, 101, 99, 100, v=1000),
        _bar("2026-08-01T09:25:00+05:30", 100, 151, 99, 100, v=1000),
    ]
    _t, px, kind, _ext = _simulate_exit_long(
        candles, sd, entry_price=100.0, sl_price=90.0, tp_price=200.0, lot_size=100, pnl_cap_enabled=True
    )
    assert kind == "pnl_cap"
    assert px == 150.0


def test_long_time_exit_at_1015():
    sd = date(2026, 8, 1)
    candles = [
        _bar("2026-08-01T09:25:00+05:30", 100, 100.5, 99.5, 100.2),
        _bar("2026-08-01T10:15:00+05:30", 100.2, 100.4, 99.8, 100.1),
    ]
    _t, px, kind, extreme = _simulate_exit_long(
        candles, sd, entry_price=100.0, sl_price=98.0, tp_price=105.0, lot_size=100
    )
    assert kind == "time_exit"
    assert px == 100.1
    assert extreme == 100.5


def test_long_tracks_max_high_before_sl():
    sd = date(2026, 8, 1)
    candles = [
        _bar("2026-08-01T09:15:00+05:30", 100, 100, 99, 100, v=1000),
        _bar("2026-08-01T09:20:00+05:30", 100, 100, 99, 100, v=1000),
        _bar("2026-08-01T09:25:00+05:30", 100, 103, 98, 98.5, v=1000),
    ]
    _t, px, kind, extreme = _simulate_exit_long(
        candles, sd, entry_price=100.0, sl_price=97.0, tp_price=105.0, lot_size=100
    )
    assert kind == "vwap_breach"
    assert px == 98.5
    assert extreme == 103


def test_first_5m_bar_prefers_0920():
    sd = date(2026, 8, 1)
    candles = [
        _bar("2026-08-01T09:15:00+05:30", 1, 2, 1, 2),
        _bar("2026-08-01T09:20:00+05:30", 10, 12, 9, 11),
    ]
    b = first_5m_bar(candles, sd)
    assert b is not None
    assert bar_move_pct(b) is not None


def test_synthetic_5m_bar_move_pct():
    sd = date(2026, 8, 28)
    b = _synthetic_5m_bar(sd, 24117.9, 24127.0)
    pct = bar_move_pct(b)
    assert pct is not None
    assert pct > 0


def test_resolve_stock_instrument_uses_nearest_fut_when_front_month_too_far():
    from datetime import date as d

    from backend.services.breakfast_strategy.universe import (
        display_symbol_for,
        format_instrument_label,
        resolve_stock_instrument,
    )

    session = d(2026, 7, 29)
    fut_by_und = {
        "TCS": [
            {
                "trading_symbol": "TCS FUT 29 SEP 26",
                "instrument_key": "NSE_FO|68797",
                "expiry": 1_790_620_200_000,
                "lot_size": 175,
            }
        ]
    }
    eq_by_symbol = {"TCS": {"instrument_key": "NSE_EQ|TCS", "lot_size": 1}}
    ref = resolve_stock_instrument("TCS", session, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
    assert ref is not None
    assert ref.source == "FUT"
    assert ref.instrument_key == "NSE_FO|68797"
    assert format_instrument_label("TCS", ref) == "SEP26 FUT"
    assert display_symbol_for("TCS", ref) == "TCS SEP26 FUT"

