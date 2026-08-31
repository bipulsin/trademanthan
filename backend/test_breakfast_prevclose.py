"""Prev-close ranking helpers — must not change engine.nifty_bias_from_bar."""
import inspect

from backend.services.breakfast_strategy.candles import bar_move_pct
from backend.services.breakfast_strategy.engine import nifty_bias_from_bar
from backend.services.breakfast_strategy.engine_prevclose import (
    nifty_bias_from_bar_vs_prev_close,
    rank_sectors_vs_prev_close,
)
from backend.services.breakfast_strategy.universe import rank_sectors, sector_index_key_for_label


def _bar(ts: str, o, h, l, c, v=1000):
    return {"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_prevclose_bias_uses_close_vs_prev_not_open():
    bar = _bar("2026-08-01T09:20:00+05:30", 100, 102, 99, 101)
    open_pct = bar_move_pct(bar)
    assert open_pct == 1.0
    bias, pct = nifty_bias_from_bar_vs_prev_close(bar, 50.0)
    assert bias == "positive"
    assert abs(pct - (101.0 - 50.0) / 50.0 * 100.0) < 1e-9
    assert pct != open_pct


def test_prevclose_negative_bias_when_close_below_prev():
    bar = _bar("2026-08-01T09:20:00+05:30", 100, 101, 99, 100.5)
    bias, pct = nifty_bias_from_bar_vs_prev_close(bar, 110.0)
    assert bias == "negative"
    assert pct < 0


def test_engine_nifty_bias_from_bar_still_bar_move_pct():
    src = inspect.getsource(nifty_bias_from_bar)
    assert "bar_move_pct" in src
    assert "prev_close" not in src
    bar = _bar("2026-08-01T09:20:00+05:30", 100, 102, 99, 101)
    bias, pct = nifty_bias_from_bar(bar)
    assert bias == "positive"
    assert pct == 1.0
    _b2, pct2 = nifty_bias_from_bar_vs_prev_close(bar, 80.0)
    assert abs(pct2 - (101.0 - 80.0) / 80.0 * 100.0) < 1e-9
    assert pct != pct2


def test_rank_sectors_live_helper_unchanged():
    src = inspect.getsource(rank_sectors)
    assert "bar_move_pct" in src
    serv = sector_index_key_for_label("Nifty Services")
    tel = sector_index_key_for_label("Nifty Telecom")
    bars = {
        serv: _bar("2026-08-01T09:20:00+05:30", 100, 101, 99, 100.5),
        tel: _bar("2026-08-01T09:20:00+05:30", 100, 100.5, 99, 99.5),
    }
    prev = {serv: 200.0, tel: 50.0}
    live = rank_sectors(bars, eligible_keys={serv, tel}, descending=True)
    exp = rank_sectors_vs_prev_close(bars, prev, eligible_keys={serv, tel}, descending=True)
    assert live[0][0] == serv  # (100.5-100)/100 = +0.5% vs (99.5-100)/100 = -0.5%
    assert exp[0][0] == tel  # (99.5-50)/50 >> (100.5-200)/200
