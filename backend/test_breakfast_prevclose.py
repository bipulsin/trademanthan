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


def test_prevclose_unknown_when_prev_missing():
    bar = _bar("2026-08-01T09:20:00+05:30", 100, 102, 99, 101)
    bias, pct = nifty_bias_from_bar_vs_prev_close(bar, None, missing="unknown")
    assert bias == "unknown"
    assert pct == 0.0


def test_select_prevclose_uses_db_prev_not_bar_open():
    from unittest.mock import patch

    from backend.services.breakfast_strategy.engine_prevclose import select_breakfast_picks_prevclose
    from backend.services.breakfast_strategy.universe import sector_index_key_for_label

    bank = sector_index_key_for_label("Nifty IT")
    session = __import__("datetime").date(2026, 9, 1)
    nifty_bar = _bar("2026-09-01T09:15:00+05:30", 100, 101, 99, 100.2)
    sector_bar = _bar("2026-09-01T09:15:00+05:30", 200, 201, 199, 201)
    with patch(
        "backend.services.breakfast_strategy.engine_prevclose.fo_eligible_sector_keys",
        return_value={bank},
    ):
        sel = select_breakfast_picks_prevclose(
            session,
            nifty_candles=[nifty_bar],
            sector_candles={bank: [sector_bar]},
            stock_candles_by_key={},
            stocks_by_sector={bank: [{"stock": "HDFCBANK"}]},
            fut_by_und={},
            eq_by_symbol={},
            nifty_bar=nifty_bar,
            sector_bar_overrides={bank: sector_bar},
            nifty_prev_close=200.0,
            sector_prev_closes={bank: 100.0},
            sectors_to_pick=1,
            stocks_per_sector=1,
        )
    assert sel is not None
    assert sel.nifty_bias == "negative"
    assert abs(sel.nifty_bias_pct - (100.2 - 200.0) / 200.0 * 100.0) < 1e-9
    assert sel.sector_picks
    assert abs(sel.sector_picks[0].sector_move_pct - (201.0 - 100.0) / 100.0 * 100.0) < 1e-9


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
