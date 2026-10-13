"""Today's trade qty and PnL: long, short, and missing exit stays blank."""
from backend.services.daily_futures_service import (
    closed_trade_pnl_rupees,
    closed_trade_win_loss,
    resolve_closed_trade_qty,
)


def test_long_and_short_pnl_use_qty():
    assert closed_trade_pnl_rupees("LONG", 100, 110, 250) == 2500.0
    assert closed_trade_pnl_rupees("SHORT", 100, 110, 250) == -2500.0
    assert closed_trade_pnl_rupees("LONG", 44385, 44270, 100) == -11500.0
    assert closed_trade_pnl_rupees("SHORT", 44385, 44270, 100) == 11500.0


def test_missing_exit_stays_blank_and_is_not_zero():
    assert closed_trade_pnl_rupees("LONG", 100, None, 250) is None
    assert closed_trade_pnl_rupees("SHORT", 44385, None, 100) is None
    assert closed_trade_pnl_rupees("SHORT", 44385, "", 100) is None
    assert closed_trade_pnl_rupees("LONG", None, 110, 250) is None
    assert closed_trade_pnl_rupees("SHORT", 44385, 44270, None) is None
    assert closed_trade_win_loss(None, None) is None
    assert closed_trade_win_loss(11500, None) is None


def test_win_loss_follows_pnl_sign():
    assert closed_trade_win_loss(11500, 44270) == "Win"
    assert closed_trade_win_loss(-10, 110) == "Loss"
    assert closed_trade_win_loss(0, 100) == "Flat"


def test_qty_comes_from_lot_master_or_stays_blank():
    assert resolve_closed_trade_qty(None, "NSE_FO|EXAMPLE", lot_fn=lambda _k: 100) == 100
    assert resolve_closed_trade_qty(175, "NSE_FO|TVSMOTOR", lot_fn=lambda _k: 1) == 175
    assert resolve_closed_trade_qty(None, "NSE_FO|UNKNOWN", lot_fn=lambda _k: None) is None
    assert resolve_closed_trade_qty(None, "", lot_fn=lambda _k: 100) is None
    assert resolve_closed_trade_qty(None, "NSE_FO|UNKNOWN", lot_fn=lambda _k: 0) is None
