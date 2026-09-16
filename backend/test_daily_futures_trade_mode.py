"""Premium Futures PAPER|LIVE trade mode helpers (no DB)."""
from backend.services.daily_futures_service import (
    TRADE_MODE_LIVE,
    TRADE_MODE_PAPER,
    normalize_trade_mode,
)


def test_normalize_trade_mode_defaults_paper():
    assert normalize_trade_mode(None) == TRADE_MODE_PAPER
    assert normalize_trade_mode("") == TRADE_MODE_PAPER
    assert normalize_trade_mode("paper") == TRADE_MODE_PAPER
    assert normalize_trade_mode("unknown") == TRADE_MODE_PAPER
    assert normalize_trade_mode("LIVE") == TRADE_MODE_LIVE
    assert normalize_trade_mode(" live ") == TRADE_MODE_LIVE
