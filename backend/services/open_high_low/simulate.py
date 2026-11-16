"""Trade simulation: entry at third candle open, fixed first-candle SL, EOD square-off."""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Dict, Optional, Sequence

from backend.services.open_high_low.bars import Bar
from backend.services.open_high_low.config import (
    SQUARE_OFF_TIME,
    THIRD_CANDLE,
    OHLConfig,
)
from backend.services.open_high_low.signals import Setup, bars_from_entry


def _holding_minutes(entry: datetime, exit_: datetime) -> float:
    return max(0.0, (exit_ - entry).total_seconds() / 60.0)


def simulate_trade(
    setup: Setup,
    bars_5m: Sequence[Bar],
    *,
    symbol: str,
    contract: str,
    expiry: date,
    lot_size: int,
    cfg: Optional[OHLConfig] = None,
) -> Dict[str, Any]:
    """Simulate one trade from a detected setup. Always returns a trade record.

    SL is fixed at the first (09:15) candle extreme for the whole trade:
    bullish → first.low, bearish → first.high. Never ratchets.
    Exit reasons are only ``SL`` or ``EOD``.
    """
    cfg = cfg or OHLConfig()
    square_off = cfg.square_off or SQUARE_OFF_TIME
    slip = float(cfg.slippage_points or 0.0)
    brokerage = float(cfg.brokerage_per_lot or 0.0)

    entry_bar = setup.third
    entry_time = entry_bar.ts
    if setup.side == "BULLISH":
        entry_price = entry_bar.open + slip
        initial_sl = setup.first.low
    else:
        entry_price = entry_bar.open - slip
        initial_sl = setup.first.high
    sl = initial_sl

    path = bars_from_entry(bars_5m, setup.session, THIRD_CANDLE)
    if not path:
        path = [entry_bar]

    exit_time = entry_time
    exit_price = entry_price
    exit_reason = "EOD"
    final_sl = sl
    mfe = 0.0
    mae = 0.0

    for bar in path:
        # Fixed SL check (entry candle can hit SL). Never ratchet.
        hit = False
        if setup.side == "BULLISH":
            if bar.open <= sl:
                hit = True
                exit_price = bar.open - slip
            elif bar.low <= sl:
                hit = True
                exit_price = sl - slip
        else:
            if bar.open >= sl:
                hit = True
                exit_price = bar.open + slip
            elif bar.high >= sl:
                hit = True
                exit_price = sl + slip

        # MFE / MAE vs entry (favorable / adverse excursion in points).
        if setup.side == "BULLISH":
            mfe = max(mfe, bar.high - entry_price)
            mae = max(mae, entry_price - bar.low)
        else:
            mfe = max(mfe, entry_price - bar.low)
            mae = max(mae, bar.high - entry_price)

        if hit:
            exit_time = bar.ts
            exit_reason = "SL"
            final_sl = sl
            break

        # EOD square-off on the configured candle close (default 15:15).
        if bar.ts.time() == square_off:
            exit_time = bar.ts
            exit_price = bar.close - slip if setup.side == "BULLISH" else bar.close + slip
            exit_reason = "EOD"
            final_sl = sl
            break
    else:
        # No square-off candle found — exit at last bar close.
        last = path[-1]
        exit_time = last.ts
        exit_price = last.close - slip if setup.side == "BULLISH" else last.close + slip
        exit_reason = "EOD"
        final_sl = sl

    if setup.side == "BULLISH":
        points = exit_price - entry_price
    else:
        points = entry_price - exit_price

    risk = abs(entry_price - initial_sl)
    r_mult = (points / risk) if risk > 1e-9 else 0.0
    pct = (points / entry_price * 100.0) if entry_price else 0.0
    rupee = points * lot_size - brokerage

    return {
        "date": setup.session.isoformat(),
        "symbol": symbol,
        "contract": contract,
        "expiry": expiry.isoformat() if isinstance(expiry, date) else str(expiry),
        "side": setup.side,
        "lot_size": int(lot_size),
        "prev_day_high": float(setup.prev_high),
        "prev_day_low": float(setup.prev_low),
        "first_open": float(setup.first.open),
        "first_high": float(setup.first.high),
        "first_low": float(setup.first.low),
        "first_close": float(setup.first.close),
        "first_vwap": float(setup.vwap),
        "second_close": float(setup.second.close),
        "entry_time": entry_time.isoformat(),
        "entry_price": float(entry_price),
        "initial_sl": float(initial_sl),
        "exit_time": exit_time.isoformat(),
        "exit_price": float(exit_price),
        "exit_reason": exit_reason,
        "final_sl": float(final_sl),
        "points_pnl": float(points),
        "pct_return": float(pct),
        "rupee_pnl": float(rupee),
        "r_multiple": float(r_mult),
        "mfe": float(mfe),
        "mae": float(mae),
        "holding_minutes": float(_holding_minutes(entry_time, exit_time)),
        "open_eq_extreme": setup.open_eq_extreme,
    }
