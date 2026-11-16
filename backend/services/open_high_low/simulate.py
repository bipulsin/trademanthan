"""Trade simulation: entry at third candle open, ATR-step trailing SL, EOD square-off.

Causal trail rule
-----------------
At each holding candle (from entry onward):

1. Check stop against the **prior** SL (gap-through at open, else wick touch).
2. If not stopped and this is the square-off candle, exit EOD at close.
3. Otherwise, if ATR is available and > 0, update the trail from **this**
   candle's favorable extreme for use on the **next** candle.

This avoids same-bar ambiguity: a candle that both extends favorably enough
to arm a trail step and also touches the prior SL is stopped on the prior SL.
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime
from typing import Any, Dict, Optional, Sequence

from backend.services.open_high_low.bars import Bar
from backend.services.open_high_low.config import (
    SQUARE_OFF_TIME,
    THIRD_CANDLE,
    OHLConfig,
)
from backend.services.open_high_low.signals import Setup, bars_from_entry

logger = logging.getLogger(__name__)


def _holding_minutes(entry: datetime, exit_: datetime) -> float:
    return max(0.0, (exit_ - entry).total_seconds() / 60.0)


def trail_target_sl(
    *,
    side: str,
    entry_price: float,
    atr: float,
    extreme: float,
    current_sl: float,
) -> float:
    """Compute the ratcheted SL from favorable extreme in ATR steps.

    ``steps = floor(favorable_move_from_entry / ATR)`` (integer ≥ 0).
    For steps ≥ N ≥ 1, target = entry ± (N-1)*ATR (bull +, bear −).
    Until steps ≥ 1 the caller should keep the initial first-candle SL;
    this helper returns ``current_sl`` unchanged when steps < 1.
    SL only tightens (bull: max, bear: min).
    """
    if atr <= 0:
        return current_sl
    if side == "BULLISH":
        fav = extreme - entry_price
        steps = max(0, int(math.floor(fav / atr)))
        if steps < 1:
            return current_sl
        target = entry_price + (steps - 1) * atr
        return max(current_sl, target)
    fav = entry_price - extreme
    steps = max(0, int(math.floor(fav / atr)))
    if steps < 1:
        return current_sl
    target = entry_price - (steps - 1) * atr
    return min(current_sl, target)


def simulate_trade(
    setup: Setup,
    bars_5m: Sequence[Bar],
    *,
    symbol: str,
    contract: str,
    expiry: date,
    lot_size: int,
    cfg: Optional[OHLConfig] = None,
    atr: Optional[float] = None,
) -> Dict[str, Any]:
    """Simulate one trade from a detected setup. Always returns a trade record.

    Initial SL: bullish → first.low, bearish → first.high.
    When ``atr`` is a positive daily ATR(14) as of the previous session close,
    SL ratchets in ATR steps from entry (see module docstring). Missing / zero
    ATR falls back to fixed initial SL only.
    Exit reasons: ``SL``, ``Trailing SL``, or ``EOD``.
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
    sl = float(initial_sl)

    atr_val: Optional[float] = None
    if atr is not None:
        try:
            atr_f = float(atr)
        except (TypeError, ValueError):
            atr_f = 0.0
        if atr_f > 0:
            atr_val = atr_f
        else:
            logger.info(
                "ohl atr missing/zero — fixed SL only %s %s atr=%s",
                symbol,
                setup.session.isoformat(),
                atr,
            )
    else:
        logger.debug(
            "ohl atr not provided — fixed SL only %s %s",
            symbol,
            setup.session.isoformat(),
        )

    path = bars_from_entry(bars_5m, setup.session, THIRD_CANDLE)
    if not path:
        path = [entry_bar]

    exit_time = entry_time
    exit_price = entry_price
    exit_reason = "EOD"
    final_sl = sl
    trail_moved = False
    extreme: Optional[float] = None
    mfe = 0.0
    mae = 0.0

    for bar in path:
        # 1) Hit check against prior SL (entry candle can hit). Gap → exit at open.
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
            exit_reason = "Trailing SL" if trail_moved else "SL"
            final_sl = sl
            break

        # 2) EOD square-off on the configured candle close (default 15:15).
        if bar.ts.time() == square_off:
            exit_time = bar.ts
            exit_price = bar.close - slip if setup.side == "BULLISH" else bar.close + slip
            exit_reason = "EOD"
            final_sl = sl
            break

        # 3) Update trail from this candle's extreme for the next candle.
        if atr_val is not None:
            if setup.side == "BULLISH":
                extreme = bar.high if extreme is None else max(extreme, bar.high)
            else:
                extreme = bar.low if extreme is None else min(extreme, bar.low)
            new_sl = trail_target_sl(
                side=setup.side,
                entry_price=entry_price,
                atr=atr_val,
                extreme=extreme,
                current_sl=sl,
            )
            if setup.side == "BULLISH":
                if new_sl > sl:
                    sl = new_sl
                    trail_moved = True
            else:
                if new_sl < sl:
                    sl = new_sl
                    trail_moved = True
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
        "atr": float(atr_val) if atr_val is not None else None,
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
