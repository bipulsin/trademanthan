"""At-expiry payoff for a multi-leg option trade.

Same formulas as frontend/public/multiLegPayoff.js payoffAt.

Per leg, at a hypothetical underlying spot S:

    CE intrinsic = max(0, S - strike)
    PE intrinsic = max(0, strike - S)
    Open leg (exit price and exit time are not both set):
        leg P&L = (intrinsic - entry) * lot_size * (+1 BUY, -1 SELL)
    Exited leg (both exit price and exit time set) is flat realized P&L:
        leg P&L = (exit_price - entry) * lot_size * (+1 BUY, -1 SELL)

Trade P&L is the sum of the legs. The spot window starts at the outer strikes
extended by 12.5% (inside the requested 10–15% buffer) and is widened so the
current underlying spot sits inside it. Breakevens are zero crossings of that
curve. Max profit / max loss are the extremes of the curve; a tail that is
still rising or falling (nonzero slope outside every strike) is Unlimited.
Net credit = sum(entry * lot * (+1 SELL, -1 BUY)). Est. margin and POP are
not computed here.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence

PAD = 0.125
SPOT_EDGE_PAD = 0.02
SAMPLE_COUNT = 201
SLOPE_EPS = 1e-6
ZERO_PNL = 1e-4


def _as_float(raw: Any) -> Optional[float]:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _direction(side: Any) -> Optional[int]:
    token = str(side or "").strip().upper()
    if token == "BUY":
        return 1
    if token == "SELL":
        return -1
    return None


def _option_type(raw: Any) -> Optional[str]:
    token = str(raw or "").strip().upper()
    if token in ("CE", "PE"):
        return token
    return None


def _exited(raw: Dict[str, Any]) -> bool:
    """True only when both the exit premium and the exit time are filled."""
    price = _as_float(raw.get("exit_price"))
    stamp = raw.get("exit_time")
    if price is None or stamp is None:
        return False
    return str(stamp).strip() != ""


def normalize_legs(legs: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep legs that have side, CE/PE, strike, entry, and a positive lot."""
    out: List[Dict[str, Any]] = []
    for raw in legs or []:
        if not isinstance(raw, dict):
            continue
        direction = _direction(raw.get("side"))
        right = _option_type(raw.get("option_type"))
        strike = _as_float(raw.get("strike_price") if raw.get("strike_price") is not None else raw.get("strike"))
        entry = _as_float(raw.get("entry_price") if raw.get("entry_price") is not None else raw.get("entry"))
        lot = _as_float(raw.get("lot_size") if raw.get("lot_size") is not None else raw.get("lot"))
        if direction is None or right is None or strike is None or strike <= 0:
            continue
        if entry is None or entry < 0 or lot is None or lot <= 0:
            continue
        out.append(
            {
                "side": "BUY" if direction > 0 else "SELL",
                "option_type": right,
                "strike_price": strike,
                "entry_price": entry,
                "lot_size": lot,
                "direction": direction,
                "exited": _exited(raw),
                "exit_price": _as_float(raw.get("exit_price")),
            }
        )
    return out


def intrinsic(option_type: str, spot: float, strike: float) -> float:
    if option_type == "CE":
        return max(0.0, spot - strike)
    return max(0.0, strike - spot)


def leg_expiry_pnl(leg: Dict[str, Any], spot: float) -> float:
    if leg.get("exited"):
        mark = leg["exit_price"] if leg.get("exit_price") is not None else leg["entry_price"]
        return (mark - leg["entry_price"]) * leg["lot_size"] * leg["direction"]
    value = intrinsic(leg["option_type"], spot, leg["strike_price"])
    return (value - leg["entry_price"]) * leg["lot_size"] * leg["direction"]


def payoff_at(legs: Sequence[Dict[str, Any]], spot: float) -> float:
    return float(sum(leg_expiry_pnl(leg, spot) for leg in legs))


def net_credit(legs: Sequence[Dict[str, Any]]) -> float:
    """Premium received minus premium paid. Positive is a net credit."""
    total = 0.0
    for leg in legs:
        total += leg["entry_price"] * leg["lot_size"] * (-leg["direction"])
    return float(total)


def tail_slope(legs: Sequence[Dict[str, Any]], tail: str) -> float:
    """d(P&L)/d(spot) outside every strike. Nonzero means that side is still trending.

    Left of all strikes only puts have intrinsic, and d(K-S)/dS = -1.
    Right of all strikes only calls have intrinsic, and d(S-K)/dS = +1.
    """
    slope = 0.0
    for leg in legs:
        if leg.get("exited"):
            continue
        if tail == "right" and leg["option_type"] == "CE":
            slope += leg["lot_size"] * leg["direction"]
        elif tail == "left" and leg["option_type"] == "PE":
            slope += -leg["lot_size"] * leg["direction"]
    return float(slope)


def spot_bounds(strikes: Sequence[float], current_spot: Optional[float]) -> tuple:
    lo = min(strikes) * (1.0 - PAD)
    hi = max(strikes) * (1.0 + PAD)
    spot = _as_float(current_spot)
    if spot is not None and spot > 0:
        if spot < lo:
            lo = spot * (1.0 - SPOT_EDGE_PAD)
        elif spot > hi:
            hi = spot * (1.0 + SPOT_EDGE_PAD)
    if hi <= lo:
        hi = lo + 1.0
    return float(lo), float(hi)


def _samples(lo: float, hi: float, strikes: Sequence[float]) -> List[float]:
    step_count = SAMPLE_COUNT - 1
    pts = [lo + (hi - lo) * i / step_count for i in range(SAMPLE_COUNT)]
    for strike in strikes:
        if lo < strike < hi:
            pts.append(float(strike))
    pts.sort()
    out: List[float] = []
    for spot in pts:
        if not out or abs(spot - out[-1]) > 1e-6:
            out.append(float(spot))
    return out


def _breakevens(spots: Sequence[float], pnls: Sequence[float]) -> List[float]:
    found: List[float] = []
    for i in range(len(spots) - 1):
        y0 = pnls[i]
        y1 = pnls[i + 1]
        x0 = spots[i]
        x1 = spots[i + 1]
        z0 = abs(y0) <= ZERO_PNL
        z1 = abs(y1) <= ZERO_PNL
        if z0 and z1:
            continue
        if z0:
            found.append(x0)
            continue
        if z1:
            found.append(x1)
            continue
        if y0 * y1 < 0:
            denom = abs(y0) + abs(y1)
            found.append(x0 + (x1 - x0) * (abs(y0) / denom))
    out: List[float] = []
    for spot in sorted(found):
        if not out or abs(spot - out[-1]) > 1e-3:
            out.append(float(spot))
    return out


def empty_summary() -> Dict[str, Any]:
    return {
        "spot_min": None,
        "spot_max": None,
        "spots": [],
        "pnl": [],
        "breakevens": [],
        "max_profit": None,
        "max_profit_unlimited": False,
        "max_loss": None,
        "max_loss_unlimited": False,
        "net_credit": 0.0,
    }


def payoff_summary(
    legs: Iterable[Dict[str, Any]],
    current_spot: Optional[float] = None,
) -> Dict[str, Any]:
    """Curve, breakevens, and max profit / max loss for one trade."""
    clean = normalize_legs(legs)
    if not clean:
        return empty_summary()
    strikes = [leg["strike_price"] for leg in clean]
    lo, hi = spot_bounds(strikes, current_spot)
    spots = _samples(lo, hi, strikes)
    pnls = [payoff_at(clean, spot) for spot in spots]
    left = tail_slope(clean, "left")
    right = tail_slope(clean, "right")
    profit_unlimited = right > SLOPE_EPS or left < -SLOPE_EPS
    loss_unlimited = right < -SLOPE_EPS or left > SLOPE_EPS
    knots = [lo, hi]
    for strike in strikes:
        if lo <= strike <= hi:
            knots.append(strike)
    extremes = [payoff_at(clean, spot) for spot in knots]
    return {
        "spot_min": lo,
        "spot_max": hi,
        "spots": spots,
        "pnl": pnls,
        "breakevens": _breakevens(spots, pnls),
        "max_profit": None if profit_unlimited else float(max(extremes)),
        "max_profit_unlimited": bool(profit_unlimited),
        "max_loss": None if loss_unlimited else float(min(extremes)),
        "max_loss_unlimited": bool(loss_unlimited),
        "net_credit": net_credit(clean),
    }
