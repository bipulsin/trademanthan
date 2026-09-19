"""Spread / condor construction + sizing helpers for Kosmic Tarang Phase 2."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.services.tarang.domain.types import OptionChain, OptionQuote


def _trade_mid(q: OptionQuote) -> Optional[float]:
    """Tradable mid requires a live two-sided book. Never use last or model-only prices."""
    if q.bid is not None and q.ask is not None and float(q.bid) > 0 and float(q.ask) > 0:
        return 0.5 * (float(q.bid) + float(q.ask))
    return None


def _abs_delta(q: OptionQuote) -> Optional[float]:
    if q.delta is None:
        return None
    return abs(float(q.delta))


def pick_short_by_delta(
    quotes: List[OptionQuote],
    right: str,
    d_min: float,
    d_max: float,
    prefer_otm_of: Optional[float] = None,
) -> Optional[OptionQuote]:
    band = [q for q in quotes if q.right == right and _abs_delta(q) is not None]
    in_band = [q for q in band if d_min <= (_abs_delta(q) or 0) <= d_max]
    pool = in_band or band
    if not pool:
        return None
    target = 0.5 * (d_min + d_max)
    if prefer_otm_of is not None:
        if right == "PE":
            pool = [q for q in pool if q.strike <= prefer_otm_of] or pool
        else:
            pool = [q for q in pool if q.strike >= prefer_otm_of] or pool
    return min(pool, key=lambda q: abs((_abs_delta(q) or 0) - target))


def pick_long_wing(
    quotes: List[OptionQuote],
    right: str,
    short: OptionQuote,
    width_steps: int,
    strike_step: float,
) -> Optional[OptionQuote]:
    width = width_steps * strike_step
    if right == "PE":
        target = short.strike - width
        cands = [q for q in quotes if q.right == "PE" and q.strike <= target + 1e-9]
        if not cands:
            return None
        return max(cands, key=lambda q: q.strike)
    target = short.strike + width
    cands = [q for q in quotes if q.right == "CE" and q.strike >= target - 1e-9]
    if not cands:
        return None
    return min(cands, key=lambda q: q.strike)


def credit_spread_legs(
    short: OptionQuote,
    long: OptionQuote,
) -> Tuple[List[Dict[str, Any]], float, float]:
    """Return legs, net_credit (pts), width (pts). Credit = short mid − long mid."""
    s_mid = _trade_mid(short)
    l_mid = _trade_mid(long)
    if s_mid is None or l_mid is None:
        return [], 0.0, abs(short.strike - long.strike)
    width = abs(short.strike - long.strike)
    credit = s_mid - l_mid
    legs = [
        {
            "side": "SELL",
            "right": short.right,
            "strike": short.strike,
            "symbol": short.symbol,
            "instrument_key": short.instrument_key,
            "bid": short.bid,
            "ask": short.ask,
            "mid": s_mid,
            "oi": short.oi,
            "delta": short.delta,
            "iv": short.iv,
            "limit_mid": s_mid,
            "limit_conservative": short.bid if short.right and short.bid is not None else s_mid,
        },
        {
            "side": "BUY",
            "right": long.right,
            "strike": long.strike,
            "symbol": long.symbol,
            "instrument_key": long.instrument_key,
            "bid": long.bid,
            "ask": long.ask,
            "mid": l_mid,
            "oi": long.oi,
            "delta": long.delta,
            "iv": long.iv,
            "limit_mid": l_mid,
            "limit_conservative": long.ask if long.ask is not None else l_mid,
        },
    ]
    return legs, credit, width


def build_structure(
    chain: OptionChain,
    structure: str,
    d_min: float,
    d_max: float,
    width_steps: int,
) -> Dict[str, Any]:
    step = float(chain.strike_step or 0)
    if step <= 0:
        return {"ok": False, "error": "strike_step_missing"}
    F = chain.futures_or_spot
    quotes = list(chain.quotes or [])
    legs: List[Dict[str, Any]] = []
    credits: List[float] = []
    widths: List[float] = []
    shorts: List[OptionQuote] = []

    def one_side(right: str) -> Optional[str]:
        short = pick_short_by_delta(quotes, right, d_min, d_max, prefer_otm_of=F)
        if not short:
            return f"no_{right}_short"
        long = pick_long_wing(quotes, right, short, width_steps, step)
        if not long:
            return f"no_{right}_long"
        side_legs, credit, width = credit_spread_legs(short, long)
        if not side_legs:
            return f"no_{right}_mids"
        legs.extend(side_legs)
        credits.append(credit)
        widths.append(width)
        shorts.append(short)
        return None

    if structure == "put_credit_spread":
        err = one_side("PE")
        if err:
            return {"ok": False, "error": err}
    elif structure == "call_credit_spread":
        err = one_side("CE")
        if err:
            return {"ok": False, "error": err}
    else:  # iron_condor
        err = one_side("PE")
        if err:
            return {"ok": False, "error": err}
        err = one_side("CE")
        if err:
            return {"ok": False, "error": err}

    net_credit = sum(credits)
    width = max(widths) if widths else 0.0
    # For condor, width for max-loss is per-side; use max side width (symmetric profiles)
    return {
        "ok": True,
        "structure": structure,
        "legs": legs,
        "net_credit": net_credit,
        "width": width,
        "short_deltas": [s.delta for s in shorts],
        "expiry": chain.expiry,
        "underlying": chain.underlying,
        "lot_size": chain.lot_size,
        "contract_value": next((q.contract_value for q in quotes if q.contract_value), None),
        "strike_step": step,
        "width_steps": width_steps,
        "futures_or_spot": F,
    }


def max_loss_per_unit_inr(
    width: float,
    net_credit: float,
    *,
    lot_size: Optional[int] = None,
    contract_value: Optional[float] = None,
    usd_inr: float = 83.0,
    venue: str = "upstox_mcx",
) -> Optional[float]:
    """Max loss per lot (MCX INR) or per contract converted to INR (Delta)."""
    risk_pts = max(0.0, float(width) - float(net_credit))
    if venue == "delta_india":
        cv = float(contract_value or 0)
        if cv <= 0:
            return None
        return risk_pts * cv * float(usd_inr)
    mult = float(lot_size or 0)
    if mult <= 0:
        return None
    return risk_pts * mult


def floor_units(budget_inr: float, max_loss_per_unit: Optional[float]) -> int:
    if max_loss_per_unit is None or max_loss_per_unit <= 0:
        return 0
    return int(float(budget_inr) // float(max_loss_per_unit))


def estimate_25d_ivs(chain: OptionChain) -> Dict[str, Optional[float]]:
    """Nearest |δ|≈0.25 put/call IVs for skew."""
    target = 0.25
    pes = [q for q in chain.quotes if q.right == "PE" and q.iv and q.delta is not None]
    ces = [q for q in chain.quotes if q.right == "CE" and q.iv and q.delta is not None]
    put_iv = call_iv = None
    if pes:
        put_iv = min(pes, key=lambda q: abs(abs(q.delta or 0) - target)).iv
    if ces:
        call_iv = min(ces, key=lambda q: abs(abs(q.delta or 0) - target)).iv
    skew = None
    if put_iv is not None and call_iv is not None:
        skew = float(put_iv) - float(call_iv)
    return {"put_iv_25d": put_iv, "call_iv_25d": call_iv, "skew_25d": skew}
