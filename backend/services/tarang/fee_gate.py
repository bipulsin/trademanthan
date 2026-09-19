"""Fee-as-%-of-credit gate and exchange position limits."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from backend.services.tarang.config import get_risk
from backend.services.tarang.fees import delta_india_option_fee_inr, upstox_mcx_option_fee_inr
from backend.services.tarang.gates import GateResult


def _fee_cfg() -> Dict[str, Any]:
    return dict(get_risk().get("fee_gate") or {})


def _limits_cfg(venue: str) -> Dict[str, Any]:
    lim = (get_risk().get("position_limits") or {}).get(venue) or {}
    return dict(lim)


def round_trip_fees_inr(
    *,
    venue: str,
    legs: Sequence[Dict[str, Any]],
    units: int,
    lot_size: Optional[float],
    contract_value: Optional[float],
    underlying_price: Optional[float],
) -> Dict[str, Any]:
    """Entry + matching exit (assume taker both ways) in INR."""
    n = max(int(units or 0), 0)
    entry = 0.0
    per_leg: List[Dict[str, Any]] = []
    if venue == "delta_india":
        for leg in legs:
            px = float(leg.get("mid") or 0)
            part = delta_india_option_fee_inr(
                premium_pts=px,
                contract_value=float(contract_value or 0),
                qty=n,
                underlying_price=underlying_price,
                taker=True,
            )
            entry += part["total_inr"]
            per_leg.append(part)
    else:
        for leg in legs:
            px = float(leg.get("mid") or 0)
            side = str(leg.get("side") or "SELL")
            part = upstox_mcx_option_fee_inr(
                premium_pts=px,
                lot_size=float(lot_size or 0),
                qty=n,
                side=side,
            )
            entry += part["total_inr"]
            per_leg.append(part)
    # Symmetric unwind: each leg flips side
    exit_fees = 0.0
    if venue == "delta_india":
        for leg in legs:
            px = float(leg.get("mid") or 0)
            part = delta_india_option_fee_inr(
                premium_pts=px,
                contract_value=float(contract_value or 0),
                qty=n,
                underlying_price=underlying_price,
                taker=True,
            )
            exit_fees += part["total_inr"]
    else:
        for leg in legs:
            px = float(leg.get("mid") or 0)
            side = str(leg.get("side") or "SELL")
            flip = "BUY" if side.upper() == "SELL" else "SELL"
            part = upstox_mcx_option_fee_inr(
                premium_pts=px,
                lot_size=float(lot_size or 0),
                qty=n,
                side=flip,
            )
            exit_fees += part["total_inr"]
    return {
        "entry_fees_inr": entry,
        "exit_fees_est_inr": exit_fees,
        "round_trip_inr": entry + exit_fees,
        "per_leg_entry": per_leg,
    }


def credit_inr(
    net_credit_pts: float,
    *,
    venue: str,
    lot_size: Optional[float],
    contract_value: Optional[float],
    units: int,
    usd_inr: float = 83.0,
) -> float:
    n = max(int(units or 0), 0)
    c = abs(float(net_credit_pts or 0))
    if venue == "delta_india":
        return c * float(contract_value or 0) * float(usd_inr) * n
    return c * float(lot_size or 0) * n


def gate_fee_and_limits(
    *,
    venue: str,
    legs: Sequence[Dict[str, Any]],
    net_credit_pts: float,
    units: int,
    lot_size: Optional[float],
    contract_value: Optional[float],
    underlying_price: Optional[float],
) -> Dict[str, Any]:
    cfg = _fee_cfg()
    max_frac = float(cfg.get("max_fees_frac_of_credit") or 0.15)
    min_net = float(cfg.get("min_net_credit_per_contract_inr") or 1.0)
    usd_inr = float((get_risk().get("paper_fills") or {}).get("usd_inr") or 83.0)
    gross = credit_inr(
        net_credit_pts,
        venue=venue,
        lot_size=lot_size,
        contract_value=contract_value,
        units=units,
        usd_inr=usd_inr,
    )
    fees = round_trip_fees_inr(
        venue=venue,
        legs=legs,
        units=units,
        lot_size=lot_size,
        contract_value=contract_value,
        underlying_price=underlying_price,
    )
    rt = float(fees["round_trip_inr"])
    frac = (rt / gross) if gross > 0 else None
    n = max(int(units or 0), 1)
    net_per = ((gross - rt) / n) if n else None
    limits = _limits_cfg(venue)
    max_order = limits.get("max_contracts_per_order")
    max_trade = limits.get("max_contracts_per_trade")
    fails: List[str] = []
    if frac is None:
        fails.append("credit_zero")
    elif frac > max_frac:
        fails.append(f"fees {frac:.1%} of credit > {max_frac:.0%}")
    if net_per is not None and net_per < min_net:
        fails.append(f"net credit/contract ₹{net_per:.2f} < ₹{min_net:.2f}")
    if max_order is not None and units > int(max_order):
        fails.append(f"units {units} > max per order {max_order}")
    if max_trade is not None and units > int(max_trade):
        fails.append(f"units {units} > max per trade {max_trade}")
    ok = not fails
    gate = GateResult(
        name="fee_gate",
        passed=ok,
        actual={"frac": frac, "net_per_contract_inr": net_per, "units": units},
        threshold={"max_fees_frac_of_credit": max_frac, "min_net_credit_per_contract_inr": min_net},
        detail="; ".join(fails) if fails else f"round-trip fees {frac:.1%} of credit" if frac is not None else "ok",
        status_hint="" if ok else "BLOCKED",
    )
    return {
        "gate": gate,
        "gross_credit_inr": gross,
        "round_trip_fees_inr": rt,
        "fees_frac_of_credit": frac,
        "net_credit_per_contract_inr": net_per,
        "max_contracts_per_order": max_order,
        "max_contracts_per_trade": max_trade,
        **fees,
    }
