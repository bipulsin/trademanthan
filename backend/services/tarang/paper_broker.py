"""PaperBroker — simulated fills for Kosmic Tarang (Phase 3). No live orders."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from backend.services.tarang.config import get_risk


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def paper_fill_config() -> Dict[str, Any]:
    return dict((get_risk().get("paper_fills") or {}))


def mid_of(bid: Optional[float], ask: Optional[float], mid: Optional[float] = None) -> Optional[float]:
    if mid is not None:
        return float(mid)
    if bid is not None and ask is not None:
        return 0.5 * (float(bid) + float(ask))
    if bid is not None:
        return float(bid)
    if ask is not None:
        return float(ask)
    return None


def simulated_fill_price(
    side: str,
    bid: Optional[float],
    ask: Optional[float],
    mid: Optional[float] = None,
    *,
    spread_fraction: Optional[float] = None,
) -> Optional[float]:
    """
    Conservative paper fill: mid − fraction×(ask−bid) for SELL, mid + for BUY.
    Fraction defaults from risk.paper_fills.spread_fraction (0.40).
    """
    cfg = paper_fill_config()
    frac = float(spread_fraction if spread_fraction is not None else cfg.get("spread_fraction") or 0.40)
    m = mid_of(bid, ask, mid)
    if m is None:
        return None
    if bid is not None and ask is not None and float(ask) >= float(bid):
        half_spread = float(ask) - float(bid)
    else:
        half_spread = 0.0
    slip = frac * half_spread
    side_u = str(side or "").upper()
    if side_u == "SELL":
        return m - slip
    return m + slip


def _multiplier(venue: str, lot_size: Optional[float], contract_value: Optional[float], usd_inr: float) -> float:
    if venue == "delta_india":
        return float(contract_value or 0) * float(usd_inr)
    return float(lot_size or 0)


def fee_for_units(venue: str, units: int, cfg: Optional[Dict[str, Any]] = None) -> float:
    cfg = cfg or paper_fill_config()
    fees = cfg.get("fees") or {}
    usd_inr = float(cfg.get("usd_inr") or 83.0)
    if venue == "delta_india":
        return float(fees.get("delta_india_per_contract_roundtrip_usd") or 0.10) * usd_inr * max(units, 0)
    return float(fees.get("upstox_mcx_per_lot_roundtrip_inr") or 80.0) * max(units, 0)


def simulate_entry_fills(
    legs: Sequence[Dict[str, Any]],
    *,
    units: int,
    venue: str,
    lot_size: Optional[float] = None,
    contract_value: Optional[float] = None,
    spread_fraction: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Simulate entry: buy protective (BUY) legs first, then sell shorts.
    Returns fills, net_credit (pts), fees INR, client_order_ids.
    """
    cfg = paper_fill_config()
    usd_inr = float(cfg.get("usd_inr") or 83.0)
    mult = _multiplier(venue, lot_size, contract_value, usd_inr)
    ordered = sorted(
        enumerate(legs),
        key=lambda pair: 0 if str(pair[1].get("side") or "").upper() == "BUY" else 1,
    )
    fills: List[Dict[str, Any]] = []
    credit_pts = 0.0
    for idx, leg in ordered:
        side = str(leg.get("side") or "").upper()
        px = simulated_fill_price(
            side,
            leg.get("bid"),
            leg.get("ask"),
            leg.get("mid"),
            spread_fraction=spread_fraction,
        )
        if px is None:
            return {"ok": False, "error": "missing_quote", "leg": leg, "status": "REJECTED"}
        signed = -px if side == "BUY" else px  # credit positive when we sell
        credit_pts += signed
        coid = f"tarang-paper-{uuid.uuid4().hex[:16]}"
        fills.append(
            {
                "leg_index": idx,
                "side": side,
                "right": leg.get("right"),
                "strike": leg.get("strike"),
                "symbol": leg.get("symbol") or leg.get("instrument_key"),
                "instrument_key": leg.get("instrument_key"),
                "qty": units,
                "price": px,
                "phase": "entry",
                "client_order_id": coid,
                "filled_at": _now_iso(),
            }
        )
    fees = fee_for_units(venue, units, cfg) * 0.5  # half of round-trip on entry
    max_credit_inr = credit_pts * mult * units if mult else None
    return {
        "ok": True,
        "fills": fills,
        "net_credit_pts": credit_pts,
        "multiplier": mult,
        "units": units,
        "fees_inr": fees,
        "entry_credit_inr": max_credit_inr,
        "venue": venue,
        "mode": "PAPER",
        "order_order": "buy_longs_first",
    }


def simulate_exit_fills(
    legs: Sequence[Dict[str, Any]],
    entry_fills: Sequence[Dict[str, Any]],
    *,
    units: int,
    venue: str,
    lot_size: Optional[float] = None,
    contract_value: Optional[float] = None,
    live_quotes: Optional[Sequence[Dict[str, Any]]] = None,
    spread_fraction: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Close by reversing sides. Uses live_quotes when provided (matched by symbol/strike+right),
    else falls back to leg bid/ask/mid on the structure legs.
    """
    cfg = paper_fill_config()
    usd_inr = float(cfg.get("usd_inr") or 83.0)
    mult = _multiplier(venue, lot_size, contract_value, usd_inr)
    quote_by_key: Dict[str, Dict[str, Any]] = {}
    for q in live_quotes or []:
        for k in (q.get("instrument_key"), q.get("symbol"), f"{q.get('right')}:{q.get('strike')}"):
            if k:
                quote_by_key[str(k)] = q

    fills: List[Dict[str, Any]] = []
    debit_pts = 0.0  # cost to close (positive = we pay)
    for ef in entry_fills:
        side_in = str(ef.get("side") or "").upper()
        side_out = "BUY" if side_in == "SELL" else "SELL"
        leg = None
        li = ef.get("leg_index")
        if li is not None and 0 <= int(li) < len(legs):
            leg = legs[int(li)]
        q = None
        for k in (ef.get("instrument_key"), ef.get("symbol"), f"{(leg or {}).get('right')}:{(leg or {}).get('strike')}"):
            if k and str(k) in quote_by_key:
                q = quote_by_key[str(k)]
                break
        src = q or leg or {}
        px = simulated_fill_price(
            side_out,
            src.get("bid"),
            src.get("ask"),
            src.get("mid") or src.get("last"),
            spread_fraction=spread_fraction,
        )
        if px is None:
            # last resort: reverse entry price with small adverse slip
            ep = float(ef.get("price") or 0)
            px = ep * 1.02 if side_out == "BUY" else ep * 0.98
        # Cost to unwind shorts (buy back) adds to debit; sell longs reduces debit
        if side_out == "BUY":
            debit_pts += px
        else:
            debit_pts -= px
        fills.append(
            {
                "leg_index": ef.get("leg_index"),
                "side": side_out,
                "right": ef.get("right") or (leg or {}).get("right"),
                "strike": ef.get("strike") or (leg or {}).get("strike"),
                "symbol": ef.get("symbol"),
                "instrument_key": ef.get("instrument_key"),
                "qty": units,
                "price": px,
                "phase": "exit",
                "client_order_id": f"tarang-paper-{uuid.uuid4().hex[:16]}",
                "filled_at": _now_iso(),
            }
        )

    entry_credit_pts = 0.0
    for ef in entry_fills:
        side = str(ef.get("side") or "").upper()
        px = float(ef.get("price") or 0)
        entry_credit_pts += -px if side == "BUY" else px

    gross_pts = entry_credit_pts - debit_pts
    gross_inr = gross_pts * mult * units if mult else 0.0
    exit_fees = fee_for_units(venue, units, cfg) * 0.5
    return {
        "ok": True,
        "fills": fills,
        "debit_to_close_pts": debit_pts,
        "entry_credit_pts": entry_credit_pts,
        "gross_pnl_pts": gross_pts,
        "gross_pnl_inr": gross_inr,
        "fees_inr": exit_fees,
        "multiplier": mult,
        "units": units,
        "mode": "PAPER",
    }


def mark_to_market(
    legs: Sequence[Dict[str, Any]],
    entry_fills: Sequence[Dict[str, Any]],
    *,
    units: int,
    venue: str,
    lot_size: Optional[float] = None,
    contract_value: Optional[float] = None,
    live_quotes: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Unrealized P&L using mid marks (no extra slippage)."""
    cfg = paper_fill_config()
    usd_inr = float(cfg.get("usd_inr") or 83.0)
    mult = _multiplier(venue, lot_size, contract_value, usd_inr)
    quote_by_key: Dict[str, Dict[str, Any]] = {}
    for q in live_quotes or []:
        for k in (q.get("instrument_key"), q.get("symbol"), f"{q.get('right')}:{q.get('strike')}"):
            if k:
                quote_by_key[str(k)] = q

    entry_credit_pts = 0.0
    for ef in entry_fills:
        side = str(ef.get("side") or "").upper()
        px = float(ef.get("price") or 0)
        entry_credit_pts += -px if side == "BUY" else px

    debit_pts = 0.0
    per_leg: List[Dict[str, Any]] = []
    for ef in entry_fills:
        side_in = str(ef.get("side") or "").upper()
        li = ef.get("leg_index")
        leg = legs[int(li)] if li is not None and 0 <= int(li) < len(legs) else {}
        q = None
        for k in (ef.get("instrument_key"), ef.get("symbol"), f"{leg.get('right')}:{leg.get('strike')}"):
            if k and str(k) in quote_by_key:
                q = quote_by_key[str(k)]
                break
        src = q or leg
        m = mid_of(src.get("bid"), src.get("ask"), src.get("mid") or src.get("last"))
        if m is None:
            m = float(ef.get("price") or 0)
        # Mark cost to close this leg
        if side_in == "SELL":
            debit_pts += m  # buy back
            mark_side = "BUY"
        else:
            debit_pts -= m  # sell long
            mark_side = "SELL"
        per_leg.append(
            {
                "leg_index": li,
                "side_open": side_in,
                "mark_side": mark_side,
                "symbol": ef.get("symbol"),
                "right": ef.get("right") or leg.get("right"),
                "strike": ef.get("strike") or leg.get("strike"),
                "bid": src.get("bid"),
                "ask": src.get("ask"),
                "mid": m,
                "delta": src.get("delta"),
                "iv": src.get("iv"),
                "entry_price": ef.get("price"),
            }
        )

    unrealized_pts = entry_credit_pts - debit_pts
    unrealized_inr = unrealized_pts * mult * units if mult else 0.0
    return {
        "entry_credit_pts": entry_credit_pts,
        "debit_to_close_pts": debit_pts,
        "unrealized_pnl_pts": unrealized_pts,
        "unrealized_pnl_inr": unrealized_inr,
        "multiplier": mult,
        "units": units,
        "per_leg": per_leg,
        "currency": "INR",
        "usd_note": "Crypto P&L converted at paper_fills.usd_inr" if venue == "delta_india" else None,
    }
