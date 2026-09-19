"""Real venue fee schedules for Kosmic Tarang paper (and later live) P&L."""
from __future__ import annotations

from typing import Any, Dict, Optional

from backend.services.tarang.config import get_risk


def fee_schedules() -> Dict[str, Any]:
    cfg = (get_risk().get("paper_fills") or {}).get("fees") or {}
    return {
        "upstox_mcx_options": dict(cfg.get("upstox_mcx_options") or {}),
        "delta_india_options": dict(cfg.get("delta_india_options") or {}),
        "usd_inr": float((get_risk().get("paper_fills") or {}).get("usd_inr") or 83.0),
    }


def upstox_mcx_option_fee_inr(
    *,
    premium_pts: float,
    lot_size: float,
    qty: int,
    side: str,
    n_orders: int = 1,
    schedule: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    """
    Upstox commodity *options* (MCX) — brokerage + statutory on premium turnover.

    Sources: Upstox help «How are charges calculated» commodity-options row
    (brokerage ₹20/order, MCX txn 0.0418% of premium, CTT 0.05% sell-side,
    stamp 0.003% buy-side, SEBI ₹10/crore, GST 18% on brokerage+txn+SEBI).
    """
    sch = schedule if schedule is not None else fee_schedules()["upstox_mcx_options"]
    premium_inr = abs(float(premium_pts or 0)) * float(lot_size or 0) * max(int(qty or 0), 0)
    brokerage = float(sch.get("brokerage_per_order_inr") or 20.0) * max(int(n_orders or 1), 1)
    exchange = premium_inr * float(sch.get("exchange_premium_frac") or 0.000418)
    sebi = premium_inr * float(sch.get("sebi_frac") or 0.000001)
    side_u = str(side or "").upper()
    ctt = premium_inr * float(sch.get("ctt_sell_frac") or 0.0005) if side_u == "SELL" else 0.0
    stamp = premium_inr * float(sch.get("stamp_buy_frac") or 0.00003) if side_u == "BUY" else 0.0
    gst = (brokerage + exchange + sebi) * float(sch.get("gst_frac") or 0.18)
    total = brokerage + exchange + sebi + ctt + stamp + gst
    return {
        "premium_inr": premium_inr,
        "brokerage": brokerage,
        "exchange": exchange,
        "sebi": sebi,
        "ctt": ctt,
        "stamp": stamp,
        "gst": gst,
        "total_inr": total,
    }


def delta_india_option_fee_inr(
    *,
    premium_pts: float,
    contract_value: float,
    qty: int,
    underlying_price: Optional[float],
    usd_inr: Optional[float] = None,
    taker: bool = True,
    schedule: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    """
    Delta Exchange India vanilla options.

    Taker 0.03% / maker 0.010% of notional, capped at 3.5% of premium, +18% GST.
    https://www.delta.exchange/fees
    """
    sch = schedule if schedule is not None else fee_schedules()["delta_india_options"]
    fx = float(usd_inr if usd_inr is not None else fee_schedules()["usd_inr"])
    cv = float(contract_value or 0)
    n = max(int(qty or 0), 0)
    px = abs(float(premium_pts or 0))
    und = float(underlying_price) if underlying_price else None
    notional_usd = (und * cv * n) if und and und > 0 else (px * cv * n)
    premium_usd = px * cv * n
    rate = float(sch.get("taker_notional_frac") if taker else sch.get("maker_notional_frac") or 0.0001)
    if taker:
        rate = float(sch.get("taker_notional_frac") or 0.0003)
    raw = notional_usd * rate
    cap = premium_usd * float(sch.get("premium_cap_frac") or 0.035)
    fee_usd = min(raw, cap) if cap > 0 else raw
    gst = fee_usd * float(sch.get("gst_frac") or 0.18)
    total_usd = fee_usd + gst
    return {
        "notional_usd": notional_usd,
        "premium_usd": premium_usd,
        "fee_usd": fee_usd,
        "gst_usd": gst,
        "total_usd": total_usd,
        "total_inr": total_usd * fx,
        "usd_inr": fx,
        "taker": taker,
    }


def fees_for_fills_inr(
    fills: list,
    *,
    venue: str,
    lot_size: Optional[float] = None,
    contract_value: Optional[float] = None,
    underlying_price: Optional[float] = None,
) -> float:
    """Sum per-fill fees (one executed order per fill)."""
    total = 0.0
    if venue == "delta_india":
        sch = fee_schedules()
        assume_taker = bool((sch["delta_india_options"].get("assume_taker_in_paper") is not False))
        for f in fills or []:
            part = delta_india_option_fee_inr(
                premium_pts=float(f.get("price") or 0),
                contract_value=float(contract_value or 0),
                qty=int(f.get("qty") or 0),
                underlying_price=underlying_price,
                usd_inr=sch["usd_inr"],
                taker=assume_taker,
                schedule=sch["delta_india_options"],
            )
            total += part["total_inr"]
        return total
    for f in fills or []:
        part = upstox_mcx_option_fee_inr(
            premium_pts=float(f.get("price") or 0),
            lot_size=float(lot_size or 0),
            qty=int(f.get("qty") or 0),
            side=str(f.get("side") or ""),
            n_orders=1,
        )
        total += part["total_inr"]
    return total
