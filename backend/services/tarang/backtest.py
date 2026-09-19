"""Replay Screener / Sizing / ExitEngine over stored full-chain snapshots."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.chain_snapshots import chain_coverage, unpack_chain_payload
from backend.services.tarang.config import get_profiles, get_risk
from backend.services.tarang.data_gaps import gaps_overlapping, recent_gaps, snapshot_in_gap
from backend.services.tarang.domain.types import OptionChain, OptionQuote
from backend.services.tarang.exit_engine import evaluate_exits
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.structures import build_structure, max_loss_per_unit_inr, floor_units

logger = logging.getLogger(__name__)

MIN_WIDE_DAYS_FOR_RUN = 5


def _quote_from_row(row: Dict[str, Any], underlying: str, expiry: str) -> OptionQuote:
    return OptionQuote(
        instrument_key=str(row.get("instrument_key") or row.get("symbol") or ""),
        symbol=str(row.get("symbol") or ""),
        underlying=underlying,
        expiry=expiry or str(row.get("expiry") or ""),
        strike=float(row.get("strike") or 0),
        right=str(row.get("right") or ""),
        bid=row.get("bid"),
        ask=row.get("ask"),
        mid=row.get("mark") or row.get("mid"),
        last=row.get("last"),
        oi=row.get("oi"),
        volume=row.get("volume"),
        iv=row.get("iv"),
        delta=row.get("delta"),
        greeks_source=row.get("greeks_source"),
        meta={"two_sided": row.get("two_sided"), "bid_qty": row.get("bid_qty"), "ask_qty": row.get("ask_qty")},
    )


def _chain_from_snapshot(row: Dict[str, Any]) -> Optional[OptionChain]:
    blob = row.get("payload_gzip")
    if not blob:
        return None
    try:
        payload = unpack_chain_payload(bytes(blob))
    except Exception:
        return None
    expiry = str(row.get("expiry_date") or payload.get("expiry") or "")[:10]
    und = row.get("underlying_symbol") or ""
    quotes = [_quote_from_row(q, und, expiry) for q in (payload.get("quotes") or [])]
    return OptionChain(
        profile_id=row.get("profile_id") or "",
        venue=row.get("venue") or "",
        underlying=und,
        expiry=expiry,
        futures_or_spot=row.get("futures_or_spot") or payload.get("underlying_price"),
        lot_size=payload.get("lot_size"),
        strike_step=payload.get("strike_step"),
        quotes=quotes,
        built_at=payload.get("captured_at"),
        meta={"window": (row.get("meta") if isinstance(row.get("meta"), dict) else {}), "from_snapshot": True},
    )


def _load_snapshots(db, *, wide_only: bool = True) -> List[Dict[str, Any]]:
    q = """
        SELECT id, captured_at, profile_id, venue, underlying_symbol, expiry_date,
               futures_or_spot, atm_iv, n_quotes, payload_gzip, meta
        FROM tarang_chain_snapshots
        ORDER BY captured_at ASC, expiry_date
    """
    rows = db.execute(text(q)).mappings().all()
    out = []
    for r in rows:
        d = dict(r)
        meta = d.get("meta") or {}
        if isinstance(meta, str):
            import json

            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        d["meta"] = meta
        kind = str(meta.get("window_kind") or "")
        if wide_only and kind == "narrow_window":
            continue
        out.append(d)
    return out


def run_backtest(*, asof: Optional[datetime] = None) -> Dict[str, Any]:
    """Replay available wide snapshots. Insufficient data → coverage timeline, no invented fills."""
    ensure_tarang_tables()
    cov = chain_coverage()
    wide_days = max((u.get("days_wide") or 0) for u in (cov.get("underlyings") or [])) if cov.get("underlyings") else 0
    timeline = cov.get("underlyings") or []
    insufficient = wide_days < MIN_WIDE_DAYS_FOR_RUN
    out: Dict[str, Any] = {
        "product": "Kosmic Tarang",
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "coverage": cov,
        "wide_days": wide_days,
        "min_days_required": MIN_WIDE_DAYS_FOR_RUN,
        "insufficient_data": insufficient,
        "trades": [],
        "metrics": {"count": 0},
        "data_gaps": [],
        "note": (
            "Replay uses Screener structure rules, SizingService floor_units, and ExitEngine. "
            "narrow_window (ATM±10) rows are excluded from short/wing validation. "
            "data_gap periods are skipped."
        ),
    }
    if insufficient:
        out["message"] = (
            f"Insufficient data: {wide_days} wide-chain day(s), need {MIN_WIDE_DAYS_FOR_RUN} "
            "to run a meaningful replay. Coverage timeline is below. Phase 4 is held."
        )
        out["timeline"] = timeline
        out["data_gaps"] = recent_gaps(40)
        return out

    db = SessionLocal()
    try:
        snaps = _load_snapshots(db, wide_only=True)
        if not snaps:
            out["insufficient_data"] = True
            out["message"] = "No delta-window snapshots yet."
            out["timeline"] = timeline
            return out
        start = snaps[0]["captured_at"]
        end = snaps[-1]["captured_at"]
        gaps = gaps_overlapping(start, end)
        profiles = get_profiles().get("profiles") or {}
        risk = get_risk()
        # Group by profile+expiry, walk time
        by_key: Dict[str, List[Dict[str, Any]]] = {}
        for s in snaps:
            if snapshot_in_gap(s["captured_at"], gaps):
                continue
            key = f"{s.get('profile_id')}|{s.get('expiry_date')}"
            by_key.setdefault(key, []).append(s)

        paper_trades: List[Dict[str, Any]] = []
        for key, series in by_key.items():
            pid = (series[0].get("profile_id") or "").upper()
            prof = profiles.get(pid) or {}
            if not prof.get("enabled"):
                continue
            d_min = float(prof.get("short_delta_min") or 0.10)
            d_max = float(prof.get("short_delta_max") or 0.16)
            width_steps = int(prof.get("width_steps_min") or 2)
            structure = "iron_condor"
            open_trade = None
            for snap in series:
                chain = _chain_from_snapshot(snap)
                if not chain or not chain.quotes:
                    continue
                if open_trade is None:
                    built = build_structure(chain, structure, d_min, d_max, width_steps)
                    if not built.get("ok"):
                        continue
                    two_sided = all(
                        (lg.get("bid") and lg.get("ask")) for lg in built.get("legs") or []
                    )
                    if not two_sided:
                        continue
                    ml = max_loss_per_unit_inr(
                        float(built["width"]),
                        float(built["net_credit"]),
                        lot_size=built.get("lot_size") or chain.lot_size,
                        contract_value=built.get("contract_value"),
                        venue=chain.venue,
                    )
                    bucket = "CRYPTO" if chain.venue == "delta_india" else "ENERGY"
                    budget = float(((risk.get("buckets") or {}).get(bucket) or {}).get("per_trade_budget_inr") or 0)
                    units = floor_units(budget, ml)
                    if units < 1:
                        continue
                    open_trade = {
                        "profile_id": pid,
                        "expiry": chain.expiry,
                        "entry_at": snap["captured_at"].isoformat() if hasattr(snap["captured_at"], "isoformat") else str(snap["captured_at"]),
                        "entry_credit": built["net_credit"],
                        "legs": built["legs"],
                        "units": units,
                        "max_loss": ml,
                        "budget_inr": budget,
                        "venue": chain.venue,
                        "holding_mode": "POSITIONAL",
                        "atm_iv": snap.get("atm_iv"),
                    }
                    continue
                # Evaluate exit on later snapshot
                debit = 0.0
                short_d = []
                for lg in open_trade["legs"]:
                    match = next(
                        (
                            q
                            for q in chain.quotes
                            if q.right == lg.get("right") and abs(float(q.strike) - float(lg["strike"])) < 1e-6
                        ),
                        None,
                    )
                    if not match or match.bid is None or match.ask is None:
                        debit = None
                        break
                    mid = 0.5 * (float(match.bid) + float(match.ask))
                    if str(lg.get("side")).upper() == "SELL":
                        debit += mid
                        short_d.append(match.delta)
                    else:
                        debit -= mid
                if debit is None:
                    continue
                now = snap["captured_at"]
                if now.tzinfo is None:
                    now = now.replace(tzinfo=timezone.utc)
                ev = evaluate_exits(
                    entry_credit_pts=float(open_trade["entry_credit"]),
                    debit_to_close_pts=float(debit),
                    unrealized_pnl_inr=0.0,
                    max_profit_inr=None,
                    max_loss_inr=open_trade.get("max_loss"),
                    budget_inr=open_trade.get("budget_inr"),
                    short_deltas=short_d,
                    entry_atm_iv=open_trade.get("atm_iv"),
                    current_atm_iv=snap.get("atm_iv"),
                    venue=open_trade["venue"],
                    profile_id=pid,
                    holding_mode="POSITIONAL",
                    expiry=open_trade["expiry"],
                    now=now,
                )
                if ev.should_exit:
                    open_trade["exit_at"] = now.isoformat()
                    open_trade["exit_reason"] = ev.reason
                    open_trade["origin"] = "BACKTEST"
                    paper_trades.append(open_trade)
                    open_trade = None
        out["trades"] = paper_trades
        out["metrics"] = {"count": len(paper_trades)}
        out["timeline"] = timeline
        out["data_gaps"] = gaps
        return out
    finally:
        db.close()
