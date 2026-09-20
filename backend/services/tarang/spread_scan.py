"""Read-only 10–16 delta condor / single-side scan from latest delta_window snapshots.

Does not change screener defaults or persist candidates.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.chain_snapshots import unpack_chain_payload
from backend.services.tarang.config import crypto_budget, get_profiles, get_risk
from backend.services.tarang.domain.types import OptionChain, OptionQuote
from backend.services.tarang.fee_gate import gate_fee_and_limits
from backend.services.tarang.gates import (
    EVAL_FAILED,
    gate_credit_fraction,
    gate_liquidity,
    gate_short_delta,
    gate_sizing,
    gate_two_sided_quotes,
)
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.structures import build_structure, floor_units, max_loss_per_unit_inr

D_MIN = 0.10
D_MAX = 0.16


def _chain_from_payload(profile_id: str, venue: str, underlying: str, expiry: str, payload: Dict[str, Any], F, lot, step) -> OptionChain:
    quotes = []
    for q in payload.get("quotes") or []:
        if not isinstance(q, dict):
            continue
        quotes.append(
            OptionQuote(
                instrument_key=str(q.get("instrument_key") or ""),
                symbol=str(q.get("symbol") or ""),
                underlying=underlying,
                expiry=expiry,
                strike=float(q.get("strike") or 0),
                right=str(q.get("right") or ""),
                bid=q.get("bid"),
                ask=q.get("ask"),
                mid=q.get("mid") or q.get("mark"),
                last=q.get("last"),
                oi=q.get("oi"),
                iv=q.get("iv"),
                delta=q.get("delta"),
                contract_value=q.get("contract_value") or (0.001 if str(profile_id).upper() == "BTC" else 0.01),
                lot_size=lot,
            )
        )
    return OptionChain(
        profile_id=profile_id,
        venue=venue,
        underlying=underlying,
        expiry=expiry,
        futures_or_spot=F,
        lot_size=lot,
        strike_step=step or payload.get("strike_step"),
        quotes=quotes,
        built_at=str(payload.get("captured_at") or ""),
        meta={"source": "snapshot", "n_quotes": len(quotes)},
    )


def conservative_credit(legs: List[Dict[str, Any]]) -> Optional[float]:
    tot = 0.0
    for leg in legs:
        side = str(leg.get("side") or "").upper()
        if side == "SELL":
            px = leg.get("bid")
        else:
            px = leg.get("ask")
        if px is None:
            return None
        tot += float(px) if side == "SELL" else -float(px)
    return tot


def _eval_built(chain: OptionChain, built: Dict[str, Any], *, d_min: float, d_max: float) -> Dict[str, Any]:
    risk = get_risk()
    per_trade = float(crypto_budget().get("per_trade_budget_inr") or 3000)
    legs = built.get("legs") or []
    credit_mid = float(built.get("net_credit") or 0)
    width = float(built.get("width") or 0)
    cons = conservative_credit(legs)
    gates = [
        gate_two_sided_quotes(legs),
        gate_liquidity(legs, max_spread_pct=float(risk.get("liquidity_max_spread_pct_of_mid") or 15)),
    ]
    for sd in built.get("short_deltas") or []:
        gates.append(gate_short_delta(sd, d_min, d_max))
    gates.append(
        gate_credit_fraction(credit_mid, width, min_frac=float(risk.get("min_credit_fraction_of_width") or 0.20))
    )
    ml = max_loss_per_unit_inr(
        width,
        credit_mid,
        lot_size=built.get("lot_size") or chain.lot_size,
        contract_value=built.get("contract_value"),
        venue=chain.venue,
    )
    units = floor_units(per_trade, ml)
    gates.append(gate_sizing(ml, per_trade, units))
    fee_info = gate_fee_and_limits(
        venue=chain.venue,
        legs=legs,
        net_credit_pts=credit_mid,
        units=max(units, 1),
        lot_size=built.get("lot_size") or chain.lot_size,
        contract_value=built.get("contract_value"),
        underlying_price=chain.futures_or_spot,
    )
    gates.append(fee_info["gate"])
    first_fail = next((g.name for g in gates if g.evaluability == EVAL_FAILED or not g.passed), None)
    return {
        "ok": True,
        "structure": built.get("structure"),
        "width_steps": built.get("width_steps"),
        "short_deltas": built.get("short_deltas"),
        "width": width,
        "credit_mid": credit_mid,
        "credit_conservative": cons,
        "credit_pct_width": (credit_mid / width) if width else None,
        "sized_contracts": units,
        "round_trip_fees_inr": fee_info.get("round_trip_fees_inr"),
        "fees_frac_of_credit": fee_info.get("fees_frac_of_credit"),
        "first_failing_gate": first_fail,
        "legs": [
            {
                "side": l.get("side"),
                "right": l.get("right"),
                "strike": l.get("strike"),
                "symbol": l.get("symbol"),
                "delta": l.get("delta"),
                "bid": l.get("bid"),
                "ask": l.get("ask"),
                "mid": l.get("mid"),
            }
            for l in legs
        ],
        "gates": [{"name": g.name, "passed": g.passed, "detail": g.detail, "evaluability": g.evaluability} for g in gates],
    }


def _best_of(chain: OptionChain, structure: str, wmin: int, wmax: int) -> Dict[str, Any]:
    best = None
    attempts = []
    for w in range(int(wmin), int(wmax) + 1):
        built = build_structure(chain, structure, D_MIN, D_MAX, w)
        if not built.get("ok"):
            attempts.append({"width_steps": w, "error": built.get("error")})
            continue
        scored = _eval_built(chain, built, d_min=D_MIN, d_max=D_MAX)
        scored["credit_pct_width"] = scored.get("credit_pct_width")
        attempts.append({"width_steps": w, "credit_pct_width": scored.get("credit_pct_width"), "first_failing_gate": scored.get("first_failing_gate")})
        if best is None or (scored.get("credit_pct_width") or -1) > (best.get("credit_pct_width") or -1):
            best = scored
    if best is None:
        return {"ok": False, "structure": structure, "error": "no_structure", "attempts": attempts}
    best["attempts"] = attempts
    return best


def scan_latest_delta_window(*, live: bool = False) -> Dict[str, Any]:
    """Each in-scope snapshot expiry: best condor + best single-side in 10–16 delta."""
    ensure_tarang_tables()
    today = datetime.now(timezone.utc).date()
    rows_out = []
    fail_counts: Dict[str, int] = {}
    if live:
        from backend.services.tarang.chain_builder import ChainBuilder

        b = ChainBuilder()
        chains = []
        for pid in ("BTC", "ETH"):
            for exp in b.snapshot_expiries(pid):
                chains.append(b.build(pid, expiry=exp))
        sources = []
        for chain in chains:
            if not chain.quotes:
                continue
            sources.append((chain, None))
    else:
        db = SessionLocal()
        try:
            snaps = db.execute(
                text(
                    """
                    SELECT DISTINCT ON (profile_id, expiry_date)
                           id, captured_at, profile_id, venue, underlying_symbol, expiry_date,
                           futures_or_spot, n_quotes, payload_gzip, meta
                    FROM tarang_chain_snapshots
                    WHERE venue = 'delta_india'
                      AND COALESCE(meta->>'window_kind','') = 'delta_window'
                    ORDER BY profile_id, expiry_date, captured_at DESC
                    """
                )
            ).mappings().all()
        finally:
            db.close()
        sources = []
        for s in snaps:
            try:
                payload = unpack_chain_payload(s["payload_gzip"])
            except Exception:
                continue
            exp = str(s["expiry_date"]) if s.get("expiry_date") else str(payload.get("expiry") or "")
            chain = _chain_from_payload(
                s["profile_id"],
                s["venue"],
                s["underlying_symbol"],
                exp,
                payload,
                s.get("futures_or_spot") or payload.get("underlying_price"),
                payload.get("lot_size"),
                payload.get("strike_step"),
            )
            sources.append((chain, s))

    for chain, snap in sources:
        try:
            exp_d = date.fromisoformat(str(chain.expiry)[:10])
            dte = (exp_d - today).days
        except ValueError:
            dte = None
        prof = (get_profiles().get("profiles") or {}).get(chain.profile_id) or {}
        wmin = int(prof.get("width_steps_min") or 2)
        wmax = int(prof.get("width_steps_max") or 4)
        condor = _best_of(chain, "iron_condor", wmin, wmax)
        pcs = _best_of(chain, "put_credit_spread", wmin, wmax)
        ccs = _best_of(chain, "call_credit_spread", wmin, wmax)
        singles = [x for x in (pcs, ccs) if x.get("ok")]
        best_side = max(singles, key=lambda x: x.get("credit_pct_width") or -1) if singles else (pcs if pcs.get("ok") else ccs)
        for blob in (condor, best_side):
            g = blob.get("first_failing_gate")
            if g:
                fail_counts[g] = fail_counts.get(g, 0) + 1
        rows_out.append(
            {
                "profile_id": chain.profile_id,
                "underlying": chain.underlying,
                "expiry": chain.expiry,
                "dte": dte,
                "futures_or_spot": chain.futures_or_spot,
                "n_quotes": len(chain.quotes),
                "captured_at": chain.built_at if not snap else (snap["captured_at"].isoformat() if snap.get("captured_at") else None),
                "delta_band": [D_MIN, D_MAX],
                "condor": condor,
                "put_credit_spread": pcs,
                "call_credit_spread": ccs,
                "best_single_side": best_side.get("structure") if best_side.get("ok") else None,
            }
        )
    top = sorted(fail_counts.items(), key=lambda kv: -kv[1])
    return {
        "ok": True,
        "source": "live_chain" if live else "latest_delta_window_snapshots",
        "delta_band": "10-16 (short |δ| 0.10–0.16; live profile, not switched)",
        "n": len(rows_out),
        "rows": rows_out,
        "first_failing_gate_counts": dict(top),
        "blocks_most": top[0][0] if top else None,
        "note": "Read-only. Screener defaults unchanged. QUALIFIED still requires live 3–7 DTE crypto profile.",
    }
