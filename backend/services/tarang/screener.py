"""Kosmic Tarang Phase 2 screener — gates, rejections, candidates."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.chain_builder import ChainBuilder
from backend.services.tarang.config import crypto_budget, energy_budget, get_events, get_profiles, get_risk
from backend.services.tarang.gates import (
    GateResult,
    aggregate_status,
    compute_iv_percentile,
    decide_structure_from_skew,
    gate_credit_fraction,
    gate_event_blackout,
    gate_expiry_dte,
    gate_iv_percentile,
    gate_iv_vs_rv,
    gate_liquidity,
    gate_portfolio_limit,
    gate_short_delta,
    gate_sizing,
    gate_stale_data,
)
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.structures import (
    build_structure,
    estimate_25d_ivs,
    floor_units,
    max_loss_per_unit_inr,
)

logger = logging.getLogger(__name__)


def _atm_iv(chain) -> Optional[float]:
    F = chain.futures_or_spot
    qs = [q for q in chain.quotes if q.iv]
    if not qs:
        return None
    if F:
        return min(qs, key=lambda q: abs(q.strike - F)).iv
    return qs[0].iv


def _load_iv_history(db, profile_id: str, limit: int = 120) -> List[float]:
    rows = db.execute(
        text(
            """
            SELECT atm_iv FROM tarang_iv_snapshots
            WHERE profile_id = :p AND atm_iv IS NOT NULL
            ORDER BY captured_at DESC
            LIMIT :lim
            """
        ),
        {"p": profile_id, "lim": limit},
    ).mappings().all()
    return [float(r["atm_iv"]) for r in rows]


def _latest_rv(db, profile_id: str) -> Optional[float]:
    row = db.execute(
        text(
            """
            SELECT realized_vol_20d FROM tarang_iv_snapshots
            WHERE profile_id = :p AND realized_vol_20d IS NOT NULL
            ORDER BY captured_at DESC LIMIT 1
            """
        ),
        {"p": profile_id},
    ).mappings().first()
    if not row:
        return None
    return float(row["realized_vol_20d"])


def _calendar_events(db) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT event_id AS id, label, starts_at, applies_to, blackout_hours_before
            FROM tarang_event_calendar
            WHERE starts_at > NOW() - INTERVAL '1 day'
            ORDER BY starts_at
            LIMIT 100
            """
        )
    ).mappings().all()
    out = []
    for r in rows:
        applies = r["applies_to"]
        if isinstance(applies, str):
            try:
                applies = json.loads(applies)
            except Exception:
                applies = []
        out.append(
            {
                "id": r["id"],
                "label": r["label"],
                "starts_at": r["starts_at"].isoformat() if r["starts_at"] else None,
                "applies_to": applies or [],
                "blackout_hours_before": float(r["blackout_hours_before"] or 2.5),
            }
        )
    return out


def _open_risk_for_bucket(db, bucket: str) -> float:
    row = db.execute(
        text(
            """
            SELECT COALESCE(SUM(max_loss * COALESCE(lots_or_contracts, 1)), 0) AS risk
            FROM tarang_trades
            WHERE risk_bucket = :b AND status IN ('open', 'ticket_taken', 'in_trade')
            """
        ),
        {"b": bucket},
    ).mappings().first()
    return float((row or {}).get("risk") or 0)


def _persist_rejection(db, profile_id: str, gate: GateResult, context: Dict[str, Any]) -> None:
    db.execute(
        text(
            """
            INSERT INTO tarang_rejections (profile_id, gate_name, detail, payload)
            VALUES (:p, :g, :d, CAST(:payload AS jsonb))
            """
        ),
        {
            "p": profile_id,
            "g": gate.name,
            "d": gate.detail[:500],
            "payload": json.dumps({"gate": gate.to_dict(), **context}),
        },
    )


def _persist_candidate(db, profile_id: str, structure: str, status: str, payload: Dict[str, Any]) -> int:
    row = db.execute(
        text(
            """
            INSERT INTO tarang_candidates (profile_id, structure, status, payload)
            VALUES (:p, :s, :st, CAST(:payload AS jsonb))
            RETURNING id
            """
        ),
        {"p": profile_id, "s": structure, "st": status, "payload": json.dumps(payload)},
    ).mappings().first()
    return int(row["id"])


def screen_profile(profile_id: str, builder: Optional[ChainBuilder] = None) -> Dict[str, Any]:
    ensure_tarang_tables()
    profiles = get_profiles().get("profiles") or {}
    prof = profiles.get(profile_id) or profiles.get(profile_id.upper())
    if not prof or not prof.get("enabled"):
        return {
            "profile_id": profile_id,
            "status": "BLOCKED",
            "gates": [],
            "detail": "profile disabled or unknown",
        }

    risk = get_risk()
    events_cfg = get_events()
    bucket = prof.get("risk_bucket") or "ENERGY"
    budget = energy_budget() if bucket == "ENERGY" else crypto_budget()
    per_trade = float(budget.get("per_trade_budget_inr") or 0)
    portfolio_cap = float(budget.get("portfolio_limit_inr") or 0)

    builder = builder or ChainBuilder()
    # Wider window for 10–16δ shorts
    chain = builder.build(profile_id, atm_window=14)

    gates: List[GateResult] = []
    db = SessionLocal()
    try:
        hist = _load_iv_history(db, profile_id)
        snap_n = len(hist)
        atm = _atm_iv(chain)
        percentile = compute_iv_percentile(list(reversed(hist)), atm)
        rv = _latest_rv(db, profile_id)
        # Fallback: if RV missing, treat IV-vs-RV as fail but note assumption
        if rv is None and atm is not None:
            # use a synthetic proxy from history stdev * sqrt — skip; leave None
            pass

        cal = _calendar_events(db)
        if not cal:
            # no dated events in DB — blackout clear (templates in events.default.json are recurrence-only)
            cal = []

        gates.append(
            gate_stale_data(chain.built_at, max_age_sec=180.0)
            if not (chain.meta or {}).get("error")
            else GateResult(
                name="stale_data",
                passed=False,
                detail=str((chain.meta or {}).get("error")),
                status_hint="BLOCKED",
            )
        )

        dte_min = prof.get("expiry_min_dte") or prof.get("expiry_dte_min")
        dte_max = prof.get("expiry_dte_max")
        gates.append(
            gate_expiry_dte(
                chain.expiry,
                None,
                min_dte=int(dte_min) if dte_min is not None else None,
                max_dte=int(dte_max) if dte_max is not None else None,
            )
        )

        gates.append(
            gate_event_blackout(
                profile_id,
                bucket,
                cal,
                default_hours_before=float(events_cfg.get("blackout_hours_before") or 2.5),
            )
        )

        gates.append(
            gate_iv_percentile(
                percentile,
                snap_n,
                min_percentile=float(risk.get("iv_percentile_min") or 50),
                min_snapshots=int(risk.get("iv_percentile_min_snapshots") or 60),
            )
        )
        gates.append(
            gate_iv_vs_rv(
                atm,
                rv,
                relative_min=float(risk.get("iv_vs_rv_relative_min") or 0.10),
            )
        )

        skew = estimate_25d_ivs(chain)
        struct_dec = decide_structure_from_skew(skew.get("put_iv_25d"), skew.get("call_iv_25d"))
        width_steps = int(prof.get("width_steps_min") or 2)
        d_min = float(prof.get("short_delta_min") or 0.10)
        d_max = float(prof.get("short_delta_max") or 0.16)

        built = build_structure(
            chain,
            struct_dec["structure"],
            d_min,
            d_max,
            width_steps,
        )

        if not built.get("ok"):
            gates.append(
                GateResult(
                    name="structure",
                    passed=False,
                    actual=built.get("error"),
                    detail=f"could not build {struct_dec['structure']}: {built.get('error')}",
                    status_hint="WATCHING",
                )
            )
            status = aggregate_status(gates)
            payload = {
                "status": status,
                "gates": [g.to_dict() for g in gates],
                "skew": skew,
                "structure_decision": struct_dec,
                "chain_meta": chain.meta,
                "evaluated_at": datetime.now(timezone.utc).isoformat(),
            }
            for g in gates:
                if not g.passed:
                    _persist_rejection(db, profile_id, g, {"status": status})
            cand_id = _persist_candidate(db, profile_id, struct_dec["structure"], status.lower(), payload)
            db.commit()
            return {"profile_id": profile_id, "candidate_id": cand_id, **payload}

        legs = built["legs"]
        gates.append(
            gate_liquidity(
                legs,
                max_spread_pct=float(risk.get("liquidity_max_spread_pct_of_mid") or 15),
            )
        )
        for sd in built.get("short_deltas") or []:
            gates.append(gate_short_delta(sd, d_min, d_max))

        gates.append(
            gate_credit_fraction(
                built.get("net_credit"),
                built.get("width"),
                min_frac=float(risk.get("min_credit_fraction_of_width") or 0.20),
            )
        )

        ml = max_loss_per_unit_inr(
            float(built["width"]),
            float(built["net_credit"]),
            lot_size=built.get("lot_size") or chain.lot_size,
            contract_value=built.get("contract_value"),
            venue=chain.venue,
        )
        units = floor_units(per_trade, ml)
        gates.append(gate_sizing(ml, per_trade, units))

        candidate_risk = float(ml or 0) * max(units, 0)
        open_risk = _open_risk_for_bucket(db, bucket)
        gates.append(gate_portfolio_limit(open_risk, candidate_risk if units >= 1 else 0, portfolio_cap))

        status = aggregate_status(gates)
        max_profit = float(built["net_credit"]) * (
            float(built.get("lot_size") or chain.lot_size or 0)
            if chain.venue != "delta_india"
            else float(built.get("contract_value") or 0) * 83.0
        ) * max(units, 1)

        exit_levels = {
            "profit_take_frac": float(risk.get("profit_take_frac_of_credit") or 0.40),
            "credit_stop_multiple": float(risk.get("credit_stop_multiple") or 2.0),
            "delta_stop_min": float(risk.get("delta_stop_min") or 0.30),
            "delta_stop_max": float(risk.get("delta_stop_max") or 0.35),
        }

        payload = {
            "status": status,
            "gates": [g.to_dict() for g in gates],
            "skew": skew,
            "structure_decision": struct_dec,
            "structure": built.get("structure"),
            "legs": legs,
            "net_credit": built.get("net_credit"),
            "width": built.get("width"),
            "width_steps": width_steps,
            "max_loss_per_unit_inr": ml,
            "lots_or_contracts": units,
            "candidate_risk_inr": candidate_risk,
            "budget_inr": per_trade,
            "max_profit_approx_inr": max_profit if units >= 1 else None,
            "exit_levels": exit_levels,
            "expiry": built.get("expiry"),
            "underlying": built.get("underlying"),
            "venue": chain.venue,
            "futures_or_spot": chain.futures_or_spot,
            "iv_percentile": percentile,
            "atm_iv": atm,
            "rv_20d": rv,
            "snapshot_count": snap_n,
            "qualified_reasons": [g.name for g in gates if g.passed],
            "failed_gates": [g.name for g in gates if not g.passed],
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "mode_default": "PAPER",
            "auto_default": False,
        }

        for g in gates:
            if not g.passed:
                _persist_rejection(db, profile_id, g, {"status": status, "structure": built.get("structure")})

        cand_status = "qualified" if status == "QUALIFIED" else status.lower()
        cand_id = _persist_candidate(db, profile_id, built.get("structure") or struct_dec["structure"], cand_status, payload)
        db.commit()
        return {"profile_id": profile_id, "candidate_id": cand_id, **payload}
    except Exception as e:
        logger.exception("screen_profile %s failed", profile_id)
        db.rollback()
        return {"profile_id": profile_id, "status": "BLOCKED", "error": str(e)[:300]}
    finally:
        db.close()


def run_screener(profile_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    ensure_tarang_tables()
    profiles = get_profiles().get("profiles") or {}
    if profile_ids is None:
        profile_ids = [k for k, v in profiles.items() if v.get("enabled")]
    builder = ChainBuilder()
    results = []
    for pid in profile_ids:
        results.append(screen_profile(pid, builder=builder))
    return {
        "product": "Kosmic Tarang",
        "phase": 2,
        "run_at": datetime.now(timezone.utc).isoformat(),
        "mode": "PAPER",
        "auto": False,
        "results": results,
    }


def list_recent_candidates(limit: int = 40) -> List[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, created_at, profile_id, structure, status, payload
                FROM tarang_candidates
                ORDER BY id DESC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).mappings().all()
        out = []
        for r in rows:
            payload = r["payload"] if isinstance(r["payload"], dict) else {}
            out.append(
                {
                    "id": r["id"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "profile_id": r["profile_id"],
                    "structure": r["structure"],
                    "status": r["status"],
                    "screen_status": payload.get("status"),
                    "payload": payload,
                }
            )
        return out
    finally:
        db.close()


def list_rejections(limit: int = 50, profile_id: Optional[str] = None) -> List[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        if profile_id:
            rows = db.execute(
                text(
                    """
                    SELECT id, created_at, profile_id, gate_name, detail, payload
                    FROM tarang_rejections
                    WHERE profile_id = :p
                    ORDER BY id DESC LIMIT :lim
                    """
                ),
                {"p": profile_id, "lim": limit},
            ).mappings().all()
        else:
            rows = db.execute(
                text(
                    """
                    SELECT id, created_at, profile_id, gate_name, detail, payload
                    FROM tarang_rejections
                    ORDER BY id DESC LIMIT :lim
                    """
                ),
                {"lim": limit},
            ).mappings().all()
        return [
            {
                "id": r["id"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "profile_id": r["profile_id"],
                "gate_name": r["gate_name"],
                "detail": r["detail"],
                "payload": r["payload"] if isinstance(r["payload"], dict) else {},
            }
            for r in rows
        ]
    finally:
        db.close()


def get_candidate(candidate_id: int) -> Optional[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        r = db.execute(
            text(
                """
                SELECT id, created_at, profile_id, structure, status, payload
                FROM tarang_candidates WHERE id = :id
                """
            ),
            {"id": candidate_id},
        ).mappings().first()
        if not r:
            return None
        payload = r["payload"] if isinstance(r["payload"], dict) else {}
        return {
            "id": r["id"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "profile_id": r["profile_id"],
            "structure": r["structure"],
            "status": r["status"],
            "payload": payload,
        }
    finally:
        db.close()
