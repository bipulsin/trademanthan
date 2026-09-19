"""Auto-record QUALIFIED screener rows as forward-test trades (always on)."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.code_rev import code_commit, rule_set_version
from backend.services.tarang.config import get_events, get_risk
from backend.services.tarang.gates import gate_event_blackout, gate_portfolio_limit
from backend.services.tarang.labels import FILL_SIMULATED, RECORD_FORWARD_TEST
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.signal_key import canonical_signal_key

logger = logging.getLogger(__name__)


def forward_test_cfg() -> Dict[str, Any]:
    risk = get_risk()
    cfg = dict(risk.get("forward_test") or {})
    cfg.setdefault("cooldown_hours", 6)
    cfg.setdefault(
        "notes",
        "After a forward-test cycle closes, the same signal_key cannot reopen until cooldown_hours elapse (default 6h, same session-ish).",
    )
    return cfg


def build_immutable_snapshot(payload: Dict[str, Any], *, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    legs = []
    for lg in payload.get("legs") or []:
        legs.append(
            {
                "side": lg.get("side"),
                "right": lg.get("right"),
                "strike": lg.get("strike"),
                "symbol": lg.get("symbol") or lg.get("instrument_key"),
                "instrument_key": lg.get("instrument_key"),
                "bid": lg.get("bid"),
                "ask": lg.get("ask"),
                "mid": lg.get("mid"),
                "qty": lg.get("qty") or lg.get("quantity"),
                "oi": lg.get("oi"),
                "volume": lg.get("volume"),
                "iv": lg.get("iv"),
                "delta": lg.get("delta"),
            }
        )
    snap = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "legs": legs,
        "atm_iv": payload.get("atm_iv"),
        "iv_percentile": payload.get("iv_percentile"),
        "iv_warmup": bool(payload.get("iv_warmup") or (payload.get("gates") or {}).get("iv_percentile", {}).get("status_hint") == "WARMING_UP"),
        "iv_vs_rv": payload.get("iv_vs_rv"),
        "skew": payload.get("skew") or payload.get("skew_25d"),
        "lots_or_contracts": payload.get("lots_or_contracts"),
        "budget_inr": payload.get("budget_inr"),
        "candidate_risk_inr": payload.get("candidate_risk_inr"),
        "rule_set_version": rule_set_version(),
        "code_commit": code_commit(),
        "underlying": payload.get("underlying"),
        "expiry": payload.get("expiry"),
        "structure": payload.get("structure"),
        "venue": payload.get("venue"),
    }
    if extra:
        snap.update(extra)
    return snap


def persist_skipped(
    db,
    *,
    signal_key: Optional[str],
    profile_id: Optional[str],
    reason: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO tarang_skipped_signals (signal_key, profile_id, reason, payload)
            VALUES (:k, :p, :r, CAST(:payload AS jsonb))
            """
        ),
        {"k": signal_key, "p": profile_id, "r": reason, "payload": json.dumps(payload or {})},
    )


def _open_risk_inr(db, bucket: str) -> float:
    row = db.execute(
        text(
            """
            SELECT COALESCE(SUM(COALESCE(max_loss,0) * COALESCE(lots_or_contracts,1)), 0) AS risk
            FROM tarang_trades
            WHERE status IN ('ENTRY_PENDING', 'IN_TRADE', 'EXIT_PENDING', 'open')
              AND risk_bucket = :b
            """
        ),
        {"b": bucket},
    ).mappings().first()
    return float((row or {}).get("risk") or 0)


def record_forward_tests_from_screen(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Always-on: every QUALIFIED row becomes a forward-test unless duplicate/cooldown/limits."""
    ensure_tarang_tables()
    recorded: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    from backend.services.tarang.lifecycle import take_trade_paper, _kill_switch_tripped

    cfg = forward_test_cfg()
    cooldown_h = float(cfg.get("cooldown_hours") or 6)
    events = (get_events().get("events") or get_events()) if isinstance(get_events(), dict) else []
    if isinstance(events, dict):
        events = events.get("events") or []

    for row in results or []:
        status = str(row.get("status") or "").upper().replace(" ", "_")
        if status != "QUALIFIED":
            continue
        cid = row.get("candidate_id")
        if not cid:
            skipped.append({"reason": "no_candidate_id", "profile_id": row.get("profile_id")})
            continue
        payload = dict(row)
        legs = payload.get("legs") or []
        sk = canonical_signal_key(
            underlying=payload.get("underlying"),
            expiry=payload.get("expiry"),
            structure=payload.get("structure") or row.get("structure"),
            legs=legs,
        )
        db = SessionLocal()
        try:
            open_row = db.execute(
                text(
                    """
                    SELECT id FROM tarang_trades
                    WHERE signal_key = :k AND record_type = 'FORWARD_TEST'
                      AND status IN ('ENTRY_PENDING', 'IN_TRADE', 'EXIT_PENDING', 'open')
                    LIMIT 1
                    """
                ),
                {"k": sk},
            ).mappings().first()
            if open_row:
                skipped.append({"reason": "already_open", "signal_key": sk, "trade_id": open_row["id"]})
                db.close()
                continue
            closed = db.execute(
                text(
                    """
                    SELECT exit_at, updated_at FROM tarang_trades
                    WHERE signal_key = :k AND record_type = 'FORWARD_TEST'
                      AND status IN ('CLOSED', 'REPORTED')
                    ORDER BY COALESCE(exit_at, updated_at) DESC
                    LIMIT 1
                    """
                ),
                {"k": sk},
            ).mappings().first()
            if closed:
                ts = closed.get("exit_at") or closed.get("updated_at")
                if ts is not None:
                    if getattr(ts, "tzinfo", None) is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    if datetime.now(timezone.utc) - ts < timedelta(hours=cooldown_h):
                        persist_skipped(db, signal_key=sk, profile_id=row.get("profile_id"), reason="cooldown", payload={"signal_key": sk})
                        db.commit()
                        skipped.append({"reason": "cooldown", "signal_key": sk})
                        db.close()
                        continue
            if _kill_switch_tripped(db):
                persist_skipped(db, signal_key=sk, profile_id=row.get("profile_id"), reason="kill_switch", payload={"signal_key": sk})
                db.commit()
                skipped.append({"reason": "kill_switch", "signal_key": sk})
                db.close()
                continue
            bucket = "CRYPTO" if str(payload.get("venue") or "") == "delta_india" else "ENERGY"
            from backend.services.tarang.config import crypto_budget, energy_budget

            cap = float((crypto_budget() if bucket == "CRYPTO" else energy_budget()).get("portfolio_limit_inr") or 0)
            cand_risk = float(payload.get("candidate_risk_inr") or payload.get("max_loss_per_unit_inr") or 0)
            port = gate_portfolio_limit(_open_risk_inr(db, bucket), cand_risk, cap)
            if not port.passed:
                persist_skipped(db, signal_key=sk, profile_id=row.get("profile_id"), reason="portfolio_cap", payload=port.to_dict())
                db.commit()
                skipped.append({"reason": "portfolio_cap", "signal_key": sk})
                db.close()
                continue
            bl = gate_event_blackout(str(row.get("profile_id") or ""), bucket, events)
            if not bl.passed:
                persist_skipped(db, signal_key=sk, profile_id=row.get("profile_id"), reason="event_blackout", payload=bl.to_dict())
                db.commit()
                skipped.append({"reason": "event_blackout", "signal_key": sk})
                db.close()
                continue
        except Exception:
            db.rollback()
            logger.exception("forward-test precheck failed")
            skipped.append({"reason": "precheck_error", "profile_id": row.get("profile_id")})
            db.close()
            continue
        db.close()
        holding = "POSITIONAL" if (payload.get("venue") or "") != "delta_india" else "INTRADAY"
        out = take_trade_paper(
            int(cid),
            note="forward_test_auto",
            actor="SYSTEM",
            holding_mode=holding,
            record_type=RECORD_FORWARD_TEST,
            fill_source=FILL_SIMULATED,
            signal_key=sk,
            snapshot=build_immutable_snapshot(payload),
        )
        if out.get("ok"):
            recorded.append({"candidate_id": cid, "trade_id": out.get("trade_id"), "signal_key": sk})
        else:
            db2 = SessionLocal()
            try:
                persist_skipped(
                    db2,
                    signal_key=sk,
                    profile_id=row.get("profile_id"),
                    reason=str(out.get("error") or "take_failed"),
                    payload=out,
                )
                db2.commit()
            finally:
                db2.close()
            skipped.append({"reason": out.get("error"), "signal_key": sk})
    return {
        "ok": True,
        "recorded": recorded,
        "skipped": skipped,
        "record_type": RECORD_FORWARD_TEST,
        "fill_source": FILL_SIMULATED,
    }
