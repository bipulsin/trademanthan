"""Trade lifecycle: PAPER take → IN_TRADE → exit → CLOSED → REPORTED."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.calendar import mcx_feed_closed, to_ist
from backend.services.tarang.config import get_profiles, get_risk
from backend.services.tarang.events import append_event, create_alert, list_alerts, list_events
from backend.services.tarang.exit_engine import evaluate_exits, net_greeks_from_legs
from backend.services.tarang.labels import (
    FILL_SIMULATED,
    FILL_USER,
    RECORD_FORWARD_TEST,
    RECORD_LIVE,
    decorate_trade,
)
from backend.services.tarang.paper_broker import mark_to_market, simulate_entry_fills, simulate_exit_fills
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.screener import get_candidate
from backend.services.tarang.signal_key import canonical_signal_key
from backend.services.tarang.state_machine import (
    CLOSED,
    DISMISSED,
    ENTRY_PENDING,
    EXIT_PENDING,
    IN_TRADE,
    InvalidTransition,
    OPEN_STATUSES,
    QUALIFIED,
    REPORTED,
    normalize_status,
)

logger = logging.getLogger(__name__)


def _json_load(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return {}
    return v or {}


def _settings_mode(db) -> str:
    row = db.execute(text("SELECT value FROM tarang_settings WHERE key = 'mode'")).mappings().first()
    if not row:
        return "PAPER"
    v = row["value"]
    if isinstance(v, str):
        return v.strip('"') or "PAPER"
    return str(v or "PAPER")


def _settings_auto(db) -> bool:
    row = db.execute(text("SELECT value FROM tarang_settings WHERE key = 'auto'")).mappings().first()
    if not row:
        return False
    v = row["value"]
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def set_paper_auto(enabled: bool) -> Dict[str, Any]:
    """AUTO may only be on in PAPER. LIVE stays admin-gated and never auto."""
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        mode = _settings_mode(db)
        if enabled:
            return {
                "ok": False,
                "error": "auto_orders_locked",
                "mode": mode,
                "auto": False,
                "display_auto": "Auto orders: locked",
            }
        db.execute(
            text(
                """
                INSERT INTO tarang_settings (key, value) VALUES ('auto', CAST(:v AS jsonb))
                ON CONFLICT (key) DO UPDATE SET value = CAST(:v AS jsonb)
                """
            ),
            {"v": "true" if enabled else "false"},
        )
        db.commit()
        return {
            "ok": True,
            "mode": mode or "PAPER",
            "auto": False,
            "display_mode": "Forward test",
            "display_auto": "Auto orders: locked",
        }
    finally:
        db.close()


def get_mode_auto() -> Dict[str, Any]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        from backend.services.tarang.labels import auto_lock_label, mode_badge

        mode = _settings_mode(db)
        return {
            "mode": mode,
            "auto": False,
            "display_mode": mode_badge(mode),
            "display_auto": auto_lock_label(),
        }
    finally:
        db.close()


def _row_to_trade(r) -> Dict[str, Any]:
    d = dict(r)
    for k in ("legs", "meta"):
        d[k] = _json_load(d.get(k))
    for ts in ("created_at", "updated_at", "entry_at", "exit_at"):
        if d.get(ts) is not None:
            d[ts] = d[ts].isoformat()
    return decorate_trade(d)


def get_trade(trade_id: int) -> Optional[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        r = db.execute(text("SELECT * FROM tarang_trades WHERE id = :id"), {"id": trade_id}).mappings().first()
        return _row_to_trade(r) if r else None
    finally:
        db.close()


def list_open_trades(mode: Optional[str] = None) -> List[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        if mode:
            rows = db.execute(
                text(
                    """
                    SELECT * FROM tarang_trades
                    WHERE mode = :mode AND status = ANY(:statuses)
                    ORDER BY id DESC
                    """
                ),
                {"mode": mode, "statuses": list(OPEN_STATUSES)},
            ).mappings().all()
        else:
            rows = db.execute(
                text(
                    """
                    SELECT * FROM tarang_trades
                    WHERE status = ANY(:statuses)
                    ORDER BY id DESC
                    """
                ),
                {"statuses": list(OPEN_STATUSES)},
            ).mappings().all()
        return [_row_to_trade(r) for r in rows]
    finally:
        db.close()


def list_closed_trades(mode: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        if mode:
            rows = db.execute(
                text(
                    """
                    SELECT * FROM tarang_trades
                    WHERE mode = :mode AND status IN ('CLOSED', 'REPORTED')
                    ORDER BY COALESCE(exit_at, updated_at) DESC
                    LIMIT :lim
                    """
                ),
                {"mode": mode, "lim": limit},
            ).mappings().all()
        else:
            rows = db.execute(
                text(
                    """
                    SELECT * FROM tarang_trades
                    WHERE status IN ('CLOSED', 'REPORTED')
                    ORDER BY COALESCE(exit_at, updated_at) DESC
                    LIMIT :lim
                    """
                ),
                {"lim": limit},
            ).mappings().all()
        return [_row_to_trade(r) for r in rows]
    finally:
        db.close()


def _persist_fills(
    db,
    trade_id: int,
    fills: List[Dict[str, Any]],
    phase: str,
    *,
    record_type: str = RECORD_FORWARD_TEST,
    fill_source: str = FILL_SIMULATED,
) -> None:
    for f in fills:
        db.execute(
            text(
                """
                INSERT INTO tarang_fills (
                    trade_id, leg_index, side, symbol, qty, price, fees, phase, raw,
                    record_type, fill_source
                ) VALUES (
                    :trade_id, :leg_index, :side, :symbol, :qty, :price, :fees, :phase,
                    CAST(:raw AS jsonb), :record_type, :fill_source
                )
                """
            ),
            {
                "trade_id": trade_id,
                "leg_index": f.get("leg_index"),
                "side": f.get("side"),
                "symbol": f.get("symbol"),
                "qty": f.get("qty"),
                "price": f.get("price"),
                "fees": float(f.get("fees") or 0),
                "phase": phase,
                "raw": json.dumps(f),
                "record_type": record_type,
                "fill_source": fill_source,
            },
        )
        coid = f.get("client_order_id")
        if coid:
            exists = db.execute(
                text("SELECT 1 FROM tarang_orders WHERE client_order_id = :coid LIMIT 1"),
                {"coid": coid},
            ).first()
            if not exists:
                db.execute(
                    text(
                        """
                        INSERT INTO tarang_orders (trade_id, client_order_id, venue, side, status, raw)
                        VALUES (:tid, :coid, 'paper', :side, 'filled', CAST(:raw AS jsonb))
                        """
                    ),
                    {
                        "tid": trade_id,
                        "coid": coid,
                        "side": f.get("side"),
                        "raw": json.dumps(f),
                    },
                )


def _load_entry_fills(db, trade_id: int, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = db.execute(
        text("SELECT raw FROM tarang_fills WHERE trade_id = :tid AND phase = 'entry' ORDER BY id"),
        {"tid": trade_id},
    ).mappings().all()
    if rows:
        out = []
        for r in rows:
            raw = _json_load(r["raw"])
            out.append(raw if isinstance(raw, dict) else {})
        return out
    return list((meta.get("entry_fills") or []))


def take_trade_paper(
    candidate_id: int,
    note: str = "",
    actor: str = "USER",
    holding_mode: str = "INTRADAY",
    *,
    record_type: str = RECORD_FORWARD_TEST,
    fill_source: str = FILL_SIMULATED,
    signal_key: Optional[str] = None,
    snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """QUALIFIED → ENTRY_PENDING → simulated fills → IN_TRADE. Never places live orders."""
    ensure_tarang_tables()
    cand = get_candidate(candidate_id)
    if not cand:
        return {"ok": False, "error": "candidate_not_found"}
    p = cand.get("payload") or {}
    status = str(p.get("status") or "").upper().replace(" ", "_")
    db_status = str(cand.get("status") or "").lower()
    if status != QUALIFIED and db_status not in ("qualified",):
        return {"ok": False, "error": "candidate_not_qualified", "status": p.get("status") or cand.get("status")}
    units = int(p.get("lots_or_contracts") or 0)
    if units < 1:
        return {"ok": False, "error": "sizing_zero"}
    legs = list(p.get("legs") or [])
    if not legs:
        return {"ok": False, "error": "no_legs"}

    profiles = get_profiles().get("profiles") or {}
    prof = profiles.get(cand["profile_id"]) or {}
    bucket = prof.get("risk_bucket") or "ENERGY"
    venue = p.get("venue") or ("delta_india" if bucket == "CRYPTO" else "upstox_mcx")
    lot_size = None
    contract_value = None
    for leg in legs:
        if leg.get("lot_size"):
            lot_size = leg.get("lot_size")
        if leg.get("contract_value"):
            contract_value = leg.get("contract_value")
    lot_size = lot_size or p.get("lot_size")
    contract_value = contract_value or p.get("contract_value")
    holding = str(holding_mode or "INTRADAY").upper()
    if holding not in ("INTRADAY", "POSITIONAL"):
        holding = "INTRADAY"
    sk = signal_key or canonical_signal_key(
        underlying=p.get("underlying"),
        expiry=p.get("expiry"),
        structure=cand.get("structure") or p.get("structure"),
        legs=legs,
    )
    from backend.services.tarang.forward_record import build_immutable_snapshot

    snap = snapshot or build_immutable_snapshot(p)

    db = SessionLocal()
    try:
        actor_u = str(actor or "USER").upper()
        if actor_u not in ("USER", "AUTO", "SYSTEM"):
            actor_u = "USER"
        if actor_u == "AUTO":
            return {"ok": False, "error": "auto_orders_locked", "display_auto": "Auto orders: locked"}

        fill_res = simulate_entry_fills(
            legs,
            units=units,
            venue=venue,
            lot_size=float(lot_size) if lot_size else None,
            contract_value=float(contract_value) if contract_value else None,
        )
        if not fill_res.get("ok"):
            db.execute(
                text(
                    """
                    INSERT INTO tarang_orders (client_order_id, venue, side, status, raw)
                    VALUES (:coid, :venue, 'ENTRY', 'REJECTED', CAST(:raw AS jsonb))
                    """
                ),
                {
                    "coid": f"tarang-paper-reject-{uuid.uuid4().hex[:16]}",
                    "venue": venue,
                    "raw": json.dumps(fill_res),
                },
            )
            create_alert(
                db,
                f"Paper entry rejected for candidate {candidate_id}: {fill_res.get('error')}",
                level="warn",
                meta=fill_res,
            )
            db.commit()
            return {
                "ok": False,
                "error": fill_res.get("error") or "entry_rejected",
                "detail": fill_res,
                "fill_status": fill_res.get("fill_status") or "REJECTED",
            }

        now = datetime.now(timezone.utc)
        meta = {
            "candidate_id": candidate_id,
            "source": "take_trade_paper",
            "note": note,
            "exit_levels": p.get("exit_levels"),
            "entry_fills": fill_res["fills"],
            "entry_credit_pts": fill_res["net_credit_pts"],
            "multiplier": fill_res["multiplier"],
            "lot_size": lot_size,
            "contract_value": contract_value,
            "max_profit_approx_inr": p.get("max_profit_approx_inr"),
            "budget_inr": p.get("budget_inr"),
            "atm_iv_entry": p.get("atm_iv"),
            "underlying": p.get("underlying"),
            "expiry": p.get("expiry"),
            "taken_at": now.isoformat(),
            "phase": 3,
            "fees_entry_inr": fill_res.get("fees_inr"),
        }
        row = db.execute(
            text(
                """
                INSERT INTO tarang_trades (
                    profile_id, risk_bucket, mode, holding_mode, status,
                    auto_managed, entry_credit, max_loss, lots_or_contracts, legs, meta,
                    candidate_id, venue, structure, entry_at, fees_total,
                    entry_iv_percentile, notes, currency, entry_debit_to_close, origin,
                    record_type, fill_source, signal_key, snapshot_immutable
                ) VALUES (
                    :profile_id, :risk_bucket, 'PAPER', :holding_mode, :status,
                    :auto_managed, :entry_credit, :max_loss, :lots, CAST(:legs AS jsonb), CAST(:meta AS jsonb),
                    :candidate_id, :venue, :structure, :entry_at, :fees,
                    :iv_pct, :notes, 'INR', :entry_credit, :origin,
                    :record_type, :fill_source, :signal_key, CAST(:snapshot AS jsonb)
                )
                RETURNING id
                """
            ),
            {
                "profile_id": cand["profile_id"],
                "risk_bucket": bucket,
                "holding_mode": holding,
                "status": ENTRY_PENDING,
                "auto_managed": actor_u in ("AUTO", "SYSTEM"),
                "origin": "AUTO" if actor_u in ("AUTO", "SYSTEM") else "USER",
                "entry_credit": fill_res["net_credit_pts"],
                "max_loss": p.get("max_loss_per_unit_inr"),
                "lots": units,
                "legs": json.dumps(legs),
                "meta": json.dumps(meta),
                "candidate_id": candidate_id,
                "venue": venue,
                "structure": cand.get("structure") or p.get("structure"),
                "entry_at": now,
                "fees": fill_res.get("fees_inr") or 0,
                "iv_pct": p.get("iv_percentile"),
                "notes": note or None,
                "record_type": record_type or RECORD_FORWARD_TEST,
                "fill_source": fill_source or FILL_SIMULATED,
                "signal_key": sk,
                "snapshot": json.dumps(snap),
            },
        ).mappings().first()
        trade_id = int(row["id"])
        db.execute(
            text(
                """
                INSERT INTO tarang_signal_snapshots (trade_id, signal_key, snapshot)
                VALUES (:tid, :k, CAST(:snap AS jsonb))
                """
            ),
            {"tid": trade_id, "k": sk, "snap": json.dumps(snap)},
        )
        append_event(
            db,
            trade_id=trade_id,
            candidate_id=candidate_id,
            from_status=QUALIFIED,
            to_status=ENTRY_PENDING,
            actor=actor_u,
            event_type="take_trade",
            payload={"mode": "PAPER", "record_type": record_type, "note": note},
        )
        _persist_fills(
            db,
            trade_id,
            fill_res["fills"],
            "entry",
            record_type=record_type or RECORD_FORWARD_TEST,
            fill_source=fill_source or FILL_SIMULATED,
        )
        # ENTRY_PENDING → IN_TRADE (paper fills immediate)
        db.execute(
            text(
                """
                UPDATE tarang_trades
                SET status = :st, updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"st": IN_TRADE, "id": trade_id},
        )
        append_event(
            db,
            trade_id=trade_id,
            candidate_id=candidate_id,
            from_status=ENTRY_PENDING,
            to_status=IN_TRADE,
            actor="SYSTEM",
            event_type="simulated_fill_entry",
            payload={"fills": fill_res["fills"], "net_credit_pts": fill_res["net_credit_pts"]},
        )
        db.execute(
            text("UPDATE tarang_candidates SET status = 'taken' WHERE id = :id"),
            {"id": candidate_id},
        )
        create_alert(
            db,
            f"Forward test #{trade_id} entered ({cand['profile_id']})",
            level="info",
            trade_id=trade_id,
            meta={"candidate_id": candidate_id},
        )
        db.commit()
        return {
            "ok": True,
            "trade_id": trade_id,
            "mode": "PAPER",
            "display_mode": "Forward test",
            "record_type": record_type or RECORD_FORWARD_TEST,
            "fill_source": fill_source or FILL_SIMULATED,
            "signal_key": sk,
            "status": IN_TRADE,
            "entry_credit_pts": fill_res["net_credit_pts"],
            "fees_inr": fill_res.get("fees_inr"),
            "message": "Forward test fills simulated. No live orders placed.",
        }
    except InvalidTransition as e:
        db.rollback()
        return {"ok": False, "error": str(e)}
    except Exception as e:
        db.rollback()
        logger.exception("take_trade_paper failed")
        return {"ok": False, "error": str(e)[:300]}
    finally:
        db.close()


def dismiss_candidate(candidate_id: int, actor: str = "USER") -> Dict[str, Any]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        r = db.execute(
            text("UPDATE tarang_candidates SET status = 'dismissed' WHERE id = :id RETURNING id, status"),
            {"id": candidate_id},
        ).mappings().first()
        if not r:
            return {"ok": False, "error": "candidate_not_found"}
        append_event(
            db,
            trade_id=None,
            candidate_id=candidate_id,
            from_status=QUALIFIED,
            to_status=DISMISSED,
            actor=actor,
            event_type="dismiss_candidate",
            payload={},
            enforce_transition=False,
        )
        db.commit()
        return {"ok": True, "candidate_id": candidate_id, "status": DISMISSED}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)[:200]}
    finally:
        db.close()


def mark_taken_manual(
    candidate_id: int,
    fills: Optional[Dict[str, Any]] = None,
    note: str = "",
) -> Dict[str, Any]:
    """Operator entered fills manually — still PAPER, goes IN_TRADE."""
    ensure_tarang_tables()
    cand = get_candidate(candidate_id)
    if not cand:
        return {"ok": False, "error": "candidate_not_found"}
    p = dict(cand.get("payload") or {})
    # Prefer paper broker path when fills empty
    if not fills:
        return take_trade_paper(candidate_id, note=note or "manual", actor="USER")
    # Otherwise record with provided credit
    note2 = (note or "") + " [manual fills]"
    out = take_trade_paper(candidate_id, note=note2, actor="USER")
    if out.get("ok") and fills.get("entry_credit") is not None:
        db = SessionLocal()
        try:
            db.execute(
                text("UPDATE tarang_trades SET entry_credit = :c, updated_at = NOW() WHERE id = :id"),
                {"c": fills["entry_credit"], "id": out["trade_id"]},
            )
            db.commit()
        finally:
            db.close()
    return out


def record_live_user_trade(
    candidate_id: int,
    *,
    fills: List[Dict[str, Any]],
    note: str = "",
    holding_mode: str = "INTRADAY",
) -> Dict[str, Any]:
    """User placed at broker; store USER_ENTERED fills only. Never sends orders."""
    ensure_tarang_tables()
    cand = get_candidate(candidate_id)
    if not cand:
        return {"ok": False, "error": "candidate_not_found"}
    p = cand.get("payload") or {}
    legs = list(p.get("legs") or [])
    if not fills:
        return {"ok": False, "error": "fills_required"}
    units = int(p.get("lots_or_contracts") or 1)
    venue = p.get("venue") or "upstox_mcx"
    credit = 0.0
    for f in fills:
        side = str(f.get("side") or "").upper()
        px = float(f.get("price") or 0)
        credit += -px if side == "BUY" else px
    now = datetime.now(timezone.utc)
    sk = canonical_signal_key(
        underlying=p.get("underlying"),
        expiry=p.get("expiry"),
        structure=cand.get("structure") or p.get("structure"),
        legs=legs,
    )
    from backend.services.tarang.forward_record import build_immutable_snapshot

    snap = build_immutable_snapshot(p, extra={"user_entered": True})
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                INSERT INTO tarang_trades (
                    profile_id, risk_bucket, mode, holding_mode, status,
                    auto_managed, entry_credit, max_loss, lots_or_contracts, legs, meta,
                    candidate_id, venue, structure, entry_at, origin,
                    record_type, fill_source, signal_key, snapshot_immutable, notes, currency
                ) VALUES (
                    :profile_id, :risk_bucket, 'LIVE', :holding_mode, :status,
                    FALSE, :entry_credit, :max_loss, :lots, CAST(:legs AS jsonb), CAST(:meta AS jsonb),
                    :candidate_id, :venue, :structure, :entry_at, 'USER',
                    :record_type, :fill_source, :signal_key, CAST(:snapshot AS jsonb), :notes, 'INR'
                )
                RETURNING id
                """
            ),
            {
                "profile_id": cand["profile_id"],
                "risk_bucket": (get_profiles().get("profiles") or {}).get(cand["profile_id"], {}).get("risk_bucket")
                or "ENERGY",
                "holding_mode": str(holding_mode or "INTRADAY").upper(),
                "status": IN_TRADE,
                "entry_credit": credit,
                "max_loss": p.get("max_loss_per_unit_inr"),
                "lots": units,
                "legs": json.dumps(legs),
                "meta": json.dumps(
                    {
                        "entry_fills": fills,
                        "source": "user_broker",
                        "note": note,
                        "expiry": p.get("expiry"),
                        "underlying": p.get("underlying"),
                    }
                ),
                "candidate_id": candidate_id,
                "venue": venue,
                "structure": cand.get("structure") or p.get("structure"),
                "entry_at": now,
                "record_type": RECORD_LIVE,
                "fill_source": FILL_USER,
                "signal_key": sk,
                "snapshot": json.dumps(snap),
                "notes": note or None,
            },
        ).mappings().first()
        trade_id = int(row["id"])
        _persist_fills(db, trade_id, fills, "entry", record_type=RECORD_LIVE, fill_source=FILL_USER)
        append_event(
            db,
            trade_id=trade_id,
            candidate_id=candidate_id,
            from_status=QUALIFIED,
            to_status=IN_TRADE,
            actor="USER",
            event_type="live_user_entered",
            payload={"fills": fills},
            enforce_transition=False,
        )
        db.commit()
        return {
            "ok": True,
            "trade_id": trade_id,
            "record_type": RECORD_LIVE,
            "fill_source": FILL_USER,
            "display_mode": "Live",
            "message": "Live fill recorded. No broker order was sent.",
            "placed": False,
        }
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)[:300]}
    finally:
        db.close()


def _refresh_quotes_for_trade(trade: Dict[str, Any]) -> List[Dict[str, Any]]:
    bundle = _refresh_quote_bundle(trade)
    return list(bundle.get("quotes") or [])


def _refresh_quote_bundle(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Live chain quotes plus freshness metadata. Never synthesizes prices."""
    meta = trade.get("meta") or {}
    profile_id = trade.get("profile_id")
    expiry = meta.get("expiry")
    try:
        from backend.services.tarang.chain_builder import ChainBuilder

        chain = ChainBuilder().build(profile_id, expiry=expiry)
        quotes = [q.to_dict() for q in (chain.quotes or [])]
        return {
            "quotes": quotes,
            "built_at": chain.built_at,
            "underlying_price": chain.futures_or_spot,
            "error": (chain.meta or {}).get("error"),
            "ok": bool(quotes) and not (chain.meta or {}).get("error"),
        }
    except Exception as e:
        logger.warning("quote refresh failed trade=%s: %s", trade.get("id"), e)
        return {"quotes": [], "built_at": None, "underlying_price": None, "error": str(e)[:200], "ok": False}


def build_in_trade_view(trade_id: int, *, refresh_quotes: bool = True) -> Dict[str, Any]:
    ensure_tarang_tables()
    trade = get_trade(trade_id)
    if not trade:
        return {"ok": False, "error": "trade_not_found"}
    meta = trade.get("meta") or {}
    legs = trade.get("legs") or []
    units = int(trade.get("lots_or_contracts") or 1)
    venue = trade.get("venue") or meta.get("venue") or "upstox_mcx"
    db = SessionLocal()
    try:
        entry_fills = _load_entry_fills(db, trade_id, meta)
        bundle = _refresh_quote_bundle(trade) if refresh_quotes else {"quotes": [], "ok": False, "built_at": None, "underlying_price": None}
        live = bundle.get("quotes") or []
        quotes_fresh = False
        quotes_status = "missing"
        stale_sec = float((get_risk().get("quote_stale_sec") or 180))
        if venue == "upstox_mcx" and mcx_feed_closed():
            quotes_status = "mcx_closed"
        elif live and bundle.get("ok"):
            age = None
            if bundle.get("built_at"):
                try:
                    bt = datetime.fromisoformat(str(bundle["built_at"]).replace("Z", "+00:00"))
                    if bt.tzinfo is None:
                        bt = bt.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - bt).total_seconds()
                except Exception:
                    age = None
            if age is None or age <= stale_sec:
                quotes_fresh = True
                quotes_status = "live"
            else:
                quotes_status = "stale"
        else:
            quotes_status = "stale" if live else "missing"
        # MTM may use last live quotes when present; never evaluate exits on stale/closed.
        mtm_quotes = live if live else None
        mtm = mark_to_market(
            legs,
            entry_fills,
            units=units,
            venue=venue,
            lot_size=meta.get("lot_size"),
            contract_value=meta.get("contract_value"),
            live_quotes=mtm_quotes,
        )
        short_deltas = []
        for pl in mtm.get("per_leg") or []:
            if str(pl.get("side_open") or "").upper() == "SELL":
                short_deltas.append(pl.get("delta"))
        max_profit = meta.get("max_profit_approx_inr")
        if max_profit is None and trade.get("entry_credit") and mtm.get("multiplier"):
            max_profit = float(trade["entry_credit"]) * float(mtm["multiplier"]) * units
        max_loss = None
        if trade.get("max_loss") is not None:
            max_loss = float(trade["max_loss"]) * units
        eval_ = None
        skip_exits = quotes_status in ("stale", "mcx_closed", "missing")
        if not skip_exits:
            eval_ = evaluate_exits(
                entry_credit_pts=float(mtm.get("entry_credit_pts") or trade.get("entry_credit") or 0),
                debit_to_close_pts=float(mtm.get("debit_to_close_pts") or 0),
                unrealized_pnl_inr=float(mtm.get("unrealized_pnl_inr") or 0),
                max_profit_inr=max_profit,
                max_loss_inr=max_loss,
                budget_inr=meta.get("budget_inr") or max_loss,
                short_deltas=short_deltas,
                entry_atm_iv=meta.get("atm_iv_entry"),
                current_atm_iv=None,
                venue=venue,
                profile_id=trade.get("profile_id") or "",
                risk_bucket=trade.get("risk_bucket") or "ENERGY",
                holding_mode=trade.get("holding_mode") or "INTRADAY",
                expiry=meta.get("expiry"),
                exit_levels=meta.get("exit_levels"),
            )
        greeks = net_greeks_from_legs(mtm.get("per_leg") or [], units)
        pnl = float(mtm.get("unrealized_pnl_inr") or 0)
        pct_max_profit = (pnl / max_profit * 100.0) if max_profit else None
        pct_max_loss = (abs(min(pnl, 0)) / max_loss * 100.0) if max_loss else None
        # Prefer last/LTP when inside bid-ask; else mid + marker
        for pl in mtm.get("per_leg") or []:
            last = pl.get("last") or pl.get("ltp")
            bid, ask = pl.get("bid"), pl.get("ask")
            mid = pl.get("mid")
            mark_src = "mid"
            mark = mid
            if last is not None:
                try:
                    lf = float(last)
                    if bid is not None and ask is not None and float(bid) <= lf <= float(ask):
                        mark, mark_src = lf, "ltp"
                    elif bid is None or ask is None:
                        mark, mark_src = lf, "ltp"
                    else:
                        mark, mark_src = mid if mid is not None else lf, "mid_fallback"
                except (TypeError, ValueError):
                    pass
            pl["mark"] = mark
            pl["mark_source"] = mark_src
            pl["ltp"] = last
        alerts = list_alerts(db, trade_id=trade_id, limit=20)
        gap = None
        new_meta = dict(meta)
        if quotes_status == "live" and venue == "upstox_mcx":
            prev = meta.get("last_underlying_price")
            cur = bundle.get("underlying_price")
            if prev is not None and cur is not None and meta.get("last_quote_status") == "mcx_closed":
                gap = {"previous": prev, "current": cur, "abs": abs(float(cur) - float(prev))}
                append_event(
                    db,
                    trade_id=trade_id,
                    from_status=IN_TRADE,
                    to_status=IN_TRADE,
                    actor="SYSTEM",
                    event_type="gap_at_open",
                    payload=gap,
                    enforce_transition=False,
                )
        new_meta["last_quote_status"] = quotes_status
        if bundle.get("underlying_price") is not None:
            new_meta["last_underlying_price"] = bundle.get("underlying_price")
        db.execute(
            text("UPDATE tarang_trades SET meta = CAST(:meta AS jsonb), updated_at = NOW() WHERE id = :id"),
            {"meta": json.dumps(new_meta), "id": trade_id},
        )
        db.commit()
        return {
            "ok": True,
            "product": "Kosmic Tarang",
            "mode": trade.get("mode") or "PAPER",
            "holding_mode": trade.get("holding_mode") or "INTRADAY",
            "trade": trade,
            "mtm": mtm,
            "pnl_inr": pnl,
            "pct_of_max_profit": pct_max_profit,
            "pct_of_max_loss": pct_max_loss,
            "net_greeks": greeks,
            "exit_eval": eval_.to_dict() if eval_ else None,
            "exit_reason_preview": (eval_.closest.reason if eval_ and eval_.closest else None),
            "skip_exits": skip_exits,
            "quotes_status": quotes_status,
            "gap_at_open": gap,
            "alerts": alerts,
            "events": list_events(db, trade_id, limit=50),
            "fills_confirmed": bool(trade.get("fills_confirmed")),
            "unconfirmed": str(trade.get("record_type") or "").upper() == RECORD_LIVE and not bool(trade.get("fills_confirmed")),
            "void_until_sec": None,
        }
    finally:
        db.close()


def exit_trade(
    trade_id: int,
    *,
    reason: str = "MANUAL",
    actor: str = "USER",
    note: str = "",
    refresh_quotes: bool = True,
    fills: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """IN_TRADE → EXIT_PENDING → paper exit fills → CLOSED → REPORTED."""
    ensure_tarang_tables()
    trade = get_trade(trade_id)
    if not trade:
        return {"ok": False, "error": "trade_not_found"}
    st = normalize_status(trade.get("status"))
    if st not in OPEN_STATUSES and st != IN_TRADE:
        if st in (CLOSED, REPORTED):
            return {"ok": False, "error": "already_closed", "status": st}
        return {"ok": False, "error": "not_in_trade", "status": st}
    if str(trade.get("mode") or "").upper() == "LIVE" and str(trade.get("record_type") or "").upper() == "LIVE":
        # User must supply exit fills separately; system never places.
        pass
    elif str(trade.get("mode") or "").upper() != "PAPER":
        return {"ok": False, "error": "live_exit_disabled", "detail": "Broker exit is locked; record user exit fills."}

    meta = dict(trade.get("meta") or {})
    legs = trade.get("legs") or []
    units = int(trade.get("lots_or_contracts") or 1)
    venue = trade.get("venue") or "upstox_mcx"

    db = SessionLocal()
    try:
        entry_fills = _load_entry_fills(db, trade_id, meta)
        bundle = _refresh_quote_bundle(trade) if refresh_quotes else {"quotes": [], "ok": False}
        live = bundle.get("quotes") or []
        if venue == "upstox_mcx" and mcx_feed_closed() and not fills:
            return {"ok": False, "error": "mcx_closed", "quotes_status": "mcx_closed"}
        if (not live or not bundle.get("ok")) and not fills:
            return {
                "ok": False,
                "error": "exit_deferred_stale_or_missing_quote",
                "quotes_status": "stale",
            }
        if fills:
            exit_res = {
                "ok": True,
                "fills": fills,
                "fees_inr": float(sum(float(f.get("fees") or 0) for f in fills)),
                "gross_pnl_inr": None,
            }
            # Gross from entry vs exit prices when both present
            entry_by = {int(f.get("leg_index") or i): f for i, f in enumerate(entry_fills)}
            gross = 0.0
            for i, f in enumerate(fills):
                ef = entry_by.get(int(f.get("leg_index") if f.get("leg_index") is not None else i), {})
                ep = float(ef.get("price") or 0)
                xp = float(f.get("price") or 0)
                side = str(ef.get("side") or f.get("side") or "").upper()
                qty = float(f.get("qty") or ef.get("qty") or 1)
                # credit spread: sold high, buy back lower is profit
                if side == "SELL":
                    gross += (ep - xp) * qty
                else:
                    gross += (xp - ep) * qty
            # Convert pts-ish via existing mtm multiplier if available
            exit_res["gross_pnl_inr"] = gross
        else:
            exit_res = simulate_exit_fills(
                legs,
                entry_fills,
                units=units,
                venue=venue,
                lot_size=meta.get("lot_size"),
                contract_value=meta.get("contract_value"),
                live_quotes=live,
            )
        if not exit_res.get("ok"):
            return {
                "ok": False,
                "error": exit_res.get("error") or "exit_rejected",
                "detail": exit_res,
                "fill_status": exit_res.get("fill_status") or "REJECTED",
            }
        entry_fees = float(trade.get("fees_total") or meta.get("fees_entry_inr") or 0)
        exit_fees = float(exit_res.get("fees_inr") or 0)
        fees_total = entry_fees + exit_fees
        gross = float(exit_res.get("gross_pnl_inr") or 0)
        net = gross - fees_total

        db.execute(
            text("UPDATE tarang_trades SET status = :st, updated_at = NOW() WHERE id = :id"),
            {"st": EXIT_PENDING, "id": trade_id},
        )
        append_event(
            db,
            trade_id=trade_id,
            from_status=st,
            to_status=EXIT_PENDING,
            actor=actor,
            event_type="exit_requested",
            payload={"reason": reason, "note": note},
        )
        _persist_fills(
            db,
            trade_id,
            exit_res["fills"],
            "exit",
            record_type=str(trade.get("record_type") or RECORD_FORWARD_TEST),
            fill_source=FILL_SIMULATED if str(trade.get("record_type") or RECORD_FORWARD_TEST) == RECORD_FORWARD_TEST else FILL_USER,
        )
        now = datetime.now(timezone.utc)
        meta["exit_fills"] = exit_res["fills"]
        meta["exit_note"] = note
        db.execute(
            text(
                """
                UPDATE tarang_trades SET
                    status = :st,
                    exit_reason = :reason,
                    exit_at = :exit_at,
                    gross_pnl = :gross,
                    net_pnl = :net,
                    fees_total = :fees,
                    exit_debit_to_close = :debit,
                    notes = CASE WHEN :note = '' THEN notes ELSE COALESCE(notes,'') || E'\n' || :note END,
                    meta = CAST(:meta AS jsonb),
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "st": CLOSED,
                "reason": reason,
                "exit_at": now,
                "gross": gross,
                "net": net,
                "fees": fees_total,
                "debit": exit_res.get("debit_to_close_pts"),
                "note": note or "",
                "meta": json.dumps(meta),
                "id": trade_id,
            },
        )
        append_event(
            db,
            trade_id=trade_id,
            from_status=EXIT_PENDING,
            to_status=CLOSED,
            actor=actor if actor != "SYSTEM" else "SYSTEM",
            event_type="simulated_fill_exit",
            payload={
                "reason": reason,
                "gross_pnl": gross,
                "net_pnl": net,
                "fees": fees_total,
                "fills": exit_res["fills"],
            },
        )
        # Auto-report
        db.execute(
            text("UPDATE tarang_trades SET status = :st, updated_at = NOW() WHERE id = :id"),
            {"st": REPORTED, "id": trade_id},
        )
        append_event(
            db,
            trade_id=trade_id,
            from_status=CLOSED,
            to_status=REPORTED,
            actor="SYSTEM",
            event_type="reported",
            payload={},
        )
        create_alert(
            db,
            f"Trade #{trade_id} closed ({reason}) net ₹{net:,.0f}",
            level="warn" if net < 0 else "info",
            trade_id=trade_id,
            meta={"reason": reason, "net_pnl": net},
        )
        db.commit()
        return {
            "ok": True,
            "trade_id": trade_id,
            "status": REPORTED,
            "exit_reason": reason,
            "gross_pnl": gross,
            "net_pnl": net,
            "fees_total": fees_total,
        }
    except InvalidTransition as e:
        db.rollback()
        return {"ok": False, "error": str(e)}
    except Exception as e:
        db.rollback()
        logger.exception("exit_trade failed")
        return {"ok": False, "error": str(e)[:300]}
    finally:
        db.close()


def exit_all_open(*, reason: str = "MANUAL", actor: str = "USER") -> Dict[str, Any]:
    results = []
    for t in list_open_trades():
        if normalize_status(t.get("status")) in (IN_TRADE, ENTRY_PENDING, EXIT_PENDING):
            results.append(exit_trade(int(t["id"]), reason=reason, actor=actor))
    return {"ok": True, "results": results}


def add_trade_note(trade_id: int, note: str, actor: str = "USER") -> Dict[str, Any]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        r = db.execute(
            text(
                """
                UPDATE tarang_trades
                SET notes = COALESCE(notes,'') || E'\n' || :note, updated_at = NOW()
                WHERE id = :id
                RETURNING id
                """
            ),
            {"id": trade_id, "note": note},
        ).mappings().first()
        if not r:
            return {"ok": False, "error": "trade_not_found"}
        append_event(
            db,
            trade_id=trade_id,
            from_status=None,
            to_status=IN_TRADE,
            actor=actor,
            event_type="note",
            payload={"note": note},
            enforce_transition=False,
        )
        db.commit()
        return {"ok": True, "trade_id": trade_id}
    finally:
        db.close()


def run_exit_engine_once(venues: Optional[List[str]] = None) -> Dict[str, Any]:
    """Evaluate all open forward-test trades; flatten when a trigger fires.

    Never evaluates exits on stale quotes. MCX closed period (23:30–09:00 IST,
    weekends, holidays) is skipped; overnight gap is logged at the next open.
    """
    ensure_tarang_tables()
    venue_filter = {str(v) for v in venues} if venues else None
    outcomes = []
    from backend.services.tarang.heartbeat import beat

    beat("exit_engine", {"venues": venues})
    for t in list_open_trades():
        if str(t.get("record_type") or RECORD_FORWARD_TEST).upper() != RECORD_FORWARD_TEST:
            continue
        tid = int(t["id"])
        venue = t.get("venue") or (t.get("meta") or {}).get("venue") or "upstox_mcx"
        if venue_filter and venue not in venue_filter:
            continue
        if normalize_status(t.get("status")) != IN_TRADE:
            continue
        view = build_in_trade_view(tid, refresh_quotes=True)
        if not view.get("ok"):
            outcomes.append({"trade_id": tid, "error": view.get("error")})
            continue
        if view.get("skip_exits"):
            if view.get("quotes_status") == "stale":
                db = SessionLocal()
                try:
                    from backend.services.tarang.alerts_telegram import notify_critical

                    notify_critical(
                        db,
                        kind="stale_feed",
                        message=f"Trade #{tid} ExitEngine skipped — stale/missing quotes ({venue})",
                        trade_id=tid,
                        dedupe_key=f"stale_feed:{tid}",
                    )
                    db.commit()
                finally:
                    db.close()
            if view.get("gap_at_open"):
                logger.info("tarang gap_at_open trade=%s %s", tid, view.get("gap_at_open"))
            outcomes.append(
                {
                    "trade_id": tid,
                    "should_exit": False,
                    "skipped": view.get("quotes_status"),
                    "gap_at_open": view.get("gap_at_open"),
                }
            )
            continue
        ev = view.get("exit_eval") or {}
        if ev.get("should_exit") and ev.get("reason"):
            out = exit_trade(tid, reason=ev["reason"], actor="SYSTEM", note="ExitEngine")
            db = SessionLocal()
            try:
                from backend.services.tarang.alerts_telegram import notify_critical

                notify_critical(
                    db,
                    kind="exit_trigger",
                    message=f"Trade #{tid} exit trigger {ev['reason']}",
                    trade_id=tid,
                    dedupe_key=f"exit_trigger:{tid}:{ev['reason']}",
                    meta={"reason": ev["reason"]},
                )
                db.commit()
            finally:
                db.close()
            outcomes.append(out)
        else:
            closest = (ev.get("closest") or {})
            prox = float(get_risk().get("alert_proximity_frac") or 0.15)
            if closest and float(closest.get("distance_frac") or 1) <= prox:
                db = SessionLocal()
                try:
                    create_alert(
                        db,
                        f"Trade #{tid} near {closest.get('reason')}: {closest.get('detail')}",
                        level="warn",
                        trade_id=tid,
                        meta=closest,
                    )
                    db.commit()
                finally:
                    db.close()
            outcomes.append({"trade_id": tid, "should_exit": False, "closest": closest.get("reason")})
    return {"ok": True, "checked": len(outcomes), "outcomes": outcomes}


def run_hard_exit_warnings() -> Dict[str, Any]:
    from backend.services.tarang.alerts_telegram import notify_critical
    from backend.services.tarang.exit_engine import hard_exit_applies, warning_datetime

    now = datetime.now(timezone.utc)
    alerts = []
    for t in list_open_trades():
        venue = t.get("venue") or "upstox_mcx"
        meta = t.get("meta") or {}
        if not hard_exit_applies(
            venue,
            holding_mode=t.get("holding_mode") or "INTRADAY",
            expiry=meta.get("expiry"),
            now=now,
        ):
            continue
        warn_at = warning_datetime(venue, now=now)
        secs = (warn_at - now.astimezone(warn_at.tzinfo)).total_seconds()
        if -60 <= secs <= 60:
            db = SessionLocal()
            try:
                out = notify_critical(
                    db,
                    kind="hard_exit_warning",
                    message=f"Hard-exit warning for trade #{t['id']} ({venue})",
                    trade_id=int(t["id"]),
                    dedupe_key=f"hard_exit_warning:{t['id']}:{to_ist(now).date().isoformat()}",
                    meta={"venue": venue},
                )
                db.commit()
                alerts.append(out.get("alert_id"))
            finally:
                db.close()
    return {"ok": True, "alerts": alerts}


def run_hard_exit_flatten() -> Dict[str, Any]:
    """Flatten PAPER trades only when the venue hard-exit rule applies."""
    from backend.services.tarang.exit_engine import hard_exit_applies, hard_exit_datetime

    now = datetime.now(timezone.utc)
    results = []
    for t in list_open_trades():
        venue = t.get("venue") or "upstox_mcx"
        meta = t.get("meta") or {}
        holding = t.get("holding_mode") or "INTRADAY"
        if not hard_exit_applies(venue, holding_mode=holding, expiry=meta.get("expiry"), now=now):
            continue
        hard = hard_exit_datetime(
            venue,
            holding_mode=holding,
            expiry=meta.get("expiry"),
            now=now,
        )
        if hard is not None and now.astimezone(hard.tzinfo) >= hard:
            results.append(
                exit_trade(int(t["id"]), reason="HARD_EXIT", actor="SYSTEM", note="scheduler hard exit")
            )
    return {"ok": True, "results": results}


def check_feed_alerts() -> Dict[str, Any]:
    """Stale feed / kill-switch. Token expiry is 08:45/09:00/09:05 IST only (never 03:30 JWT expiry)."""
    ensure_tarang_tables()
    from backend.services.tarang.alerts_telegram import notify_critical
    from backend.services.tarang.chain_snapshots import chain_coverage
    from backend.services.tarang.token_check import should_alert_token

    fired = []
    db = SessionLocal()
    try:
        if should_alert_token():
            pass  # token alerts belong to token_check jobs, not this 10-minute tick
        cov = chain_coverage()
        now = datetime.now(timezone.utc)
        for row in cov.get("underlyings") or []:
            latest = row.get("latest")
            if not latest:
                continue
            try:
                lt = datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
                if lt.tzinfo is None:
                    lt = lt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            age_min = (now - lt).total_seconds() / 60.0
            us = row.get("underlying")
            limit = 90 if us in ("BTC", "ETH") else 180
            if us in ("BTC", "ETH") and age_min > limit:
                notify_critical(
                    db,
                    kind="stale_feed",
                    message=f"Delta full-chain snapshot for {us} is {age_min:.0f}m old",
                    dedupe_key=f"stale_feed:{us}",
                )
                fired.append(f"stale:{us}")
        # Kill switch: consecutive closed losses
        kill_n = int(get_risk().get("kill_consecutive_losses") or 3)
        closed = db.execute(
            text(
                """
                SELECT net_pnl FROM tarang_trades
                WHERE status IN ('CLOSED', 'REPORTED') AND mode = 'PAPER'
                ORDER BY COALESCE(exit_at, updated_at) DESC
                LIMIT :n
                """
            ),
            {"n": kill_n},
        ).mappings().all()
        if len(closed) >= kill_n and all(float(r["net_pnl"] or 0) < 0 for r in closed):
            notify_critical(
                db,
                kind="kill_switch",
                message=f"Kill switch: {kill_n} consecutive forward-test losses",
                dedupe_key="kill_switch_consecutive",
            )
            fired.append("kill_switch")
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("check_feed_alerts failed")
    finally:
        db.close()
    return {"ok": True, "fired": fired}


def _kill_switch_tripped(db) -> bool:
    from backend.services.tarang.config import get_risk

    kill_n = int(get_risk().get("kill_consecutive_losses") or 3)
    closed = db.execute(
        text(
            """
            SELECT net_pnl FROM tarang_trades
            WHERE status IN ('CLOSED', 'REPORTED') AND mode = 'PAPER'
            ORDER BY COALESCE(exit_at, updated_at) DESC
            LIMIT :n
            """
        ),
        {"n": kill_n},
    ).mappings().all()
    return len(closed) >= kill_n and all(float(r["net_pnl"] or 0) < 0 for r in closed)


def run_auto_paper_once() -> Dict[str, Any]:
    """Scheduled screener tick: always record forward tests. Auto live orders stay locked."""
    from backend.services.tarang.heartbeat import beat
    from backend.services.tarang.screener import run_screener

    beat("scheduler", {"job": "forward_test_tick"})
    screen = run_screener()
    return {
        "ok": True,
        "display_auto": "Auto orders: locked",
        "screened": len(screen.get("results") or []),
        "forward_tests": screen.get("forward_tests") or {},
        "taken": (screen.get("forward_tests") or {}).get("recorded") or [],
    }


def list_open_user_trades() -> List[Dict[str, Any]]:
    """Trades the operator started (Trade button / Live / unconfirmed). Not auto forward-tests."""
    rows = []
    for t in list_open_trades():
        if t.get("voided"):
            continue
        rt = str(t.get("record_type") or "").upper()
        orig = str(t.get("origin") or "").upper()
        if rt == RECORD_FORWARD_TEST and orig in ("AUTO", "SYSTEM"):
            continue
        if orig == "USER" or rt == RECORD_LIVE:
            rows.append(t)
    return rows


def _open_user_profiles() -> set:
    return {str(t.get("profile_id") or "").upper() for t in list_open_user_trades()}


def start_user_trade(
    candidate_id: int,
    *,
    note: str = "",
    holding_mode: str = "INTRADAY",
) -> Dict[str, Any]:
    """Start at suggested prices. Unconfirmed fills until Edit save or tick. No broker send."""
    cand = get_candidate(candidate_id)
    if not cand:
        return {"ok": False, "error": "candidate_not_found"}
    p = cand.get("payload") or {}
    status = str(p.get("status") or "").upper().replace(" ", "_")
    if status != QUALIFIED and str(cand.get("status") or "").lower() not in ("qualified",):
        return {"ok": False, "error": "candidate_not_qualified", "reason": "Not qualified"}
    pid = str(cand.get("profile_id") or "").upper()
    if pid in _open_user_profiles():
        return {"ok": False, "error": "active_user_trade", "reason": "You already have an active trade in this symbol"}
    out = take_trade_paper(
        candidate_id,
        note=note or "user_trade_suggested",
        actor="USER",
        holding_mode=holding_mode,
        record_type=RECORD_LIVE,
        fill_source=FILL_USER,
    )
    if not out.get("ok"):
        return out
    tid = int(out["trade_id"])
    linked = None
    db = SessionLocal()
    try:
        ft = db.execute(
            text(
                """
                SELECT id FROM tarang_trades
                WHERE record_type = 'FORWARD_TEST'
                  AND status = ANY(:st)
                  AND profile_id = :p
                  AND COALESCE(voided, false) = false
                ORDER BY id DESC LIMIT 1
                """
            ),
            {"st": list(OPEN_STATUSES), "p": pid},
        ).mappings().first()
        if ft:
            linked = int(ft["id"])
        db.execute(
            text(
                """
                UPDATE tarang_trades
                SET fills_confirmed = false, linked_ft_trade_id = :ft, origin = 'USER',
                    record_type = 'LIVE', fill_source = 'USER_ENTERED', mode = 'LIVE',
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": tid, "ft": linked},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        logger.exception("start_user_trade flag update failed")
        return {"ok": False, "error": str(e)[:200]}
    finally:
        db.close()
    out["fills_confirmed"] = False
    out["record_type"] = RECORD_LIVE
    out["display_mode"] = "Live"
    out["linked_ft_trade_id"] = linked
    out["confirm_fills_banner"] = "Confirm your actual fills."
    out["message"] = "Started at suggested prices. Confirm your actual fills."
    return out


def confirm_or_edit_fills(
    trade_id: int,
    *,
    fills: Optional[List[Dict[str, Any]]] = None,
    lots: Optional[int] = None,
    fees: Optional[float] = None,
    notes: Optional[str] = None,
    legs: Optional[List[Dict[str, Any]]] = None,
    confirm: bool = True,
) -> Dict[str, Any]:
    """Timestamped overrides. Original snapshot stays immutable."""
    trade = get_trade(trade_id)
    if not trade:
        return {"ok": False, "error": "trade_not_found"}
    if trade.get("voided"):
        return {"ok": False, "error": "voided"}
    db = SessionLocal()
    try:
        meta = dict(trade.get("meta") or {})
        prev = {
            "entry_fills": meta.get("entry_fills"),
            "lots": trade.get("lots_or_contracts"),
            "fees_total": trade.get("fees_total"),
            "legs": trade.get("legs"),
            "notes": trade.get("notes"),
        }
        hist = list(meta.get("fill_override_history") or [])
        hist.append({"at": datetime.now(timezone.utc).isoformat(), "previous": prev})
        meta["fill_override_history"] = hist[-20:]
        if fills:
            meta["entry_fills"] = fills
            credit = 0.0
            for f in fills:
                side = str(f.get("side") or "").upper()
                px = float(f.get("price") or 0)
                credit += -px if side == "BUY" else px
            db.execute(text("UPDATE tarang_trades SET entry_credit = :c WHERE id = :id"), {"c": credit, "id": trade_id})
            db.execute(text("DELETE FROM tarang_fills WHERE trade_id = :id AND phase = 'entry'"), {"id": trade_id})
            _persist_fills(db, trade_id, fills, "entry", record_type=RECORD_LIVE, fill_source=FILL_USER)
        if lots is not None:
            db.execute(
                text("UPDATE tarang_trades SET lots_or_contracts = :n WHERE id = :id"),
                {"n": int(lots), "id": trade_id},
            )
        if fees is not None:
            db.execute(text("UPDATE tarang_trades SET fees_total = :f WHERE id = :id"), {"f": float(fees), "id": trade_id})
            meta["fees_entry_inr"] = float(fees)
        if notes is not None:
            db.execute(text("UPDATE tarang_trades SET notes = :n WHERE id = :id"), {"n": notes, "id": trade_id})
        if legs is not None:
            db.execute(
                text("UPDATE tarang_trades SET legs = CAST(:l AS jsonb) WHERE id = :id"),
                {"l": json.dumps(legs), "id": trade_id},
            )
        warn = []
        max_loss = trade.get("max_loss")
        from backend.services.tarang.config import energy_budget, crypto_budget

        cap = float((crypto_budget() if trade.get("risk_bucket") == "CRYPTO" else energy_budget()).get("per_trade_budget_inr") or 0)
        units = int(lots if lots is not None else trade.get("lots_or_contracts") or 1)
        if max_loss is not None and cap and float(max_loss) * units > cap:
            warn.append("Risk may exceed the per-trade cap after this edit.")
        if legs is not None and len(legs) < 2:
            warn.append("Legs may no longer be defined-risk.")
        meta["fills_confirmed"] = bool(confirm)
        db.execute(
            text(
                """
                UPDATE tarang_trades
                SET fills_confirmed = :c, meta = CAST(:m AS jsonb), fill_overrides = CAST(:m AS jsonb), updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"c": bool(confirm), "m": json.dumps(meta), "id": trade_id},
        )
        append_event(
            db,
            trade_id=trade_id,
            from_status=IN_TRADE,
            to_status=IN_TRADE,
            actor="USER",
            event_type="fills_edited",
            payload={"warn": warn, "confirm": confirm},
            enforce_transition=False,
        )
        db.commit()
        return {"ok": True, "trade_id": trade_id, "fills_confirmed": bool(confirm), "warnings": warn}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)[:300]}
    finally:
        db.close()


def void_user_trade(trade_id: int, reason: str) -> Dict[str, Any]:
    reason_n = (reason or "").strip()
    if not reason_n:
        return {"ok": False, "error": "reason_required"}
    trade = get_trade(trade_id)
    if not trade:
        return {"ok": False, "error": "trade_not_found"}
    entry = trade.get("entry_at")
    try:
        ts = datetime.fromisoformat(str(entry).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except Exception:
        ts = datetime.now(timezone.utc)
    age = (datetime.now(timezone.utc) - ts).total_seconds()
    if age > 300:
        return {"ok": False, "error": "void_window_elapsed", "age_sec": age}
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE tarang_trades
                SET voided = true, void_reason = :r, status = 'VOIDED', updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": trade_id, "r": reason_n[:500]},
        )
        append_event(
            db,
            trade_id=trade_id,
            from_status=IN_TRADE,
            to_status="VOIDED",
            actor="USER",
            event_type="void_within_5min",
            payload={"reason": reason_n, "age_sec": age},
            enforce_transition=False,
        )
        db.commit()
        return {"ok": True, "trade_id": trade_id, "voided": True, "reason": reason_n}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)[:200]}
    finally:
        db.close()


def exclude_forward_test(trade_id: int, reason: str, actor: str = "USER") -> Dict[str, Any]:
    """Admin-only: keep the row visible, omit from metrics. Requires a reason."""
    reason_n = (reason or "").strip()
    if not reason_n:
        return {"ok": False, "error": "exclude_reason_required"}
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        row = db.execute(text("SELECT id, record_type FROM tarang_trades WHERE id = :id"), {"id": trade_id}).mappings().first()
        if not row:
            return {"ok": False, "error": "not_found"}
        db.execute(
            text(
                """
                UPDATE tarang_trades
                SET excluded = true, exclude_reason = :r, updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": trade_id, "r": reason_n[:500]},
        )
        db.commit()
        return {"ok": True, "trade_id": trade_id, "excluded": True, "exclude_reason": reason_n, "actor": actor}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)[:200]}
    finally:
        db.close()

