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
from backend.services.tarang.paper_broker import mark_to_market, simulate_entry_fills, simulate_exit_fills
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.screener import get_candidate
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


def _row_to_trade(r) -> Dict[str, Any]:
    d = dict(r)
    for k in ("legs", "meta"):
        d[k] = _json_load(d.get(k))
    for ts in ("created_at", "updated_at", "entry_at", "exit_at"):
        if d.get(ts) is not None:
            d[ts] = d[ts].isoformat()
    return d


def get_trade(trade_id: int) -> Optional[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        r = db.execute(text("SELECT * FROM tarang_trades WHERE id = :id"), {"id": trade_id}).mappings().first()
        return _row_to_trade(r) if r else None
    finally:
        db.close()


def list_open_trades(mode: str = "PAPER") -> List[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
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


def _persist_fills(db, trade_id: int, fills: List[Dict[str, Any]], phase: str) -> None:
    for f in fills:
        db.execute(
            text(
                """
                INSERT INTO tarang_fills (
                    trade_id, leg_index, side, symbol, qty, price, fees, phase, raw
                ) VALUES (
                    :trade_id, :leg_index, :side, :symbol, :qty, :price, :fees, :phase,
                    CAST(:raw AS jsonb)
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
) -> Dict[str, Any]:
    """QUALIFIED → ENTRY_PENDING → paper fills → IN_TRADE. Never places live orders."""
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
    # fallback from structure payload
    lot_size = lot_size or p.get("lot_size")
    contract_value = contract_value or p.get("contract_value")
    holding = str(holding_mode or "INTRADAY").upper()
    if holding not in ("INTRADAY", "POSITIONAL"):
        holding = "INTRADAY"

    db = SessionLocal()
    try:
        mode = _settings_mode(db)
        if str(mode).upper() == "LIVE":
            return {
                "ok": False,
                "error": "live_placement_disabled_phase3",
                "detail": "Phase 3 only supports PAPER fills. LIVE is Phase 4.",
            }

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
                    entry_iv_percentile, notes, currency, entry_debit_to_close
                ) VALUES (
                    :profile_id, :risk_bucket, 'PAPER', :holding_mode, :status,
                    FALSE, :entry_credit, :max_loss, :lots, CAST(:legs AS jsonb), CAST(:meta AS jsonb),
                    :candidate_id, :venue, :structure, :entry_at, :fees,
                    :iv_pct, :notes, 'INR', :entry_credit
                )
                RETURNING id
                """
            ),
            {
                "profile_id": cand["profile_id"],
                "risk_bucket": bucket,
                "holding_mode": holding,
                "status": ENTRY_PENDING,
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
            },
        ).mappings().first()
        trade_id = int(row["id"])
        append_event(
            db,
            trade_id=trade_id,
            candidate_id=candidate_id,
            from_status=QUALIFIED,
            to_status=ENTRY_PENDING,
            actor=actor,
            event_type="take_trade",
            payload={"mode": "PAPER", "note": note},
        )
        _persist_fills(db, trade_id, fill_res["fills"], "entry")
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
            event_type="paper_fill_entry",
            payload={"fills": fill_res["fills"], "net_credit_pts": fill_res["net_credit_pts"]},
        )
        db.execute(
            text("UPDATE tarang_candidates SET status = 'taken' WHERE id = :id"),
            {"id": candidate_id},
        )
        create_alert(
            db,
            f"PAPER trade #{trade_id} entered ({cand['profile_id']})",
            level="info",
            trade_id=trade_id,
            meta={"candidate_id": candidate_id},
        )
        db.commit()
        return {
            "ok": True,
            "trade_id": trade_id,
            "mode": "PAPER",
            "status": IN_TRADE,
            "entry_credit_pts": fill_res["net_credit_pts"],
            "fees_inr": fill_res.get("fees_inr"),
            "message": "PAPER fills simulated. No live orders placed.",
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
    if str(trade.get("mode") or "").upper() != "PAPER":
        return {"ok": False, "error": "live_exit_disabled_phase3"}

    meta = dict(trade.get("meta") or {})
    legs = trade.get("legs") or []
    units = int(trade.get("lots_or_contracts") or 1)
    venue = trade.get("venue") or "upstox_mcx"

    db = SessionLocal()
    try:
        entry_fills = _load_entry_fills(db, trade_id, meta)
        bundle = _refresh_quote_bundle(trade) if refresh_quotes else {"quotes": [], "ok": False}
        live = bundle.get("quotes") or []
        if venue == "upstox_mcx" and mcx_feed_closed():
            return {"ok": False, "error": "mcx_closed", "quotes_status": "mcx_closed"}
        if not live or not bundle.get("ok"):
            return {
                "ok": False,
                "error": "exit_deferred_stale_or_missing_quote",
                "quotes_status": "stale",
            }
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
        _persist_fills(db, trade_id, exit_res["fills"], "exit")
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
            event_type="paper_fill_exit",
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
    for t in list_open_trades("PAPER"):
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
    """Evaluate all open PAPER trades; flatten when a trigger fires.

    Never evaluates exits on stale quotes. MCX closed period (23:30–09:00 IST,
    weekends, holidays) is skipped; overnight gap is logged at the next open.
    """
    ensure_tarang_tables()
    venue_filter = {str(v) for v in venues} if venues else None
    outcomes = []
    for t in list_open_trades("PAPER"):
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
    for t in list_open_trades("PAPER"):
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
    for t in list_open_trades("PAPER"):
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
    """Stale feed / expired Upstox token / kill-switch consecutive losses."""
    ensure_tarang_tables()
    from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter
    from backend.services.tarang.alerts_telegram import notify_critical
    from backend.services.tarang.chain_snapshots import chain_coverage

    fired = []
    db = SessionLocal()
    try:
        ux = UpstoxMcxAdapter().health()
        token_state = ux.get("token") or "unknown"
        if token_state in ("missing", "expired") or not ux.get("ok"):
            if token_state in ("missing", "expired"):
                notify_critical(
                    db,
                    kind="upstox_token_expired",
                    message=f"Upstox token {token_state} — MCX chain snapshots blocked",
                    dedupe_key="upstox_token_expired",
                )
                fired.append("upstox_token_expired")
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
                message=f"Kill switch: {kill_n} consecutive PAPER losses",
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
