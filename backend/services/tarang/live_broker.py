"""Shadow + mock live brokers. Production send path is locked (sends nothing)."""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict, Optional, Protocol

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.live_control import LivePlacementDisabled, live_limits, live_send_allowed
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

ORDER_TAG = "tarang-"


class LiveBroker(Protocol):
    venue: str

    def place(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def modify(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def cancel(self, order_id: str) -> Dict[str, Any]:
        ...

    def status(self, order_id: str) -> Dict[str, Any]:
        ...

    def margin(self) -> Dict[str, Any]:
        ...

    def funds(self) -> Dict[str, Any]:
        ...


def _log_shadow(action: str, payload: Dict[str, Any], *, trade_id: Optional[int] = None, venue: str = "") -> str:
    ensure_tarang_tables()
    coid = str(payload.get("client_order_id") or f"{ORDER_TAG}{uuid.uuid4().hex[:16]}")
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                INSERT INTO tarang_shadow_orders (trade_id, venue, client_order_id, action, payload, sent)
                VALUES (:tid, :venue, :coid, :action, CAST(:payload AS jsonb), FALSE)
                """
            ),
            {
                "tid": trade_id,
                "venue": venue or payload.get("venue") or "",
                "coid": coid,
                "action": action,
                "payload": json.dumps(payload),
            },
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("shadow log failed")
    finally:
        db.close()
    return coid


class ShadowBroker:
    """Logs exact payloads; never transmits."""

    venue = "shadow"

    def place(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        coid = _log_shadow("place", payload, trade_id=payload.get("trade_id"), venue=str(payload.get("venue") or ""))
        return {"ok": True, "sent": False, "shadow": True, "client_order_id": coid, "status": "SHADOW"}

    def modify(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload)
        p["order_id"] = order_id
        _log_shadow("modify", p, trade_id=p.get("trade_id"))
        return {"ok": True, "sent": False, "shadow": True, "order_id": order_id}

    def cancel(self, order_id: str) -> Dict[str, Any]:
        _log_shadow("cancel", {"order_id": order_id}, )
        return {"ok": True, "sent": False, "shadow": True, "order_id": order_id}

    def status(self, order_id: str) -> Dict[str, Any]:
        return {"ok": True, "sent": False, "status": "SHADOW", "order_id": order_id}

    def margin(self) -> Dict[str, Any]:
        return {"ok": True, "sent": False, "margin": None}

    def funds(self) -> Dict[str, Any]:
        return {"ok": True, "sent": False, "funds": None}


class LockedLiveBroker:
    """Fail-closed production broker. Never places."""

    venue = "locked"

    def place(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        _log_shadow("place_blocked", payload, trade_id=payload.get("trade_id"), venue=str(payload.get("venue") or ""))
        raise LivePlacementDisabled("live_placement_disabled")

    def modify(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise LivePlacementDisabled("live_placement_disabled")

    def cancel(self, order_id: str) -> Dict[str, Any]:
        raise LivePlacementDisabled("live_placement_disabled")

    def status(self, order_id: str) -> Dict[str, Any]:
        return {"ok": False, "error": "live_placement_disabled"}

    def margin(self) -> Dict[str, Any]:
        return {"ok": False, "error": "live_placement_disabled"}

    def funds(self) -> Dict[str, Any]:
        return {"ok": False, "error": "live_placement_disabled"}


class MockBroker:
    """Test doubles: partial fills, rejects, timeouts, token expiry, ws drop."""

    venue = "mock"

    def __init__(self, scenario: str = "ok"):
        self.scenario = scenario
        self.calls: list = []

    def place(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.calls.append(("place", payload))
        if self.scenario == "reject":
            return {"ok": False, "sent": False, "status": "REJECTED", "error": "rejected_leg"}
        if self.scenario == "partial":
            qty = float(payload.get("qty") or 1)
            return {
                "ok": True,
                "sent": False,
                "status": "PARTIAL",
                "filled_qty": qty / 2,
                "qty": qty,
            }
        if self.scenario == "timeout":
            time.sleep(0)
            return {"ok": False, "sent": False, "status": "TIMEOUT", "error": "timeout"}
        if self.scenario == "token_expiry":
            return {"ok": False, "sent": False, "status": "AUTH", "error": "token_expiry"}
        if self.scenario == "ws_drop":
            return {"ok": False, "sent": False, "status": "WS_DROP", "error": "websocket_drop"}
        if self.scenario == "duplicate":
            return {"ok": True, "sent": False, "status": "DUPLICATE", "idempotent": True}
        coid = payload.get("client_order_id") or f"{ORDER_TAG}mock"
        return {"ok": True, "sent": False, "status": "FILLED", "client_order_id": coid}

    def modify(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.calls.append(("modify", order_id, payload))
        return {"ok": True, "sent": False, "order_id": order_id}

    def cancel(self, order_id: str) -> Dict[str, Any]:
        self.calls.append(("cancel", order_id))
        return {"ok": True, "sent": False}

    def status(self, order_id: str) -> Dict[str, Any]:
        return {"ok": True, "status": self.scenario, "order_id": order_id}

    def margin(self) -> Dict[str, Any]:
        return {"ok": True, "margin": 0}

    def funds(self) -> Dict[str, Any]:
        return {"ok": True, "funds": 0}


def get_live_broker() -> Any:
    if live_send_allowed():
        return LockedLiveBroker()
    return ShadowBroker()


def place_or_shadow(payload: Dict[str, Any]) -> Dict[str, Any]:
    broker = get_live_broker()
    tagged = dict(payload)
    limits = live_limits()
    tag = str(limits.get("order_tag") or ORDER_TAG)
    tagged["client_order_id"] = tagged.get("client_order_id") or f"{tag}{uuid.uuid4().hex[:16]}"
    tagged["tag"] = tag
    if not live_send_allowed():
        return ShadowBroker().place(tagged)
    try:
        return broker.place(tagged)
    except LivePlacementDisabled as e:
        return {"ok": False, "sent": False, "error": e.reason}
