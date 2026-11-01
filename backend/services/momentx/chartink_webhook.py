"""ChartInk → MomentX webhook: store-first inbox, then activate day-board cards.

ChartInk is a second entry gate: symbols are treated as Stage-1 already passed.
Stage-2 (5m EMA) still runs via ``tracker.refresh_emas``.
"""
from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.momentx import state as mx_state
from backend.services.momentx.status_rules import activate_card, past_lock_all
from backend.services.momentx.universe import load_momentx_universe

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

ENTRY_GATE_CHARTINK = "chartink"


def chartink_momentx_inbox_dir() -> Path:
    override = (os.getenv("CHARTINK_MOMENTX_WEBHOOK_INBOX") or "").strip()
    if override:
        return Path(override).expanduser()
    root = Path(__file__).resolve().parents[3]
    return root / "inbox" / "chartink_momentx"


def persist_momentx_chartink_raw_body(body: bytes) -> str:
    """Write exact ChartInk bytes before any processing. Returns absolute path."""
    d = chartink_momentx_inbox_dir()
    d.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    short = uuid.uuid4().hex[:8]
    p = d / f"{ts}_{short}.raw.json"
    p.write_bytes(body or b"")
    return str(p)


def _split_csv(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    s = str(raw).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def parse_side_from_alert(alert_name: Any, scan_name: Any = None) -> str:
    """Bullish if alert/scan mentions bullish; bearish if bearish; default bullish.

    Matches ChartInk names like ``...Bearish_HA`` / ``...Bullish_HA`` (underscore
    is a word char, so bare ``\\b`` after the token would miss).
    """
    blob = f"{alert_name or ''} {scan_name or ''}".lower()
    if re.search(r"bearish", blob):
        return "bearish"
    if re.search(r"bullish", blob):
        return "bullish"
    return "bullish"


def parse_chartink_momentx_payload(raw: bytes, content_type: Optional[str] = None) -> Dict[str, Any]:
    """Normalize ChartInk MomentX webhook body to stocks / prices / meta."""
    text = (raw or b"").decode("utf-8", errors="replace").strip()
    obj: Any = {}
    if text:
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            # URL-encoded fallback: stocks=A,B&alert_name=...
            from urllib.parse import parse_qs

            qs = parse_qs(text, keep_blank_values=True)
            obj = {k: (v[0] if isinstance(v, list) and v else v) for k, v in qs.items()}
    if not isinstance(obj, dict):
        raise ValueError("webhook body must be a JSON object")

    # ChartInk sometimes nests under "data"
    if "stocks" not in obj and isinstance(obj.get("data"), dict):
        obj = {**obj, **obj["data"]}

    stocks = _split_csv(obj.get("stocks") or obj.get("stock") or obj.get("symbols"))
    prices = _split_csv(obj.get("trigger_prices") or obj.get("trigger_price") or "")
    alert_name = str(obj.get("alert_name") or "").strip()
    scan_name = str(obj.get("scan_name") or "").strip()
    side = parse_side_from_alert(alert_name, scan_name)

    price_by_stock: Dict[str, Optional[float]] = {}
    for i, sym in enumerate(stocks):
        px: Optional[float] = None
        if i < len(prices):
            try:
                px = float(str(prices[i]).replace(",", ""))
            except (TypeError, ValueError):
                px = None
        price_by_stock[sym.upper()] = px

    return {
        "stocks": [s.upper() for s in stocks],
        "trigger_prices": prices,
        "price_by_stock": price_by_stock,
        "triggered_at": str(obj.get("triggered_at") or "").strip() or None,
        "scan_name": scan_name or None,
        "scan_url": str(obj.get("scan_url") or "").strip() or None,
        "alert_name": alert_name or None,
        "webhook_url": str(obj.get("webhook_url") or "").strip() or None,
        "side": side,
        "raw_keys": sorted(obj.keys()),
    }


def is_chartink_entry_gate(card: Optional[Dict[str, Any]]) -> bool:
    if not card:
        return False
    if str(card.get("entry_gate") or "").lower() == ENTRY_GATE_CHARTINK:
        return True
    s1 = card.get("stage1") if isinstance(card.get("stage1"), dict) else {}
    return str(s1.get("source") or "").lower() == ENTRY_GATE_CHARTINK


def _universe_by_stock() -> Dict[str, Dict[str, Any]]:
    return {str(u["stock"]).upper(): u for u in load_momentx_universe()}


def _build_chartink_card(
    *,
    uni: Dict[str, Any],
    side: str,
    trigger_price: Optional[float],
    meta: Dict[str, Any],
    prev: Optional[Dict[str, Any]],
    now: datetime,
) -> Dict[str, Any]:
    stock = str(uni["stock"]).upper()
    now_iso = now.isoformat()
    prev = prev or {}
    stage1 = {
        "price": trigger_price,
        "side": side,
        "core_qualified": True,
        "volume_ok": True,
        "source": ENTRY_GATE_CHARTINK,
        "conditions": {
            "chartink_gate": True,
            "volume_ok": True,
        },
    }
    # Preserve richer Stage-1 metrics if the card already had a scan pass.
    prev_s1 = prev.get("stage1") if isinstance(prev.get("stage1"), dict) else {}
    if prev_s1 and prev_s1.get("source") != ENTRY_GATE_CHARTINK:
        merged = dict(prev_s1)
        merged["side"] = side
        merged["core_qualified"] = True
        merged["source"] = ENTRY_GATE_CHARTINK
        conds = dict(merged.get("conditions") or {})
        conds["chartink_gate"] = True
        merged["conditions"] = conds
        if trigger_price is not None and merged.get("price") is None:
            merged["price"] = trigger_price
        stage1 = merged

    card = {
        "stock": stock,
        "equity_instrument_key": uni.get("equity_instrument_key"),
        "future_symbol": uni.get("future_symbol"),
        "future_instrument_key": uni.get("future_instrument_key"),
        "lot_size": uni.get("lot_size") if uni.get("lot_size") is not None else prev.get("lot_size"),
        "side": side,
        "entry_gate": ENTRY_GATE_CHARTINK,
        "filters_ok": True,
        "volume_ok": True,
        "active": True,
        "stage1": stage1,
        "chartink": {
            "alert_name": meta.get("alert_name"),
            "scan_name": meta.get("scan_name"),
            "scan_url": meta.get("scan_url"),
            "triggered_at": meta.get("triggered_at"),
            "trigger_price": trigger_price,
            "received_at": now_iso,
        },
        "first_seen_at": prev.get("first_seen_at") or now_iso,
    }
    # Preserve Stage-2 / LTP fields from an existing card.
    for fld in (
        "ltp",
        "ltp_at",
        "ltp_dir",
        "ema5",
        "ema10",
        "ema15",
        "ema50",
        "ema_ordered",
        "ema_at",
        "trigger",
        "trigger_since",
        "first_active_at",
        "active_since",
        "status",
    ):
        if prev.get(fld) is not None and card.get(fld) is None:
            card[fld] = prev.get(fld)
    return card


def process_momentx_chartink_payload(
    parsed: Dict[str, Any],
    *,
    inbox_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Activate / create MomentX day-board cards for ChartInk symbols (no Stage-1 recheck)."""
    now = datetime.now(IST)
    today_iso = now.date().isoformat()
    mx_state.ensure_board_date(today_iso)

    stocks: List[str] = list(parsed.get("stocks") or [])
    side = str(parsed.get("side") or "bullish").lower()
    if side not in ("bullish", "bearish"):
        side = "bullish"
    price_by = parsed.get("price_by_stock") or {}

    uni_map = _universe_by_stock()
    existing = {str(c.get("stock") or "").upper(): c for c in mx_state.ready_cards()}

    activated: List[str] = []
    created: List[str] = []
    updated: List[str] = []
    skipped: List[str] = []
    new_cards: List[Dict[str, Any]] = []

    lock_all = past_lock_all(now)

    for sym in stocks:
        key = str(sym or "").strip().upper()
        if not key:
            continue
        uni = uni_map.get(key)
        if not uni:
            skipped.append(f"{key}:not_in_universe")
            continue
        prev = existing.get(key)
        was_new = prev is None
        trigger_px = price_by.get(key)
        if trigger_px is None and isinstance(price_by.get(sym), (int, float)):
            trigger_px = float(price_by.get(sym))

        card = _build_chartink_card(
            uni=uni,
            side=side,
            trigger_price=trigger_px,
            meta=parsed,
            prev=prev,
            now=now,
        )
        if lock_all:
            card["status"] = "locked"
            card["active"] = False
            card["filters_ok"] = True
        else:
            activate_card(card, now)
            activated.append(key)

        if was_new:
            created.append(key)
            new_cards.append(card)
        else:
            updated.append(key)
        existing[key] = card

    mx_state.set_ready_cards(list(existing.values()))

    if new_cards:
        try:
            from backend.services.momentx.persistence import record_filtered_symbols

            record_filtered_symbols(new_cards, trade_date=now.date(), filtered_at=now)
        except Exception:
            logger.exception("momentx chartink record_filtered_symbols failed")

    # Stage-2: 5m EMA check for all board cards (incl. newly activated).
    ema_out: Dict[str, Any] = {}
    try:
        from backend.services.momentx.tracker import refresh_emas

        ema_out = refresh_emas(force=True) or {}
    except Exception as e:
        logger.exception("momentx chartink ema refresh failed: %s", e)
        ema_out = {"ok": False, "error": str(e)}

    result = {
        "ok": True,
        "side": side,
        "stocks_received": len(stocks),
        "activated": activated,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "locked_session": lock_all,
        "inbox_path": inbox_path,
        "ema": {"ok": ema_out.get("ok"), "updated": ema_out.get("updated"), "skipped": ema_out.get("skipped")},
    }
    logger.info(
        "momentx chartink processed side=%s recv=%s created=%s updated=%s skipped=%s",
        side,
        len(stocks),
        len(created),
        len(updated),
        skipped,
    )
    return result
