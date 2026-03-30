"""
15-minute scan-algo job: if a live entry order never becomes a broker position within two
consecutive checks (~30 minutes), cancel the order and mark the trade Exit-Slip (slippage).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy.orm.attributes import flag_modified

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption
from backend.services import live_trading
from backend.services.live_trading import sync_trade_buy_fill_from_broker
from backend.services.upstox_service import upstox_service

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


def _is_stock_option_instrument_key(key: Optional[str]) -> bool:
    if not key:
        return False
    k = str(key).strip().upper()
    return k.startswith("NSE_FO|") or k.startswith("BSE_FO|")


def _broker_net_long_qty_for_instrument(positions: List[Dict[str, Any]], instrument_key: str) -> int:
    total = 0
    ik = (instrument_key or "").strip()
    for row in positions:
        if not isinstance(row, dict):
            continue
        if (row.get("instrument_token") or "") != ik:
            continue
        try:
            total += int(float(row.get("quantity") or 0))
        except (TypeError, ValueError):
            continue
    return total


def _cancel_entry_order(buy_order_id: str) -> Dict[str, Any]:
    oid = (buy_order_id or "").strip()
    if not oid or not upstox_service:
        return {"success": False, "error": "missing order or service"}
    if oid.upper().startswith("GTT"):
        return upstox_service.cancel_gtt_order(oid)
    return upstox_service.cancel_order(oid)


def _is_market_hours_ist(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    if h < 9 or (h == 9 and m < 15):
        return False
    if h > 15 or (h == 15 and m > 30):
        return False
    return True


def run_entry_slip_monitor() -> Dict[str, Any]:
    """
    Query today's ``bought`` trades with a broker order id; if short-term positions show no
    net long for the option after two consecutive checks, cancel the order and set
    ``exit_reason='Exit-Slip'``, ``status='cancelled'``.
    """
    if not live_trading.is_trading_live_enabled():
        logger.debug("Entry slip monitor: live trading off — skip")
        return {"skipped": True, "reason": "live_off"}
    if not upstox_service or not getattr(upstox_service, "access_token", None):
        logger.debug("Entry slip monitor: Upstox not configured — skip")
        return {"skipped": True, "reason": "no_upstox"}

    now = datetime.now(IST)
    if not _is_market_hours_ist(now):
        logger.debug("Entry slip monitor: outside market hours — skip")
        return {"skipped": True, "reason": "market_closed"}

    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)

    db = SessionLocal()
    try:
        trades = (
            db.query(IntradayStockOption)
            .filter(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.trade_date < tomorrow,
                IntradayStockOption.status == "bought",
                IntradayStockOption.exit_reason.is_(None),
                IntradayStockOption.buy_order_id.isnot(None),
                IntradayStockOption.instrument_key.isnot(None),
            )
            .all()
        )

        candidates = [t for t in trades if _is_stock_option_instrument_key(getattr(t, "instrument_key", None))]
        if not candidates:
            return {"success": True, "checked": 0, "actions": []}

        pos_res = upstox_service.get_short_term_positions()
        if not pos_res.get("success"):
            logger.warning(
                "Entry slip monitor: aborting tick — positions API failed: %s",
                pos_res.get("error"),
            )
            return {"success": False, "error": pos_res.get("error"), "aborted": True}

        positions = [p for p in (pos_res.get("positions") or []) if isinstance(p, dict)]

        actions: List[Dict[str, Any]] = []

        for trade in candidates:
            try:
                if getattr(trade, "buy_price", None) and float(trade.buy_price) > 0:
                    sync_trade_buy_fill_from_broker(db, trade)

                qty = int(getattr(trade, "qty", None) or 0)
                if qty <= 0:
                    continue

                ikey = (getattr(trade, "instrument_key", None) or "").strip()
                net = _broker_net_long_qty_for_instrument(positions, ikey) if positions else 0

                if net >= int(qty * 0.99):
                    if (getattr(trade, "entry_slip_checks", None) or 0) != 0:
                        trade.entry_slip_checks = 0
                        flag_modified(trade, "entry_slip_checks")
                    continue

                checks = int(getattr(trade, "entry_slip_checks", None) or 0) + 1
                trade.entry_slip_checks = checks
                flag_modified(trade, "entry_slip_checks")

                if checks < 2:
                    actions.append(
                        {
                            "id": trade.id,
                            "stock": trade.stock_name,
                            "action": "tick",
                            "entry_slip_checks": checks,
                        }
                    )
                    continue

                oid = (getattr(trade, "buy_order_id", None) or "").strip()
                cancel_res = _cancel_entry_order(oid)
                exit_ts = datetime.now(IST)

                trade.status = "cancelled"
                trade.exit_reason = "Exit-Slip"
                trade.sell_time = exit_ts
                trade.sell_price = None
                trade.pnl = 0.0
                trade.no_entry_reason = (
                    "Entry slip: no broker position after 30m; order cancelled at broker"
                )[:255]
                trade.entry_slip_checks = 0
                flag_modified(trade, "status")
                flag_modified(trade, "exit_reason")

                actions.append(
                    {
                        "id": trade.id,
                        "stock": trade.stock_name,
                        "action": "exit_slip",
                        "buy_order_id": oid,
                        "cancel_ok": bool(cancel_res.get("success")),
                        "cancel_error": cancel_res.get("error"),
                    }
                )
                logger.warning(
                    "🧾 EXIT-SLIP: %s id=%s order=%s cancel_ok=%s",
                    trade.stock_name,
                    trade.id,
                    oid,
                    cancel_res.get("success"),
                )
            except Exception as ex:
                logger.error(
                    "Entry slip monitor: error on trade id=%s: %s",
                    getattr(trade, "id", None),
                    ex,
                    exc_info=True,
                )

        db.commit()
        return {"success": True, "checked": len(candidates), "actions": actions}
    except Exception as e:
        logger.error("Entry slip monitor failed: %s", e, exc_info=True)
        try:
            db.rollback()
        except Exception:
            pass
        return {"success": False, "error": str(e)}
    finally:
        db.close()
