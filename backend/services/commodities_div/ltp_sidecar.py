"""Sidecar LTP for the single In-Trade Commodities Div row — does not touch arbitrage_master."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.commodities_div.mapping import attach_instrument_fields
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.commodities_div.webhook import now_ist_second

logger = logging.getLogger(__name__)


def refresh_in_trade_ltp() -> Dict[str, Any]:
    """
    If exactly one In-Trade row has (or can resolve) instrument_key, fetch LTP via Upstox
    and UPDATE commodities_div_signals only.
    """
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT id, symbol_raw, symbol_mapped, instrument_key, status
                FROM commodities_div_signals
                WHERE status = 'In-Trade'
                ORDER BY id DESC
                LIMIT 1
                """
            )
        ).mappings().first()
        if not row:
            return {"ok": True, "updated": False, "reason": "no_in_trade"}

        ik = (row.get("instrument_key") or "").strip()
        if not ik:
            inst = attach_instrument_fields(str(row["symbol_raw"]))
            ik = (inst.get("instrument_key") or "").strip()
            if ik:
                db.execute(
                    text(
                        """
                        UPDATE commodities_div_signals SET
                            instrument_key = :ik,
                            contract = COALESCE(:contract, contract),
                            lot_size = COALESCE(:lot, lot_size),
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "ik": ik,
                        "contract": inst.get("contract"),
                        "lot": inst.get("lot_size"),
                        "id": int(row["id"]),
                    },
                )
                db.commit()
            else:
                return {
                    "ok": True,
                    "updated": False,
                    "reason": "no_instrument_key",
                    "signal_id": int(row["id"]),
                    "symbol": row.get("symbol_mapped"),
                }

        try:
            from backend.services.upstox_service import UpstoxService

            upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        except Exception as e:
            logger.warning("commodities_div LTP: Upstox init failed: %s", e)
            return {"ok": False, "updated": False, "error": str(e)}

        if not getattr(upstox, "access_token", None):
            return {"ok": False, "updated": False, "error": "upstox_not_connected"}

        ltp: Optional[float] = None
        try:
            quotes = upstox.get_market_quotes_batch_by_keys([ik]) or {}
            raw = quotes.get(ik)
            if raw is None:
                # fuzzy key match
                for k, v in quotes.items():
                    if str(k).replace("|", "").endswith(ik.split("|")[-1]):
                        raw = v
                        break
            if raw is not None:
                ltp = float(raw)
        except Exception as e:
            logger.warning("commodities_div LTP quote failed id=%s: %s", row["id"], e)
            return {"ok": False, "updated": False, "error": str(e), "signal_id": int(row["id"])}

        if ltp is None:
            return {"ok": True, "updated": False, "reason": "no_ltp", "instrument_key": ik}

        now = now_ist_second()
        db.execute(
            text(
                """
                UPDATE commodities_div_signals SET
                    ltp = :ltp,
                    ltp_updated_at = :ts,
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"ltp": ltp, "ts": now, "id": int(row["id"])},
        )
        db.commit()
        return {
            "ok": True,
            "updated": True,
            "signal_id": int(row["id"]),
            "ltp": ltp,
            "instrument_key": ik,
            "ltp_updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        db.rollback()
        logger.exception("commodities_div LTP sidecar failed: %s", e)
        return {"ok": False, "updated": False, "error": str(e)}
    finally:
        db.close()
