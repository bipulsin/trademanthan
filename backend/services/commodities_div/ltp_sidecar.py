"""Sidecar LTP for all In-Trade Commodities Div rows — does not touch arbitrage_master."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.commodities_div.mapping import attach_instrument_fields
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.commodities_div.webhook import now_ist_second

logger = logging.getLogger(__name__)


def refresh_in_trade_ltp() -> Dict[str, Any]:
    """
    Refresh LTP for every In-Trade row that has (or can resolve) instrument_key.
    UPDATE commodities_div_signals only.
    """
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol_raw, symbol_mapped, instrument_key, status
                FROM commodities_div_signals
                WHERE status = 'In-Trade'
                ORDER BY id DESC
                """
            )
        ).mappings().all()
        if not rows:
            return {"ok": True, "updated": False, "reason": "no_in_trade", "results": []}

        try:
            from backend.services.upstox_service import UpstoxService

            upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        except Exception as e:
            logger.warning("commodities_div LTP: Upstox init failed: %s", e)
            return {"ok": False, "updated": False, "error": str(e), "results": []}

        if not getattr(upstox, "access_token", None):
            return {"ok": False, "updated": False, "error": "upstox_not_connected", "results": []}

        results: List[Dict[str, Any]] = []
        any_updated = False
        now = now_ist_second()

        for row in rows:
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
                    results.append(
                        {
                            "signal_id": int(row["id"]),
                            "updated": False,
                            "reason": "no_instrument_key",
                            "symbol": row.get("symbol_mapped"),
                        }
                    )
                    continue

            ltp: Optional[float] = None
            try:
                quotes = upstox.get_market_quotes_batch_by_keys([ik]) or {}
                raw = quotes.get(ik)
                if raw is None:
                    for k, v in quotes.items():
                        if str(k).replace("|", "").endswith(ik.split("|")[-1]):
                            raw = v
                            break
                if raw is not None:
                    ltp = float(raw)
            except Exception as e:
                logger.warning("commodities_div LTP quote failed id=%s: %s", row["id"], e)
                results.append(
                    {
                        "signal_id": int(row["id"]),
                        "updated": False,
                        "error": str(e),
                    }
                )
                continue

            if ltp is None:
                results.append(
                    {
                        "signal_id": int(row["id"]),
                        "updated": False,
                        "reason": "no_ltp",
                        "instrument_key": ik,
                    }
                )
                continue

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
            any_updated = True
            results.append(
                {
                    "signal_id": int(row["id"]),
                    "updated": True,
                    "ltp": ltp,
                    "instrument_key": ik,
                    "ltp_updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        return {
            "ok": True,
            "updated": any_updated,
            "results": results,
            "count": len(results),
        }
    except Exception as e:
        db.rollback()
        logger.exception("commodities_div LTP sidecar failed: %s", e)
        return {"ok": False, "updated": False, "error": str(e)}
    finally:
        db.close()
