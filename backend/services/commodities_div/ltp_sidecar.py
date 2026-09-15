"""Sidecar LTP for all In-Trade Commodities Div rows — does not touch arbitrage_master."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.commodities_div.mapping import attach_instrument_fields
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.commodities_div.webhook import now_ist_second

logger = logging.getLogger(__name__)


def _norm_ik(k: str) -> str:
    return str(k or "").replace(" ", "").replace(":", "|").upper()


def _ltp_from_upstox_ltp_api(upstox: Any, instrument_keys: List[str]) -> Dict[str, float]:
    """
    Prefer dedicated LTP endpoint — response keys are often trading symbols, but each
    payload includes instrument_token matching MCX_FO|nnnnnn.
    """
    out: Dict[str, float] = {}
    if not instrument_keys:
        return out
    want = {_norm_ik(k): k for k in instrument_keys if (k or "").strip()}
    try:
        keys_param = ",".join(instrument_keys)
        url = (
            "https://api.upstox.com/v2/market-quote/ltp"
            f"?instrument_key={quote(keys_param, safe=',')}"
        )
        data = upstox.make_api_request(url, method="GET", timeout=15, max_retries=2)
        if not data or data.get("status") != "success":
            return out
        raw = data.get("data") or {}
        if not isinstance(raw, dict):
            return out
        for _sym, payload in raw.items():
            if not isinstance(payload, dict):
                continue
            tok = (
                payload.get("instrument_token")
                or payload.get("instrument_key")
                or payload.get("instrumentKey")
            )
            lp = payload.get("last_price")
            try:
                price = float(lp) if lp is not None else 0.0
            except (TypeError, ValueError):
                continue
            if price <= 0 or not tok:
                continue
            nk = _norm_ik(str(tok))
            req = want.get(nk)
            if req:
                out[req] = price
    except Exception as e:
        logger.warning("commodities_div LTP API failed: %s", e)
    return out


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
        resolved: List[Dict[str, Any]] = []

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
            resolved.append({"id": int(row["id"]), "instrument_key": ik, "symbol": row.get("symbol_mapped")})

        keys = [r["instrument_key"] for r in resolved]
        ltp_map: Dict[str, float] = {}
        try:
            ltp_map = upstox.get_market_quotes_batch_by_keys(keys) or {}
        except Exception as e:
            logger.warning("commodities_div LTP quotes batch failed: %s", e)

        missing = [k for k in keys if k not in ltp_map]
        if missing:
            ltp_api = _ltp_from_upstox_ltp_api(upstox, missing)
            ltp_map.update(ltp_api)

        for row in resolved:
            ik = row["instrument_key"]
            ltp: Optional[float] = None
            if ik in ltp_map:
                try:
                    ltp = float(ltp_map[ik])
                except (TypeError, ValueError):
                    ltp = None
            if ltp is None:
                # Fuzzy: match by normalized token suffix
                want = _norm_ik(ik)
                for k, v in ltp_map.items():
                    if _norm_ik(k) == want:
                        try:
                            ltp = float(v)
                        except (TypeError, ValueError):
                            ltp = None
                        break

            if ltp is None or ltp <= 0:
                results.append(
                    {
                        "signal_id": row["id"],
                        "updated": False,
                        "reason": "no_ltp",
                        "instrument_key": ik,
                        "symbol": row.get("symbol"),
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
                {"ltp": ltp, "ts": now, "id": row["id"]},
            )
            db.commit()
            any_updated = True
            results.append(
                {
                    "signal_id": row["id"],
                    "updated": True,
                    "ltp": ltp,
                    "instrument_key": ik,
                    "symbol": row.get("symbol"),
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
