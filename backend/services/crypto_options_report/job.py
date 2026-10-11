"""Ingest Delta India BTC straddle fills and refresh open-leg marks."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from backend.services.crypto_options_report.delta_client import (
    fetch_btc_option_fills,
    fetch_option_marks,
    missing_credential_env,
)
from backend.services.crypto_options_report.pairing import apply_open_marks, pair_straddle_fills
from backend.services.crypto_options_report.store import sync_straddles

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
LOOKBACK_DAYS = 40


def run_crypto_options_refresh(*, trigger: str = "manual") -> Dict[str, Any]:
    """Pull recent option fills, pair straddles, mark open shorts, and upsert."""
    missing = missing_credential_env()
    if missing:
        logger.warning("crypto options report skipped; missing env %s", ",".join(missing))
        return {"ok": False, "missing_env": missing, "trigger": trigger, "trades": 0}

    pulled = fetch_btc_option_fills(lookback_days=LOOKBACK_DAYS)
    if not pulled.get("ok"):
        logger.warning(
            "crypto options report fill pull failed trigger=%s error=%s",
            trigger,
            pulled.get("error"),
        )
        return {
            "ok": False,
            "missing_env": pulled.get("missing_env") or [],
            "error": pulled.get("error"),
            "trigger": trigger,
            "trades": 0,
        }

    raw_fills: List[Dict[str, Any]] = pulled.get("fills") or []
    trades = pair_straddle_fills(raw_fills)
    open_symbols: List[str] = []
    for trade in trades:
        if trade.get("status") != "CLOSED":
            open_symbols.append(trade["ce_symbol"])
            open_symbols.append(trade["pe_symbol"])
    marks = fetch_option_marks(open_symbols) if open_symbols else {}
    trades = apply_open_marks(trades, marks)

    window_start = (datetime.now(IST) - timedelta(days=LOOKBACK_DAYS)).date()
    stored = sync_straddles(
        trades,
        window_start=window_start,
        replace_window=not pulled.get("truncated"),
    )
    open_n = sum(1 for t in trades if t.get("status") != "CLOSED")
    logger.info(
        "crypto options report trigger=%s fills=%s straddles=%s open=%s marks=%s",
        trigger,
        len(raw_fills),
        stored,
        open_n,
        len(marks),
    )
    return {
        "ok": True,
        "missing_env": [],
        "trigger": trigger,
        "fills": len(raw_fills),
        "trades": stored,
        "open": open_n,
        "truncated": bool(pulled.get("truncated")),
    }
