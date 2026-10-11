"""Persist paired BTC straddles. One row per straddle."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import pytz
from sqlalchemy import bindparam, text

from backend.database import SessionLocal, engine

_IST = pytz.timezone("Asia/Kolkata")

_ENSURE_SQL = """
CREATE TABLE IF NOT EXISTS crypto_option_straddles (
    id BIGSERIAL PRIMARY KEY,
    underlying TEXT NOT NULL DEFAULT 'BTC',
    session_date DATE NOT NULL,
    expiry DATE,
    strike NUMERIC,
    ce_symbol TEXT NOT NULL,
    pe_symbol TEXT NOT NULL,
    automation_id TEXT NOT NULL DEFAULT '',
    quote_currency TEXT,
    contract_value NUMERIC,
    ce_sell_price NUMERIC,
    pe_sell_price NUMERIC,
    ce_buy_price NUMERIC,
    pe_buy_price NUMERIC,
    ce_mark NUMERIC,
    pe_mark NUMERIC,
    ce_size NUMERIC,
    pe_size NUMERIC,
    ce_buy_size NUMERIC,
    pe_buy_size NUMERIC,
    entry_time TIMESTAMPTZ,
    exit_time TIMESTAMPTZ,
    status TEXT NOT NULL,
    entry_premium NUMERIC,
    exit_premium NUMERIC,
    pnl NUMERIC,
    ce_sell_fill_id TEXT,
    pe_sell_fill_id TEXT,
    ce_buy_fill_id TEXT,
    pe_buy_fill_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_crypto_option_straddle_key
    ON crypto_option_straddles (underlying, session_date, ce_symbol, pe_symbol, automation_id);
CREATE INDEX IF NOT EXISTS ix_crypto_option_straddle_session
    ON crypto_option_straddles (session_date DESC);
"""

_UPSERT = text(
    """
    INSERT INTO crypto_option_straddles (
        underlying, session_date, expiry, strike, ce_symbol, pe_symbol, automation_id,
        quote_currency, contract_value,
        ce_sell_price, pe_sell_price, ce_buy_price, pe_buy_price, ce_mark, pe_mark,
        ce_size, pe_size, ce_buy_size, pe_buy_size,
        entry_time, exit_time, status, entry_premium, exit_premium, pnl,
        ce_sell_fill_id, pe_sell_fill_id, ce_buy_fill_id, pe_buy_fill_id, updated_at
    ) VALUES (
        :underlying, :session_date, :expiry, :strike, :ce_symbol, :pe_symbol, :automation_id,
        :quote_currency, :contract_value,
        :ce_sell_price, :pe_sell_price, :ce_buy_price, :pe_buy_price, :ce_mark, :pe_mark,
        :ce_size, :pe_size, :ce_buy_size, :pe_buy_size,
        :entry_time, :exit_time, :status, :entry_premium, :exit_premium, :pnl,
        :ce_sell_fill_id, :pe_sell_fill_id, :ce_buy_fill_id, :pe_buy_fill_id, :updated_at
    )
    ON CONFLICT (underlying, session_date, ce_symbol, pe_symbol, automation_id)
    DO UPDATE SET
        expiry = EXCLUDED.expiry,
        strike = EXCLUDED.strike,
        quote_currency = EXCLUDED.quote_currency,
        contract_value = EXCLUDED.contract_value,
        ce_sell_price = EXCLUDED.ce_sell_price,
        pe_sell_price = EXCLUDED.pe_sell_price,
        ce_buy_price = EXCLUDED.ce_buy_price,
        pe_buy_price = EXCLUDED.pe_buy_price,
        ce_mark = EXCLUDED.ce_mark,
        pe_mark = EXCLUDED.pe_mark,
        ce_size = EXCLUDED.ce_size,
        pe_size = EXCLUDED.pe_size,
        ce_buy_size = EXCLUDED.ce_buy_size,
        pe_buy_size = EXCLUDED.pe_buy_size,
        entry_time = EXCLUDED.entry_time,
        exit_time = EXCLUDED.exit_time,
        status = EXCLUDED.status,
        entry_premium = EXCLUDED.entry_premium,
        exit_premium = EXCLUDED.exit_premium,
        pnl = EXCLUDED.pnl,
        ce_sell_fill_id = EXCLUDED.ce_sell_fill_id,
        pe_sell_fill_id = EXCLUDED.pe_sell_fill_id,
        ce_buy_fill_id = EXCLUDED.ce_buy_fill_id,
        pe_buy_fill_id = EXCLUDED.pe_buy_fill_id,
        updated_at = EXCLUDED.updated_at
    """
)


def ensure_crypto_option_tables() -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        for stmt in _ENSURE_SQL.split(";"):
            sql = stmt.strip()
            if sql:
                conn.execute(text(sql))


def _params(trade: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "underlying": trade.get("underlying") or "BTC",
        "session_date": trade.get("session_date"),
        "expiry": trade.get("expiry"),
        "strike": trade.get("strike"),
        "ce_symbol": trade.get("ce_symbol"),
        "pe_symbol": trade.get("pe_symbol"),
        "automation_id": trade.get("automation_id") or "",
        "quote_currency": trade.get("quote_currency"),
        "contract_value": trade.get("contract_value"),
        "ce_sell_price": trade.get("ce_sell_price"),
        "pe_sell_price": trade.get("pe_sell_price"),
        "ce_buy_price": trade.get("ce_buy_price"),
        "pe_buy_price": trade.get("pe_buy_price"),
        "ce_mark": trade.get("ce_mark"),
        "pe_mark": trade.get("pe_mark"),
        "ce_size": trade.get("ce_size"),
        "pe_size": trade.get("pe_size"),
        "ce_buy_size": trade.get("ce_buy_size"),
        "pe_buy_size": trade.get("pe_buy_size"),
        "entry_time": trade.get("entry_time"),
        "exit_time": trade.get("exit_time"),
        "status": trade.get("status") or "OPEN",
        "entry_premium": trade.get("entry_premium"),
        "exit_premium": trade.get("exit_premium"),
        "pnl": trade.get("pnl"),
        "ce_sell_fill_id": trade.get("ce_sell_fill_id"),
        "pe_sell_fill_id": trade.get("pe_sell_fill_id"),
        "ce_buy_fill_id": trade.get("ce_buy_fill_id"),
        "pe_buy_fill_id": trade.get("pe_buy_fill_id"),
        "updated_at": datetime.now(timezone.utc),
    }


def sync_straddles(trades: Sequence[Dict[str, Any]], *, window_start: date, replace_window: bool) -> int:
    """Upsert straddles. When the fill pull was complete, drop rows in the window that are no longer paired."""
    ensure_crypto_option_tables()
    if SessionLocal is None:
        raise RuntimeError("database unavailable")
    db = SessionLocal()
    try:
        keys = [
            (
                t.get("underlying") or "BTC",
                t.get("session_date"),
                t.get("ce_symbol"),
                t.get("pe_symbol"),
                t.get("automation_id") or "",
            )
            for t in trades
        ]
        if replace_window:
            existing = db.execute(
                text(
                    """
                    SELECT id, underlying, session_date, ce_symbol, pe_symbol, automation_id
                    FROM crypto_option_straddles
                    WHERE session_date >= :window_start
                    """
                ),
                {"window_start": window_start},
            ).mappings().all()
            keep = set(keys)
            stale = [
                row["id"]
                for row in existing
                if (
                    row["underlying"],
                    row["session_date"],
                    row["ce_symbol"],
                    row["pe_symbol"],
                    row["automation_id"] or "",
                )
                not in keep
            ]
            if stale:
                stmt = text("DELETE FROM crypto_option_straddles WHERE id IN :ids").bindparams(
                    bindparam("ids", expanding=True)
                )
                db.execute(stmt, {"ids": stale})
        for trade in trades:
            db.execute(_UPSERT, _params(trade))
        db.commit()
        return len(trades)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(_IST).strftime("%Y-%m-%d %H:%M")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _num(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def list_straddles(start_date: Optional[date], end_date: Optional[date]) -> List[Dict[str, Any]]:
    ensure_crypto_option_tables()
    if SessionLocal is None:
        return []
    clauses = ["1=1"]
    params: Dict[str, Any] = {}
    if start_date is not None:
        clauses.append("session_date >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        clauses.append("session_date <= :end_date")
        params["end_date"] = end_date
    sql = f"""
        SELECT *
        FROM crypto_option_straddles
        WHERE {' AND '.join(clauses)}
        ORDER BY session_date DESC, entry_time DESC NULLS LAST, id DESC
    """
    db = SessionLocal()
    try:
        rows = db.execute(text(sql), params).mappings().all()
    finally:
        db.close()
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in ("session_date", "expiry", "entry_time", "exit_time", "updated_at"):
            item[key] = _iso(item.get(key))
        for key in (
            "strike",
            "contract_value",
            "ce_sell_price",
            "pe_sell_price",
            "ce_buy_price",
            "pe_buy_price",
            "ce_mark",
            "pe_mark",
            "ce_size",
            "pe_size",
            "ce_buy_size",
            "pe_buy_size",
            "entry_premium",
            "exit_premium",
            "pnl",
        ):
            item[key] = _num(item.get(key))
        item.pop("id", None)
        out.append(item)
    return out
