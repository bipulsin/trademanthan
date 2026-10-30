"""Postgres storage for MomentX Stage-1 backtest qualified symbols (working / temp tables)."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)

_SCHEMA_READY = False

_DDL = """
CREATE TABLE IF NOT EXISTS momentx_backtest_runs (
    id BIGSERIAL PRIMARY KEY,
    date_from DATE NOT NULL,
    date_to DATE NOT NULL,
    asof_hhmm TEXT NOT NULL DEFAULT '14:45',
    status TEXT NOT NULL DEFAULT 'queued',
    trading_days INT,
    qualified_rows INT,
    universe_size INT,
    error TEXT,
    params JSONB,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS momentx_backtest_qualified (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES momentx_backtest_runs(id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    stock TEXT NOT NULL,
    equity_instrument_key TEXT,
    future_symbol TEXT,
    future_instrument_key TEXT,
    lot_size INT,
    price DOUBLE PRECISION,
    sma200 DOUBLE PRECISION,
    prev_close DOUBLE PRECISION,
    pct_change DOUBLE PRECISION,
    rsi DOUBLE PRECISION,
    williams_r DOUBLE PRECISION,
    adx DOUBLE PRECISION,
    today_volume DOUBLE PRECISION,
    volume_sma5 DOUBLE PRECISION,
    first_filtered_at TIMESTAMPTZ,
    conditions JSONB,
    asof_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, trade_date, stock)
);

CREATE INDEX IF NOT EXISTS ix_mx_bt_qual_run_date
    ON momentx_backtest_qualified (run_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_mx_bt_qual_trade_date
    ON momentx_backtest_qualified (trade_date);
CREATE INDEX IF NOT EXISTS ix_mx_bt_runs_status
    ON momentx_backtest_runs (status, created_at DESC);
"""


_ALTERS = (
    "ALTER TABLE momentx_backtest_qualified ADD COLUMN IF NOT EXISTS first_filtered_at TIMESTAMPTZ",
)


def ensure_backtest_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    db = SessionLocal()
    try:
        for stmt in _DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                db.execute(text(s))
        for stmt in _ALTERS:
            db.execute(text(stmt))
        db.commit()
        _SCHEMA_READY = True
    except Exception:
        db.rollback()
        logger.exception("momentx backtest schema ensure failed")
        raise
    finally:
        db.close()


def create_run(
    *,
    date_from: date,
    date_to: date,
    asof_hhmm: str,
    params: Optional[Dict[str, Any]] = None,
) -> int:
    ensure_backtest_schema()
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                INSERT INTO momentx_backtest_runs
                    (date_from, date_to, asof_hhmm, status, params, created_at)
                VALUES
                    (:date_from, :date_to, :asof_hhmm, 'queued', CAST(:params AS JSONB), NOW())
                RETURNING id
                """
            ),
            {
                "date_from": date_from,
                "date_to": date_to,
                "asof_hhmm": asof_hhmm,
                "params": json.dumps(params or {}),
            },
        ).fetchone()
        db.commit()
        return int(row[0])
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def mark_run(
    run_id: int,
    *,
    status: str,
    trading_days: Optional[int] = None,
    qualified_rows: Optional[int] = None,
    universe_size: Optional[int] = None,
    error: Optional[str] = None,
    started: bool = False,
    finished: bool = False,
) -> None:
    ensure_backtest_schema()
    sets = ["status = :status"]
    params: Dict[str, Any] = {"id": run_id, "status": status}
    if trading_days is not None:
        sets.append("trading_days = :trading_days")
        params["trading_days"] = trading_days
    if qualified_rows is not None:
        sets.append("qualified_rows = :qualified_rows")
        params["qualified_rows"] = qualified_rows
    if universe_size is not None:
        sets.append("universe_size = :universe_size")
        params["universe_size"] = universe_size
    if error is not None:
        sets.append("error = :error")
        params["error"] = error[:2000]
    if started:
        sets.append("started_at = NOW()")
    if finished:
        sets.append("finished_at = NOW()")
    db = SessionLocal()
    try:
        db.execute(
            text(f"UPDATE momentx_backtest_runs SET {', '.join(sets)} WHERE id = :id"),
            params,
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("momentx backtest mark_run failed")
    finally:
        db.close()


def delete_qualified_in_range(date_from: date, date_to: date) -> int:
    """Clear prior qualified rows overlapping the range (all runs) for a clean re-run."""
    ensure_backtest_schema()
    db = SessionLocal()
    try:
        res = db.execute(
            text(
                """
                DELETE FROM momentx_backtest_qualified
                WHERE trade_date BETWEEN :d0 AND :d1
                """
            ),
            {"d0": date_from, "d1": date_to},
        )
        db.commit()
        return int(res.rowcount or 0)
    except Exception:
        db.rollback()
        logger.exception("momentx backtest delete range failed")
        return 0
    finally:
        db.close()


def insert_qualified_batch(run_id: int, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    ensure_backtest_schema()
    db = SessionLocal()
    n = 0
    try:
        for r in rows:
            db.execute(
                text(
                    """
                    INSERT INTO momentx_backtest_qualified (
                        run_id, trade_date, stock, equity_instrument_key,
                        future_symbol, future_instrument_key, lot_size,
                        price, sma200, prev_close, pct_change, rsi, williams_r, adx,
                        today_volume, volume_sma5, first_filtered_at, conditions, asof_at, created_at
                    ) VALUES (
                        :run_id, :trade_date, :stock, :equity_instrument_key,
                        :future_symbol, :future_instrument_key, :lot_size,
                        :price, :sma200, :prev_close, :pct_change, :rsi, :williams_r, :adx,
                        :today_volume, :volume_sma5, :first_filtered_at,
                        CAST(:conditions AS JSONB), :asof_at, NOW()
                    )
                    ON CONFLICT (run_id, trade_date, stock) DO UPDATE SET
                        equity_instrument_key = EXCLUDED.equity_instrument_key,
                        future_symbol = EXCLUDED.future_symbol,
                        future_instrument_key = EXCLUDED.future_instrument_key,
                        lot_size = EXCLUDED.lot_size,
                        price = EXCLUDED.price,
                        sma200 = EXCLUDED.sma200,
                        prev_close = EXCLUDED.prev_close,
                        pct_change = EXCLUDED.pct_change,
                        rsi = EXCLUDED.rsi,
                        williams_r = EXCLUDED.williams_r,
                        adx = EXCLUDED.adx,
                        today_volume = EXCLUDED.today_volume,
                        volume_sma5 = EXCLUDED.volume_sma5,
                        first_filtered_at = EXCLUDED.first_filtered_at,
                        conditions = EXCLUDED.conditions,
                        asof_at = EXCLUDED.asof_at
                    """
                ),
                {
                    "run_id": run_id,
                    "trade_date": r["trade_date"],
                    "stock": r["stock"],
                    "equity_instrument_key": r.get("equity_instrument_key"),
                    "future_symbol": r.get("future_symbol"),
                    "future_instrument_key": r.get("future_instrument_key"),
                    "lot_size": r.get("lot_size"),
                    "price": r.get("price"),
                    "sma200": r.get("sma200"),
                    "prev_close": r.get("prev_close"),
                    "pct_change": r.get("pct_change"),
                    "rsi": r.get("rsi"),
                    "williams_r": r.get("williams_r"),
                    "adx": r.get("adx"),
                    "today_volume": r.get("today_volume"),
                    "volume_sma5": r.get("volume_sma5"),
                    "first_filtered_at": r.get("first_filtered_at"),
                    "conditions": json.dumps(r.get("conditions") or {}),
                    "asof_at": r.get("asof_at"),
                },
            )
            n += 1
        db.commit()
        return n
    except Exception:
        db.rollback()
        logger.exception("momentx backtest insert_qualified_batch failed")
        raise
    finally:
        db.close()


def _iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def fetch_runs(limit: int = 20) -> List[Dict[str, Any]]:
    ensure_backtest_schema()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, date_from, date_to, asof_hhmm, status, trading_days,
                       qualified_rows, universe_size, error, params,
                       started_at, finished_at, created_at
                FROM momentx_backtest_runs
                ORDER BY id DESC
                LIMIT :lim
                """
            ),
            {"lim": max(1, min(100, int(limit)))},
        ).mappings().fetchall()
        out = []
        for r in rows:
            d = dict(r)
            for k in ("date_from", "date_to", "started_at", "finished_at", "created_at"):
                d[k] = _iso(d.get(k))
            if isinstance(d.get("params"), str):
                try:
                    d["params"] = json.loads(d["params"])
                except Exception:
                    pass
            out.append(d)
        return out
    finally:
        db.close()


def fetch_run(run_id: int) -> Optional[Dict[str, Any]]:
    ensure_backtest_schema()
    db = SessionLocal()
    try:
        r = db.execute(
            text(
                """
                SELECT id, date_from, date_to, asof_hhmm, status, trading_days,
                       qualified_rows, universe_size, error, params,
                       started_at, finished_at, created_at
                FROM momentx_backtest_runs WHERE id = :id
                """
            ),
            {"id": run_id},
        ).mappings().fetchone()
        if not r:
            return None
        d = dict(r)
        for k in ("date_from", "date_to", "started_at", "finished_at", "created_at"):
            d[k] = _iso(d.get(k))
        return d
    finally:
        db.close()


def latest_done_run_id() -> Optional[int]:
    ensure_backtest_schema()
    db = SessionLocal()
    try:
        r = db.execute(
            text(
                """
                SELECT id FROM momentx_backtest_runs
                WHERE status = 'done'
                ORDER BY id DESC LIMIT 1
                """
            )
        ).fetchone()
        return int(r[0]) if r else None
    finally:
        db.close()


def fetch_qualified(
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    run_id: Optional[int] = None,
) -> Dict[str, Any]:
    ensure_backtest_schema()
    rid = run_id
    if rid is None:
        rid = latest_done_run_id()
    db = SessionLocal()
    try:
        clauses = []
        params: Dict[str, Any] = {}
        if rid is not None:
            clauses.append("run_id = :run_id")
            params["run_id"] = rid
        if date_from is not None:
            clauses.append("trade_date >= :d0")
            params["d0"] = date_from
        if date_to is not None:
            clauses.append("trade_date <= :d1")
            params["d1"] = date_to
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = db.execute(
            text(
                f"""
                SELECT id, run_id, trade_date, stock, equity_instrument_key,
                       future_symbol, future_instrument_key, lot_size,
                       price, sma200, prev_close, pct_change, rsi, williams_r, adx,
                       today_volume, volume_sma5, first_filtered_at, conditions, asof_at
                FROM momentx_backtest_qualified
                {where}
                ORDER BY trade_date ASC, first_filtered_at ASC NULLS LAST, stock ASC
                """
            ),
            params,
        ).mappings().fetchall()

        by_day: Dict[str, List[Dict[str, Any]]] = {}
        flat: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            td = _iso(d.get("trade_date"))
            d["trade_date"] = td
            d["asof_at"] = _iso(d.get("asof_at"))
            d["first_filtered_at"] = _iso(d.get("first_filtered_at"))
            cond = d.get("conditions")
            if isinstance(cond, str):
                try:
                    d["conditions"] = json.loads(cond)
                except Exception:
                    d["conditions"] = {}
            flat.append(d)
            by_day.setdefault(td or "", []).append(d)

        days = [
            {"trade_date": k, "count": len(v), "symbols": v}
            for k, v in by_day.items()
        ]
        return {
            "ok": True,
            "run_id": rid,
            "total": len(flat),
            "day_count": len(days),
            "days": days,
            "rows": flat,
        }
    finally:
        db.close()
