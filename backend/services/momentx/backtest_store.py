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

CREATE TABLE IF NOT EXISTS momentx_backtest_day_coverage (
    trade_date DATE NOT NULL,
    asof_hhmm TEXT NOT NULL DEFAULT '14:45',
    run_id BIGINT,
    qualified_count INT NOT NULL DEFAULT 0,
    scanned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, asof_hhmm)
);
CREATE INDEX IF NOT EXISTS ix_mx_bt_day_cov_date
    ON momentx_backtest_day_coverage (trade_date);
"""


# ruleset: v1 bull-only; v2 bull+bear; v3 + stale guards + full-day vol baseline.
BACKTEST_RULESET = "stage1_v3"

_ALTERS = (
    "ALTER TABLE momentx_backtest_qualified ADD COLUMN IF NOT EXISTS first_filtered_at TIMESTAMPTZ",
    "ALTER TABLE momentx_backtest_qualified ADD COLUMN IF NOT EXISTS side TEXT NOT NULL DEFAULT 'bullish'",
    "ALTER TABLE momentx_backtest_day_coverage ADD COLUMN IF NOT EXISTS ruleset TEXT NOT NULL DEFAULT 'stage1_v1'",
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
        # Seed coverage from existing qualified rows so prior runs are reusable.
        db.execute(
            text(
                """
                INSERT INTO momentx_backtest_day_coverage
                    (trade_date, asof_hhmm, run_id, qualified_count, scanned_at)
                SELECT q.trade_date,
                       COALESCE(r.asof_hhmm, '14:45') AS asof_hhmm,
                       MAX(q.run_id) AS run_id,
                       COUNT(*)::INT AS qualified_count,
                       NOW() AS scanned_at
                FROM momentx_backtest_qualified q
                LEFT JOIN momentx_backtest_runs r ON r.id = q.run_id
                GROUP BY q.trade_date, COALESCE(r.asof_hhmm, '14:45')
                ON CONFLICT (trade_date, asof_hhmm) DO NOTHING
                """
            )
        )
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
        db.execute(
            text(
                """
                DELETE FROM momentx_backtest_day_coverage
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


def record_day_coverage(
    *,
    trade_date: date,
    asof_hhmm: str,
    run_id: int,
    qualified_count: int,
) -> None:
    ensure_backtest_schema()
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                INSERT INTO momentx_backtest_day_coverage
                    (trade_date, asof_hhmm, run_id, qualified_count, scanned_at, ruleset)
                VALUES
                    (:trade_date, :asof_hhmm, :run_id, :qualified_count, NOW(), :ruleset)
                ON CONFLICT (trade_date, asof_hhmm) DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    qualified_count = EXCLUDED.qualified_count,
                    scanned_at = NOW(),
                    ruleset = EXCLUDED.ruleset
                """
            ),
            {
                "trade_date": trade_date,
                "asof_hhmm": asof_hhmm,
                "run_id": run_id,
                "qualified_count": int(qualified_count or 0),
                "ruleset": BACKTEST_RULESET,
            },
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("momentx backtest record_day_coverage failed")
    finally:
        db.close()


def covered_trade_dates(
    date_from: date,
    date_to: date,
    *,
    asof_hhmm: str = "14:45",
) -> List[date]:
    ensure_backtest_schema()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT trade_date FROM momentx_backtest_day_coverage
                WHERE trade_date BETWEEN :d0 AND :d1
                  AND asof_hhmm = :asof
                  AND ruleset = :ruleset
                ORDER BY trade_date
                """
            ),
            {
                "d0": date_from,
                "d1": date_to,
                "asof": asof_hhmm,
                "ruleset": BACKTEST_RULESET,
            },
        ).fetchall()
        out: List[date] = []
        for r in rows:
            d = r[0]
            if isinstance(d, datetime):
                out.append(d.date())
            elif isinstance(d, date):
                out.append(d)
        return out
    finally:
        db.close()


def _iter_sessions_local(date_from: date, date_to: date) -> List[date]:
    """Weekend + holiday filter without importing the backtest engine (avoids cycles)."""
    from datetime import timedelta

    holidays: set = set()
    try:
        from backend.services.market_holiday import refresh_holiday_dates_from_db

        holidays = refresh_holiday_dates_from_db() or set()
    except Exception:
        holidays = set()
    out: List[date] = []
    d = date_from
    while d <= date_to:
        if d.weekday() < 5 and d not in holidays:
            out.append(d)
        d += timedelta(days=1)
    return out


def coverage_report(
    date_from: date,
    date_to: date,
    *,
    asof_hhmm: str = "14:45",
    sessions: Optional[Sequence[date]] = None,
) -> Dict[str, Any]:
    """Which trading days in range are already stored vs still need a live scan."""
    sess = list(sessions) if sessions is not None else _iter_sessions_local(date_from, date_to)
    covered = set(covered_trade_dates(date_from, date_to, asof_hhmm=asof_hhmm))
    missing = [d for d in sess if d not in covered]
    return {
        "ok": True,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "asof_hhmm": asof_hhmm,
        "trading_days": len(sess),
        "covered_days": len(sess) - len(missing),
        "missing_days": len(missing),
        "complete": len(missing) == 0 and len(sess) > 0,
        "missing": [d.isoformat() for d in missing],
        "covered": [d.isoformat() for d in sess if d in covered],
    }


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
                        future_symbol, future_instrument_key, lot_size, side,
                        price, sma200, prev_close, pct_change, rsi, williams_r, adx,
                        today_volume, volume_sma5, first_filtered_at, conditions, asof_at, created_at
                    ) VALUES (
                        :run_id, :trade_date, :stock, :equity_instrument_key,
                        :future_symbol, :future_instrument_key, :lot_size, :side,
                        :price, :sma200, :prev_close, :pct_change, :rsi, :williams_r, :adx,
                        :today_volume, :volume_sma5, :first_filtered_at,
                        CAST(:conditions AS JSONB), :asof_at, NOW()
                    )
                    ON CONFLICT (run_id, trade_date, stock) DO UPDATE SET
                        equity_instrument_key = EXCLUDED.equity_instrument_key,
                        future_symbol = EXCLUDED.future_symbol,
                        future_instrument_key = EXCLUDED.future_instrument_key,
                        lot_size = EXCLUDED.lot_size,
                        side = EXCLUDED.side,
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
                    "side": (str(r.get("side") or "bullish").lower()
                             if str(r.get("side") or "").lower() in ("bullish", "bearish")
                             else "bullish"),
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
    """Load qualified rows for a date window.

    When ``run_id`` is omitted, merge across all runs (latest run wins per stock/day)
    so appended storage can be reused without re-scanning.
    """
    ensure_backtest_schema()
    rid = run_id
    db = SessionLocal()
    try:
        params: Dict[str, Any] = {}
        if rid is not None:
            clauses = ["run_id = :run_id"]
            params["run_id"] = rid
            if date_from is not None:
                clauses.append("trade_date >= :d0")
                params["d0"] = date_from
            if date_to is not None:
                clauses.append("trade_date <= :d1")
                params["d1"] = date_to
            where = "WHERE " + " AND ".join(clauses)
            sql = f"""
                SELECT id, run_id, trade_date, stock, equity_instrument_key,
                       future_symbol, future_instrument_key, lot_size, side,
                       price, sma200, prev_close, pct_change, rsi, williams_r, adx,
                       today_volume, volume_sma5, first_filtered_at, conditions, asof_at
                FROM momentx_backtest_qualified
                {where}
                ORDER BY trade_date ASC, first_filtered_at ASC NULLS LAST, stock ASC
            """
        else:
            # Append-friendly: one row per (trade_date, stock) from the newest run.
            clauses = []
            if date_from is not None:
                clauses.append("trade_date >= :d0")
                params["d0"] = date_from
            if date_to is not None:
                clauses.append("trade_date <= :d1")
                params["d1"] = date_to
            where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
            sql = f"""
                SELECT DISTINCT ON (trade_date, stock)
                       id, run_id, trade_date, stock, equity_instrument_key,
                       future_symbol, future_instrument_key, lot_size, side,
                       price, sma200, prev_close, pct_change, rsi, williams_r, adx,
                       today_volume, volume_sma5, first_filtered_at, conditions, asof_at
                FROM momentx_backtest_qualified
                {where}
                ORDER BY trade_date ASC, stock ASC, run_id DESC,
                         first_filtered_at ASC NULLS LAST
            """
        rows = db.execute(text(sql), params).mappings().fetchall()

        by_day: Dict[str, List[Dict[str, Any]]] = {}
        flat: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            td = _iso(d.get("trade_date"))
            d["trade_date"] = td
            d["asof_at"] = _iso(d.get("asof_at"))
            d["first_filtered_at"] = _iso(d.get("first_filtered_at"))
            d["side"] = str(d.get("side") or "bullish")
            cond = d.get("conditions")
            if isinstance(cond, str):
                try:
                    d["conditions"] = json.loads(cond)
                except Exception:
                    d["conditions"] = {}
            conds = d.get("conditions") if isinstance(d.get("conditions"), dict) else {}
            d["volume_ok"] = bool(conds.get("volume_ok")) if conds else None
            flat.append(d)
            by_day.setdefault(td or "", []).append(d)

        days = [
            {"trade_date": k, "count": len(v), "symbols": v}
            for k, v in by_day.items()
        ]
        cov = None
        if date_from is not None and date_to is not None and rid is None:
            try:
                cov = coverage_report(date_from, date_to)
            except Exception:
                cov = None
        return {
            "ok": True,
            "run_id": rid,
            "from_storage": rid is None,
            "total": len(flat),
            "day_count": len(days),
            "days": days,
            "rows": flat,
            "coverage": cov,
        }
    finally:
        db.close()
