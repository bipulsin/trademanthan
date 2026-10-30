"""Persist MomentX day-board filter / activation / trade log to Postgres."""
from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_DDL = """
CREATE TABLE IF NOT EXISTS momentx_filtered_symbols (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    stock VARCHAR(64) NOT NULL,
    future_symbol VARCHAR(128),
    future_instrument_key VARCHAR(128),
    lot_size INTEGER,
    filtered_at TIMESTAMPTZ NOT NULL,
    first_active_at TIMESTAMPTZ,
    first_active_ltp DOUBLE PRECISION,
    last_active_at TIMESTAMPTZ,
    traded_at TIMESTAMPTZ,
    entry_price DOUBLE PRECISION,
    entry_time TIMESTAMPTZ,
    exit_price DOUBLE PRECISION,
    exit_time TIMESTAMPTZ,
    pnl DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_momentx_filtered_stock_date
    ON momentx_filtered_symbols (trade_date, stock);
CREATE INDEX IF NOT EXISTS ix_momentx_filtered_trade_date
    ON momentx_filtered_symbols (trade_date);
"""

_ALTERS = [
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS first_active_at TIMESTAMPTZ",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS first_active_ltp DOUBLE PRECISION",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS last_active_at TIMESTAMPTZ",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS traded_at TIMESTAMPTZ",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS entry_price DOUBLE PRECISION",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS entry_time TIMESTAMPTZ",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS exit_price DOUBLE PRECISION",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS exit_time TIMESTAMPTZ",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS pnl DOUBLE PRECISION",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    "ALTER TABLE momentx_filtered_symbols ADD COLUMN IF NOT EXISTS side TEXT NOT NULL DEFAULT 'bullish'",
]


def _aware(dt: Optional[datetime] = None) -> datetime:
    t = dt or datetime.now(IST)
    if t.tzinfo is None:
        return IST.localize(t)
    return t.astimezone(IST)


def _f(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_dt(v: Any) -> Optional[datetime]:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return _aware(v)
    try:
        s = str(v).replace("Z", "+00:00")
        # datetime-local from browser often "YYYY-MM-DDTHH:MM"
        if len(s) == 16 and "T" in s:
            s = s + ":00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    except Exception:
        return None


def compute_pnl(
    entry: Optional[float],
    exit_: Optional[float],
    lot_size: Optional[int],
    *,
    side: Optional[str] = None,
) -> Optional[float]:
    if entry is None or exit_ is None:
        return None
    lot = int(lot_size or 1)
    # Bearish = short: profit when exit < entry.
    if str(side or "").lower() == "bearish":
        return round((float(entry) - float(exit_)) * float(lot), 2)
    return round((float(exit_) - float(entry)) * float(lot), 2)


def ensure_momentx_tables() -> None:
    db = SessionLocal()
    try:
        for stmt in _DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                db.execute(text(s))
        for alt in _ALTERS:
            try:
                db.execute(text(alt))
                db.commit()
            except Exception:
                db.rollback()
                logger.info("momentx alter skipped: %s", alt[:60])
        try:
            # Deduplicate before unique index if legacy multi-rows exist
            db.execute(
                text(
                    """
                    DELETE FROM momentx_filtered_symbols a
                    USING momentx_filtered_symbols b
                    WHERE a.trade_date = b.trade_date
                      AND a.stock = b.stock
                      AND a.id > b.id
                    """
                )
            )
            db.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_momentx_filtered_stock_date
                    ON momentx_filtered_symbols (trade_date, stock)
                    """
                )
            )
            db.commit()
        except Exception:
            db.rollback()
            logger.info("momentx unique index create skipped/failed")
    except Exception:
        db.rollback()
        logger.exception("momentx ensure tables failed")
        raise
    finally:
        db.close()


def record_filtered_symbols(
    cards: Iterable[Dict[str, Any]],
    *,
    trade_date: date,
    filtered_at: datetime,
) -> int:
    """Upsert day-log rows when symbols are Stage-1 filtered onto the board."""
    rows = list(cards or [])
    if not rows:
        return 0
    filtered_at = _aware(filtered_at)
    db = SessionLocal()
    inserted = 0
    try:
        for c in rows:
            stock = str(c.get("stock") or "").strip().upper()
            if not stock:
                continue
            lot = c.get("lot_size")
            try:
                lot_i = int(lot) if lot is not None else None
            except (TypeError, ValueError):
                lot_i = None
            side = str(c.get("side") or (c.get("stage1") or {}).get("side") or "bullish").lower()
            if side not in ("bullish", "bearish"):
                side = "bullish"
            db.execute(
                text(
                    """
                    INSERT INTO momentx_filtered_symbols
                        (trade_date, stock, future_symbol, future_instrument_key, lot_size, side, filtered_at, updated_at)
                    VALUES
                        (:trade_date, :stock, :future_symbol, :future_instrument_key, :lot_size, :side, :filtered_at, NOW())
                    ON CONFLICT (trade_date, stock) DO UPDATE SET
                        future_symbol = COALESCE(EXCLUDED.future_symbol, momentx_filtered_symbols.future_symbol),
                        future_instrument_key = COALESCE(EXCLUDED.future_instrument_key, momentx_filtered_symbols.future_instrument_key),
                        lot_size = COALESCE(EXCLUDED.lot_size, momentx_filtered_symbols.lot_size),
                        side = EXCLUDED.side,
                        filtered_at = LEAST(momentx_filtered_symbols.filtered_at, EXCLUDED.filtered_at),
                        updated_at = NOW()
                    """
                ),
                {
                    "trade_date": trade_date,
                    "stock": stock,
                    "future_symbol": (c.get("future_symbol") or None),
                    "future_instrument_key": (c.get("future_instrument_key") or None),
                    "lot_size": lot_i,
                    "side": side,
                    "filtered_at": filtered_at,
                },
            )
            inserted += 1
        db.commit()
        return inserted
    except Exception:
        db.rollback()
        logger.exception("momentx record_filtered_symbols failed")
        return 0
    finally:
        db.close()


def record_activation(
    *,
    stock: str,
    future_symbol: Optional[str] = None,
    future_instrument_key: Optional[str] = None,
    lot_size: Optional[int] = None,
    ltp: Optional[float] = None,
    activated_at: Optional[datetime] = None,
    trade_date: Optional[date] = None,
) -> None:
    """Stamp first/last activation times (and first-active LTP) for the day."""
    stock_u = str(stock or "").strip().upper()
    if not stock_u:
        return
    now = _aware(activated_at)
    day = trade_date or now.date()
    ltp_f = _f(ltp)
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                INSERT INTO momentx_filtered_symbols
                    (trade_date, stock, future_symbol, future_instrument_key, lot_size,
                     filtered_at, first_active_at, first_active_ltp, last_active_at, updated_at)
                VALUES
                    (:trade_date, :stock, :future_symbol, :future_instrument_key, :lot_size,
                     :now, :now, :ltp, :now, NOW())
                ON CONFLICT (trade_date, stock) DO UPDATE SET
                    future_symbol = COALESCE(EXCLUDED.future_symbol, momentx_filtered_symbols.future_symbol),
                    future_instrument_key = COALESCE(EXCLUDED.future_instrument_key, momentx_filtered_symbols.future_instrument_key),
                    lot_size = COALESCE(EXCLUDED.lot_size, momentx_filtered_symbols.lot_size),
                    first_active_at = COALESCE(momentx_filtered_symbols.first_active_at, EXCLUDED.first_active_at),
                    first_active_ltp = COALESCE(momentx_filtered_symbols.first_active_ltp, EXCLUDED.first_active_ltp),
                    last_active_at = EXCLUDED.last_active_at,
                    updated_at = NOW()
                """
            ),
            {
                "trade_date": day,
                "stock": stock_u,
                "future_symbol": future_symbol,
                "future_instrument_key": future_instrument_key,
                "lot_size": int(lot_size) if lot_size is not None else None,
                "now": now,
                "ltp": ltp_f,
            },
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("momentx record_activation failed stock=%s", stock_u)
    finally:
        db.close()


def record_take_trade(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Persist Take Trade: entry (editable), exit = last LTP, pnl."""
    stock = str((payload or {}).get("stock") or "").strip().upper()
    if not stock:
        return {"ok": False, "error": "stock required"}
    now = _aware()
    entry_price = _f((payload or {}).get("entry_price"))
    if entry_price is None:
        entry_price = _f((payload or {}).get("ltp"))
    entry_time = _parse_dt((payload or {}).get("entry_time")) or now
    exit_price = _f((payload or {}).get("exit_price"))
    if exit_price is None:
        exit_price = _f((payload or {}).get("ltp"))
    exit_time = _parse_dt((payload or {}).get("exit_time")) or now
    lot = (payload or {}).get("lot_size")
    try:
        lot_i = int(lot) if lot is not None else None
    except (TypeError, ValueError):
        lot_i = None
    side = str((payload or {}).get("side") or "bullish").lower()
    if side not in ("bullish", "bearish"):
        side = "bullish"
    day = entry_time.date()
    db = SessionLocal()
    try:
        existing = db.execute(
            text(
                """
                SELECT side FROM momentx_filtered_symbols
                WHERE trade_date = :trade_date AND stock = :stock
                """
            ),
            {"trade_date": day, "stock": stock},
        ).mappings().first()
        if existing and existing.get("side"):
            side = str(existing["side"] or side).lower()
        pnl = compute_pnl(entry_price, exit_price, lot_i, side=side)
        db.execute(
            text(
                """
                INSERT INTO momentx_filtered_symbols
                    (trade_date, stock, future_symbol, future_instrument_key, lot_size, side,
                     filtered_at, traded_at, entry_price, entry_time, exit_price, exit_time, pnl, updated_at)
                VALUES
                    (:trade_date, :stock, :future_symbol, :future_instrument_key, :lot_size, :side,
                     :now, :now, :entry_price, :entry_time, :exit_price, :exit_time, :pnl, NOW())
                ON CONFLICT (trade_date, stock) DO UPDATE SET
                    future_symbol = COALESCE(EXCLUDED.future_symbol, momentx_filtered_symbols.future_symbol),
                    future_instrument_key = COALESCE(EXCLUDED.future_instrument_key, momentx_filtered_symbols.future_instrument_key),
                    lot_size = COALESCE(EXCLUDED.lot_size, momentx_filtered_symbols.lot_size),
                    side = COALESCE(momentx_filtered_symbols.side, EXCLUDED.side),
                    traded_at = EXCLUDED.traded_at,
                    entry_price = EXCLUDED.entry_price,
                    entry_time = EXCLUDED.entry_time,
                    exit_price = EXCLUDED.exit_price,
                    exit_time = EXCLUDED.exit_time,
                    pnl = EXCLUDED.pnl,
                    updated_at = NOW()
                """
            ),
            {
                "trade_date": day,
                "stock": stock,
                "future_symbol": (payload or {}).get("future_symbol"),
                "future_instrument_key": (payload or {}).get("future_instrument_key"),
                "lot_size": lot_i,
                "side": side,
                "now": now,
                "entry_price": entry_price,
                "entry_time": entry_time,
                "exit_price": exit_price,
                "exit_time": exit_time,
                "pnl": pnl,
            },
        )
        db.commit()
        return {
            "ok": True,
            "stock": stock,
            "entry_price": entry_price,
            "entry_time": entry_time.isoformat(),
            "exit_price": exit_price,
            "exit_time": exit_time.isoformat(),
            "pnl": pnl,
        }
    except Exception as e:
        db.rollback()
        logger.exception("momentx record_take_trade failed")
        return {"ok": False, "error": str(e)[:200]}
    finally:
        db.close()


def update_trade_fields(row_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
    """Manual edit of entry/exit price & time; recomputes PnL."""
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT id, lot_size, side, entry_price, entry_time, exit_price, exit_time, pnl, traded_at
                FROM momentx_filtered_symbols WHERE id = :id
                """
            ),
            {"id": int(row_id)},
        ).mappings().first()
        if not row:
            return {"ok": False, "error": "not_found"}
        entry_price = _f(patch.get("entry_price"))
        if entry_price is None:
            entry_price = _f(row["entry_price"])
        exit_price = _f(patch.get("exit_price"))
        if exit_price is None:
            exit_price = _f(row["exit_price"])
        entry_time = _parse_dt(patch.get("entry_time"))
        if entry_time is None:
            entry_time = row["entry_time"]
        exit_time = _parse_dt(patch.get("exit_time"))
        if exit_time is None:
            exit_time = row["exit_time"]
        lot_i = row["lot_size"]
        pnl = compute_pnl(entry_price, exit_price, lot_i, side=row.get("side"))
        traded_at = row["traded_at"] or entry_time or _aware()
        db.execute(
            text(
                """
                UPDATE momentx_filtered_symbols SET
                    entry_price = :entry_price,
                    entry_time = :entry_time,
                    exit_price = :exit_price,
                    exit_time = :exit_time,
                    pnl = :pnl,
                    traded_at = COALESCE(traded_at, :traded_at),
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "id": int(row_id),
                "entry_price": entry_price,
                "entry_time": entry_time,
                "exit_price": exit_price,
                "exit_time": exit_time,
                "pnl": pnl,
                "traded_at": traded_at,
            },
        )
        db.commit()
        return {
            "ok": True,
            "id": int(row_id),
            "entry_price": entry_price,
            "entry_time": entry_time.isoformat() if entry_time else None,
            "exit_price": exit_price,
            "exit_time": exit_time.isoformat() if exit_time else None,
            "pnl": pnl,
        }
    except Exception as e:
        db.rollback()
        logger.exception("momentx update_trade_fields failed")
        return {"ok": False, "error": str(e)[:200]}
    finally:
        db.close()


def _row_to_dict(r: Any) -> Dict[str, Any]:
    def iso(v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, datetime):
            return _aware(v).isoformat()
        return str(v)

    return {
        "id": int(r["id"]),
        "trade_date": str(r["trade_date"]),
        "stock": r["stock"],
        "side": str(r.get("side") or "bullish"),
        "future_symbol": r["future_symbol"],
        "future_instrument_key": r["future_instrument_key"],
        "lot_size": r["lot_size"],
        "filtered_at": iso(r["filtered_at"]),
        "first_active_at": iso(r["first_active_at"]),
        "first_active_ltp": r["first_active_ltp"],
        "last_active_at": iso(r["last_active_at"]),
        "traded_at": iso(r["traded_at"]),
        "entry_price": r["entry_price"],
        "entry_time": iso(r["entry_time"]),
        "exit_price": r["exit_price"],
        "exit_time": iso(r["exit_time"]),
        "pnl": r["pnl"],
    }


def fetch_log(
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Dict[str, Any]:
    """Return rows grouped by trade_date (latest first) with per-date PnL totals."""
    db = SessionLocal()
    try:
        clauses = []
        params: Dict[str, Any] = {}
        if date_from is not None:
            clauses.append("trade_date >= :date_from")
            params["date_from"] = date_from
        if date_to is not None:
            clauses.append("trade_date <= :date_to")
            params["date_to"] = date_to
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = db.execute(
            text(
                f"""
                SELECT id, trade_date, stock, side, future_symbol, future_instrument_key, lot_size,
                       filtered_at, first_active_at, first_active_ltp, last_active_at,
                       traded_at, entry_price, entry_time, exit_price, exit_time, pnl
                FROM momentx_filtered_symbols
                {where}
                ORDER BY trade_date DESC, filtered_at ASC NULLS LAST, stock ASC
                """
            ),
            params,
        ).mappings().all()
        by_date: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            d = str(r["trade_date"])
            by_date.setdefault(d, []).append(_row_to_dict(r))
        groups = []
        for d, items in by_date.items():
            pnls = [x["pnl"] for x in items if x.get("pnl") is not None]
            groups.append(
                {
                    "trade_date": d,
                    "total_pnl": round(sum(pnls), 2) if pnls else None,
                    "traded_count": sum(1 for x in items if x.get("traded_at")),
                    "rows": items,
                }
            )
        return {"ok": True, "groups": groups, "count": len(rows)}
    except Exception as e:
        logger.exception("momentx fetch_log failed")
        return {"ok": False, "error": str(e)[:200], "groups": [], "count": 0}
    finally:
        db.close()


def log_csv(
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Tuple[str, str]:
    """Return (filename, csv_text)."""
    data = fetch_log(date_from=date_from, date_to=date_to)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "trade_date",
            "stock",
            "side",
            "future_symbol",
            "lot_size",
            "filtered_at",
            "first_active_at",
            "first_active_ltp",
            "last_active_at",
            "traded_at",
            "entry_price",
            "entry_time",
            "exit_price",
            "exit_time",
            "pnl",
            "day_total_pnl",
        ]
    )
    for g in data.get("groups") or []:
        day_total = g.get("total_pnl")
        for r in g.get("rows") or []:
            w.writerow(
                [
                    r.get("trade_date"),
                    r.get("stock"),
                    r.get("side") or "bullish",
                    r.get("future_symbol"),
                    r.get("lot_size"),
                    r.get("filtered_at"),
                    r.get("first_active_at"),
                    r.get("first_active_ltp"),
                    r.get("last_active_at"),
                    r.get("traded_at"),
                    r.get("entry_price"),
                    r.get("entry_time"),
                    r.get("exit_price"),
                    r.get("exit_time"),
                    r.get("pnl"),
                    day_total,
                ]
            )
    fname = "momentx_log_{}_{}.csv".format(
        (date_from or date(2020, 1, 1)).isoformat(),
        (date_to or date.today()).isoformat(),
    )
    return fname, buf.getvalue()
