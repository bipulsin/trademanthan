"""Persistence for Smart Futures (SQL via SQLAlchemy engine)."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)

_SKIP = object()


def get_config() -> Dict[str, Any]:
    row = None
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT live_enabled, position_size, partial_exit_enabled,
                           brick_atr_period, brick_atr_override, updated_at
                    FROM smart_futures_config WHERE id = 1
                    """
                )
            ).fetchone()
    except Exception:
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        """
                        SELECT live_enabled, position_size, partial_exit_enabled, updated_at
                        FROM smart_futures_config WHERE id = 1
                        """
                    )
                ).fetchone()
        except Exception:
            row = None
    if row and len(row) == 4:
        return {
            "live_enabled": bool(row[0]),
            "position_size": int(row[1] or 1),
            "partial_exit_enabled": bool(row[2]),
            "brick_atr_period": 10,
            "brick_atr_override": None,
            "updated_at": row[3].isoformat() if row[3] else None,
        }
    if not row:
        return {
            "live_enabled": False,
            "position_size": 1,
            "partial_exit_enabled": False,
            "brick_atr_period": 10,
            "brick_atr_override": None,
            "updated_at": None,
        }
    ov = row[4]
    return {
        "live_enabled": bool(row[0]),
        "position_size": int(row[1] or 1),
        "partial_exit_enabled": bool(row[2]),
        "brick_atr_period": int(row[3] or 10),
        "brick_atr_override": float(ov) if ov is not None else None,
        "updated_at": row[5].isoformat() if row[5] else None,
    }


def merge_config(patch: Dict[str, Any]) -> Dict[str, Any]:
    """Merge partial update into smart_futures_config (id=1)."""
    if not patch:
        return get_config()
    cur = get_config()
    if "live_enabled" in patch:
        cur["live_enabled"] = bool(patch["live_enabled"])
    if "position_size" in patch:
        ps = int(patch["position_size"] or 1)
        cur["position_size"] = ps if ps in (1, 2, 3) else 1
    if "partial_exit_enabled" in patch:
        cur["partial_exit_enabled"] = bool(patch["partial_exit_enabled"])
    if "brick_atr_period" in patch:
        p = int(patch["brick_atr_period"] or 10)
        cur["brick_atr_period"] = max(2, min(99, p))
    if "brick_atr_override" in patch:
        v = patch["brick_atr_override"]
        if v is None or (isinstance(v, str) and str(v).strip() == ""):
            cur["brick_atr_override"] = None
        else:
            fv = float(v)
            cur["brick_atr_override"] = fv if fv > 0 else None

    le = cur["live_enabled"]
    ps = cur["position_size"]
    pe = cur["partial_exit_enabled"]
    bap = int(cur["brick_atr_period"] or 10)
    bao = cur["brick_atr_override"]

    with engine.begin() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM smart_futures_config WHERE id = 1")).scalar()
        if n:
            conn.execute(
                text(
                    """
                    UPDATE smart_futures_config
                    SET live_enabled = :le, position_size = :ps, partial_exit_enabled = :pe,
                        brick_atr_period = :bap, brick_atr_override = :bao,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                    """
                ),
                {"le": le, "ps": ps, "pe": pe, "bap": bap, "bao": bao},
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO smart_futures_config (
                        id, live_enabled, position_size, partial_exit_enabled,
                        brick_atr_period, brick_atr_override, updated_at
                    )
                    VALUES (1, :le, :ps, :pe, :bap, :bao, CURRENT_TIMESTAMP)
                    """
                ),
                {"le": le, "ps": ps, "pe": pe, "bap": bap, "bao": bao},
            )
    logger.info(
        "smart_futures_config updated live=%s position_size=%s brick_atr_period=%s override=%s",
        le,
        ps,
        bap,
        bao,
    )
    return get_config()


def set_config(
    *,
    live_enabled: Optional[bool] = None,
    position_size: Optional[int] = None,
    partial_exit_enabled: Optional[bool] = None,
    brick_atr_period: Optional[int] = None,
    brick_atr_override: Any = _SKIP,
) -> Dict[str, Any]:
    """Backward-compatible setter; unspecified fields keep current values."""
    patch: Dict[str, Any] = {}
    if live_enabled is not None:
        patch["live_enabled"] = live_enabled
    if position_size is not None:
        patch["position_size"] = position_size
    if partial_exit_enabled is not None:
        patch["partial_exit_enabled"] = partial_exit_enabled
    if brick_atr_period is not None:
        patch["brick_atr_period"] = brick_atr_period
    if brick_atr_override is not _SKIP:
        patch["brick_atr_override"] = brick_atr_override
    if not patch:
        return get_config()
    return merge_config(patch)


def fetch_future_symbols() -> List[Dict[str, str]]:
    """Rows from arbitrage_master with current-month future keys."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT stock, currmth_future_symbol, currmth_future_instrument_key
                FROM arbitrage_master
                WHERE currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
    out: List[Dict[str, str]] = []
    for r in rows:
        out.append(
            {
                "stock": (r[0] or "").strip(),
                "symbol": (r[1] or "").strip(),
                "instrument_key": (r[2] or "").strip(),
            }
        )
    return out


def replace_candidates_session(session_d: date, rows: List[Dict[str, Any]]) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM smart_futures_candidate WHERE session_date = :d"), {"d": session_d})
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO smart_futures_candidate (
                      session_date, symbol, instrument_key, score, direction, last_brick_color,
                      entry_signal, exit_ready, main_brick_size, ltp, prefilter_pass, structure_pass, updated_at
                    ) VALUES (
                      :session_date, :symbol, :instrument_key, :score, :direction, :last_brick_color,
                      :entry_signal, :exit_ready, :main_brick_size, :ltp, :prefilter_pass, :structure_pass, CURRENT_TIMESTAMP
                    )
                    """
                ),
                {
                    "session_date": session_d,
                    "symbol": row["symbol"],
                    "instrument_key": row["instrument_key"],
                    "score": int(row.get("score") or 0),
                    "direction": row.get("direction") or "NONE",
                    "last_brick_color": row.get("last_brick_color"),
                    "entry_signal": bool(row.get("entry_signal")),
                    "exit_ready": bool(row.get("exit_ready")),
                    "main_brick_size": row.get("main_brick_size"),
                    "ltp": row.get("ltp"),
                    "prefilter_pass": bool(row.get("prefilter_pass")),
                    "structure_pass": bool(row.get("structure_pass")),
                },
            )


def get_exit_ready_by_instrument(session_d: date) -> Dict[str, bool]:
    """Map instrument_key -> exit_ready from today's candidate rows (updated by exit-check job)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT instrument_key, exit_ready
                FROM smart_futures_candidate
                WHERE session_date = :d
                """
            ),
            {"d": session_d},
        ).fetchall()
    return {str(r[0]): bool(r[1]) for r in rows}


def _candidate_row_to_dict(r) -> Dict[str, Any]:
    return {
        "symbol": r[0],
        "instrument_key": r[1],
        "score": int(r[2] or 0),
        "direction": r[3],
        "last_brick_color": r[4],
        "entry_signal": bool(r[5]),
        "exit_ready": bool(r[6]),
        "main_brick_size": float(r[7]) if r[7] is not None else None,
        "ltp": float(r[8]) if r[8] is not None else None,
        "prefilter_pass": bool(r[9]),
        "structure_pass": bool(r[10]),
        "updated_at": r[11].isoformat() if r[11] else None,
    }


def get_top_candidates(session_d: date, limit: int = 5) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT symbol, instrument_key, score, direction, last_brick_color,
                       entry_signal, exit_ready, main_brick_size, ltp, prefilter_pass, structure_pass, updated_at
                FROM smart_futures_candidate
                WHERE session_date = :d
                ORDER BY score DESC, symbol ASC
                LIMIT :lim
                """
            ),
            {"d": session_d, "lim": limit},
        ).fetchall()
    return [_candidate_row_to_dict(r) for r in rows]


def get_top_candidates_min_score(session_d: date, min_score_exclusive: int, limit: int) -> List[Dict[str, Any]]:
    """Highest-scoring rows for the session with score > min_score_exclusive (e.g. min_score_exclusive=4 → score ≥ 5)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT symbol, instrument_key, score, direction, last_brick_color,
                       entry_signal, exit_ready, main_brick_size, ltp, prefilter_pass, structure_pass, updated_at
                FROM smart_futures_candidate
                WHERE session_date = :d AND score > :min_sc
                ORDER BY score DESC, symbol ASC
                LIMIT :lim
                """
            ),
            {"d": session_d, "min_sc": min_score_exclusive, "lim": limit},
        ).fetchall()
    return [_candidate_row_to_dict(r) for r in rows]


def get_recent_candidates_min_score(session_d: date, min_score_exclusive: int, limit: int) -> List[Dict[str, Any]]:
    """Most recently updated rows for the session with score > min_score_exclusive."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT symbol, instrument_key, score, direction, last_brick_color,
                       entry_signal, exit_ready, main_brick_size, ltp, prefilter_pass, structure_pass, updated_at
                FROM smart_futures_candidate
                WHERE session_date = :d AND score > :min_sc
                ORDER BY updated_at DESC NULLS LAST, symbol ASC
                LIMIT :lim
                """
            ),
            {"d": session_d, "min_sc": min_score_exclusive, "lim": limit},
        ).fetchall()
    return [_candidate_row_to_dict(r) for r in rows]


def list_open_positions(session_d: date) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, user_id, symbol, instrument_key, direction, lots_open, lots_total,
                       entry_price, main_brick_size, half_brick_size, entry_order_id, created_at
                FROM smart_futures_position
                WHERE session_date = :d AND status = 'OPEN' AND lots_open > 0
                ORDER BY id
                """
            ),
            {"d": session_d},
        ).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "user_id": r[1],
                "symbol": r[2],
                "instrument_key": r[3],
                "direction": r[4],
                "lots_open": int(r[5]),
                "lots_total": int(r[6]),
                "entry_price": float(r[7]) if r[7] is not None else None,
                "main_brick_size": float(r[8]) if r[8] is not None else None,
                "half_brick_size": float(r[9]) if r[9] is not None else None,
                "entry_order_id": r[10],
                "created_at": r[11].isoformat() if r[11] else None,
            }
        )
    return out


def insert_position(
    session_d: date,
    user_id: Optional[int],
    symbol: str,
    instrument_key: str,
    direction: str,
    lots: int,
    entry_price: Optional[float],
    main_brick: float,
    half_brick: float,
    entry_order_id: Optional[str],
) -> int:
    with engine.begin() as conn:
        r = conn.execute(
            text(
                """
                INSERT INTO smart_futures_position (
                  session_date, user_id, symbol, instrument_key, direction,
                  lots_open, lots_total, entry_price, main_brick_size, half_brick_size, entry_order_id, status
                ) VALUES (
                  :sd, :uid, :sym, :ik, :dir, :lots, :lots, :ep, :mb, :hb, :eoid, 'OPEN'
                )
                RETURNING id
                """
            ),
            {
                "sd": session_d,
                "uid": user_id,
                "sym": symbol,
                "ik": instrument_key,
                "dir": direction,
                "lots": lots,
                "ep": entry_price,
                "mb": main_brick,
                "hb": half_brick,
                "eoid": entry_order_id,
            },
        )
        row = r.fetchone()
        pid = int(row[0]) if row else 0
    return pid


def update_position_lots(position_id: int, lots_open: int, status: str = "OPEN") -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE smart_futures_position
                SET lots_open = :lo, status = :st,
                    closed_at = CASE WHEN :st = 'CLOSED' THEN CURRENT_TIMESTAMP ELSE closed_at END
                WHERE id = :id
                """
            ),
            {"lo": lots_open, "st": status, "id": position_id},
        )


def close_position(position_id: int) -> None:
    update_position_lots(position_id, 0, "CLOSED")


def insert_order_audit(
    user_id: Optional[int],
    position_id: Optional[int],
    side: str,
    order_id: Optional[str],
    quantity: int,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO smart_futures_order_audit (user_id, position_id, side, order_id, quantity)
                VALUES (:uid, :pid, :side, :oid, :qty)
                """
            ),
            {
                "uid": user_id,
                "pid": position_id,
                "side": side,
                "oid": order_id,
                "qty": quantity,
            },
        )
