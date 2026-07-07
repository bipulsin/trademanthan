"""Shadow RS selection logging — research only, zero live-facing effect.

Logs actual vs volume-weighted Top-5 at 09:25 / 09:45 / 10:15 and the
10:15 tardy addendum. Does NOT write daily_snapshot, snapshot_lock, or
conviction board state.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.daily_checklist_snapshot import get_locked_symbols
from backend.services.kavach_engine import RANKING_BEARISH, RANKING_BULLISH

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

SHADOW_CHECKPOINTS = ("09:25", "09:45", "10:15")
SESSION_OPEN_MIN = 9 * 60 + 20

_INSERT_SELECTION = text(
    """
    INSERT INTO rs_shadow_selection_log (
        session_date, checkpoint_label, checkpoint_time, side, selection_method,
        rank_position, symbol, relative_strength, volume_ratio, vw_score,
        trade_score, kavach_state, instrument_key, scan_time
    ) VALUES (
        :session_date, :checkpoint_label, :checkpoint_time, :side, :selection_method,
        :rank_position, :symbol, :relative_strength, :volume_ratio, :vw_score,
        :trade_score, :kavach_state, :instrument_key, :scan_time
    )
    ON CONFLICT (session_date, checkpoint_label, side, selection_method, rank_position)
    DO UPDATE SET
        checkpoint_time = EXCLUDED.checkpoint_time,
        symbol = EXCLUDED.symbol,
        relative_strength = EXCLUDED.relative_strength,
        volume_ratio = EXCLUDED.volume_ratio,
        vw_score = EXCLUDED.vw_score,
        trade_score = EXCLUDED.trade_score,
        kavach_state = EXCLUDED.kavach_state,
        instrument_key = EXCLUDED.instrument_key,
        scan_time = EXCLUDED.scan_time
    """
)

_INSERT_TARDY = text(
    """
    INSERT INTO rs_shadow_tardy_addendum (
        session_date, checkpoint_label, symbol, side, relative_strength,
        volume_ratio, trade_score, kavach_state, instrument_key,
        on_morning_lock, logged_at
    ) VALUES (
        :session_date, :checkpoint_label, :symbol, :side, :relative_strength,
        :volume_ratio, :trade_score, :kavach_state, :instrument_key,
        :on_morning_lock, :logged_at
    )
    ON CONFLICT (session_date, symbol, side) DO UPDATE SET
        checkpoint_label = EXCLUDED.checkpoint_label,
        relative_strength = EXCLUDED.relative_strength,
        volume_ratio = EXCLUDED.volume_ratio,
        trade_score = EXCLUDED.trade_score,
        kavach_state = EXCLUDED.kavach_state,
        instrument_key = EXCLUDED.instrument_key,
        on_morning_lock = EXCLUDED.on_morning_lock,
        logged_at = EXCLUDED.logged_at
    """
)


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _checkpoint_target_minutes(label: str) -> int:
    h, m = label.split(":")
    return int(h) * 60 + int(m)


def _nearest_scan(db, session_date: str, target_min: int) -> Optional[Dict[str, Any]]:
    row = db.execute(
        text(
            """
            SELECT scan_time,
                   EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                     + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata') AS mins
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
            GROUP BY scan_time
            ORDER BY ABS(
                (EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                 + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata')) - :tgt
            ), scan_time
            LIMIT 1
            """
        ),
        {"d": session_date, "tgt": target_min},
    ).mappings().first()
    return dict(row) if row else None


def _top5_at_scan(db, session_date: str, scan_time, ranking_type: str) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT symbol, rank_position, relative_strength, volume_ratio,
                   trade_score, kavach_state, instrument_key
            FROM relative_strength_snapshot
            WHERE scan_time = :t AND ranking_type = :rt AND rank_position <= 5
            ORDER BY rank_position
            """
        ),
        {"t": scan_time, "rt": ranking_type},
    ).mappings().all()
    return [dict(r) for r in rows]


def _intraday_pool(
    db, session_date: str, ranking_type: str, end_min: int
) -> List[Dict[str, Any]]:
    """Distinct symbols seen in top-5 between 09:20 and checkpoint (first row per symbol)."""
    rows = db.execute(
        text(
            """
            SELECT DISTINCT ON (symbol) symbol, relative_strength, volume_ratio,
                   trade_score, kavach_state, instrument_key, scan_time
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
              AND ranking_type = :rt
              AND rank_position <= 5
              AND EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                  + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata')
                  BETWEEN :open_min AND :end_min
            ORDER BY symbol, scan_time
            """
        ),
        {
            "d": session_date,
            "rt": ranking_type,
            "open_min": SESSION_OPEN_MIN,
            "end_min": end_min,
        },
    ).mappings().all()
    return [dict(r) for r in rows]


def vw_score(rs: float, vol_ratio: float, side: str) -> float:
    mult = min(2.0, max(0.6, float(vol_ratio or 1.0)))
    if side == "BEAR":
        return -float(rs) * mult
    return float(rs) * mult


def _volume_weighted_top5(pool: List[Dict[str, Any]], side: str) -> List[Dict[str, Any]]:
    if not pool:
        return []
    ranked = sorted(
        pool,
        key=lambda r: vw_score(
            float(r.get("relative_strength") or 0),
            float(r.get("volume_ratio") or 1),
            side,
        ),
        reverse=(side == "BULL"),
    )
    out: List[Dict[str, Any]] = []
    for i, r in enumerate(ranked[:5], start=1):
        row = dict(r)
        row["rank_position"] = i
        row["vw_score"] = vw_score(
            float(r.get("relative_strength") or 0),
            float(r.get("volume_ratio") or 1),
            side,
        )
        out.append(row)
    return out


def _persist_selection(
    db,
    session_date: str,
    checkpoint_label: str,
    checkpoint_time: datetime,
    side: str,
    method: str,
    picks: List[Dict[str, Any]],
    scan_time,
) -> int:
    n = 0
    for r in picks:
        db.execute(
            _INSERT_SELECTION,
            {
                "session_date": session_date,
                "checkpoint_label": checkpoint_label,
                "checkpoint_time": checkpoint_time,
                "side": side,
                "selection_method": method,
                "rank_position": int(r.get("rank_position") or 0),
                "symbol": r.get("symbol"),
                "relative_strength": r.get("relative_strength"),
                "volume_ratio": r.get("volume_ratio"),
                "vw_score": r.get("vw_score"),
                "trade_score": r.get("trade_score"),
                "kavach_state": r.get("kavach_state"),
                "instrument_key": r.get("instrument_key"),
                "scan_time": scan_time,
            },
        )
        n += 1
    return n


def run_shadow_selection_log(checkpoint_label: str) -> Dict[str, Any]:
    """Log shadow Top-5 selections at a checkpoint. Read-only w.r.t. live checklist."""
    if checkpoint_label not in SHADOW_CHECKPOINTS:
        return {"ok": False, "reason": "invalid_checkpoint"}

    sd = today_ist()
    now = datetime.now(IST)
    target_min = _checkpoint_target_minutes(checkpoint_label)
    db = SessionLocal()
    try:
        scan = _nearest_scan(db, sd, target_min)
        if not scan:
            return {"ok": False, "reason": "no_scan_near_checkpoint", "session_date": sd}

        scan_time = scan["scan_time"]
        logged = 0
        tardy = 0
        morning_lock: Set[str] = set(get_locked_symbols(db, sd))

        for ranking, side in ((RANKING_BULLISH, "BULL"), (RANKING_BEARISH, "BEAR")):
            actual = _top5_at_scan(db, sd, scan_time, ranking)
            pool = _intraday_pool(db, sd, ranking, target_min)
            vw = _volume_weighted_top5(pool, side)

            logged += _persist_selection(
                db, sd, checkpoint_label, now, side, "actual", actual, scan_time
            )
            logged += _persist_selection(
                db, sd, checkpoint_label, now, side, "volume_weighted", vw, scan_time
            )

            if checkpoint_label == "10:15":
                actual_syms = {r["symbol"] for r in actual}
                for sym in actual_syms:
                    if sym in morning_lock:
                        continue
                    row = next((r for r in actual if r["symbol"] == sym), None)
                    if not row:
                        continue
                    db.execute(
                        _INSERT_TARDY,
                        {
                            "session_date": sd,
                            "checkpoint_label": checkpoint_label,
                            "symbol": sym,
                            "side": side,
                            "relative_strength": row.get("relative_strength"),
                            "volume_ratio": row.get("volume_ratio"),
                            "trade_score": row.get("trade_score"),
                            "kavach_state": row.get("kavach_state"),
                            "instrument_key": row.get("instrument_key"),
                            "on_morning_lock": False,
                            "logged_at": now,
                        },
                    )
                    tardy += 1

        db.commit()
        out = {
            "ok": True,
            "session_date": sd,
            "checkpoint": checkpoint_label,
            "rows_logged": logged,
            "tardy_addendum": tardy,
            "scan_time": str(scan_time),
        }
        logger.info("rs_shadow_selection: %s", out)
        return out
    except Exception as exc:
        db.rollback()
        logger.warning("rs_shadow_selection failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()
