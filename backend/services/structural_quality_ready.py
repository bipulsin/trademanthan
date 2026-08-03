"""SQ ≥75 READY NOW promotion + lifecycle tracking.

On each checklist enrich: for Garuda Top-6 + grade A/B symbols, if additive
Total ≥ threshold, force READY (bypass pullback/expired/secondary blocked),
stamp promoted_via_structural_score + score breakdown, and log lifecycle row.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import engine
from backend.services.structural_quality_score import (
    _dir_sign,
    composite_total,
    enrich_session_10m_bars,
    grade_ab_ok,
    grade_bonus,
    promote_enabled,
    promote_threshold,
    score_bars_through,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_ENSURED = False

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS sq_ready_promotion_log (
    id BIGSERIAL PRIMARY KEY,
    session_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT,
    promoted_at TIMESTAMPTZ NOT NULL,
    total_score DOUBLE PRECISION,
    rs_score DOUBLE PRECISION,
    garuda_score DOUBLE PRECISION,
    ow DOUBLE PRECISION,
    vw DOUBLE PRECISION,
    ew DOUBLE PRECISION,
    grade_bonus DOUBLE PRECISION,
    confidence_grade TEXT,
    garuda_top6_rank INTEGER,
    also_organic BOOLEAN,
    pre_state TEXT,
    rendered_state TEXT,
    score_breakdown JSONB,
    outcome_favorable BOOLEAN,
    outcome_note TEXT,
    rule15_2candle_ok BOOLEAN,
    exit_kind TEXT,
    closed_at TIMESTAMPTZ,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (session_date, symbol)
);
CREATE INDEX IF NOT EXISTS ix_sq_ready_session ON sq_ready_promotion_log (session_date DESC);
CREATE INDEX IF NOT EXISTS ix_sq_ready_sym ON sq_ready_promotion_log (symbol, session_date DESC);
"""


def ensure_sq_ready_promotion_log() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        for stmt in _CREATE_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
    _ENSURED = True


def load_latest_garuda_top6(db, session_date: str) -> Dict[str, Dict[str, Any]]:
    """Latest Garuda Top-6 rows for session → symbol → {rank, rank_score, side}."""
    out: Dict[str, Dict[str, Any]] = {}
    try:
        from backend.services.garuda_screener.job import ensure_garuda_screener_log

        ensure_garuda_screener_log()
        rows = db.execute(
            text(
                """
                SELECT DISTINCT ON (UPPER(symbol))
                       UPPER(symbol) AS symbol, side, top6_rank, rank_score, bar_end
                FROM garuda_screener_log
                WHERE session_date = CAST(:d AS date)
                  AND top6_rank IS NOT NULL
                ORDER BY UPPER(symbol), bar_end DESC
                """
            ),
            {"d": session_date},
        ).mappings().all()
        for r in rows:
            out[str(r["symbol"]).upper()] = {
                "top6_rank": int(r["top6_rank"]) if r["top6_rank"] is not None else None,
                "rank_score": float(r["rank_score"]) if r["rank_score"] is not None else None,
                "side": r["side"],
                "bar_end": r["bar_end"],
            }
    except Exception as exc:
        logger.debug("garuda top6 load skipped: %s", exc)
    return out


def load_universe_rs_scores(db, session_date: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    try:
        from sqlalchemy.sql import bindparam

        rows = db.execute(
            text(
                """
                SELECT DISTINCT ON (UPPER(symbol))
                       UPPER(symbol) AS symbol, trade_score, relative_strength,
                       confidence_grade, market_regime
                FROM rs_universe_score_snapshot
                WHERE session_date = CAST(:d AS date)
                  AND UPPER(symbol) IN :syms
                ORDER BY UPPER(symbol), scan_time DESC
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"d": session_date, "syms": [s.upper() for s in symbols]},
        ).mappings().all()
        for r in rows:
            out[str(r["symbol"]).upper()] = dict(r)
    except Exception as exc:
        logger.debug("universe RS load skipped: %s", exc)
    return out


def _locf_garuda_rank(db, session_date: str, symbol: str) -> Optional[float]:
    try:
        row = db.execute(
            text(
                """
                SELECT rank_score FROM garuda_screener_log
                WHERE session_date = CAST(:d AS date)
                  AND UPPER(symbol) = :sym
                  AND rank_score IS NOT NULL
                ORDER BY bar_end DESC
                LIMIT 1
                """
            ),
            {"d": session_date, "sym": symbol.upper()},
        ).mappings().first()
        if row and row["rank_score"] is not None:
            return float(row["rank_score"])
    except Exception:
        pass
    return None


def evaluate_sq_for_stock(
    *,
    db,
    stock: Dict[str, Any],
    session_date: str,
    candles: List[Dict[str, Any]],
    garuda_meta: Optional[Dict[str, Any]],
    rs_meta: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not garuda_meta or garuda_meta.get("top6_rank") is None:
        return None
    grade = (
        (rs_meta or {}).get("confidence_grade")
        or stock.get("confidence")
        or stock.get("dashboard_kavach")
    )
    if not grade_ab_ok(grade):
        return None
    rs_score = (rs_meta or {}).get("trade_score")
    if rs_score is None:
        rs_score = stock.get("trade_score")
    if rs_score is None:
        return None
    garuda_score = garuda_meta.get("rank_score")
    if garuda_score is None:
        garuda_score = _locf_garuda_rank(db, session_date, stock.get("symbol") or "")
    if garuda_score is None:
        return None
    side = garuda_meta.get("side") or stock.get("direction")
    dir_sign = _dir_sign(side)
    if dir_sign == 0:
        dir_sign = _dir_sign(stock.get("direction"))
    bars = enrich_session_10m_bars(candles, session_date)
    if len(bars) < 1:
        return None
    breakdown = score_bars_through(
        bars,
        dir_sign=dir_sign,
        rs_score=float(rs_score),
        garuda_score=float(garuda_score),
        grade=str(grade),
    )
    if not breakdown:
        return None
    breakdown["garuda_top6_rank"] = garuda_meta.get("top6_rank")
    breakdown["meets_threshold"] = breakdown["total"] >= promote_threshold()
    breakdown["threshold"] = promote_threshold()
    return breakdown


def apply_sq_ready_promotions(
    stocks: List[Dict[str, Any]],
    *,
    db,
    session_date: str,
    candle_cache: Dict[str, Any],
) -> Dict[str, int]:
    """Force READY when SQ Total ≥ threshold for Top-6 + grade A/B.

    Runs after FSM downgrades so SQ can bypass pullback/expired/secondary blocks.
    Grade gate remains required inside evaluate_sq_for_stock.
    """
    stats = {"checked": 0, "eligible": 0, "promoted": 0, "already_ready": 0, "logged": 0}
    if not promote_enabled():
        return stats
    ensure_sq_ready_promotion_log()
    top6 = load_latest_garuda_top6(db, session_date)
    if not top6:
        return stats
    syms = list(top6.keys())
    rs_map = load_universe_rs_scores(db, session_date, syms)

    for s in stocks:
        sym = (s.get("symbol") or "").upper()
        if not sym or sym not in top6:
            continue
        stats["checked"] += 1
        candles = candle_cache.get(sym) or []
        br = evaluate_sq_for_stock(
            db=db,
            stock=s,
            session_date=session_date,
            candles=candles,
            garuda_meta=top6.get(sym),
            rs_meta=rs_map.get(sym),
        )
        if not br:
            continue
        stats["eligible"] += 1
        s["structural_quality"] = br
        s["sq_total"] = br["total"]
        if not br.get("meets_threshold"):
            continue

        pre = s.get("trade_state")
        s["sq_pre_state"] = pre
        already = str(pre or "").upper() in ("READY", "READY(RECHECK)")
        if already:
            stats["already_ready"] += 1
            s["promoted_via_structural_score"] = True
            s["also_organic_ready"] = True
            s["sq_promoted_this_cycle"] = False
            badges = list(s.get("gate_badges") or [])
            if "SQ" not in badges:
                badges.insert(0, "SQ")
            s["gate_badges"] = badges
            if _log_promotion(db, session_date, s, br, pre_state=pre):
                stats["logged"] += 1
            continue

        # Bypass secondary FSM: promote directly to READY.
        s["trade_state"] = "READY"
        s["trade_state_reason"] = (
            f"READY · SQ Total {br['total']:.1f} ≥ {promote_threshold():.0f} "
            f"(bypass secondary FSM)"
        )
        s["promoted_via_structural_score"] = True
        s["also_organic_ready"] = False
        s["sq_promoted_this_cycle"] = True
        s["trade_take_enabled"] = True
        s["trade_take_disable_reason"] = None
        # Entry from live EMA5 when available
        ema5 = None
        try:
            raw = s.get("live_candle_ema5") or (s.get("_live_kavach_metrics") or {}).get("ema5")
            if raw is not None:
                ema5 = float(raw)
        except (TypeError, ValueError):
            ema5 = None
        if ema5 is not None and ema5 > 0 and s.get("trade_entry") is None:
            s["trade_entry"] = round(float(ema5), 2)
            s["trade_entry_source"] = "sq_ema5"
            s["trade_entry_source_label"] = "Entry (EMA5 · SQ)"
        badges = list(s.get("gate_badges") or [])
        if "SQ" not in badges:
            badges.insert(0, "SQ")
        s["gate_badges"] = badges
        stats["promoted"] += 1
        if _log_promotion(db, session_date, s, br, pre_state=pre):
            stats["logged"] += 1
    return stats


def ensure_sq_consistency_rows(
    consistency_rows: List[Dict[str, Any]],
    stocks: List[Dict[str, Any]],
    *,
    session_date: str,
) -> int:
    """Append consistency-log stubs for SQ-only promotes missing from pre-SQ collection.

    Organic READY rows are already queued before SQ runs; SQ-only promotions
    otherwise never appear in ``kavach_ready_consistency_log``. Finalize pass
    fills take/entry/score the same way as organic rows.
    """
    existing = {(r.get("symbol") or "").upper() for r in consistency_rows}
    n = 0
    for s in stocks:
        if not s.get("sq_promoted_this_cycle"):
            continue
        sym = (s.get("symbol") or "").upper()
        if not sym or sym in existing:
            continue
        vq = s.get("vwap_quality") if isinstance(s.get("vwap_quality"), dict) else {}
        pre = s.get("sq_pre_state")
        consistency_rows.append(
            {
                "session_date": session_date,
                "symbol": sym,
                "direction": s.get("direction"),
                "rendered_state": s.get("trade_state"),
                "pre_gate_state": pre,
                "pre_stack_state": pre,
                "in_lock": bool(s.get("in_lock")),
                "lock_rank": s.get("lock_rank"),
                "lock_direction": s.get("lock_direction"),
                "lock_mismatch": False,
                "vwap_slope_score": vq.get("slope_score"),
                "steep_ok": vq.get("steep_ok"),
                "flip_flop": vq.get("flip_flop"),
                "whipsaw_crosses": vq.get("whipsaw_crosses"),
                "quality_pass": vq.get("quality_pass"),
                "vwap_gate_enabled": None,
                "vwap_would_block": False,
                "vwap_gate_applied": False,
                "vwap_extension_pct": None,
                "inputs": {
                    "confidence": s.get("confidence") or s.get("dashboard_kavach"),
                    "trade_entry": s.get("trade_entry"),
                    "trade_sl": s.get("trade_sl"),
                    "sq_appended_post_promote": True,
                },
            }
        )
        existing.add(sym)
        n += 1
    return n


def _log_promotion(
    db,
    session_date: str,
    stock: Dict[str, Any],
    br: Dict[str, Any],
    *,
    pre_state: Any,
) -> bool:
    try:
        ensure_sq_ready_promotion_log()
        now = datetime.now(IST)
        import json

        db.execute(
            text(
                """
                INSERT INTO sq_ready_promotion_log (
                    session_date, symbol, direction, promoted_at,
                    total_score, rs_score, garuda_score, ow, vw, ew,
                    grade_bonus, confidence_grade, garuda_top6_rank,
                    also_organic, pre_state, rendered_state, score_breakdown
                ) VALUES (
                    CAST(:d AS date), :sym, :dir, :pat,
                    :total, :rs, :garuda, :ow, :vw, :ew,
                    :gb, :grade, :rank,
                    :organic, :pre, :rendered, CAST(:bd AS jsonb)
                )
                ON CONFLICT (session_date, symbol) DO NOTHING
                """
            ),
            {
                "d": session_date,
                "sym": (stock.get("symbol") or "").upper(),
                "dir": stock.get("direction"),
                "pat": now,
                "total": br.get("total"),
                "rs": br.get("rs_score"),
                "garuda": br.get("garuda_score"),
                "ow": br.get("OW"),
                "vw": br.get("VW"),
                "ew": br.get("EW"),
                "gb": br.get("grade_bonus"),
                "grade": br.get("confidence_grade"),
                "rank": br.get("garuda_top6_rank"),
                "organic": bool(stock.get("also_organic_ready")),
                "pre": str(pre_state) if pre_state is not None else None,
                "rendered": stock.get("trade_state"),
                "bd": json.dumps(br, default=str),
            },
        )
        db.commit()
        return True
    except Exception as exc:
        logger.warning("sq_ready_promotion_log write failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return False


def update_sq_lifecycle_outcomes(
    stocks: List[Dict[str, Any]],
    *,
    db,
    session_date: str,
) -> int:
    """Cheap price-action outcome stamp for open SQ promotions (favorable = LTP beyond entry in dir)."""
    ensure_sq_ready_promotion_log()
    updated = 0
    try:
        open_rows = db.execute(
            text(
                """
                SELECT id, symbol, direction, score_breakdown
                FROM sq_ready_promotion_log
                WHERE session_date = CAST(:d AS date)
                  AND closed_at IS NULL
                """
            ),
            {"d": session_date},
        ).mappings().all()
        by_sym = {(s.get("symbol") or "").upper(): s for s in stocks}
        for r in open_rows:
            sym = str(r["symbol"]).upper()
            s = by_sym.get(sym)
            if not s:
                continue
            entry = s.get("trade_entry")
            px = None
            try:
                px = float(s.get("ltp") or s.get("price") or 0) or None
            except (TypeError, ValueError):
                px = None
            if entry is None or px is None:
                continue
            is_long = str(r.get("direction") or s.get("direction") or "LONG").upper() != "SHORT"
            fav = (px >= float(entry)) if is_long else (px <= float(entry))
            # Rule 15 2-candle: use readiness flag if present
            r15 = s.get("rule15_2candle_ok")
            if r15 is None:
                r15 = s.get("two_candle_validation")
            db.execute(
                text(
                    """
                    UPDATE sq_ready_promotion_log
                    SET outcome_favorable = :fav,
                        outcome_note = :note,
                        rule15_2candle_ok = :r15,
                        also_organic = CASE
                            WHEN :organic THEN TRUE ELSE also_organic END
                    WHERE id = :id
                    """
                ),
                {
                    "fav": bool(fav),
                    "note": f"ltp={px} entry={entry}",
                    "r15": bool(r15) if r15 is not None else None,
                    "organic": bool(
                        s.get("also_organic_ready")
                        or (
                            str(s.get("trade_state") or "").upper()
                            in ("READY", "READY(RECHECK)")
                            and not s.get("promoted_via_structural_score")
                        )
                    ),
                    "id": r["id"],
                },
            )
            updated += 1
        if updated:
            db.commit()
    except Exception as exc:
        logger.debug("sq lifecycle update skipped: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
    return updated


def daily_sq_rollup(db, session_date: str) -> Dict[str, Any]:
    ensure_sq_ready_promotion_log()
    rows = db.execute(
        text(
            """
            SELECT symbol, also_organic, total_score, outcome_favorable, pre_state, rendered_state
            FROM sq_ready_promotion_log
            WHERE session_date = CAST(:d AS date)
            """
        ),
        {"d": session_date},
    ).mappings().all()
    n = len(rows)
    organic = sum(1 for r in rows if r["also_organic"])
    fav = sum(1 for r in rows if r["outcome_favorable"] is True)
    unfav = sum(1 for r in rows if r["outcome_favorable"] is False)
    return {
        "session_date": session_date,
        "sq_promotions": n,
        "also_organic": organic,
        "pure_sq": n - organic,
        "outcome_favorable": fav,
        "outcome_unfavorable": unfav,
        "symbols": sorted({str(r["symbol"]).upper() for r in rows}),
    }
