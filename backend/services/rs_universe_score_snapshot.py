#!/usr/bin/env python3
"""Full-universe RS score shadow persist + Top-10 membership hysteresis.

Shadow-only by default (``RS_UNIVERSE_SCORE_SHADOW=1``). Does not change
``relative_strength_snapshot`` Top-10 truncate or lock/R1/R2 consumers.

Membership bonus applies only to sort keys for Top-10/Top-5 flags — raw
``relative_strength`` / ``trade_score`` / ``confidence_grade`` are unchanged.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

RANKING_BULLISH = "BULLISH"
RANKING_BEARISH = "BEARISH"
RANKING_NEUTRAL = "NEUTRAL"

# §9.2 — explicit default for review; override via env without code change.
INCUMBENT_RS_BONUS = float(os.getenv("RS_INCUMBENT_RS_BONUS", "0.20"))
SHADOW_ENABLED = os.getenv("RS_UNIVERSE_SCORE_SHADOW", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Grade-first cutover: live grade readers prefer rs_universe_score_snapshot when set.
# Default on (2026-08-02): grade coverage only — lock/R2 still on Top-10 RSS.
GRADE_CUTOVER = os.getenv("RS_UNIVERSE_GRADE_CUTOVER", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

_ENSURED = False

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS rs_universe_score_snapshot (
    id BIGSERIAL PRIMARY KEY,
    scan_time TIMESTAMPTZ NOT NULL,
    session_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    instrument_key TEXT,
    current_price DOUBLE PRECISION,
    previous_close DOUBLE PRECISION,
    stock_percent DOUBLE PRECISION,
    nifty_percent DOUBLE PRECISION,
    relative_strength DOUBLE PRECISION,
    relative_strength_membership DOUBLE PRECISION,
    ema5 DOUBLE PRECISION,
    ema9 DOUBLE PRECISION,
    ema10 DOUBLE PRECISION,
    vwap DOUBLE PRECISION,
    supertrend DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_histogram DOUBLE PRECISION,
    adx DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    avg_volume DOUBLE PRECISION,
    volume_ratio DOUBLE PRECISION,
    volume_tod_ratio DOUBLE PRECISION,
    volume_label TEXT,
    vwap_purity_pct DOUBLE PRECISION,
    market_regime TEXT,
    confidence_grade TEXT,
    kavach_state TEXT,
    kavach_strength DOUBLE PRECISION,
    trade_score DOUBLE PRECISION,
    ranking_type TEXT,
    rank_raw INTEGER,
    rank_membership INTEGER,
    in_top10_membership BOOLEAN NOT NULL DEFAULT FALSE,
    in_top5_membership BOOLEAN NOT NULL DEFAULT FALSE,
    incumbent_bonus_applied BOOLEAN NOT NULL DEFAULT FALSE,
    incumbent_rs_bonus DOUBLE PRECISION,
    scan_trigger TEXT,
    cache_only BOOLEAN,
    from_cache BOOLEAN,
    exclusion_reason TEXT,
    detail TEXT,
    rocket_score INTEGER,
    rocket_signals TEXT,
    rocket_label TEXT,
    UNIQUE (scan_time, symbol)
);
CREATE INDEX IF NOT EXISTS ix_rs_univ_scan ON rs_universe_score_snapshot (scan_time DESC);
CREATE INDEX IF NOT EXISTS ix_rs_univ_sym_scan ON rs_universe_score_snapshot (symbol, scan_time DESC);
CREATE INDEX IF NOT EXISTS ix_rs_univ_session ON rs_universe_score_snapshot (session_date, scan_time DESC);
CREATE INDEX IF NOT EXISTS ix_rs_univ_top10 ON rs_universe_score_snapshot (scan_time, in_top10_membership)
    WHERE in_top10_membership;
"""

_UPSERT = text(
    """
    INSERT INTO rs_universe_score_snapshot (
        scan_time, session_date, symbol, instrument_key,
        current_price, previous_close, stock_percent, nifty_percent,
        relative_strength, relative_strength_membership,
        ema5, ema9, ema10, vwap, supertrend, macd, macd_signal, macd_histogram, adx,
        volume, avg_volume, volume_ratio, volume_tod_ratio, volume_label,
        vwap_purity_pct, market_regime, confidence_grade, kavach_state, kavach_strength,
        trade_score, ranking_type, rank_raw, rank_membership,
        in_top10_membership, in_top5_membership, incumbent_bonus_applied, incumbent_rs_bonus,
        scan_trigger, cache_only, from_cache, exclusion_reason, detail,
        rocket_score, rocket_signals, rocket_label
    ) VALUES (
        :scan_time, CAST(:session_date AS date), :symbol, :instrument_key,
        :current_price, :previous_close, :stock_percent, :nifty_percent,
        :relative_strength, :relative_strength_membership,
        :ema5, :ema9, :ema10, :vwap, :supertrend, :macd, :macd_signal, :macd_histogram, :adx,
        :volume, :avg_volume, :volume_ratio, :volume_tod_ratio, :volume_label,
        :vwap_purity_pct, :market_regime, :confidence_grade, :kavach_state, :kavach_strength,
        :trade_score, :ranking_type, :rank_raw, :rank_membership,
        :in_top10_membership, :in_top5_membership, :incumbent_bonus_applied, :incumbent_rs_bonus,
        :scan_trigger, :cache_only, :from_cache, :exclusion_reason, :detail,
        :rocket_score, :rocket_signals, :rocket_label
    )
    ON CONFLICT (scan_time, symbol) DO UPDATE SET
        ranking_type = EXCLUDED.ranking_type,
        relative_strength = EXCLUDED.relative_strength,
        relative_strength_membership = EXCLUDED.relative_strength_membership,
        trade_score = EXCLUDED.trade_score,
        confidence_grade = EXCLUDED.confidence_grade,
        rank_raw = EXCLUDED.rank_raw,
        rank_membership = EXCLUDED.rank_membership,
        in_top10_membership = EXCLUDED.in_top10_membership,
        in_top5_membership = EXCLUDED.in_top5_membership,
        incumbent_bonus_applied = EXCLUDED.incumbent_bonus_applied,
        exclusion_reason = EXCLUDED.exclusion_reason,
        detail = EXCLUDED.detail,
        rocket_score = EXCLUDED.rocket_score,
        rocket_signals = EXCLUDED.rocket_signals,
        rocket_label = EXCLUDED.rocket_label
    """
)


def ensure_rs_universe_score_snapshot() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        for stmt in _CREATE_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
        conn.execute(text(
            "ALTER TABLE rs_universe_score_snapshot "
            "ADD COLUMN IF NOT EXISTS rocket_score INTEGER"
        ))
        conn.execute(text(
            "ALTER TABLE rs_universe_score_snapshot "
            "ADD COLUMN IF NOT EXISTS rocket_signals TEXT"
        ))
        conn.execute(text(
            "ALTER TABLE rs_universe_score_snapshot "
            "ADD COLUMN IF NOT EXISTS rocket_label TEXT"
        ))
    _ENSURED = True


def shadow_enabled() -> bool:
    return SHADOW_ENABLED


def grade_cutover_enabled() -> bool:
    return GRADE_CUTOVER


def incumbent_bonus() -> float:
    return float(INCUMBENT_RS_BONUS)


def _session_date(scan_time: datetime) -> str:
    st = scan_time.astimezone(IST) if scan_time.tzinfo else IST.localize(scan_time)
    return st.strftime("%Y-%m-%d")


def load_prior_top10_incumbents(db, scan_time: datetime) -> Dict[str, Set[str]]:
    """Prior-scan Top-10 membership by side; seed from RSS Top-10 if no universe yet."""
    out = {RANKING_BULLISH: set(), RANKING_BEARISH: set()}
    try:
        prior = db.execute(
            text(
                """
                SELECT ranking_type, symbol
                FROM rs_universe_score_snapshot
                WHERE scan_time = (
                    SELECT MAX(scan_time) FROM rs_universe_score_snapshot
                    WHERE scan_time < :st AND in_top10_membership
                )
                AND in_top10_membership
                """
            ),
            {"st": scan_time},
        ).fetchall()
        if prior:
            for side, sym in prior:
                if side in out and sym:
                    out[side].add(str(sym).upper())
            return out
    except Exception as exc:
        logger.debug("universe prior top10 skipped: %s", exc)

    try:
        rows = db.execute(
            text(
                """
                SELECT ranking_type, symbol
                FROM relative_strength_snapshot
                WHERE scan_time = (
                    SELECT MAX(scan_time) FROM relative_strength_snapshot
                    WHERE scan_time < :st
                )
                AND rank_position <= 10
                """
            ),
            {"st": scan_time},
        ).fetchall()
        for side, sym in rows:
            side_u = str(side or "").upper()
            if "BULL" in side_u:
                out[RANKING_BULLISH].add(str(sym).upper())
            elif "BEAR" in side_u:
                out[RANKING_BEARISH].add(str(sym).upper())
    except Exception as exc:
        logger.debug("RSS seed incumbents skipped: %s", exc)
    return out


def apply_membership_ranks(
    bucket: List[Dict[str, Any]],
    *,
    side: str,
    incumbents: Set[str],
    bonus: float,
) -> List[Dict[str, Any]]:
    """Assign rank_raw (no bonus) and rank_membership (with incumbent RS bonus)."""
    if not bucket:
        return []
    # raw order
    if side == RANKING_BULLISH:
        raw_sorted = sorted(
            bucket, key=lambda x: (-float(x.get("relative_strength") or 0), -float(x.get("trade_score") or 0))
        )
    else:
        raw_sorted = sorted(
            bucket, key=lambda x: (float(x.get("relative_strength") or 0), -float(x.get("trade_score") or 0))
        )
    for i, row in enumerate(raw_sorted, start=1):
        row["rank_raw"] = i

    scored: List[Dict[str, Any]] = []
    for row in bucket:
        r = dict(row)
        sym = str(r.get("symbol") or "").upper()
        rs = float(r.get("relative_strength") or 0.0)
        is_inc = sym in incumbents
        if side == RANKING_BULLISH:
            rs_m = rs + bonus if is_inc else rs
        else:
            rs_m = rs - bonus if is_inc else rs
        r["relative_strength_membership"] = rs_m
        r["incumbent_bonus_applied"] = is_inc and bonus != 0
        r["incumbent_rs_bonus"] = bonus if is_inc else 0.0
        scored.append(r)

    if side == RANKING_BULLISH:
        scored.sort(
            key=lambda x: (
                -float(x.get("relative_strength_membership") or 0),
                -float(x.get("trade_score") or 0),
            )
        )
    else:
        scored.sort(
            key=lambda x: (
                float(x.get("relative_strength_membership") or 0),
                -float(x.get("trade_score") or 0),
            )
        )
    for i, row in enumerate(scored, start=1):
        row["rank_membership"] = i
        row["in_top10_membership"] = i <= 10
        row["in_top5_membership"] = i <= 5
        if "rank_raw" not in row:
            row["rank_raw"] = None
    # restore rank_raw from raw_sorted map
    raw_map = {str(r.get("symbol") or "").upper(): r["rank_raw"] for r in raw_sorted}
    for row in scored:
        row["rank_raw"] = raw_map.get(str(row.get("symbol") or "").upper())
    return scored


def _signals_dump(value: Any) -> str:
    if value is None or value == "":
        return "[]"
    if isinstance(value, str):
        return value
    try:
        return json.dumps(list(value), separators=(",", ":"))
    except Exception:
        return "[]"


def _row_params(
    scan_time: datetime,
    session_date: str,
    r: Dict[str, Any],
    *,
    scan_trigger: str,
    cache_only: Optional[bool],
) -> Dict[str, Any]:
    return {
        "scan_time": scan_time,
        "session_date": session_date,
        "symbol": r.get("symbol"),
        "instrument_key": r.get("instrument_key"),
        "current_price": r.get("current_price"),
        "previous_close": r.get("previous_close"),
        "stock_percent": r.get("stock_percent"),
        "nifty_percent": r.get("nifty_percent"),
        "relative_strength": r.get("relative_strength"),
        "relative_strength_membership": r.get("relative_strength_membership"),
        "ema5": r.get("ema5"),
        "ema9": r.get("ema9"),
        "ema10": r.get("ema10"),
        "vwap": r.get("vwap"),
        "supertrend": r.get("supertrend"),
        "macd": r.get("macd"),
        "macd_signal": r.get("macd_signal"),
        "macd_histogram": r.get("macd_histogram"),
        "adx": r.get("adx"),
        "volume": r.get("volume"),
        "avg_volume": r.get("avg_volume"),
        "volume_ratio": r.get("volume_ratio"),
        "volume_tod_ratio": r.get("volume_tod_ratio"),
        "volume_label": r.get("volume_label"),
        "vwap_purity_pct": r.get("vwap_purity_pct"),
        "market_regime": r.get("market_regime"),
        "confidence_grade": r.get("confidence_grade"),
        "kavach_state": r.get("kavach_state"),
        "kavach_strength": r.get("kavach_strength"),
        "trade_score": r.get("trade_score"),
        "ranking_type": r.get("ranking_type"),
        "rank_raw": r.get("rank_raw"),
        "rank_membership": r.get("rank_membership"),
        "in_top10_membership": bool(r.get("in_top10_membership")),
        "in_top5_membership": bool(r.get("in_top5_membership")),
        "incumbent_bonus_applied": bool(r.get("incumbent_bonus_applied")),
        "incumbent_rs_bonus": r.get("incumbent_rs_bonus"),
        "scan_trigger": scan_trigger,
        "cache_only": cache_only,
        "from_cache": r.get("from_cache"),
        "exclusion_reason": r.get("exclusion_reason"),
        "detail": r.get("detail"),
        "rocket_score": int(r.get("rocket_score") or 0),
        "rocket_signals": _signals_dump(r.get("rocket_signals")),
        "rocket_label": r.get("rocket_label") or "",
    }


def _apply_vwap_2candle_ranking_gate(
    bull: List[Dict[str, Any]],
    bear: List[Dict[str, Any]],
    neutral: List[Dict[str, Any]],
    *,
    scan_time: datetime,
    session_date: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Apply 2-candle VWAP confirmation to RS ranking_type (force confirm / reject raw flips)."""
    from backend.services.rs_conviction_candles import candles_cache_only
    from backend.services.vwap_2candle_side import (
        bars_with_session_vwap,
        log_side_resolution,
        ranking_type_from_side,
        resolve_directional_side,
        side_from_ranking_type,
        vwap_2candle_side_enabled,
    )

    if not vwap_2candle_side_enabled():
        return bull, bear, neutral

    prior: Dict[str, str] = {}
    db = SessionLocal()
    try:
        try:
            rows = db.execute(
                text(
                    """
                    SELECT DISTINCT ON (UPPER(symbol))
                           UPPER(symbol) AS symbol, ranking_type
                    FROM rs_universe_score_snapshot
                    WHERE session_date = CAST(:d AS date)
                      AND scan_time < :st
                    ORDER BY UPPER(symbol), scan_time DESC
                    """
                ),
                {"d": session_date, "st": scan_time},
            ).mappings().all()
            for r in rows:
                if r["ranking_type"]:
                    prior[str(r["symbol"]).upper()] = str(r["ranking_type"])
        except Exception as exc:
            logger.debug("rs prior ranking load skipped: %s", exc)

        new_bull: List[Dict[str, Any]] = []
        new_bear: List[Dict[str, Any]] = []
        new_neutral: List[Dict[str, Any]] = []
        for row0 in bull + bear + neutral:
            row = dict(row0)
            sym = str(row.get("symbol") or "").upper()
            raw_rt = row.get("ranking_type")
            prev_rt = prior.get(sym)
            prev_side = side_from_ranking_type(prev_rt)
            raw_side = side_from_ranking_type(raw_rt)
            ik = row.get("instrument_key")
            bars: List[Dict[str, Any]] = []
            if ik:
                try:
                    bars = bars_with_session_vwap(
                        candles_cache_only(str(ik)) or [], now=scan_time
                    )
                except Exception:
                    bars = []
            if sym and len(bars) >= 2 and (prev_side or raw_side):
                resolved = resolve_directional_side(
                    prev_side, raw_side, bars, len(bars) - 1
                )
                new_rt = ranking_type_from_side(resolved.get("side"))
                if new_rt:
                    row["ranking_type_raw"] = raw_rt
                    row["ranking_type"] = new_rt
                    row["ranking_resolve_action"] = resolved.get("action")
                    try:
                        log_side_resolution(
                            db,
                            session_date=str(session_date),
                            symbol=sym,
                            source="rs_universe",
                            bar_end=bars[-1].get("bar_end"),
                            resolved=resolved,
                        )
                    except Exception:
                        pass

            rt = row.get("ranking_type")
            if rt == RANKING_BULLISH:
                new_bull.append(row)
            elif rt == RANKING_BEARISH:
                new_bear.append(row)
            else:
                row["ranking_type"] = RANKING_NEUTRAL
                row["rank_raw"] = None
                row["rank_membership"] = None
                row["relative_strength_membership"] = row.get("relative_strength")
                row["in_top10_membership"] = False
                row["in_top5_membership"] = False
                row["incumbent_bonus_applied"] = False
                row["incumbent_rs_bonus"] = 0.0
                new_neutral.append(row)
        try:
            db.commit()
        except Exception:
            db.rollback()
        return new_bull, new_bear, new_neutral
    finally:
        db.close()


def persist_universe_shadow(
    *,
    scan_time: datetime,
    scored_rows: List[Dict[str, Any]],
    unscored: List[Dict[str, Any]],
    scan_trigger: str,
    cache_only: Optional[bool],
    bonus: Optional[float] = None,
) -> Dict[str, Any]:
    """Write full-universe rows with hysteresis Top-10 flags. No-op if shadow disabled."""
    if not shadow_enabled():
        return {"ok": False, "reason": "shadow_disabled"}
    ensure_rs_universe_score_snapshot()
    bonus_v = float(bonus if bonus is not None else incumbent_bonus())
    session_date = _session_date(scan_time)

    from backend.services.kavach_engine import (
        BEARISH_STATES,
        BULLISH_STATES,
        RANKING_BEARISH as RBE,
        RANKING_BULLISH as RB,
    )

    bull: List[Dict[str, Any]] = []
    bear: List[Dict[str, Any]] = []
    neutral: List[Dict[str, Any]] = []
    for r in scored_rows:
        row = dict(r)
        # metrics rows may have popped from_cache; keep if present
        state = row.get("kavach_state")
        if state in BULLISH_STATES:
            row["ranking_type"] = RB
            bull.append(row)
        elif state in BEARISH_STATES:
            row["ranking_type"] = RBE
            bear.append(row)
        else:
            row["ranking_type"] = RANKING_NEUTRAL
            row["rank_raw"] = None
            row["rank_membership"] = None
            row["relative_strength_membership"] = row.get("relative_strength")
            row["in_top10_membership"] = False
            row["in_top5_membership"] = False
            row["incumbent_bonus_applied"] = False
            row["incumbent_rs_bonus"] = 0.0
            neutral.append(row)

    # 2-candle VWAP side gate on ranking_type (same rule as Garuda side).
    try:
        bull, bear, neutral = _apply_vwap_2candle_ranking_gate(
            bull, bear, neutral, scan_time=scan_time, session_date=session_date
        )
    except Exception as exc:
        logger.debug("rs vwap_2candle ranking gate skipped: %s", exc)

    db = SessionLocal()
    try:
        incumbents = load_prior_top10_incumbents(db, scan_time)
        bull_m = apply_membership_ranks(
            bull, side=RB, incumbents=incumbents.get(RB) or set(), bonus=bonus_v
        )
        bear_m = apply_membership_ranks(
            bear, side=RBE, incumbents=incumbents.get(RBE) or set(), bonus=bonus_v
        )

        params: List[Dict[str, Any]] = []
        for r in bull_m + bear_m + neutral:
            params.append(
                _row_params(scan_time, session_date, r, scan_trigger=scan_trigger, cache_only=cache_only)
            )
        for u in unscored:
            params.append(
                _row_params(
                    scan_time,
                    session_date,
                    {
                        "symbol": u.get("symbol"),
                        "instrument_key": u.get("instrument_key"),
                        "ranking_type": None,
                        "exclusion_reason": u.get("exclusion_reason"),
                        "detail": u.get("detail"),
                        "in_top10_membership": False,
                        "in_top5_membership": False,
                        "incumbent_bonus_applied": False,
                        "incumbent_rs_bonus": 0.0,
                    },
                    scan_trigger=scan_trigger,
                    cache_only=cache_only,
                )
            )

        # chunk upserts
        for i in range(0, len(params), 100):
            db.execute(_UPSERT, params[i : i + 100])
        db.commit()
        return {
            "ok": True,
            "n_rows": len(params),
            "n_bull": len(bull_m),
            "n_bear": len(bear_m),
            "n_neutral": len(neutral),
            "n_unscored": len(unscored),
            "bonus": bonus_v,
            "top10_bull": [r["symbol"] for r in bull_m if r.get("in_top10_membership")],
            "top10_bear": [r["symbol"] for r in bear_m if r.get("in_top10_membership")],
        }
    except Exception as exc:
        db.rollback()
        logger.exception("rs_universe_score_snapshot persist failed: %s", exc)
        return {"ok": False, "reason": str(exc)[:200]}
    finally:
        db.close()
