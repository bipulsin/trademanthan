"""After-hours full-universe Kavach archive — read-only research persistence.

Runs post-15:30 IST; does NOT affect live RS ranking, checklist, or Fast Watch.
Stores per-symbol Kavach metrics for the full arbitrage_master universe (~203).
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.kavach_confidence import (
    REGIME_TREND,
    compute_confidence_grade,
    format_confidence_display,
)
from backend.services.kavach_engine import (
    BEARISH_STATES,
    BULLISH_STATES,
    RANKING_BEARISH,
    RANKING_BULLISH,
    compute_trade_score,
)
from backend.services.relative_strength_scanner import (
    TOP_N,
    _compute_symbol_metrics,
    _nifty_change_pct,
)
from backend.services.upstox_service import UpstoxService
from backend.services.vajra.job import load_arbitrage_curr_mth_universe

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_INSERT_ROW = text(
    """
    INSERT INTO rs_universe_kavach_archive (
        session_date, symbol, instrument_key, future_symbol, archive_time,
        kavach_state, kavach_strength, relative_strength, stock_percent, nifty_percent,
        volume_ratio, volume_tod_ratio, volume_label, adx, trade_score, confidence_grade,
        ranking_side, would_be_rank_bull, would_be_rank_bear, universe_size
    ) VALUES (
        :session_date, :symbol, :instrument_key, :future_symbol, :archive_time,
        :kavach_state, :kavach_strength, :relative_strength, :stock_percent, :nifty_percent,
        :volume_ratio, :volume_tod_ratio, :volume_label, :adx, :trade_score, :confidence_grade,
        :ranking_side, :would_be_rank_bull, :would_be_rank_bear, :universe_size
    )
    ON CONFLICT (session_date, symbol) DO UPDATE SET
        instrument_key = EXCLUDED.instrument_key,
        future_symbol = EXCLUDED.future_symbol,
        archive_time = EXCLUDED.archive_time,
        kavach_state = EXCLUDED.kavach_state,
        kavach_strength = EXCLUDED.kavach_strength,
        relative_strength = EXCLUDED.relative_strength,
        stock_percent = EXCLUDED.stock_percent,
        nifty_percent = EXCLUDED.nifty_percent,
        volume_ratio = EXCLUDED.volume_ratio,
        volume_tod_ratio = EXCLUDED.volume_tod_ratio,
        volume_label = EXCLUDED.volume_label,
        adx = EXCLUDED.adx,
        trade_score = EXCLUDED.trade_score,
        confidence_grade = EXCLUDED.confidence_grade,
        ranking_side = EXCLUDED.ranking_side,
        would_be_rank_bull = EXCLUDED.would_be_rank_bull,
        would_be_rank_bear = EXCLUDED.would_be_rank_bear,
        universe_size = EXCLUDED.universe_size
    """
)

_INSERT_RUN = text(
    """
    INSERT INTO rs_universe_kavach_archive_run (
        session_date, archive_time, universe_size, symbols_archived,
        directional_bull, directional_bear, contract_month_hint,
        instrument_key_sample, prev_session_instrument_key_sample, rollover_detected
    ) VALUES (
        :session_date, :archive_time, :universe_size, :symbols_archived,
        :directional_bull, :directional_bear, :contract_month_hint,
        :instrument_key_sample, :prev_session_instrument_key_sample, :rollover_detected
    )
    ON CONFLICT (session_date) DO UPDATE SET
        archive_time = EXCLUDED.archive_time,
        universe_size = EXCLUDED.universe_size,
        symbols_archived = EXCLUDED.symbols_archived,
        directional_bull = EXCLUDED.directional_bull,
        directional_bear = EXCLUDED.directional_bear,
        contract_month_hint = EXCLUDED.contract_month_hint,
        instrument_key_sample = EXCLUDED.instrument_key_sample,
        prev_session_instrument_key_sample = EXCLUDED.prev_session_instrument_key_sample,
        rollover_detected = EXCLUDED.rollover_detected
    """
)


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _after_archive_window(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return mins >= 15 * 60 + 30


def _contract_month_hint(future_symbol: str) -> Optional[str]:
    if not future_symbol:
        return None
    m = re.search(r"(\d{2}[A-Z]{3}\d*)", future_symbol.upper())
    return m.group(1) if m else None


def _score_directional(rows: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict]]:
    """Score all Kavach-directional rows (no TOP_N slice) — shadow of _rank()."""
    bullish: List[Dict] = []
    bearish: List[Dict] = []
    for r in rows:
        state = r.get("kavach_state")
        if state in BULLISH_STATES:
            ranking_type = RANKING_BULLISH
            bucket = bullish
        elif state in BEARISH_STATES:
            ranking_type = RANKING_BEARISH
            bucket = bearish
        else:
            continue
        row = dict(r)
        row["ranking_type"] = ranking_type
        row["trade_score"] = compute_trade_score(
            rs=row["relative_strength"],
            state=state,
            volume_ratio=row["volume_ratio"],
            adx=row["adx"],
            price=row["current_price"],
            vwap=row["vwap"],
            ranking_type=ranking_type,
        )
        grade, floor = compute_confidence_grade(
            row["trade_score"],
            row.get("volume_label") or "Low",
            row.get("vwap_purity_pct") or 0.0,
            row.get("market_regime") or REGIME_TREND,
        )
        row["confidence_grade"] = format_confidence_display(grade, floor)
        bucket.append(row)
    bullish.sort(key=lambda x: (-x["relative_strength"], -x["trade_score"]))
    bearish.sort(key=lambda x: (x["relative_strength"], -x["trade_score"]))
    for i, row in enumerate(bullish, start=1):
        row["would_be_rank_bull"] = i if i <= TOP_N else None
    for i, row in enumerate(bearish, start=1):
        row["would_be_rank_bear"] = i if i <= TOP_N else None
    return bullish, bearish


def _prev_run_sample(db, session_date: str) -> Optional[str]:
    row = db.execute(
        text(
            """
            SELECT instrument_key_sample FROM rs_universe_kavach_archive_run
            WHERE session_date < CAST(:d AS date)
            ORDER BY session_date DESC LIMIT 1
            """
        ),
        {"d": session_date},
    ).fetchone()
    return str(row.instrument_key_sample) if row and row.instrument_key_sample else None


def run_universe_kavach_archive(*, force: bool = False) -> Dict[str, Any]:
    """Archive full-universe Kavach state for research. No live side effects."""
    now = datetime.now(IST)
    if not force and not _after_archive_window(now):
        return {"ok": False, "reason": "before_1530_ist_window"}

    sd = today_ist()
    universe = load_arbitrage_curr_mth_universe()
    if not universe:
        return {"ok": False, "reason": "empty_universe"}

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    nifty_pct = _nifty_change_pct(upstox)
    if nifty_pct is None:
        return {"ok": False, "reason": "nifty_unavailable"}

    rows: List[Dict[str, Any]] = []
    errors = 0
    for entry in universe:
        try:
            m = _compute_symbol_metrics(upstox, entry, nifty_pct, cache_only=False)
            if m:
                m.pop("from_cache", None)
                rows.append(m)
        except Exception as exc:
            errors += 1
            logger.debug("archive metrics failed %s: %s", entry.get("stock"), exc)

    bull_scored, bear_scored = _score_directional(rows)
    rank_bull = {r["symbol"]: r.get("would_be_rank_bull") for r in bull_scored}
    rank_bear = {r["symbol"]: r.get("would_be_rank_bear") for r in bear_scored}
    bull_syms = {r["symbol"] for r in bull_scored}
    bear_syms = {r["symbol"] for r in bear_scored}
    scored_by_sym = {r["symbol"]: r for r in bull_scored + bear_scored}

    universe_size = len(universe)
    sample_entry = universe[0] if universe else {}
    sample_key = sample_entry.get("instrument_key") or ""
    sample_fut = sample_entry.get("future_symbol") or ""
    prev_sample = None
    rollover = False

    params: List[Dict[str, Any]] = []
    for r in rows:
        sym = r["symbol"]
        state = r.get("kavach_state") or ""
        if sym in bull_syms:
            side = RANKING_BULLISH
        elif sym in bear_syms:
            side = RANKING_BEARISH
        else:
            side = "NEUTRAL"
        sc = scored_by_sym.get(sym, {})
        params.append(
            {
                "session_date": sd,
                "symbol": sym,
                "instrument_key": r.get("instrument_key") or "",
                "future_symbol": r.get("future_symbol") or "",
                "archive_time": now,
                "kavach_state": state,
                "kavach_strength": r.get("kavach_strength"),
                "relative_strength": r.get("relative_strength"),
                "stock_percent": r.get("stock_percent"),
                "nifty_percent": r.get("nifty_percent"),
                "volume_ratio": r.get("volume_ratio"),
                "volume_tod_ratio": r.get("volume_tod_ratio"),
                "volume_label": r.get("volume_label"),
                "adx": r.get("adx"),
                "trade_score": sc.get("trade_score"),
                "confidence_grade": sc.get("confidence_grade"),
                "ranking_side": side,
                "would_be_rank_bull": rank_bull.get(sym),
                "would_be_rank_bear": rank_bear.get(sym),
                "universe_size": universe_size,
            }
        )

    db = SessionLocal()
    try:
        prev_sample = _prev_run_sample(db, sd)
        if prev_sample and sample_key and prev_sample != sample_key:
            rollover = True
        if params:
            db.execute(_INSERT_ROW, params)
        db.execute(
            _INSERT_RUN,
            {
                "session_date": sd,
                "archive_time": now,
                "universe_size": universe_size,
                "symbols_archived": len(params),
                "directional_bull": len(bull_scored),
                "directional_bear": len(bear_scored),
                "contract_month_hint": _contract_month_hint(sample_fut),
                "instrument_key_sample": sample_key[:120] if sample_key else None,
                "prev_session_instrument_key_sample": prev_sample,
                "rollover_detected": rollover,
            },
        )
        db.commit()
    finally:
        db.close()

    out = {
        "ok": True,
        "session_date": sd,
        "universe_size": universe_size,
        "symbols_archived": len(params),
        "directional_bull": len(bull_scored),
        "directional_bear": len(bear_scored),
        "errors": errors,
        "contract_month_hint": _contract_month_hint(sample_fut),
        "rollover_detected": rollover,
        "instrument_key_sample": sample_key[:80] if sample_key else None,
    }
    logger.info("rs_universe_kavach_archive: %s", out)
    return out
