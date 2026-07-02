"""RS Scanner anchor snapshots — fixed IST decision-time archives."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.kavach_engine import RANKING_BEARISH, RANKING_BULLISH
from backend.services.relative_strength_scanner import get_latest_snapshot

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

ANCHOR_LABELS: tuple[str, ...] = ("09:25", "09:45", "10:30", "12:30", "14:30")

_INSERT = text(
    """
    INSERT INTO rs_anchor_snapshot (
        session_date, capture_label, capture_time, rank_position, symbol, direction,
        current_price, relative_strength, trade_score, confidence_grade, market_regime,
        adx, volume_label, volume_ratio, vwap_purity_pct, supertrend, macd, macd_signal,
        ema5, vwap, maturity_tag, sector
    ) VALUES (
        :session_date, :capture_label, :capture_time, :rank_position, :symbol, :direction,
        :current_price, :relative_strength, :trade_score, :confidence_grade, :market_regime,
        :adx, :volume_label, :volume_ratio, :vwap_purity_pct, :supertrend, :macd, :macd_signal,
        :ema5, :vwap, :maturity_tag, :sector
    )
    ON CONFLICT (session_date, capture_label, symbol, direction) DO UPDATE SET
        capture_time = EXCLUDED.capture_time,
        rank_position = EXCLUDED.rank_position,
        current_price = EXCLUDED.current_price,
        relative_strength = EXCLUDED.relative_strength,
        trade_score = EXCLUDED.trade_score,
        confidence_grade = EXCLUDED.confidence_grade,
        market_regime = EXCLUDED.market_regime,
        adx = EXCLUDED.adx,
        volume_label = EXCLUDED.volume_label,
        volume_ratio = EXCLUDED.volume_ratio,
        vwap_purity_pct = EXCLUDED.vwap_purity_pct,
        supertrend = EXCLUDED.supertrend,
        macd = EXCLUDED.macd,
        macd_signal = EXCLUDED.macd_signal,
        ema5 = EXCLUDED.ema5,
        vwap = EXCLUDED.vwap,
        maturity_tag = EXCLUDED.maturity_tag,
        sector = EXCLUDED.sector
    """
)


def _today() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _sector_for(symbol: str) -> Optional[str]:
    db = SessionLocal()
    try:
        r = db.execute(
            text(
                "SELECT sector_index FROM arbitrage_master WHERE stock = :s LIMIT 1"
            ),
            {"s": symbol},
        ).fetchone()
        return str(r.sector_index).strip() if r and r.sector_index else None
    except Exception:
        return None
    finally:
        db.close()


def _latest_raw_rows() -> List[Any]:
    db = SessionLocal()
    try:
        return db.execute(
            text(
                """
                SELECT s.*, h.maturity_tag
                FROM relative_strength_snapshot s
                LEFT JOIN rs_scanner_history h
                  ON h.symbol = s.symbol
                 AND h.date = (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')::date
                WHERE s.scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
                ORDER BY s.ranking_type, s.rank_position
                """
            )
        ).fetchall()
    finally:
        db.close()


def capture_anchor_snapshot(capture_label: str) -> Dict[str, Any]:
    """Persist Top-5 bull/bear from latest RS scan at a fixed anchor label."""
    if capture_label not in ANCHOR_LABELS:
        raise ValueError(f"invalid capture_label: {capture_label}")
    rows = _latest_raw_rows()
    if not rows:
        return {"ok": False, "reason": "no_scan_data"}
    now = datetime.now(IST)
    sd = _today()
    params: List[Dict[str, Any]] = []
    for r in rows:
        if int(r.rank_position or 0) > 5:
            continue
        direction = "BEARISH" if (r.ranking_type or "").upper() == RANKING_BEARISH else "BULLISH"
        ema_vwap_pct = None
        if r.vwap and r.ema5:
            ema_vwap_pct = (float(r.ema5) - float(r.vwap)) / float(r.vwap) * 100.0
        params.append(
            {
                "session_date": sd,
                "capture_label": capture_label,
                "capture_time": now,
                "rank_position": int(r.rank_position),
                "symbol": r.symbol,
                "direction": direction,
                "current_price": r.current_price,
                "relative_strength": r.relative_strength,
                "trade_score": r.trade_score,
                "confidence_grade": getattr(r, "confidence_grade", None),
                "market_regime": getattr(r, "market_regime", None),
                "adx": r.adx,
                "volume_label": getattr(r, "volume_label", None),
                "volume_ratio": r.volume_ratio,
                "vwap_purity_pct": getattr(r, "vwap_purity_pct", None),
                "supertrend": r.supertrend,
                "macd": r.macd,
                "macd_signal": r.macd_signal,
                "ema5": r.ema5,
                "vwap": r.vwap,
                "maturity_tag": getattr(r, "maturity_tag", None),
                "sector": _sector_for(r.symbol),
                "_ema_vwap_pct": ema_vwap_pct,
            }
        )
    db = SessionLocal()
    try:
        for p in params:
            db.execute(_INSERT, {k: v for k, v in p.items() if not k.startswith("_")})
        db.commit()
    finally:
        db.close()
    logger.info("rs_anchor: captured %d rows at %s for %s", len(params), capture_label, sd)
    return {"ok": True, "capture_label": capture_label, "rows": len(params)}


def query_anchor_snapshots(
    *,
    session_date: Optional[str] = None,
    capture_label: Optional[str] = None,
    symbol: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    clauses = ["1=1"]
    bind: Dict[str, Any] = {"lim": int(limit)}
    if session_date:
        clauses.append("session_date = CAST(:sd AS DATE)")
        bind["sd"] = session_date
    if capture_label:
        clauses.append("capture_label = :lbl")
        bind["lbl"] = capture_label
    if symbol:
        clauses.append("UPPER(symbol) = :sym")
        bind["sym"] = symbol.strip().upper()
    sql = (
        "SELECT session_date, capture_label, capture_time, rank_position, symbol, direction, "
        "current_price, relative_strength, trade_score, confidence_grade, market_regime, "
        "adx, volume_label, volume_ratio, vwap_purity_pct, supertrend, macd, macd_signal, "
        "ema5, vwap, maturity_tag, sector "
        f"FROM rs_anchor_snapshot WHERE {' AND '.join(clauses)} "
        "ORDER BY session_date DESC, capture_label, direction, rank_position LIMIT :lim"
    )
    db = SessionLocal()
    try:
        rows = db.execute(text(sql), bind).fetchall()
    finally:
        db.close()
    out: List[Dict[str, Any]] = []
    for r in rows:
        ema_vwap_pct = None
        if r.vwap and r.ema5 and float(r.vwap) != 0:
            ema_vwap_pct = round((float(r.ema5) - float(r.vwap)) / float(r.vwap) * 100.0, 2)
        out.append(
            {
                "session_date": str(r.session_date),
                "capture_label": r.capture_label,
                "capture_time": r.capture_time.isoformat() if r.capture_time else None,
                "rank": int(r.rank_position),
                "symbol": r.symbol,
                "direction": r.direction,
                "price": r.current_price,
                "rs_percent": r.relative_strength,
                "trade_score": r.trade_score,
                "confidence_grade": r.confidence_grade,
                "market_regime": r.market_regime,
                "adx": r.adx,
                "volume_label": r.volume_label,
                "volume_ratio": r.volume_ratio,
                "vwap_purity_pct": r.vwap_purity_pct,
                "supertrend_bullish": float(r.supertrend or 0) > 0,
                "macd_bullish": float(r.macd or 0) > float(r.macd_signal or 0),
                "ema_vwap_pct": ema_vwap_pct,
                "maturity_tag": r.maturity_tag,
                "sector": r.sector,
            }
        )
    return out


def anchor_overlap_at_0925() -> Dict[str, Any]:
    """Compare today's 09:25 anchor vs yesterday's final 14:30 lists."""
    db = SessionLocal()
    try:
        today = _today()
        yday_row = db.execute(
            text(
                """
                SELECT MAX(session_date) FROM rs_anchor_snapshot
                WHERE session_date < CAST(:t AS DATE)
                """
            ),
            {"t": today},
        ).fetchone()
        if not yday_row or not yday_row[0]:
            return {"rotation_day_type": "MIXED", "bull_overlap": 0, "bear_overlap": 0}
        yday = str(yday_row[0])

        def syms(d: str, lbl: str, direction: str) -> set:
            rows = db.execute(
                text(
                    """
                    SELECT symbol FROM rs_anchor_snapshot
                    WHERE session_date = CAST(:d AS DATE)
                      AND capture_label = :lbl AND direction = :dir
                    """
                ),
                {"d": d, "lbl": lbl, "dir": direction},
            ).fetchall()
            return {r.symbol for r in rows}

        bull_t = syms(today, "09:25", "BULLISH")
        bear_t = syms(today, "09:25", "BEARISH")
        bull_y = syms(yday, "14:30", "BULLISH")
        bear_y = syms(yday, "14:30", "BEARISH")
        bull_ov = len(bull_t & bull_y)
        bear_ov = len(bear_t & bear_y)
        if bull_ov >= 3 or bear_ov >= 3:
            rtype = "CONTINUATION"
        elif bull_ov == 0 and bear_ov == 0:
            rtype = "ROTATION"
        else:
            rtype = "MIXED"
        return {
            "rotation_day_type": rtype,
            "bull_overlap": bull_ov,
            "bear_overlap": bear_ov,
            "yesterday_date": yday,
            "today_bull": sorted(bull_t),
            "today_bear": sorted(bear_t),
            "yesterday_bull": sorted(bull_y),
            "yesterday_bear": sorted(bear_y),
        }
    finally:
        db.close()
