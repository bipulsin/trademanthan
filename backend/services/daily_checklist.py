"""Daily RS Trade Checklist — pre-trade decision logic + persistence.

Backs the ``dailyRSchecklist.html`` page: a per-stock intraday entry checklist for
the Top-5 Bullish / Top-5 Bearish names from the Relative Strength scanner.

The decision engine here is the single source of truth. The browser sends raw
field values (e.g. ``confidence="A"``); this module derives the per-condition
PASS/FAIL flags, the 9-condition gate score, and the final decision/section, then
persists them. Direction (LONG for bullish RS, SHORT for bearish RS) drives every
directional rule; ``counter_rs`` flips the confidence requirement to A-only.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Entry window (hard rule): 10:15 – 14:30 IST.
ENTRY_START_MIN = 10 * 60 + 15
ENTRY_END_MIN = 14 * 60 + 30

LONG = "LONG"
SHORT = "SHORT"

# Decision strings (match the spec exactly, emoji included).
D_GO = "🟢 GO — ENTER"
D_WATCH = "🟡 WATCH — WAIT"
D_NOTRADE = "🔴 NO TRADE"
D_ELIMINATED = "🔴 ELIMINATED"
D_UNASSESSED = "⬜ Not assessed"

# Section buckets the page renders into.
SEC_GO = "GO"
SEC_WATCH = "WATCH"
SEC_OUT = "OUT"  # eliminated + hard-fail + low-score no-trade

# Raw fields the client may update (whitelist for safe casting/persistence).
TEXT_FIELDS = {
    "nifty_open_direction", "entry_time", "confidence", "trading_state",
    "ema_vs_vwap", "supertrend", "macd", "di_alignment", "volume", "notes",
}
NUM_FIELDS = {"adx_935", "kavach_score_entry", "adx_entry", "rs_pct", "vol_multiplier", "dashboard_score"}
BOOL_FIELDS = {"news_clean", "counter_rs"}
PAGE_LEVEL_FIELDS = {"nifty_open_direction"}  # one value applied to all stocks

# Columns the evaluator derives and persists.
DERIVED_COLS = (
    "adx_935_status", "time_ok", "score_ok", "confidence_ok", "state_ok",
    "ema_ok", "st_ok", "macd_ok", "adx_ok", "volume_ok", "gate_score",
    "decision", "section",
)


def _to_min(hhmm: Optional[str]) -> Optional[int]:
    """Parse 'HH:MM' to minutes since midnight, or None."""
    if not hhmm:
        return None
    try:
        h, m = str(hhmm).split(":")[:2]
        return int(h) * 60 + int(m)
    except (ValueError, TypeError):
        return None


def adx_935_status(adx: Optional[float]) -> str:
    """Pre-market 9:35 ADX bucket: immediate / recheck / watch / '' if unset."""
    if adx is None:
        return ""
    if adx >= 25:
        return "immediate"
    if adx >= 20:
        return "recheck"
    return "watch"


def evaluate(row: Dict[str, Any]) -> Dict[str, Any]:
    """Derive per-condition flags + gate score + decision/section from raw inputs.

    ``row`` carries the raw field values plus ``direction`` and ``counter_rs``.
    Returns a dict of the derived columns (booleans are True/False/None where None
    means "not yet assessed").
    """
    direction = (row.get("direction") or LONG).upper()
    is_long = direction == LONG
    counter = bool(row.get("counter_rs"))

    # --- pre-market: news + ADX(9:35) ---
    news_clean = row.get("news_clean")  # True=clean, False=adverse, None=unset
    adverse_news = news_clean is False
    a935 = _num(row.get("adx_935"))

    # --- entry gate (conditions 4-12) ---
    # 4. Entry time window (also a hard fail when outside).
    t_min = _to_min(row.get("entry_time"))
    time_ok: Optional[bool] = None
    time_hardfail = False
    if t_min is not None:
        time_ok = ENTRY_START_MIN <= t_min <= ENTRY_END_MIN
        time_hardfail = not time_ok

    # 5. Kavach score >= 70.
    score = _num(row.get("kavach_score_entry"))
    score_ok = (score >= 70) if score is not None else None

    # 6. Confidence: same-direction A/B; counter-RS A only.
    conf = (row.get("confidence") or "").strip().upper() or None
    if conf is None:
        confidence_ok = None
    elif counter:
        confidence_ok = conf == "A"
    else:
        confidence_ok = conf in ("A", "B")

    # 7. Trading state aligned with direction (misalignment is a hard fail).
    state = (row.get("trading_state") or "").strip().upper() or None
    state_ok: Optional[bool] = None
    state_hardfail = False
    if state is not None:
        if is_long:
            state_ok = state in ("BUY", "MANAGE LONG")
        else:
            state_ok = state in ("SELL", "MANAGE SHORT")
        state_hardfail = not state_ok

    # 8. EMA5 vs VWAP.
    ema = (row.get("ema_vs_vwap") or "").strip().lower() or None
    if ema is None:
        ema_ok = None
    else:
        ema_ok = ema == "above" if is_long else ema == "below"

    # 9. Supertrend.
    st = (row.get("supertrend") or "").strip().lower() or None
    if st is None:
        st_ok = None
    else:
        st_ok = st == "bullish" if is_long else st == "bearish"

    # 10. MACD (Crossing counts toward the trade direction).
    macd = (row.get("macd") or "").strip().lower() or None
    if macd is None:
        macd_ok = None
    elif is_long:
        macd_ok = macd in ("bullish", "crossing")
    else:
        macd_ok = macd in ("bearish", "crossing")

    # 11. ADX >= 25 with DI alignment.
    adx_e = _num(row.get("adx_entry"))
    di = (row.get("di_alignment") or "").strip() or None
    if adx_e is None:
        adx_ok = None
    else:
        di_ok = True
        if di is not None:
            di_ok = di == "DI+>DI-" if is_long else di == "DI->DI+"
        adx_ok = (adx_e >= 25) and di_ok

    # 12. Volume (soft fail — Low fails the gate but is not a hard fail).
    vol = (row.get("volume") or "").strip().lower() or None
    if vol is None:
        volume_ok = None
    else:
        volume_ok = vol in ("high", "normal")

    gate_flags = [time_ok, score_ok, confidence_ok, state_ok, ema_ok, st_ok, macd_ok, adx_ok, volume_ok]
    gate_score = sum(1 for f in gate_flags if f is True)

    hard_fail = adverse_news or time_hardfail or state_hardfail

    if adverse_news:
        decision, section = D_ELIMINATED, SEC_OUT
    elif time_hardfail or state_hardfail:
        decision, section = D_NOTRADE, SEC_OUT
    elif gate_score == 9:
        decision, section = D_GO, SEC_GO
    elif gate_score >= 6:
        decision, section = D_WATCH, SEC_WATCH
    elif gate_score > 0:
        decision, section = D_NOTRADE, SEC_OUT
    else:
        decision, section = D_UNASSESSED, SEC_WATCH

    return {
        "adx_935_status": adx_935_status(a935),
        "time_ok": time_ok,
        "score_ok": score_ok,
        "confidence_ok": confidence_ok,
        "state_ok": state_ok,
        "ema_ok": ema_ok,
        "st_ok": st_ok,
        "macd_ok": macd_ok,
        "adx_ok": adx_ok,
        "volume_ok": volume_ok,
        "gate_score": gate_score,
        "decision": decision,
        "section": section,
    }


def _num(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# --- RS snapshot → checklist auto-fill ---------------------------------------

_RS_ALL_SQL = text(
    """
    SELECT s.symbol, s.relative_strength, s.trade_score, s.volume_ratio,
           s.kavach_state, s.ema5, s.vwap, s.supertrend, s.macd, s.macd_signal,
           s.macd_histogram, s.adx, s.ranking_type
    FROM relative_strength_snapshot s
    WHERE s.scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
    ORDER BY s.ranking_type, s.rank_position
    """
)

_RS_DETAIL_SQL = text(
    """
    SELECT s.symbol, s.relative_strength, s.trade_score, s.volume_ratio,
           s.kavach_state, s.ema5, s.vwap, s.supertrend, s.macd, s.macd_signal,
           s.macd_histogram, s.adx, s.ranking_type
    FROM relative_strength_snapshot s
    WHERE s.scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
      AND s.symbol = :sym
    LIMIT 1
    """
)

# User-owned fields preserved across RS sync/populate refreshes.
_PRESERVE_ON_RS_SYNC = frozenset({"news_clean", "notes", "counter_rs"})


def _direction_from_ranking(ranking_type: Optional[str]) -> str:
    return SHORT if (ranking_type or "").upper() == "BEARISH" else LONG


def _confidence_grade(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    s = int(round(score))
    if s >= 90:
        return "A"
    if s >= 80:
        return "B"
    if s >= 70:
        return "C"
    return "D"


def _trading_state_label(kavach_state: Optional[str], direction: str) -> Optional[str]:
    if not kavach_state:
        return None
    k = kavach_state.upper().strip()
    if k == "BUY":
        return "BUY"
    if k == "SELL":
        return "SELL"
    if k == "READY":
        return "MANAGE LONG" if direction == LONG else "MANAGE SHORT"
    if k == "READY SHORT":
        return "MANAGE SHORT"
    if k in ("WATCH", "WATCH SHORT", "NEUTRAL"):
        return "HOLD/WATCH"
    return None


def _ema_vs_vwap_label(ema5: Optional[float], vwap: Optional[float]) -> Optional[str]:
    if ema5 is None or vwap is None or vwap == 0:
        return None
    if abs(ema5 - vwap) / vwap < 0.0005:
        return "At VWAP"
    return "Above" if ema5 > vwap else "Below"


def _supertrend_label(st: Optional[float]) -> Optional[str]:
    if st is None:
        return None
    return "Bullish" if float(st) > 0 else "Bearish"


def _macd_label(
    macd: Optional[float], sig: Optional[float], hist: Optional[float]
) -> Optional[str]:
    if macd is None or sig is None:
        return None
    if hist is not None and abs(hist) < max(abs(macd), 1.0) * 0.03:
        return "Crossing"
    return "Bullish" if macd > sig else "Bearish"


def _volume_label(ratio: Optional[float]) -> Optional[str]:
    if ratio is None:
        return None
    r = float(ratio)
    if r >= 1.2:
        return "High"
    if r >= 0.65:
        return "Normal"
    return "Low"


def _current_entry_time_ist() -> Optional[str]:
    now = datetime.now(IST)
    m = now.hour * 60 + now.minute
    if ENTRY_START_MIN <= m <= ENTRY_END_MIN:
        return f"{now.hour:02d}:{now.minute:02d}"
    return None


def _auto_fields_from_rs(row: Any, direction: str) -> Dict[str, Any]:
    """Map a relative_strength_snapshot row to checklist fields the system can fill."""
    score = _num(row.trade_score)
    adx = _num(row.adx)
    ema5 = _num(row.ema5)
    vwap = _num(row.vwap)
    ema_lbl = _ema_vs_vwap_label(ema5, vwap)
    di = None
    if adx is not None and adx >= 25:
        di = "DI+>DI-" if direction == LONG else "DI->DI+"
    fields: Dict[str, Any] = {
        "rs_pct": round(_num(row.relative_strength) or 0, 2),
        "dashboard_score": int(round(score or 0)) if score is not None else None,
        "dashboard_kavach": row.kavach_state,
        "vol_multiplier": round(_num(row.volume_ratio) or 0, 2),
        "kavach_score_entry": int(round(score or 0)) if score is not None else None,
        "confidence": _confidence_grade(score),
        "trading_state": _trading_state_label(row.kavach_state, direction),
        "ema_vs_vwap": ema_lbl,
        "supertrend": _supertrend_label(_num(row.supertrend)),
        "macd": _macd_label(_num(row.macd), _num(row.macd_signal), _num(row.macd_histogram)),
        "adx_entry": round(adx, 1) if adx is not None else None,
        "adx_935": round(adx, 2) if adx is not None else None,
        "di_alignment": di,
        "volume": _volume_label(_num(row.volume_ratio)),
        "entry_time": _current_entry_time_ist(),
    }
    return {k: v for k, v in fields.items() if v is not None}


def _merge_rs_into_existing(existing: Optional[Dict[str, Any]], auto: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(auto)
    if existing:
        for k in _PRESERVE_ON_RS_SYNC:
            if existing.get(k) is not None:
                merged[k] = existing[k]
    if merged.get("news_clean") is None:
        merged["news_clean"] = True
    return merged


_UPSERT_COLS = (
    "rs_pct", "dashboard_score", "dashboard_kavach", "vol_multiplier",
    "news_clean", "adx_935", "entry_time", "kavach_score_entry", "confidence",
    "trading_state", "ema_vs_vwap", "supertrend", "macd", "adx_entry",
    "di_alignment", "volume", "counter_rs", "notes",
)


def _upsert_stock(db, sd: str, symbol: str, direction: str, fields: Dict[str, Any]) -> None:
    """Insert or update a checklist row, then derive decision flags."""
    params: Dict[str, Any] = {
        "d": sd, "sym": symbol, "dir": direction,
        "dec": D_UNASSESSED, "sec": SEC_WATCH,
    }
    for c in _UPSERT_COLS:
        params[c] = fields.get(c)
    col_names = ", ".join(_UPSERT_COLS)
    placeholders = ", ".join(f":{c}" for c in _UPSERT_COLS)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in _UPSERT_COLS)
    db.execute(
        text(
            f"""
            INSERT INTO daily_checklist
                (session_date, symbol, direction, {col_names}, decision, section, updated_at)
            VALUES (:d, :sym, :dir, {placeholders}, :dec, :sec, NOW())
            ON CONFLICT (session_date, symbol) DO UPDATE SET
                direction = EXCLUDED.direction,
                {updates},
                updated_at = NOW()
            """
        ),
        params,
    )
    _reevaluate_symbol(db, sd, symbol)


_SELECT_COLS = """
    symbol, direction, rs_pct, dashboard_score, dashboard_kavach, vol_multiplier,
    news_clean, adx_935, adx_935_status, nifty_open_direction, entry_time, time_ok,
    kavach_score_entry, score_ok, confidence, confidence_ok, trading_state, state_ok,
    ema_vs_vwap, ema_ok, supertrend, st_ok, macd, macd_ok, adx_entry, di_alignment,
    adx_ok, volume, volume_ok, counter_rs, gate_score, decision, section, notes,
    updated_at
"""


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _row_to_dict(r) -> Dict[str, Any]:
    d = {c: getattr(r, c) for c in (
        "symbol", "direction", "rs_pct", "dashboard_score", "dashboard_kavach",
        "vol_multiplier", "news_clean", "adx_935", "adx_935_status",
        "nifty_open_direction", "entry_time", "time_ok", "kavach_score_entry",
        "score_ok", "confidence", "confidence_ok", "trading_state", "state_ok",
        "ema_vs_vwap", "ema_ok", "supertrend", "st_ok", "macd", "macd_ok",
        "adx_entry", "di_alignment", "adx_ok", "volume", "volume_ok", "counter_rs",
        "gate_score", "decision", "section", "notes",
    )}
    d["updated_at"] = r.updated_at.isoformat() if r.updated_at else None
    return d


def _section_of(stock: Dict[str, Any]) -> str:
    return stock.get("section") or SEC_WATCH


def _counts(stocks: List[Dict[str, Any]]) -> Dict[str, int]:
    c = {"go": 0, "watch": 0, "out": 0}
    for s in stocks:
        sec = _section_of(s)
        if sec == SEC_GO:
            c["go"] += 1
        elif sec == SEC_OUT:
            c["out"] += 1
        else:
            c["watch"] += 1
    return c


def _nifty_levels() -> Dict[str, Optional[float]]:
    """Latest NIFTY50 / BANKNIFTY ltp from index_prices (best-effort)."""
    out: Dict[str, Optional[float]] = {"nifty50": None, "banknifty": None}
    db = SessionLocal()
    try:
        for key, name in (("nifty50", "NIFTY50"), ("banknifty", "BANKNIFTY")):
            r = db.execute(
                text(
                    "SELECT ltp FROM index_prices WHERE index_name=:n AND ltp > 0 "
                    "ORDER BY price_time DESC LIMIT 1"
                ),
                {"n": name},
            ).fetchone()
            if r and r.ltp:
                out[key] = round(float(r.ltp), 2)
    except Exception as exc:
        logger.debug("nifty levels lookup failed: %s", exc)
    finally:
        db.close()
    return out


def get_state(session_date: Optional[str] = None) -> Dict[str, Any]:
    """Full page state for a date: stocks (bullish first), counts, nifty levels."""
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                f"SELECT {_SELECT_COLS} FROM daily_checklist WHERE session_date = :d "
                "ORDER BY CASE direction WHEN 'LONG' THEN 0 ELSE 1 END, "
                "COALESCE(rs_pct, 0) DESC"
            ),
            {"d": sd},
        ).fetchall()
    finally:
        db.close()

    stocks = [_row_to_dict(r) for r in rows]
    nifty_dir = ""
    for s in stocks:
        if s.get("nifty_open_direction"):
            nifty_dir = s["nifty_open_direction"]
            break
    levels = _nifty_levels()
    return {
        "session_date": sd,
        "nifty_open_direction": nifty_dir,
        "nifty50": levels["nifty50"],
        "banknifty": levels["banknifty"],
        "stocks": stocks,
        "counts": _counts(stocks),
    }


def populate_from_rs() -> Dict[str, Any]:
    """Seed today's checklist from the latest RS snapshot with system auto-fill."""
    sd = today_ist()
    count = 0
    db = SessionLocal()
    try:
        rows = db.execute(_RS_ALL_SQL).fetchall()
        count = len(rows)
        for row in rows:
            direction = _direction_from_ranking(row.ranking_type)
            existing = _load_raw(db, sd, row.symbol)
            auto = _auto_fields_from_rs(row, direction)
            merged = _merge_rs_into_existing(existing, auto)
            _upsert_stock(db, sd, row.symbol, direction, merged)
        db.commit()
    finally:
        db.close()
    logger.info("daily_checklist: populated %d stocks for %s", count, sd)
    return get_state(sd)


def sync_symbol_from_rs(symbol: str, session_date: Optional[str] = None) -> Dict[str, Any]:
    """Refresh one symbol's system-derived checklist fields from the latest RS scan."""
    sd = session_date or today_ist()
    sym = (symbol or "").strip().upper()
    if not sym:
        raise ValueError("symbol required")
    db = SessionLocal()
    try:
        row = db.execute(_RS_DETAIL_SQL, {"sym": sym}).fetchone()
        if not row:
            raise ValueError(f"no RS data for {sym}")
        existing = _load_raw(db, sd, sym)
        direction = (existing or {}).get("direction") or _direction_from_ranking(row.ranking_type)
        auto = _auto_fields_from_rs(row, direction)
        merged = _merge_rs_into_existing(existing, auto)
        _upsert_stock(db, sd, sym, direction, merged)
        db.commit()
    finally:
        db.close()
    return get_state(sd)


def _cast_field(field: str, value: Any) -> Any:
    if field in NUM_FIELDS:
        return _num(value)
    if field in BOOL_FIELDS:
        if isinstance(value, bool):
            return value
        if value is None or value == "":
            return None
        return str(value).strip().lower() in ("1", "true", "yes", "clean", "on")
    # text
    v = ("" if value is None else str(value)).strip()
    return v or None


def update_field(symbol: str, field: str, value: Any, session_date: Optional[str] = None) -> Dict[str, Any]:
    """Apply one field update, re-evaluate, persist, and return full page state."""
    sd = session_date or today_ist()
    if field not in (TEXT_FIELDS | NUM_FIELDS | BOOL_FIELDS):
        raise ValueError(f"unknown field: {field}")
    casted = _cast_field(field, value)

    db = SessionLocal()
    try:
        if field in PAGE_LEVEL_FIELDS:
            # Apply to every stock for the day, then re-evaluate all.
            db.execute(
                text("UPDATE daily_checklist SET nifty_open_direction = :v, updated_at = NOW() WHERE session_date = :d"),
                {"v": casted, "d": sd},
            )
            db.commit()
            _reevaluate_all(db, sd)
        else:
            db.execute(
                text(
                    f"UPDATE daily_checklist SET {field} = :v, updated_at = NOW() "
                    "WHERE session_date = :d AND symbol = :s"
                ),
                {"v": casted, "d": sd, "s": symbol},
            )
            db.commit()
            _reevaluate_symbol(db, sd, symbol)
        db.commit()
    finally:
        db.close()
    return get_state(sd)


def _load_raw(db, sd: str, symbol: str) -> Optional[Dict[str, Any]]:
    r = db.execute(
        text(f"SELECT {_SELECT_COLS} FROM daily_checklist WHERE session_date=:d AND symbol=:s"),
        {"d": sd, "s": symbol},
    ).fetchone()
    return _row_to_dict(r) if r else None


def _persist_derived(db, sd: str, symbol: str, derived: Dict[str, Any]) -> None:
    sets = ", ".join(f"{c} = :{c}" for c in DERIVED_COLS)
    params = {c: derived[c] for c in DERIVED_COLS}
    params.update({"d": sd, "s": symbol})
    db.execute(
        text(f"UPDATE daily_checklist SET {sets}, updated_at = NOW() WHERE session_date=:d AND symbol=:s"),
        params,
    )


def _reevaluate_symbol(db, sd: str, symbol: str) -> None:
    raw = _load_raw(db, sd, symbol)
    if not raw:
        return
    _persist_derived(db, sd, symbol, evaluate(raw))


def _reevaluate_all(db, sd: str) -> None:
    rows = db.execute(
        text("SELECT symbol FROM daily_checklist WHERE session_date=:d"), {"d": sd}
    ).fetchall()
    for r in rows:
        _reevaluate_symbol(db, sd, r.symbol)


def reset_day(session_date: Optional[str] = None) -> Dict[str, Any]:
    """Delete today's checklist rows (history table retains prior days)."""
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM daily_checklist WHERE session_date = :d"), {"d": sd})
        db.commit()
    finally:
        db.close()
    return get_state(sd)


def history(limit: int = 30) -> List[Dict[str, Any]]:
    """Past days' summaries: counts of GO/WATCH/OUT per session_date."""
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT session_date,
                       COUNT(*) AS total,
                       SUM(CASE WHEN section = 'GO' THEN 1 ELSE 0 END) AS go,
                       SUM(CASE WHEN section = 'WATCH' THEN 1 ELSE 0 END) AS watch,
                       SUM(CASE WHEN section = 'OUT' THEN 1 ELSE 0 END) AS out_
                FROM daily_checklist
                GROUP BY session_date
                ORDER BY session_date DESC
                LIMIT :lim
                """
            ),
            {"lim": int(limit)},
        ).fetchall()
    finally:
        db.close()
    return [
        {
            "session_date": r.session_date.isoformat() if hasattr(r.session_date, "isoformat") else str(r.session_date),
            "total": int(r.total or 0),
            "go": int(r.go or 0),
            "watch": int(r.watch or 0),
            "out": int(r.out_ or 0),
        }
        for r in rows
    ]
