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

from backend.services.kavach_confidence import (
    confidence_passes_gate,
    format_quality_row,
)
from backend.services.rs_scanner_anchors import anchor_overlap_at_0925
from backend.services.rs_scanner_maturity import MATURITY_CLIMACTIC
from backend.services.daily_checklist_snapshot import (
    at_or_after_lock_time,
    audit_checklist_lock_coverage,
    clear_snapshot_for_date,
    get_lock_info,
    get_locked_symbol_rows,
    get_locked_symbols,
    is_snapshot_locked,
    lock_morning_snapshot,
    locked_direction_map,
    persistence_map_for_session,
    promote_intraday_from_rs,
    sort_by_persistence,
    sort_by_snapshot_rank,
)

from backend.database import SessionLocal

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Entry window (hard rule): 9:45 – 14:30 IST.
ENTRY_START_MIN = 9 * 60 + 45
ENTRY_END_MIN = 14 * 60 + 30

LONG = "LONG"
SHORT = "SHORT"

# Decision strings (match the spec exactly, emoji included).
D_GO = "🟢 GO — ENTER"
D_WATCH = "🟡 WATCH — WAIT"
D_NOTRADE = "🔴 NO TRADE"
D_ELIMINATED = "🔴 ELIMINATED"
D_UNASSESSED = "⬜ Not assessed"
D_CHART_REVERSED = "🔄 CHART REVERSED"

# Section buckets the page renders into.
SEC_GO = "GO"
SEC_WATCH = "WATCH"
SEC_OUT = "OUT"  # eliminated + hard-fail + low-score no-trade

# Raw fields the client may update (whitelist for safe casting/persistence).
TEXT_FIELDS = {
    "nifty_open_direction", "fii_dii_flow", "entry_time", "confidence", "trading_state",
    "ema_vs_vwap", "supertrend", "macd", "di_alignment", "volume", "notes",
}
NUM_FIELDS = {"adx_935", "kavach_score_entry", "adx_entry", "rs_pct", "vol_multiplier", "dashboard_score"}
BOOL_FIELDS = {"news_clean", "counter_rs"}
PAGE_LEVEL_FIELDS = {"nifty_open_direction", "fii_dii_flow"}  # one value applied to all stocks

# Columns the evaluator derives and persists.
DERIVED_COLS = (
    "adx_935_status", "time_ok", "score_ok", "confidence_ok", "state_ok",
    "ema_ok", "st_ok", "macd_ok", "adx_ok", "volume_ok", "gate_score",
    "decision", "section", "eligibility_note",
)
TIMING_COLS = (
    "go_enter_first_at", "go_sticky_until", "indicator_stale",
)
PERSIST_EVAL_COLS = DERIVED_COLS + TIMING_COLS


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


def _clock_minutes_now() -> int:
    now = datetime.now(IST)
    return now.hour * 60 + now.minute


def evaluate(row: Dict[str, Any]) -> Dict[str, Any]:
    """Derive per-condition flags + gate score + decision/section from raw inputs.

    ``row`` carries the raw field values plus ``direction`` and ``counter_rs``.
    When ``server_eval`` is True (background refresh), entry-time gate uses the
    live IST clock instead of a stale stored entry_time.
    """
    direction = (row.get("direction") or LONG).upper()
    is_long = direction == LONG
    counter = bool(row.get("counter_rs"))
    maturity = (row.get("maturity_tag") or "").strip().upper()
    maturity_a_only = maturity in ("EXTENDED", "STRETCHED", MATURITY_CLIMACTIC)
    climactic = maturity == MATURITY_CLIMACTIC

    news_clean = row.get("news_clean")
    adverse_news = news_clean is False
    a935 = _num(row.get("adx_935"))

    # Entry time gate — server refresh always uses live clock.
    if row.get("server_eval"):
        t_min = _clock_minutes_now()
    else:
        t_min = _to_min(row.get("entry_time"))
        if t_min is None:
            t_min = _clock_minutes_now()

    time_ok: Optional[bool] = None
    time_hardfail = False
    time_wait = False
    if t_min is not None:
        if t_min < ENTRY_START_MIN:
            time_ok = False
            time_wait = True
        elif t_min > ENTRY_END_MIN:
            time_ok = False
            time_hardfail = True
        else:
            time_ok = True

    score = _num(row.get("kavach_score_entry"))
    score_ok = (score >= 70) if score is not None else None

    conf = (row.get("confidence") or "").strip().upper().replace("*", "") or None
    conf_display = (row.get("confidence") or "").strip().upper() or None
    if conf is None:
        confidence_ok = None
    else:
        confidence_ok = confidence_passes_gate(
            conf_display or conf,
            counter_rs=counter,
            maturity_a_only=maturity_a_only and not climactic,
            climactic=climactic,
        )
        if climactic and conf in ("A+", "A") and (row.get("volume") or "").strip().lower() != "high":
            confidence_ok = False

    state = (row.get("trading_state") or "").strip().upper() or None
    state_ok: Optional[bool] = None
    state_hardfail = False
    if state is not None:
        if is_long:
            state_ok = state in ("BUY", "MANAGE LONG")
        else:
            state_ok = state in ("SELL", "MANAGE SHORT")
        state_hardfail = not state_ok

    ema = (row.get("ema_vs_vwap") or "").strip().lower() or None
    if ema is None:
        ema_ok = None
    else:
        ema_ok = ema == "above" if is_long else ema == "below"

    st = (row.get("supertrend") or "").strip().lower() or None
    if st is None:
        st_ok = None
    else:
        st_ok = st == "bullish" if is_long else st == "bearish"

    macd = (row.get("macd") or "").strip().lower() or None
    if macd is None:
        macd_ok = None
    elif is_long:
        macd_ok = macd in ("bullish", "crossing")
    else:
        macd_ok = macd in ("bearish", "crossing")

    adx_e = _num(row.get("adx_entry"))
    di = (row.get("di_alignment") or "").strip() or None
    if adx_e is None:
        adx_ok = None
    else:
        di_ok = True
        if di is not None:
            di_ok = di == "DI+>DI-" if is_long else di == "DI->DI+"
        adx_ok = (adx_e >= 25) and di_ok

    vol = (row.get("volume") or "").strip().lower() or None
    if vol is None:
        volume_ok = None
    else:
        volume_ok = vol in ("high", "normal", "average")

    gate_flags = [time_ok, score_ok, confidence_ok, state_ok, ema_ok, st_ok, macd_ok, adx_ok, volume_ok]
    gate_score = sum(1 for f in gate_flags if f is True)

    hard_fail = adverse_news or time_hardfail or state_hardfail
    chart_reversed = bool(row.get("chart_reversed"))

    if adverse_news:
        decision, section = D_ELIMINATED, SEC_OUT
    elif chart_reversed:
        decision, section = D_CHART_REVERSED, SEC_WATCH
    elif time_hardfail or state_hardfail:
        decision, section = D_NOTRADE, SEC_OUT
    elif time_wait:
        decision, section = "🟡 WAIT — entry window not yet open", SEC_WATCH
    elif gate_score == 9:
        if climactic and not (conf in ("A+", "A") and vol == "high"):
            decision, section = D_WATCH, SEC_WATCH
        else:
            decision, section = D_GO, SEC_GO
    elif gate_score >= 6:
        decision, section = D_WATCH, SEC_WATCH
    elif gate_score > 0:
        decision, section = D_NOTRADE, SEC_OUT
    else:
        decision, section = D_UNASSESSED, SEC_WATCH

    eligibility_note = ""
    if confidence_ok is False:
        if climactic:
            eligibility_note = "CLIMACTIC — requires A-grade and High volume for GO"
        elif counter:
            eligibility_note = "Requires A-grade — counter-RS direction"
        elif maturity_a_only:
            eligibility_note = f"Requires A-grade — {maturity} move maturity"
        elif conf == "B":
            eligibility_note = "Requires A-grade — counter-RS direction"

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
        "eligibility_note": eligibility_note or None,
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
           s.volume_label, s.vwap_purity_pct, s.market_regime, s.confidence_grade,
           s.kavach_state, s.ema5, s.vwap, s.supertrend, s.macd, s.macd_signal,
           s.macd_histogram, s.adx, s.ranking_type, s.scan_time, s.rank_position,
           h.maturity_tag, h.consecutive_days_on_list, h.range_vs_atr_ratio
    FROM relative_strength_snapshot s
    LEFT JOIN rs_scanner_history h
      ON h.symbol = s.symbol
     AND h.date = (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')::date
    WHERE s.scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
      AND s.rank_position <= 5
    ORDER BY s.ranking_type, s.rank_position
    """
)

_RS_DETAIL_SQL = text(
    """
    SELECT s.symbol, s.relative_strength, s.trade_score, s.volume_ratio,
           s.volume_label, s.vwap_purity_pct, s.market_regime, s.confidence_grade,
           s.kavach_state, s.ema5, s.vwap, s.supertrend, s.macd, s.macd_signal,
           s.macd_histogram, s.adx, s.ranking_type, s.scan_time,
           h.maturity_tag, h.consecutive_days_on_list, h.range_vs_atr_ratio
    FROM relative_strength_snapshot s
    LEFT JOIN rs_scanner_history h
      ON h.symbol = s.symbol
     AND h.date = (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')::date
    WHERE s.scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
      AND s.symbol = :sym
    LIMIT 1
    """
)

_RS_LOCKED_FALLBACK_SQL = text(
    """
    SELECT s.symbol, s.relative_strength, s.trade_score, s.volume_ratio,
           s.volume_label, s.vwap_purity_pct, s.market_regime, s.confidence_grade,
           s.kavach_state, s.ema5, s.vwap, s.supertrend, s.macd, s.macd_signal,
           s.macd_histogram, s.adx, s.ranking_type, s.scan_time, s.rank_position,
           h.maturity_tag, h.consecutive_days_on_list, h.range_vs_atr_ratio
    FROM relative_strength_snapshot s
    LEFT JOIN rs_scanner_history h
      ON h.symbol = s.symbol
     AND h.date = (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')::date
    WHERE s.symbol = :sym
      AND s.scan_time::date = :d
    ORDER BY s.scan_time DESC
    LIMIT 1
    """
)

_RS_LIVE_DIRECTION_SQL = text(
    """
    SELECT symbol, ranking_type, scan_time
    FROM relative_strength_snapshot
    WHERE scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
      AND rank_position <= 5
    """
)

# User-owned fields preserved across RS sync/populate refreshes.
_PRESERVE_ON_RS_SYNC = frozenset({"news_clean", "notes", "counter_rs"})


def _direction_from_ranking(ranking_type: Optional[str]) -> str:
    return SHORT if (ranking_type or "").upper() == "BEARISH" else LONG


def _confidence_grade(score: Optional[float], rs_row: Any = None) -> Optional[str]:
    if rs_row is not None and getattr(rs_row, "confidence_grade", None):
        return str(rs_row.confidence_grade).strip().upper() or None
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


def _volume_label(ratio: Optional[float], label: Optional[str] = None) -> Optional[str]:
    if label:
        lv = str(label).strip().lower()
        if lv == "average":
            return "Average"
        if lv == "high":
            return "High"
        if lv == "low":
            return "Low"
    if ratio is None:
        return None
    r = float(ratio)
    if r >= 1.2:
        return "High"
    if r >= 0.65:
        return "Average"
    return "Low"


def _current_entry_time_ist() -> Optional[str]:
    now = datetime.now(IST)
    m = now.hour * 60 + now.minute
    if ENTRY_START_MIN <= m <= ENTRY_END_MIN:
        return f"{now.hour:02d}:{now.minute:02d}"
    return None


def _auto_fields_from_rs(row: Any, direction: str, live_map: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Map a relative_strength_snapshot row to checklist fields the system can fill."""
    score = _num(row.trade_score)
    adx = _num(row.adx)
    ema5 = _num(row.ema5)
    vwap = _num(row.vwap)
    ema_lbl = _ema_vs_vwap_label(ema5, vwap)
    di = None
    if adx is not None and adx >= 25:
        di = "DI+>DI-" if direction == LONG else "DI->DI+"
    maturity_tag = getattr(row, "maturity_tag", None) or "FRESH"
    consecutive = getattr(row, "consecutive_days_on_list", None)
    range_ratio = _num(getattr(row, "range_vs_atr_ratio", None))
    vol_lbl = _volume_label(_num(row.volume_ratio), getattr(row, "volume_label", None))
    purity = _num(getattr(row, "vwap_purity_pct", None)) or 0.0
    regime = getattr(row, "market_regime", None) or "TREND"
    conf = _confidence_grade(score, row)
    sym = getattr(row, "symbol", "") or ""
    live_dir = (live_map or {}).get(sym)
    fields: Dict[str, Any] = {
        "rs_pct": round(_num(row.relative_strength) or 0, 2),
        "dashboard_score": int(round(score or 0)) if score is not None else None,
        "dashboard_kavach": row.kavach_state,
        "vol_multiplier": round(_num(row.volume_ratio) or 0, 2),
        "kavach_score_entry": int(round(score or 0)) if score is not None else None,
        "confidence": conf,
        "trading_state": _trading_state_label(row.kavach_state, direction),
        "ema_vs_vwap": ema_lbl,
        "supertrend": _supertrend_label(_num(row.supertrend)),
        "macd": _macd_label(_num(row.macd), _num(row.macd_signal), _num(row.macd_histogram)),
        "adx_entry": round(adx, 1) if adx is not None else None,
        "adx_935": round(adx, 2) if adx is not None else None,
        "di_alignment": di,
        "volume": vol_lbl,
        "entry_time": _current_entry_time_ist(),
        "maturity_tag": maturity_tag,
        "consecutive_days_on_list": int(consecutive) if consecutive is not None else 1,
        "range_vs_atr_ratio": round(range_ratio, 2) if range_ratio is not None else 0.0,
        "vwap_purity_pct": round(purity, 1),
        "market_regime": regime,
        "quality_display": format_quality_row(
            "High" if vol_lbl == "High" else ("Average" if vol_lbl == "Average" else "Low"),
            purity,
            score or 0,
            regime,
        ),
        "live_rs_direction": live_dir,
        "live_rs_updated_at": getattr(row, "scan_time", None),
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


def _live_direction_map(db) -> Dict[str, str]:
    rows = db.execute(_RS_LIVE_DIRECTION_SQL).fetchall()
    out: Dict[str, str] = {}
    for r in rows:
        out[r.symbol] = (r.ranking_type or "BULLISH").upper()
    return out


def _rotation_carryover_flags(
    rotation: Dict[str, Any], symbol: str, direction: str
) -> Tuple[bool, Optional[str]]:
    """Dim cards on ROTATION days when symbol was on yesterday but not today's 09:25."""
    if rotation.get("rotation_day_type") != "ROTATION":
        return False, rotation.get("rotation_day_type")
    sym = symbol.upper()
    is_long = direction == LONG
    today_set = set(rotation.get("today_bull" if is_long else "today_bear") or [])
    yday_set = set(rotation.get("yesterday_bull" if is_long else "yesterday_bear") or [])
    carry = sym in yday_set and sym not in today_set
    return carry, rotation.get("rotation_day_type")


_UPSERT_COLS = (
    "rs_pct", "dashboard_score", "dashboard_kavach", "vol_multiplier",
    "news_clean", "adx_935", "entry_time", "kavach_score_entry", "confidence",
    "trading_state", "ema_vs_vwap", "supertrend", "macd", "adx_entry",
    "di_alignment", "volume", "counter_rs", "notes",
    "maturity_tag", "consecutive_days_on_list", "range_vs_atr_ratio",
    "vwap_purity_pct", "market_regime", "quality_display",
    "live_rs_direction", "live_rs_updated_at", "rotation_day_type",
    "carryover_warning", "sector_badge", "data_refreshed_at",
    "indicator_as_of", "indicator_source", "chart_reversed",
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
    news_clean, adx_935, adx_935_status, nifty_open_direction, fii_dii_flow, entry_time, time_ok,
    kavach_score_entry, score_ok, confidence, confidence_ok, trading_state, state_ok,
    ema_vs_vwap, ema_ok, supertrend, st_ok, macd, macd_ok, adx_entry, di_alignment,
    adx_ok, volume, volume_ok, counter_rs, gate_score, decision, section, notes,
    maturity_tag, consecutive_days_on_list, range_vs_atr_ratio, eligibility_note,
    vwap_purity_pct, market_regime, quality_display, live_rs_direction,
    live_rs_updated_at, rotation_day_type, carryover_warning, sector_badge,
    data_refreshed_at, go_enter_first_at, go_sticky_until, indicator_as_of,
    indicator_source, indicator_stale, chart_reversed, updated_at
"""


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _parse_iso_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(IST) if v.tzinfo else v.replace(tzinfo=IST)
    try:
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(IST) if dt.tzinfo else dt.replace(tzinfo=IST)
    except (TypeError, ValueError):
        return None


def _latest_rs_scan_time(db) -> Optional[datetime]:
    row = db.execute(
        text("SELECT MAX(scan_time) AS latest_scan FROM relative_strength_snapshot")
    ).fetchone()
    if not row:
        return None
    t = row._mapping.get("latest_scan") if hasattr(row, "_mapping") else row[0]
    if not isinstance(t, datetime):
        return None
    return t.astimezone(IST) if t.tzinfo else t.replace(tzinfo=IST)


def _apply_live_recompute(db, sd: str, symbol: str, direction: str, merged: Dict[str, Any], rs_row) -> None:
    """L1: refresh indicator fields from candle cache when possible."""
    from backend.services.daily_checklist_live import recompute_locked_symbol

    if not is_snapshot_locked(db, sd) or symbol not in set(get_locked_symbols(db, sd)):
        if rs_row is not None and getattr(rs_row, "scan_time", None):
            merged["indicator_as_of"] = rs_row.scan_time
            merged["indicator_source"] = "rs_snapshot"
        return
    live = recompute_locked_symbol(db, symbol, direction)
    if live:
        merged.update(live["fields"])
        merged["indicator_as_of"] = live["indicator_as_of"]
        merged["indicator_source"] = live["source"]
    elif rs_row is not None and getattr(rs_row, "scan_time", None):
        merged["indicator_as_of"] = rs_row.scan_time
        merged["indicator_source"] = "rs_fallback"


def _derive_with_timing(db, raw: Dict[str, Any], *, prev: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Evaluate + L2/L3 staleness cap and GO sticky timing."""
    from backend.services.daily_checklist_live import is_indicator_stale
    from backend.services.daily_checklist_timing import apply_go_timing, apply_staleness_cap

    prev = prev or raw
    ia = _parse_iso_dt(raw.get("indicator_as_of"))
    latest = _latest_rs_scan_time(db)
    stale = is_indicator_stale(ia, latest)
    ev_row = dict(raw)
    ev_row["server_eval"] = True
    derived = evaluate(ev_row)
    derived = apply_staleness_cap(derived, stale=stale)
    derived = apply_go_timing(derived, prev=prev, stale=stale)
    return derived


def _row_to_dict(r) -> Dict[str, Any]:
    d = {c: getattr(r, c) for c in (
        "symbol", "direction", "rs_pct", "dashboard_score", "dashboard_kavach",
        "vol_multiplier", "news_clean", "adx_935", "adx_935_status",
        "nifty_open_direction", "entry_time", "time_ok", "kavach_score_entry",
        "score_ok", "confidence", "confidence_ok", "trading_state", "state_ok",
        "ema_vs_vwap", "ema_ok", "supertrend", "st_ok", "macd", "macd_ok",
        "adx_entry", "di_alignment", "adx_ok", "volume", "volume_ok", "counter_rs",
        "gate_score", "decision", "section", "notes",
        "maturity_tag", "consecutive_days_on_list", "range_vs_atr_ratio",
        "eligibility_note", "vwap_purity_pct", "market_regime", "quality_display",
        "live_rs_direction", "rotation_day_type", "carryover_warning", "sector_badge",
        "indicator_source", "indicator_stale",
    )}
    d["live_rs_updated_at"] = (
        r.live_rs_updated_at.isoformat() if getattr(r, "live_rs_updated_at", None) else None
    )
    d["data_refreshed_at"] = (
        r.data_refreshed_at.isoformat() if getattr(r, "data_refreshed_at", None) else None
    )
    d["go_enter_first_at"] = (
        r.go_enter_first_at.isoformat() if getattr(r, "go_enter_first_at", None) else None
    )
    d["go_sticky_until"] = (
        r.go_sticky_until.isoformat() if getattr(r, "go_sticky_until", None) else None
    )
    d["indicator_as_of"] = (
        r.indicator_as_of.isoformat() if getattr(r, "indicator_as_of", None) else None
    )
    d["go_sticky_active"] = False
    if d.get("go_sticky_until") and d.get("section") == SEC_GO:
        sticky = _parse_iso_dt(d["go_sticky_until"])
        if sticky and sticky > datetime.now(IST):
            d["go_sticky_active"] = True
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
    """Full page state: locked today list, carryover, preview (pre-lock), counts."""
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        lock_info = get_lock_info(db, sd)
        locked = lock_info is not None
        locked_syms = set(get_locked_symbols(db, sd)) if locked else set()

        rows = db.execute(
            text(
                f"SELECT {_SELECT_COLS} FROM daily_checklist WHERE session_date = :d "
                "ORDER BY CASE direction WHEN 'LONG' THEN 0 ELSE 1 END, "
                "COALESCE(rs_pct, 0) DESC"
            ),
            {"d": sd},
        ).fetchall()
        all_stocks = [_row_to_dict(r) for r in rows]

        rank_map: Dict[str, Tuple[int, int]] = {}
        for r in get_locked_symbol_rows(db, sd):
            bucket = 0 if r.direction == "BULL" else 1
            rank_map[r.symbol] = (bucket, int(r.rank))

        today_stocks: List[Dict[str, Any]] = []
        carryover_stocks: List[Dict[str, Any]] = []
        preview_stocks: List[Dict[str, Any]] = []

        if locked:
            pers_map = {}
            try:
                pers_map = persistence_map_for_session(db, sd)
            except Exception as exc:
                logger.debug("persistence map skipped: %s", exc)
            locked_list = [s for s in all_stocks if s["symbol"] in locked_syms]
            for s in locked_list:
                p = pers_map.get(s["symbol"]) or {}
                s["persistence_top5_frac"] = p.get("top5_fraction")
                s["persistence_clean_bars"] = p.get("clean_vwap_bars")
                s["persistence_scans"] = p.get("scans_since_promote")
            today_stocks = sort_by_persistence(locked_list, pers_map, rank_map)
            carryover_stocks = [s for s in all_stocks if s["symbol"] not in locked_syms]
            for s in carryover_stocks:
                s["is_carryover"] = True
        else:
            preview_stocks = _preview_stocks_from_rs(db)
            for s in preview_stocks:
                s["is_preview"] = True
    finally:
        db.close()

    display_stocks = today_stocks if locked else preview_stocks
    nifty_dir = ""
    fii_dii = ""
    for s in all_stocks:
        if s.get("nifty_open_direction"):
            nifty_dir = s["nifty_open_direction"]
        if s.get("fii_dii_flow"):
            fii_dii = s["fii_dii_flow"]
        if nifty_dir and fii_dii:
            break
    levels = _nifty_levels()
    rotation = anchor_overlap_at_0925()
    latest_refresh = None
    for s in display_stocks:
        if s.get("data_refreshed_at"):
            latest_refresh = s["data_refreshed_at"]
            break

    live_setups: List[Dict[str, Any]] = []
    radar_map: Dict[str, Dict[str, Any]] = {}
    try:
        from backend.services.rs_setup_radar import get_live_setups, get_radar_for_symbols

        syms = [s["symbol"] for s in display_stocks]
        radar_map = get_radar_for_symbols(syms)
        live_setups = get_live_setups()
    except Exception as exc:
        logger.debug("checklist radar enrichment failed: %s", exc)

    _LIVE_SETUP = frozenset({"CONVERGING", "TRIGGERED", "TRIGGERED_CHOP", "LATE"})
    for s in display_stocks:
        radar = radar_map.get(s["symbol"], {})
        setup = (radar.get("setup_state") or "NEUTRAL").upper()
        s["setup_state"] = setup
        s["sl_pct"] = radar.get("sl_pct")
        s["sl_rupees"] = radar.get("sl_rupees")
        s["grade_gate_locked"] = setup in _LIVE_SETUP and (
            s.get("section") == SEC_OUT or s.get("confidence_ok") is False
        )

    ignition_map: Dict[str, Dict[str, Any]] = {}
    try:
        from backend.services.kavach_momentum_ignition import get_ignition_for_symbols

        ignition_map = get_ignition_for_symbols([s["symbol"] for s in display_stocks], sd)
    except Exception as exc:
        logger.debug("checklist ignition enrichment failed: %s", exc)
    for s in display_stocks:
        ig = ignition_map.get(s["symbol"], {})
        s["ignition_score"] = ig.get("ignition_score")
        s["ignition_building"] = bool(ig.get("ignition_building"))

    fast_watch: Dict[str, Any] = {"featured": {"long": [], "short": []}, "all": [], "total_count": 0}
    checklist_cfg: Dict[str, Any] = {}
    try:
        from backend.services.rs_conviction_config import get_config
        from backend.services.rs_fast_watch import get_fast_watch

        checklist_cfg = {
            "go_alert_sound_enabled": bool(get_config().get("go_alert_sound_enabled")),
            "fast_watch_ui_enabled": bool(get_config().get("fast_watch_ui_enabled")),
            "go_board_ui_enabled": bool(get_config().get("go_board_ui_enabled")),
        }
        if checklist_cfg.get("fast_watch_ui_enabled"):
            fast_watch = get_fast_watch(sd)
    except Exception as exc:
        logger.debug("checklist fast watch enrichment failed: %s", exc)

    go_board: Dict[str, Any] = {"symbols": [], "empty": True, "window": None}
    try:
        from backend.services.rs_go_board import get_go_board

        if bool(get_config().get("go_board_ui_enabled")):
            go_board = get_go_board(sd)
            checklist_cfg["go_board_ui_enabled"] = True
    except Exception as exc:
        logger.debug("checklist go board enrichment failed: %s", exc)

    trade_obs: Dict[str, Any] = {
        "churn_warning": False,
        "churn_symbols": [],
        "churn_count": 0,
        "recent_removals": [],
    }
    try:
        from backend.services.daily_checklist_trade_state import (
            enrich_stocks_trade_state,
            sort_stocks_by_trade_state,
        )

        trade_obs = enrich_stocks_trade_state(display_stocks, sd)
        sorted_stocks = sort_stocks_by_trade_state(display_stocks, rank_map)
        display_stocks[:] = sorted_stocks
        if locked:
            today_stocks[:] = sorted_stocks
        else:
            preview_stocks[:] = sorted_stocks
    except Exception as exc:
        logger.debug("checklist trade-state enrichment failed: %s", exc)

    return {
        "session_date": sd,
        "locked": locked,
        "locked_at": lock_info["locked_at"] if lock_info else None,
        "locked_by": lock_info["locked_by"] if lock_info else None,
        "nifty_open_direction": nifty_dir,
        "fii_dii_flow": fii_dii,
        "nifty50": levels["nifty50"],
        "banknifty": levels["banknifty"],
        "today": today_stocks,
        "carryover": carryover_stocks,
        "preview": preview_stocks,
        "stocks": display_stocks,
        "counts": _counts(today_stocks if locked else preview_stocks),
        "rotation_day": rotation,
        "data_refreshed_at": latest_refresh,
        "live_setups": live_setups,
        "fast_watch": fast_watch,
        "go_board": go_board,
        "checklist_config": checklist_cfg,
        "trade_state_obs": trade_obs,
    }


def _preview_stocks_from_rs(db) -> List[Dict[str, Any]]:
    """Build read-only preview cards from latest RS top-5+5 (no DB writes)."""
    rows = db.execute(_RS_ALL_SQL).fetchall()
    if not rows:
        return []
    live_map = _live_direction_map(db)
    out: List[Dict[str, Any]] = []
    for row in rows:
        direction = _direction_from_ranking(row.ranking_type)
        auto = _auto_fields_from_rs(row, direction, live_map)
        merged = dict(auto)
        merged["symbol"] = row.symbol
        merged["direction"] = direction
        merged["news_clean"] = True
        merged["decision"] = D_UNASSESSED
        merged["section"] = SEC_WATCH
        merged["gate_score"] = 0
        derived = evaluate({**merged, "server_eval": True})
        out.append({**merged, **{k: derived[k] for k in DERIVED_COLS if k in derived}})
    return sort_by_snapshot_rank(
        out,
        {
            r.symbol: (0 if (r.ranking_type or "").upper() != "BEARISH" else 1, int(r.rank_position or 99))
            for r in rows
        },
    )


def populate_from_rs() -> Dict[str, Any]:
    """Seed today's checklist from the latest RS snapshot with system auto-fill."""
    return _refresh_checklist_from_rs(full_populate=True)


def refresh_checklist_from_rs() -> Dict[str, Any]:
    """Background job: re-sync all checklist rows from latest RS + re-evaluate."""
    return _refresh_checklist_from_rs(full_populate=False)


def _sector_badges_for_top5(db) -> Dict[str, str]:
    """S1/S2/S3 for bullish top-gainer sectors, W1/W2/W3 for bearish."""
    try:
        from backend.services.rs_scanner_sectors import compute_sector_badges

        return compute_sector_badges(db)
    except Exception as exc:
        logger.debug("sector badges skipped: %s", exc)
        return {}


def _refresh_checklist_from_rs(*, full_populate: bool) -> Dict[str, Any]:
    """Sync checklist from RS. Locks Top-5+5 once at/after 09:25; then refreshes locked set.

    After morning lock, also promotes any symbol with 2 consecutive Top-5 RS scans
    (either direction) through 14:30 into daily_snapshot so late/flip names reach
    checklist / Fast Watch / GO Board.
    """
    sd = today_ist()
    now = datetime.now(IST)
    db = SessionLocal()
    try:
        rows = db.execute(_RS_ALL_SQL).fetchall()
        live_map = _live_direction_map(db)
        rotation = anchor_overlap_at_0925()
        badges = _sector_badges_for_top5(db)

        locked = is_snapshot_locked(db, sd)
        if not locked and at_or_after_lock_time(now) and rows:
            # Prefer conviction Core board if already seeded; else lock raw top-5+5
            try:
                from backend.services.rs_conviction_board import (
                    SIDE_BEAR,
                    SIDE_BULL,
                    _load_core_board,
                    run_conviction_board_cycle,
                )

                run_conviction_board_cycle(force=True)
                bull_core = _load_core_board(db, sd, SIDE_BULL)
                bear_core = _load_core_board(db, sd, SIDE_BEAR)
                if bull_core or bear_core:
                    bull_rows = [r for r in rows if (r.ranking_type or "").upper() != "BEARISH"]
                    bear_rows = [r for r in rows if (r.ranking_type or "").upper() == "BEARISH"]
                    lock_morning_snapshot(
                        db, sd, bull_rows, bear_rows, locked_by="manual" if full_populate else "auto",
                    )
                    locked = True
                else:
                    bull_rows = [r for r in rows if (r.ranking_type or "").upper() != "BEARISH"]
                    bear_rows = [r for r in rows if (r.ranking_type or "").upper() == "BEARISH"]
                    lock_morning_snapshot(
                        db, sd, bull_rows, bear_rows, locked_by="manual" if full_populate else "auto",
                    )
                    locked = True
            except Exception as exc:
                logger.warning("conviction lock fallback: %s", exc)
                bull_rows = [r for r in rows if (r.ranking_type or "").upper() != "BEARISH"]
                bear_rows = [r for r in rows if (r.ranking_type or "").upper() == "BEARISH"]
                lock_morning_snapshot(
                    db, sd, bull_rows, bear_rows, locked_by="manual" if full_populate else "auto",
                )
                locked = True

        if locked:
            # Morning daily_snapshot is the checklist source of truth — not conviction Core board.
            # Core board can diverge (hysteresis, bench, pre-market state); using it here dropped
            # valid BULL names when board membership != 09:25 RS top-5 lock (2026-07-06).
            try:
                promo = promote_intraday_from_rs(db, sd, now=now)
                if promo.get("promoted") or promo.get("flipped"):
                    logger.info(
                        "daily_checklist: intraday promotion applied promoted=%d flipped=%d",
                        len(promo.get("promoted") or []),
                        len(promo.get("flipped") or []),
                    )
            except Exception as exc:
                logger.warning("daily_checklist: intraday promotion failed: %s", exc)
            locked_syms = set(get_locked_symbols(db, sd))
        else:
            logger.debug("daily_checklist: pre-09:25 lock — skip persist for %s", sd)
            db.commit()
            state = get_state(sd)
            state["refresh_status"] = "no_lock"
            state["refresh_message"] = "Morning snapshot not yet taken (locks at/after 09:25 IST)"
            return state

        row_by_sym = {r.symbol: r for r in rows}
        lock_dirs = locked_direction_map(db, sd)
        refreshed = 0

        for sym in locked_syms:
            row = row_by_sym.get(sym)
            if row is None:
                row = db.execute(_RS_LOCKED_FALLBACK_SQL, {"sym": sym, "d": sd}).fetchone()
            if row is None:
                _reevaluate_symbol(db, sd, sym)
                continue
            # Prefer live RS side when present so flipped promotions recompute correctly;
            # fall back to daily_snapshot direction when symbol is off the current top-5.
            direction = lock_dirs.get(sym) or _direction_from_ranking(row.ranking_type)
            existing = _load_raw(db, sd, sym)
            auto = _auto_fields_from_rs(row, direction, live_map)
            merged = _merge_rs_into_existing(existing, auto)
            carry, rtype = _rotation_carryover_flags(rotation, sym, direction)
            merged["rotation_day_type"] = rtype
            merged["carryover_warning"] = carry
            merged["sector_badge"] = badges.get(sym)
            merged["data_refreshed_at"] = now
            _apply_live_recompute(db, sd, sym, direction, merged, row)
            _upsert_stock(db, sd, sym, direction, merged)
            refreshed += 1

        flip_updates = []
        eligible_fw = set(locked_syms) | {r.symbol for r in rows if r.symbol}
        for sym in eligible_fw:
            if sym in locked_syms:
                raw = _load_raw(db, sd, sym)
                if raw:
                    flip_updates.append({**raw, "lock_direction": lock_dirs.get(sym)})
                continue
            row = row_by_sym.get(sym)
            if row is not None:
                auto = _auto_fields_from_rs(row, _direction_from_ranking(row.ranking_type), live_map)
                flip_updates.append({**auto, "symbol": sym, "direction": _direction_from_ranking(row.ranking_type)})
        try:
            from backend.services.rs_fast_watch import record_fast_watch_flips

            record_fast_watch_flips(
                sd,
                flip_updates,
                locked_symbols=locked_syms,
                top5_symbols={r.symbol for r in rows if r.symbol},
            )
        except Exception as exc:
            logger.debug("fast watch record skipped: %s", exc)

        logger.info("daily_checklist: refreshed %d locked stocks for %s", refreshed, sd)
        audit_checklist_lock_coverage(db, sd, rs_rows=rows)
        db.commit()
    finally:
        db.close()
    state = get_state(sd)
    state["refresh_status"] = "ok"
    return state


def sync_symbol_from_rs(symbol: str, session_date: Optional[str] = None) -> Dict[str, Any]:
    """Refresh one symbol's system-derived checklist fields from the latest RS scan."""
    sd = session_date or today_ist()
    sym = (symbol or "").strip().upper()
    if not sym:
        raise ValueError("symbol required")
    db = SessionLocal()
    try:
        if is_snapshot_locked(db, sd):
            locked_syms = set(get_locked_symbols(db, sd))
            if sym not in locked_syms:
                raise ValueError(f"{sym} is not on today's locked watchlist")
        row = db.execute(_RS_DETAIL_SQL, {"sym": sym}).fetchone()
        if not row:
            raise ValueError(f"no RS data for {sym}")
        existing = _load_raw(db, sd, sym)
        direction = (existing or {}).get("direction") or _direction_from_ranking(row.ranking_type)
        live_map = _live_direction_map(db)
        auto = _auto_fields_from_rs(row, direction, live_map)
        merged = _merge_rs_into_existing(existing, auto)
        merged["data_refreshed_at"] = datetime.now(IST)
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
            db.execute(
                text(f"UPDATE daily_checklist SET {field} = :v, updated_at = NOW() WHERE session_date = :d"),
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
    sets = ", ".join(f"{c} = :{c}" for c in PERSIST_EVAL_COLS)
    params: Dict[str, Any] = {}
    for c in PERSIST_EVAL_COLS:
        val = derived.get(c)
        if c in ("go_enter_first_at", "go_sticky_until") and val is not None:
            val = _parse_iso_dt(val)
        params[c] = val
    params.update({"d": sd, "s": symbol})
    db.execute(
        text(f"UPDATE daily_checklist SET {sets}, updated_at = NOW() WHERE session_date=:d AND symbol=:s"),
        params,
    )


def _reevaluate_symbol(db, sd: str, symbol: str) -> None:
    raw = _load_raw(db, sd, symbol)
    if not raw:
        return
    direction = raw.get("direction") or LONG
    if is_snapshot_locked(db, sd) and symbol in set(get_locked_symbols(db, sd)):
        merged = dict(raw)
        _apply_live_recompute(db, sd, symbol, direction, merged, None)
        for k in _UPSERT_COLS:
            if k in merged and merged.get(k) is not None:
                raw[k] = merged[k]
    derived = _derive_with_timing(db, raw, prev=raw)
    _persist_derived(db, sd, symbol, derived)


def _reevaluate_all(db, sd: str) -> None:
    rows = db.execute(
        text("SELECT symbol FROM daily_checklist WHERE session_date=:d"), {"d": sd}
    ).fetchall()
    for r in rows:
        _reevaluate_symbol(db, sd, r.symbol)


def reset_day(session_date: Optional[str] = None) -> Dict[str, Any]:
    """Delete today's checklist rows and clear morning snapshot + conviction board."""
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM daily_checklist WHERE session_date = :d"), {"d": sd})
        clear_snapshot_for_date(db, sd)
        db.commit()
    finally:
        db.close()
    try:
        from backend.services.rs_conviction_board import reset_conviction_day

        reset_conviction_day(sd)
    except Exception as exc:
        logger.warning("conviction board reset failed: %s", exc)
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
