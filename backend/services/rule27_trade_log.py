"""Rule 27 trade log — persistent DB store for discretionary trade journaling.

Mirrors the Excel ``Trade Log`` sheet columns plus the extended fields that
were previously packed into Notes (qty, slippage, MFE/MAE, scores at exit, etc.).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

TABLE = "trade_log"

_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id BIGSERIAL PRIMARY KEY,
    session_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    contract TEXT,
    direction TEXT NOT NULL,
    qty INTEGER,
    entry_time TIME NOT NULL,
    entry_price DOUBLE PRECISION NOT NULL,
    exit_time TIME,
    exit_price DOUBLE PRECISION,
    exit_price_intended DOUBLE PRECISION,
    slippage_pts DOUBLE PRECISION,
    points_captured DOUBLE PRECISION,
    ema10_at_entry DOUBLE PRECISION,
    ema5_at_entry DOUBLE PRECISION,
    vwap_at_entry DOUBLE PRECISION,
    entry_to_ema10_buffer_pct DOUBLE PRECISION,
    planned_risk_pts DOUBLE PRECISION,
    planned_risk_inr DOUBLE PRECISION,
    confidence_at_entry TEXT,
    trade_score_at_entry DOUBLE PRECISION,
    adx_at_entry DOUBLE PRECISION,
    confidence_at_exit TEXT,
    trade_score_at_exit DOUBLE PRECISION,
    mfe_r DOUBLE PRECISION,
    mae_r DOUBLE PRECISION,
    r_realized DOUBLE PRECISION,
    bars_held_10m INTEGER,
    exit_trigger TEXT,
    exit_trigger_type TEXT,
    notes TEXT,
    source TEXT DEFAULT 'manual',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (session_date, symbol, direction, entry_time)
)
"""

_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS idx_trade_log_session
ON {TABLE} (session_date DESC, symbol)
"""

# Permanent classifier for rule-driven vs manual exits (research / backtests).
EXIT_TRIGGER_TYPES = ("rule_compliant", "discretionary")

_UPSERT = text(
    f"""
    INSERT INTO {TABLE} (
        session_date, symbol, contract, direction, qty,
        entry_time, entry_price, exit_time, exit_price, exit_price_intended,
        slippage_pts, points_captured,
        ema10_at_entry, ema5_at_entry, vwap_at_entry, entry_to_ema10_buffer_pct,
        planned_risk_pts, planned_risk_inr,
        confidence_at_entry, trade_score_at_entry, adx_at_entry,
        confidence_at_exit, trade_score_at_exit,
        mfe_r, mae_r, r_realized, bars_held_10m,
        exit_trigger, exit_trigger_type, notes, source, updated_at
    ) VALUES (
        CAST(:session_date AS date), :symbol, :contract, :direction, :qty,
        CAST(:entry_time AS time), :entry_price, CAST(:exit_time AS time),
        :exit_price, :exit_price_intended,
        :slippage_pts, :points_captured,
        :ema10_at_entry, :ema5_at_entry, :vwap_at_entry, :entry_to_ema10_buffer_pct,
        :planned_risk_pts, :planned_risk_inr,
        :confidence_at_entry, :trade_score_at_entry, :adx_at_entry,
        :confidence_at_exit, :trade_score_at_exit,
        :mfe_r, :mae_r, :r_realized, :bars_held_10m,
        :exit_trigger, :exit_trigger_type, :notes, :source, NOW()
    )
    ON CONFLICT (session_date, symbol, direction, entry_time) DO UPDATE SET
        contract = EXCLUDED.contract,
        qty = COALESCE(EXCLUDED.qty, {TABLE}.qty),
        exit_time = COALESCE(EXCLUDED.exit_time, {TABLE}.exit_time),
        exit_price = COALESCE(EXCLUDED.exit_price, {TABLE}.exit_price),
        exit_price_intended = COALESCE(EXCLUDED.exit_price_intended, {TABLE}.exit_price_intended),
        slippage_pts = COALESCE(EXCLUDED.slippage_pts, {TABLE}.slippage_pts),
        points_captured = COALESCE(EXCLUDED.points_captured, {TABLE}.points_captured),
        ema10_at_entry = COALESCE(EXCLUDED.ema10_at_entry, {TABLE}.ema10_at_entry),
        ema5_at_entry = COALESCE(EXCLUDED.ema5_at_entry, {TABLE}.ema5_at_entry),
        vwap_at_entry = COALESCE(EXCLUDED.vwap_at_entry, {TABLE}.vwap_at_entry),
        entry_to_ema10_buffer_pct = COALESCE(
            EXCLUDED.entry_to_ema10_buffer_pct, {TABLE}.entry_to_ema10_buffer_pct
        ),
        planned_risk_pts = COALESCE(EXCLUDED.planned_risk_pts, {TABLE}.planned_risk_pts),
        planned_risk_inr = COALESCE(EXCLUDED.planned_risk_inr, {TABLE}.planned_risk_inr),
        confidence_at_entry = COALESCE(EXCLUDED.confidence_at_entry, {TABLE}.confidence_at_entry),
        trade_score_at_entry = COALESCE(EXCLUDED.trade_score_at_entry, {TABLE}.trade_score_at_entry),
        adx_at_entry = COALESCE(EXCLUDED.adx_at_entry, {TABLE}.adx_at_entry),
        confidence_at_exit = COALESCE(EXCLUDED.confidence_at_exit, {TABLE}.confidence_at_exit),
        trade_score_at_exit = COALESCE(EXCLUDED.trade_score_at_exit, {TABLE}.trade_score_at_exit),
        mfe_r = COALESCE(EXCLUDED.mfe_r, {TABLE}.mfe_r),
        mae_r = COALESCE(EXCLUDED.mae_r, {TABLE}.mae_r),
        r_realized = COALESCE(EXCLUDED.r_realized, {TABLE}.r_realized),
        bars_held_10m = COALESCE(EXCLUDED.bars_held_10m, {TABLE}.bars_held_10m),
        exit_trigger = COALESCE(EXCLUDED.exit_trigger, {TABLE}.exit_trigger),
        exit_trigger_type = COALESCE(EXCLUDED.exit_trigger_type, {TABLE}.exit_trigger_type),
        notes = COALESCE(EXCLUDED.notes, {TABLE}.notes),
        source = EXCLUDED.source,
        updated_at = NOW()
    RETURNING id
    """
)


def ensure_trade_log_table() -> None:
    with engine.begin() as conn:
        conn.execute(text(_CREATE_SQL))
        conn.execute(text(_INDEX_SQL))
        # Shadow-only research field — no live gate.
        conn.execute(
            text(
                f"ALTER TABLE {TABLE} "
                "ADD COLUMN IF NOT EXISTS entry_to_ema10_buffer_pct DOUBLE PRECISION"
            )
        )
        conn.execute(
            text(
                f"ALTER TABLE {TABLE} "
                "ADD COLUMN IF NOT EXISTS exit_trigger_type TEXT"
            )
        )


def normalize_exit_trigger_type(val: Any) -> Optional[str]:
    if val is None or val == "":
        return None
    s = str(val).strip().lower().replace("-", "_").replace(" ", "_")
    if s in ("rule", "rule_compliant", "compliant"):
        return "rule_compliant"
    if s in ("discretionary", "manual", "discretion"):
        return "discretionary"
    if s in EXIT_TRIGGER_TYPES:
        return s
    return s  # allow forward-compatible labels; callers should prefer the enum



def compute_entry_to_ema10_buffer_pct(
    entry_price: Optional[float], ema10_at_entry: Optional[float]
) -> Optional[float]:
    """|entry − EMA10| / entry × 100. Shadow logging only — never used to gate."""
    if entry_price is None or ema10_at_entry is None:
        return None
    try:
        ep = float(entry_price)
        e10 = float(ema10_at_entry)
    except (TypeError, ValueError):
        return None
    if ep == 0:
        return None
    return round(abs(ep - e10) / ep * 100.0, 6)


def _as_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return date.fromisoformat(str(val)[:10])


def _as_time(val: Any) -> Optional[time]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.time().replace(microsecond=0)
    if isinstance(val, time):
        return val.replace(microsecond=0)
    s = str(val).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None


def _f(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _points(direction: str, entry: float, exit_px: Optional[float]) -> Optional[float]:
    if exit_px is None:
        return None
    d = (direction or "").upper()
    if d == "SHORT":
        return round(entry - exit_px, 4)
    return round(exit_px - entry, 4)


def row_params(payload: Dict[str, Any]) -> Dict[str, Any]:
    direction = str(payload.get("direction") or "").upper()
    entry = float(payload["entry_price"])
    exit_px = _f(payload.get("exit_price"))
    points = payload.get("points_captured")
    if points is None:
        points = _points(direction, entry, exit_px)
    et = _as_time(payload["entry_time"])
    xt = _as_time(payload.get("exit_time"))
    return {
        "session_date": str(_as_date(payload["session_date"])),
        "symbol": str(payload["symbol"]).strip().upper(),
        "contract": payload.get("contract"),
        "direction": direction,
        "qty": int(payload["qty"]) if payload.get("qty") is not None else None,
        "entry_time": et.strftime("%H:%M:%S") if et else None,
        "entry_price": entry,
        "exit_time": xt.strftime("%H:%M:%S") if xt else None,
        "exit_price": exit_px,
        "exit_price_intended": _f(payload.get("exit_price_intended")),
        "slippage_pts": _f(payload.get("slippage_pts")),
        "points_captured": _f(points),
        "ema10_at_entry": _f(payload.get("ema10_at_entry")),
        "ema5_at_entry": _f(payload.get("ema5_at_entry")),
        "vwap_at_entry": _f(payload.get("vwap_at_entry")),
        "entry_to_ema10_buffer_pct": (
            _f(payload.get("entry_to_ema10_buffer_pct"))
            if payload.get("entry_to_ema10_buffer_pct") is not None
            else compute_entry_to_ema10_buffer_pct(entry, _f(payload.get("ema10_at_entry")))
        ),
        "planned_risk_pts": _f(payload.get("planned_risk_pts")),
        "planned_risk_inr": _f(payload.get("planned_risk_inr")),
        "confidence_at_entry": payload.get("confidence_at_entry") or payload.get("confidence_grade"),
        "trade_score_at_entry": _f(payload.get("trade_score_at_entry")),
        "adx_at_entry": _f(payload.get("adx_at_entry")),
        "confidence_at_exit": payload.get("confidence_at_exit"),
        "trade_score_at_exit": _f(payload.get("trade_score_at_exit")),
        "mfe_r": _f(payload.get("mfe_r")),
        "mae_r": _f(payload.get("mae_r")),
        "r_realized": _f(payload.get("r_realized")),
        "bars_held_10m": int(payload["bars_held_10m"]) if payload.get("bars_held_10m") is not None else None,
        "exit_trigger": payload.get("exit_trigger"),
        "exit_trigger_type": normalize_exit_trigger_type(payload.get("exit_trigger_type")),
        "notes": payload.get("notes"),
        "source": payload.get("source") or "manual",
    }


def upsert_trade(db, payload: Dict[str, Any]) -> int:
    params = row_params(payload)
    rid = db.execute(_UPSERT, params).scalar()
    return int(rid)


def load_from_xlsx(path: str) -> List[Dict[str, Any]]:
    import openpyxl

    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb["Trade Log"]
    rows = list(ws.iter_rows(values_only=True))
    headers = [str(h or "").strip() for h in rows[0]]
    col = {h: i for i, h in enumerate(headers)}
    out: List[Dict[str, Any]] = []
    for r in rows[1:]:
        if not r or not any(c is not None and str(c).strip() for c in r):
            continue
        sym = str(r[col["Symbol"]] or "").strip().upper()
        if not sym:
            continue
        notes = str(r[col["Notes"]] or "").strip()
        # Drop the packed "Extra (no columns yet)" suffix when promoting to DB —
        # structured fields for those trades are supplied separately.
        if " | Extra (no columns yet):" in notes:
            notes = notes.split(" | Extra (no columns yet):", 1)[0].strip()
        out.append(
            {
                "session_date": _as_date(r[col["Date"]]),
                "symbol": sym,
                "direction": str(r[col["Direction"]] or "").strip().upper(),
                "entry_time": _as_time(r[col["Entry Time"]]),
                "entry_price": _f(r[col["Entry Price"]]),
                "exit_time": _as_time(r[col["Exit Time"]]),
                "exit_price": _f(r[col["Exit Price"]]),
                "ema10_at_entry": _f(r[col["Planned SL (EMA10 at entry)"]]),
                "vwap_at_entry": (
                    _f(r[col["VWAP (SL if EMA10 is far)"]])
                    if "VWAP (SL if EMA10 is far)" in col
                    else None
                ),
                "confidence_at_entry": str(r[col["Confidence Grade"]] or "").strip() or None,
                "notes": notes,
                "source": "excel_import",
            }
        )
    return out


# Explicit enriched rows for 21-Jul (overrides excel-only import for these keys).
JUL21_ENRICHED: List[Dict[str, Any]] = [
    {
        "session_date": date(2026, 7, 21),
        "symbol": "HDFCBANK",
        "contract": "Jul 2026 Future",
        "direction": "SHORT",
        "entry_time": time(8, 46),
        "entry_price": 769.90,
        "exit_time": time(12, 6),
        "exit_price": 766.40,
        "points_captured": 3.50,
        "ema10_at_entry": 772.66,
        "vwap_at_entry": 769.52,
        "planned_risk_pts": 0.38,
        "confidence_at_entry": "A+",
        "trade_score_at_entry": 97,
        "confidence_at_exit": "B",
        "trade_score_at_exit": 77,
        "mfe_r": 13.02,
        "mae_r": 0.27,
        "r_realized": 9.2,
        "bars_held_10m": 12,
        "exit_trigger": "Confirmed 10m close above EMA5 (766.23), 1R ratchet engaged",
        "notes": (
            "Clean execution, minimal drawdown. Discretionary urge to widen exit to EMA10 "
            "mid-trade was resisted; EMA5 trail correctly captured bulk of move. "
            "Peak-to-exit give-back (13.02R→9.2R) noted as minor instance for "
            "profit-protection research — not a scratch/round-trip case like "
            "FEDERALBNK/TATAELXSI. Context: 2nd consecutive day of stock-specific "
            "HDFCBANK weakness post-Q1 NIM miss; sector (Nifty Bank/Pvt Bank) was "
            "flat-to-positive same session."
        ),
        "source": "manual_enriched",
    },
    {
        "session_date": date(2026, 7, 21),
        "symbol": "MUTHOOTFIN",
        "contract": "Jul 2026 Future",
        "direction": "LONG",
        "qty": 275,
        "entry_time": time(13, 26),
        "entry_price": 3076.00,
        "exit_time": time(13, 45),
        "exit_price": 3090.60,
        "exit_price_intended": 3093.00,
        "slippage_pts": 2.4,
        "points_captured": 14.6,
        "ema10_at_entry": 3069.00,
        "ema5_at_entry": 3075.00,
        "planned_risk_pts": 7.0,
        "planned_risk_inr": 1925,
        "confidence_at_entry": "A",
        "trade_score_at_entry": 93,
        "adx_at_entry": 45.78,
        "r_realized": 2.09,
        "exit_trigger": "Target Exit",
        "notes": (
            "Clean pullback entry (Pullback #1) on fresh strong leg. 2-candle validation "
            "passed (candle 2 high 3085 vs entry-candle high 3081.8). 1R touched intrabar, "
            "ratchet to EMA5 engaged, but position closed on discretionary target before an "
            "EMA5 close-break occurred. Log as Target Exit per Rule 25 — not a rule-triggered "
            "exit. 2.4-pt slippage on exit — flag for monitoring if pattern recurs on this or "
            "similar-liquidity names."
        ),
        "source": "manual_enriched",
    },
]


def import_excel_and_enriched(xlsx_path: str) -> Dict[str, Any]:
    ensure_trade_log_table()
    excel_rows = load_from_xlsx(xlsx_path)
    # Prefer enriched payloads for Jul-21 keys.
    enrich_keys = {
        (
            r["session_date"],
            r["symbol"],
            r["direction"],
            r["entry_time"].replace(microsecond=0)
            if isinstance(r["entry_time"], time)
            else r["entry_time"],
        )
        for r in JUL21_ENRICHED
    }
    merged: List[Dict[str, Any]] = []
    for r in excel_rows:
        key = (r["session_date"], r["symbol"], r["direction"], r["entry_time"])
        if key in enrich_keys:
            continue  # replaced by enriched
        merged.append(r)
    merged.extend(JUL21_ENRICHED)

    db = SessionLocal()
    inserted = 0
    ids: List[int] = []
    try:
        for r in merged:
            if r.get("entry_price") is None or r.get("entry_time") is None:
                continue
            rid = upsert_trade(db, r)
            ids.append(rid)
            inserted += 1
        db.commit()
        total = db.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
        by_day = [
            dict(x)
            for x in db.execute(
                text(
                    f"""
                    SELECT session_date, COUNT(*) AS n
                    FROM {TABLE}
                    GROUP BY session_date
                    ORDER BY session_date
                    """
                )
            ).mappings()
        ]
    finally:
        db.close()
    return {
        "upserted": inserted,
        "total_rows": int(total or 0),
        "by_session_date": [{**d, "session_date": str(d["session_date"])} for d in by_day],
        "ids_sample": ids[:5],
    }
