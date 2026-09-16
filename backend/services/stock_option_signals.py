"""Stock Options algo: scheduled WR(280) Radar scan, Upstox 2h EMA arm/demote.

Permanent indices NIFTY/BANKNIFTY (arbitrage_master): always on Radar, no WR gate,
arm only on EMA9 cross vs EMA30+EMA100, ~15Δ/~2Δ spreads; Active→Radar demote on hold fail.

Stocks: Radar entry from the 2h WR scan (not ChartInk) — WR > -1 BEAR / WR < -99 BULL.
Radar→Active requires ema_condition_holds(side) AND a fresh 1-candle EMA9 cross matching
that WR side. ChartInk ``/webhook/stockOption`` is logged only (no stock Radar inserts).
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.ist_datetime import naive_ist

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
_MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "add_stock_option_signals.sql"
_CONTRACT_MIGRATION = (
    Path(__file__).resolve().parents[1] / "migrations" / "add_stock_option_contract_mmm_yyyy.sql"
)
_TRADE_MODE_MIGRATION = (
    Path(__file__).resolve().parents[1] / "migrations" / "add_stock_option_trade_mode_datetime.sql"
)
_ENSURED = False

STATUS_RADAR = "Radar"
STATUS_ACTIVE = "Active"
STATUS_EXECUTED = "Executed"
STATUS_COMPLETED = "Completed"
STATUS_REJECTED = "Rejected"
SIDE_BEAR = "BEAR CALL"
SIDE_BULL = "BULL PUT"
TRADE_MODE_PAPER = "PAPER"
TRADE_MODE_LIVE = "LIVE"
INVALIDATE_REMARKS = "Trade not executed for this symbol"
ACTIVE_MAX_HOURS = 72
EXPIRY_REMARKS = "Auto-expired after 72 hours from armed time"

_MONTH_NUM = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_MONTH_ABBR = {v: k for k, v in _MONTH_NUM.items()}

WR_PERIOD = 280
WR_BEAR_GT = -1.0
WR_BULL_LT = -99.0
# EOD Radar cleanup: remove only if WR has moved well off the entry extreme.
WR_EOD_BULL_REMOVE_GT = -5.0  # BULL PUT + WR > -5 → delete Radar
WR_EOD_BEAR_REMOVE_LT = -95.0  # BEAR CALL + WR < -95 → delete Radar
# When hours/2 already has ≥ EMA_SLOW closes, only pull a short hours/1 window for
# session freshness (hours/2 is not in the Upstox intraday-merge set).
EMA_OVERLAY_DAYS_WHEN_READY = 10
WR_CHUNK_DAYS = 60
WR_CHUNK_STEP_DAYS = 50
WR_MAX_CHUNKS = 5
DEMOTE_REMARKS = "Demoted to Radar: EMA hold failed after arm"
SCAN_NAME_WR = "WR280-2h-scan"
DELTA_SELL = 28.0
DELTA_BUY = 18.0
# Permanent index underlyings (arbitrage_master): no WR gate; EMA cross to arm.
INDEX_SYMBOLS = frozenset({"NIFTY", "BANKNIFTY"})
INDEX_PIN_ORDER = ("NIFTY", "BANKNIFTY")
DELTA_SELL_INDEX = 15.0
DELTA_BUY_INDEX = 2.0
EMA_FAST = 9
EMA_MID = 30
EMA_SLOW = 100
# Reject EMA snaps where EMA9 is farther than this fraction from the latest 2h close.
# Catches newest-first / reversed close series (EMA tracks old highs while spot is lower).
EMA_PLAUSIBLE_MAX_REL = 0.12
SESSION_OPEN = dt_time(9, 15)
BAR_MINUTES = 120
FETCH_SLEEP_SEC = 0.4
EMA_FETCH_RETRIES = 3
EMA_FETCH_RETRY_PAUSE_SEC = 1.5

JSON_FIELDS = (
    "stocks",
    "trigger_prices",
    "triggered_at",
    "scan_name",
    "scan_url",
    "alert_name",
    "webhook_url",
    "columns",
)
COLUMN_KEYS = ("symbol",)


def now_ist_second() -> datetime:
    return naive_ist(datetime.now(IST))


def ensure_stock_option_tables() -> None:
    global _ENSURED
    if _ENSURED:
        return
    if not _MIGRATION.is_file():
        raise FileNotFoundError(f"migration missing: {_MIGRATION}")
    sql = _MIGRATION.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
        for stmt in (
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS exit_date TIMESTAMP WITHOUT TIME ZONE",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS sell_exit_price DOUBLE PRECISION",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS buy_exit_price DOUBLE PRECISION",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS realized_pnl DOUBLE PRECISION",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS contract_mmm_yyyy TEXT",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS ema_fetch_ok BOOLEAN",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS trade_mode TEXT NOT NULL DEFAULT 'PAPER'",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS arm_caution BOOLEAN NOT NULL DEFAULT FALSE",
        ):
            conn.execute(text(stmt))
        if _CONTRACT_MIGRATION.is_file():
            conn.execute(text(_CONTRACT_MIGRATION.read_text(encoding="utf-8")))
        if _TRADE_MODE_MIGRATION.is_file():
            conn.execute(text(_TRADE_MODE_MIGRATION.read_text(encoding="utf-8")))
    backfill_contract_mmm_yyyy()
    _ENSURED = True
    logger.info("stock_option tables ensured")


def normalize_trade_mode(value: Any) -> str:
    """PAPER|LIVE. Untagged / unknown / historical → PAPER."""
    s = str(value or "").strip().upper()
    if s == TRADE_MODE_LIVE:
        return TRADE_MODE_LIVE
    return TRADE_MODE_PAPER


def format_contract_mmm_yyyy(expiry: date) -> str:
    """Format FUT expiry as MMM-YYYY (e.g. SEP-2026)."""
    return f"{_MONTH_ABBR[int(expiry.month)]}-{int(expiry.year):04d}"


def parse_fut_trading_symbol_expiry(trading_symbol: Any) -> Optional[date]:
    """Parse 'BAJAJFINSV FUT 29 SEP 26' → date(2026, 9, 29)."""
    parts = str(trading_symbol or "").strip().upper().split()
    if "FUT" not in parts:
        return None
    i = parts.index("FUT")
    if i + 3 >= len(parts):
        return None
    day_s, mon_s, yy_s = parts[i + 1], parts[i + 2], parts[i + 3]
    mon = _MONTH_NUM.get(mon_s)
    if mon is None or not day_s.isdigit() or not yy_s.isdigit():
        return None
    year = 2000 + int(yy_s) if len(yy_s) == 2 else int(yy_s)
    try:
        return date(year, mon, int(day_s))
    except ValueError:
        return None


def expiry_date_from_instrument(inst: Dict[str, Any]) -> Optional[date]:
    exp = inst.get("expiry")
    if exp is not None:
        try:
            return datetime.fromtimestamp(float(exp) / 1000.0, tz=IST).date()
        except (TypeError, ValueError, OSError):
            pass
    return parse_fut_trading_symbol_expiry(inst.get("trading_symbol"))


def _pick_currmth_fut_as_of(
    contracts: Sequence[Dict[str, Any]],
    as_of: datetime,
) -> Optional[Dict[str, Any]]:
    """Front / roll-window currmth FUT as of ``as_of`` (mirrors arbitrage daily setup)."""
    if not contracts:
        return None
    if as_of.tzinfo is None:
        as_of_ist = IST.localize(as_of)
    else:
        as_of_ist = as_of.astimezone(IST)
    as_of_ms = int(as_of_ist.timestamp() * 1000)
    upcoming = [c for c in contracts if int(c.get("expiry") or 0) >= as_of_ms]
    if not upcoming:
        return None
    first = upcoming[0]
    first_exp = expiry_date_from_instrument(first)
    in_roll = (
        first_exp is not None
        and as_of_ist.day > 20
        and (as_of_ist.year, as_of_ist.month) == (first_exp.year, first_exp.month)
        and as_of_ist.date() < first_exp
    )
    if in_roll:
        return upcoming[1] if len(upcoming) >= 2 else None
    return upcoming[0]


def contract_from_arbitrage_master(db: Any, symbol: str) -> Optional[str]:
    """MMM-YYYY from arbitrage_master currmth FUT symbol / instrument key."""
    sym = _norm_symbol(symbol)
    if not sym or db is None:
        return None
    try:
        row = db.execute(
            text(
                """
                SELECT currmth_future_symbol AS tsym,
                       currmth_future_instrument_key AS ikey
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) = :s
                LIMIT 1
                """
            ),
            {"s": sym},
        ).mappings().first()
    except Exception:
        logger.exception("stock_option contract master lookup failed for %s", sym)
        return None
    if not row:
        return None
    exp = parse_fut_trading_symbol_expiry(row.get("tsym"))
    if exp is not None:
        return format_contract_mmm_yyyy(exp)
    ikey = str(row.get("ikey") or "").strip()
    if not ikey:
        return None
    for inst in _fut_contracts_for_symbol(sym):
        if str(inst.get("instrument_key") or "").strip() == ikey:
            exp2 = expiry_date_from_instrument(inst)
            if exp2 is not None:
                return format_contract_mmm_yyyy(exp2)
    return None


def _expected_contract_from_as_of(as_of: datetime) -> str:
    """MMM-YYYY guess from armed_at when historical FUT rows are gone from instruments.

    Mirrors arbitrage roll window: after day 20, treat next calendar month as currmth.
    """
    if as_of.day > 20:
        y, m = as_of.year, as_of.month + 1
        if m > 12:
            y, m = y + 1, 1
        return format_contract_mmm_yyyy(date(y, m, 1))
    return format_contract_mmm_yyyy(date(as_of.year, as_of.month, 1))


def resolve_contract_mmm_yyyy(
    symbol: str,
    armed_at: Any = None,
    db: Any = None,
) -> Optional[str]:
    """Sticky contract label from currmth FUT expiry as of armed_at (or now).

    Prefer instruments-file pick as of armed_at; if that month is no longer in the
    file (rolled off), map armed_at via the day-20 roll heuristic; else
    arbitrage_master currmth.
    """
    armed = _naive_ist(armed_at)
    as_of = armed or now_ist_second()
    contracts = _fut_contracts_for_symbol(symbol)
    picked = _pick_currmth_fut_as_of(contracts, as_of)
    if picked is not None:
        exp = expiry_date_from_instrument(picked)
        if exp is not None:
            # Instruments often only keep upcoming months — for older armed_at the
            # front contract month may already be gone; fall back to as_of mapping.
            if armed is not None and as_of.day <= 20:
                has_asof_month = any(
                    (expiry_date_from_instrument(c) or date.min).year == as_of.year
                    and (expiry_date_from_instrument(c) or date.min).month == as_of.month
                    for c in contracts
                )
                if not has_asof_month and (exp.year, exp.month) > (as_of.year, as_of.month):
                    return _expected_contract_from_as_of(as_of)
            return format_contract_mmm_yyyy(exp)
    label = contract_from_arbitrage_master(db, symbol) if db is not None else None
    if label:
        # Master is always "today's" currmth — only trust for live arm / same month.
        if armed is None or (
            as_of.year == now_ist_second().year and as_of.month == now_ist_second().month
        ):
            return label
    if armed is None:
        return None
    return _expected_contract_from_as_of(as_of)


def backfill_contract_mmm_yyyy(*, overwrite: bool = False) -> int:
    """Fill contract_mmm_yyyy for Active/Executed/Completed rows (armed_at set).

    By default only NULL rows. Pass overwrite=True to recompute (used once after
    improving historical as-of mapping when instruments dropped old months).
    """
    db = SessionLocal()
    updated = 0
    try:
        where_null = "" if overwrite else "AND contract_mmm_yyyy IS NULL"
        rows = db.execute(
            text(
                f"""
                SELECT id, symbol, armed_at, status, contract_mmm_yyyy
                FROM stock_option_signals
                WHERE armed_at IS NOT NULL
                  AND LOWER(TRIM(status)) IN ('active', 'executed', 'completed')
                  {where_null}
                ORDER BY id
                """
            )
        ).mappings().all()
        for row in rows:
            label = resolve_contract_mmm_yyyy(row.get("symbol"), row.get("armed_at"), db=db)
            if not label:
                continue
            if not overwrite and row.get("contract_mmm_yyyy"):
                continue
            if overwrite and row.get("contract_mmm_yyyy") == label:
                continue
            db.execute(
                text(
                    """
                    UPDATE stock_option_signals
                    SET contract_mmm_yyyy = :label, updated_at = :ts
                    WHERE id = :id
                    """
                ),
                {"label": label, "ts": now_ist_second(), "id": row["id"]},
            )
            updated += 1
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("stock_option contract backfill failed")
        return 0
    finally:
        db.close()
    if updated:
        logger.info("stock_option contract backfill: %s rows", updated)
    return updated


def decode_raw_payload(body: bytes) -> Tuple[Any, Dict[str, Any]]:
    text_body = body.decode("utf-8", errors="replace") if body else ""
    if not text_body.strip():
        return None, {}
    try:
        parsed = json.loads(text_body)
        if isinstance(parsed, dict):
            return parsed, parsed
        return parsed, {"_json": parsed}
    except (json.JSONDecodeError, ValueError):
        return None, {"_raw": text_body}


def side_from_williamsr(williamsr: Any) -> Optional[str]:
    """> -1 → BEAR CALL; < -99 → BULL PUT. -1 and -99 themselves do not qualify."""
    try:
        wr = float(williamsr)
    except (TypeError, ValueError):
        return None
    if wr > WR_BEAR_GT:
        return SIDE_BEAR
    if wr < WR_BULL_LT:
        return SIDE_BULL
    return None


def should_insert_new_signal(
    existing_statuses: Sequence[str],
    *,
    block_executed: bool = True,
) -> bool:
    """New Radar only when no open-lifecycle row exists for the symbol.

    By default blocks Radar, Active, and Executed (stock WR scan). Completed /
    Rejected (and empty history) are eligible. Permanent index seeding passes
    ``block_executed=False`` so a prior Executed index trade does not prevent
    re-pinning Radar.
    """
    for raw in existing_statuses or []:
        st = (raw or "").strip().lower()
        if st in ("radar", "active"):
            return False
        if block_executed and st == "executed":
            return False
    return True


def should_remove_radar_eod(side: Any, williamsr: Any) -> bool:
    """EOD cleanup gate for Radar-only rows (not Active / Executed)."""
    try:
        wr = float(williamsr)
    except (TypeError, ValueError):
        return False
    side_n = (str(side or "")).strip().upper()
    if side_n == SIDE_BULL and wr > WR_EOD_BULL_REMOVE_GT:
        return True
    if side_n == SIDE_BEAR and wr < WR_EOD_BEAR_REMOVE_LT:
        return True
    return False


# Per-tick OHLC cache (instrument_key → completed WR bars) so WR scan + EMA share fetches.
_TICK_WR_BARS_CACHE: Dict[str, List[Dict[str, Any]]] = {}


def clear_tick_wr_bars_cache() -> None:
    _TICK_WR_BARS_CACHE.clear()


def cache_tick_wr_bars(instrument_key: str, bars: Sequence[Dict[str, Any]]) -> None:
    ik = (instrument_key or "").strip()
    if not ik or not bars:
        return
    _TICK_WR_BARS_CACHE[ik] = list(bars)


def get_cached_tick_wr_bars(instrument_key: str) -> Optional[List[Dict[str, Any]]]:
    ik = (instrument_key or "").strip()
    if not ik:
        return None
    return _TICK_WR_BARS_CACHE.get(ik)


def _norm_symbol(raw: Any) -> str:
    s = str(raw or "").strip().upper()
    if ":" in s:
        s = s.split(":")[-1].strip()
    for suf in ("-EQ", ".NS", ".BO"):
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.strip()


def is_index_symbol(symbol: Any) -> bool:
    """NIFTY / BANKNIFTY permanent Stock Options treatment (no ChartInk / WR)."""
    return _norm_symbol(symbol) in INDEX_SYMBOLS


def delta_targets_for_symbol(symbol: Any) -> Tuple[float, float]:
    """(sell_delta, buy_delta) targets. Indices use ~15Δ / ~2Δ; stocks ~28Δ / ~18Δ."""
    if is_index_symbol(symbol):
        return DELTA_SELL_INDEX, DELTA_BUY_INDEX
    return DELTA_SELL, DELTA_BUY


def parse_chartink_symbols(parsed: Any) -> Tuple[str, List[Dict[str, Any]], Dict[str, Optional[str]]]:
    """Return (parse_status, candidates, meta). candidates are symbols only.

    ChartInk ``columns[].williamsr`` is ignored. Side comes from Upstox WR(280).
    The raw webhook body (including any williamsr) is stored only on the webhook log.
    """
    meta = {"triggered_at_raw": None, "scan_name": None, "alert_name": None}
    if not isinstance(parsed, dict):
        return "failed", [], meta

    def _s(key: str) -> Optional[str]:
        if key not in parsed or parsed[key] is None:
            return None
        val = str(parsed[key]).strip()
        return val or None

    meta = {
        "triggered_at_raw": _s("triggered_at"),
        "scan_name": _s("scan_name"),
        "alert_name": _s("alert_name"),
    }

    rows: List[Dict[str, Any]] = []
    columns = parsed.get("columns")
    if isinstance(columns, list) and columns:
        for col in columns:
            if not isinstance(col, dict):
                continue
            sym = _norm_symbol(col.get("symbol"))
            if not sym:
                continue
            rows.append({"symbol": sym})
    else:
        stocks = parsed.get("stocks")
        parts: List[str] = []
        if isinstance(stocks, list):
            parts = [_norm_symbol(x) for x in stocks if _norm_symbol(x)]
        elif stocks is not None:
            parts = [_norm_symbol(p) for p in str(stocks).split(",") if _norm_symbol(p)]
        for sym in parts:
            rows.append({"symbol": sym})

    if not rows:
        return "failed", [], meta
    return "success", rows, meta


def ema_series(values: Sequence[float], span: int) -> List[float]:
    if not values or span < 1:
        return []
    k = 2.0 / (float(span) + 1.0)
    out = [float(values[0])]
    for v in values[1:]:
        out.append(float(v) * k + out[-1] * (1.0 - k))
    return out


def ema_last(values: Sequence[float], span: int) -> Optional[float]:
    if not values or len(values) < span:
        return None
    series = ema_series(values, span)
    if not series:
        return None
    return float(series[-1])


def ema_snapshot(closes: Sequence[float]) -> Dict[str, Optional[float]]:
    return {
        "ema9": ema_last(closes, EMA_FAST),
        "ema30": ema_last(closes, EMA_MID),
        "ema100": ema_last(closes, EMA_SLOW),
    }


def ema_plausible_vs_last_close(
    ema9: Any,
    last_close: Any,
    *,
    max_rel: float = EMA_PLAUSIBLE_MAX_REL,
) -> bool:
    """True when EMA9 is within ``max_rel`` of the chronologically last 2h close."""
    try:
        e9 = float(ema9)
        lc = float(last_close)
    except (TypeError, ValueError):
        return False
    if lc == 0.0 or max_rel < 0:
        return False
    return abs(e9 - lc) / abs(lc) <= float(max_rel)


def ema_snapshot_pair(
    closes: Sequence[float],
) -> Optional[Tuple[Dict[str, float], Dict[str, float]]]:
    """(prev, curr) EMA snaps on the last two completed bars. Needs ≥ EMA100+1 closes."""
    if len(closes) < EMA_SLOW + 1:
        return None
    prev_raw = ema_snapshot(closes[:-1])
    curr_raw = ema_snapshot(closes)
    if any(prev_raw[k] is None or curr_raw[k] is None for k in ("ema9", "ema30", "ema100")):
        return None
    prev = {k: float(prev_raw[k]) for k in ("ema9", "ema30", "ema100")}
    curr = {k: float(curr_raw[k]) for k in ("ema9", "ema30", "ema100")}
    return prev, curr


def ema_condition_holds(side: Optional[str], ema9: Any, ema30: Any, ema100: Any) -> bool:
    try:
        e9 = float(ema9)
        e30 = float(ema30)
        e100 = float(ema100)
    except (TypeError, ValueError):
        return False
    if side == SIDE_BEAR:
        return e9 < e30 and e9 < e100
    if side == SIDE_BULL:
        return e9 > e30 and e9 > e100
    return False


def detect_ema_cross_side(
    prev_ema9: Any,
    prev_ema30: Any,
    prev_ema100: Any,
    curr_ema9: Any,
    curr_ema30: Any,
    curr_ema100: Any,
) -> Optional[str]:
    """EMA9 cross vs EMA30+EMA100 on consecutive completed 2h bars (1-candle lookback).

    Finish must be strictly below both (BEAR) or above both (BULL) — EMA9 must not
    sit between EMA30 and EMA100 on the current bar.

    BULL PUT: previous bar EMA9 was *not* above both; current is *strictly* above both.
    BEAR CALL: previous bar EMA9 was *not* below both; current is *strictly* below both.
    Hold already true on the previous bar does not qualify as a fresh cross.
    """
    try:
        p9, p30, p100 = float(prev_ema9), float(prev_ema30), float(prev_ema100)
        c9, c30, c100 = float(curr_ema9), float(curr_ema30), float(curr_ema100)
    except (TypeError, ValueError):
        return None
    bull_now = c9 > c30 and c9 > c100
    bull_prev = p9 > p30 and p9 > p100
    if bull_now and not bull_prev:
        return SIDE_BULL
    bear_now = c9 < c30 and c9 < c100
    bear_prev = p9 < p30 and p9 < p100
    if bear_now and not bear_prev:
        return SIDE_BEAR
    return None


def next_stock_ema_action(
    status: str,
    side: Optional[str],
    prev_ema9: Any,
    prev_ema30: Any,
    prev_ema100: Any,
    curr_ema9: Any,
    curr_ema30: Any,
    curr_ema100: Any,
    trade_submitted: bool,
) -> str:
    """Stock path: arm | demote | hold.

    Radar→Active only when ``ema_condition_holds(side)`` on the latest bar **and**
    a fresh 1-candle cross matches the existing WR-derived ``side`` (no reverse flip).
    Active (not submitted) → demote to Radar when hold fails.
    Incomplete EMAs never arm or demote.
    """
    st = (status or "").strip().lower()
    incomplete = any(
        v is None
        for v in (
            prev_ema9,
            prev_ema30,
            prev_ema100,
            curr_ema9,
            curr_ema30,
            curr_ema100,
        )
    )
    if incomplete:
        return "hold"
    holds = ema_condition_holds(side, curr_ema9, curr_ema30, curr_ema100)
    if st == "radar":
        if not holds or not side:
            return "hold"
        crossed = detect_ema_cross_side(
            prev_ema9, prev_ema30, prev_ema100, curr_ema9, curr_ema30, curr_ema100
        )
        if crossed == side:
            return "arm"
        return "hold"
    if st == "active":
        if trade_submitted:
            return "hold"
        if not holds:
            return "demote"
        return "hold"
    return "hold"


def next_ema_action(
    status: str,
    side: Optional[str],
    ema9: Any,
    ema30: Any,
    ema100: Any,
    trade_submitted: bool,
    *,
    prev_ema9: Any = None,
    prev_ema30: Any = None,
    prev_ema100: Any = None,
) -> str:
    """Compat wrapper: prefer ``next_stock_ema_action`` with prev+curr snaps.

    Without prev snaps, Radar never arms (hold-only is insufficient); Active still
    demotes when the current bar fails ``ema_condition_holds``.
    """
    if prev_ema9 is None or prev_ema30 is None or prev_ema100 is None:
        st = (status or "").strip().lower()
        incomplete = ema9 is None or ema30 is None or ema100 is None
        if incomplete:
            return "hold"
        if st == "active" and not trade_submitted:
            if not ema_condition_holds(side, ema9, ema30, ema100):
                return "demote"
            return "hold"
        return "hold"
    return next_stock_ema_action(
        status,
        side,
        prev_ema9,
        prev_ema30,
        prev_ema100,
        ema9,
        ema30,
        ema100,
        trade_submitted,
    )


def next_index_ema_action(
    status: str,
    side: Optional[str],
    prev_ema9: Any,
    prev_ema30: Any,
    prev_ema100: Any,
    curr_ema9: Any,
    curr_ema30: Any,
    curr_ema100: Any,
    trade_submitted: bool,
) -> Tuple[str, Optional[str]]:
    """Index path: Radar arms only on EMA *cross*; Active demotes when hold fails.

    Returns ``(action, side_on_arm)``. ``side_on_arm`` is set when arming from Radar
    (may be blank before arm). Incomplete prev/curr EMAs → hold.
    """
    st = (status or "").strip().lower()
    incomplete = any(
        v is None
        for v in (
            prev_ema9,
            prev_ema30,
            prev_ema100,
            curr_ema9,
            curr_ema30,
            curr_ema100,
        )
    )
    if incomplete:
        return "hold", None
    if st == "radar":
        crossed = detect_ema_cross_side(
            prev_ema9, prev_ema30, prev_ema100, curr_ema9, curr_ema30, curr_ema100
        )
        if crossed:
            return "arm", crossed
        return "hold", None
    if st == "active":
        if trade_submitted:
            return "hold", None
        if not ema_condition_holds(side, curr_ema9, curr_ema30, curr_ema100):
            return "demote", None
        return "hold", None
    return "hold", None


def _naive_ist(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(IST).replace(tzinfo=None)
        return value
    return None


def active_past_max_age(
    armed_at: Any,
    now: datetime,
    hours: int = ACTIVE_MAX_HOURS,
) -> bool:
    """True once Active has been armed for 72 hours or longer."""
    armed = _naive_ist(armed_at)
    if armed is None:
        return False
    current = _naive_ist(now)
    if current is None:
        return False
    return (current - armed) >= timedelta(hours=hours)


def expiry_remarks(existing: Any) -> str:
    note = (existing or "").strip()
    if not note:
        return EXPIRY_REMARKS
    if EXPIRY_REMARKS in note:
        return note
    return f"{note} | {EXPIRY_REMARKS}"


def invalidate_outcome(
    armed_at: Any,
    sell_strike: Any,
    buy_strike: Any,
) -> str:
    """EMA-invalidated before Trade: keep Executed only when arm date and strikes exist.

    Strikes are never invented. Missing either field → Rejected (hidden from tabs).
    """
    if armed_at is None or sell_strike is None or buy_strike is None:
        return STATUS_REJECTED
    return STATUS_EXECUTED


def trade_is_submitted(date_traded: Any, sell_cost: Any = None, buy_cost: Any = None) -> bool:
    if date_traded is not None and str(date_traded).strip() != "":
        return True
    return sell_cost is not None and buy_cost is not None


def _delta_points(raw: Any) -> Optional[float]:
    try:
        d = abs(float(raw))
    except (TypeError, ValueError):
        return None
    if d <= 1.5:
        return d * 100.0
    return d


def _as_float(raw: Any) -> Optional[float]:
    try:
        if raw is None or str(raw).strip() == "":
            return None
        return float(raw)
    except (TypeError, ValueError):
        return None


def _leg_from_option_node(node: Any, strike: float) -> Optional[Dict[str, Any]]:
    if isinstance(node, list):
        node = node[0] if node and isinstance(node[0], dict) else None
    if not isinstance(node, dict):
        return None
    md = node.get("market_data") if isinstance(node.get("market_data"), dict) else {}
    greeks = node.get("option_greeks") or node.get("greeks") or {}
    if not isinstance(greeks, dict):
        greeks = {}
    delta = greeks.get("delta")
    if delta is None:
        delta = node.get("delta", md.get("delta") if isinstance(md, dict) else None)
    pts = _delta_points(delta)
    if pts is None:
        return None
    vol = 0.0
    if isinstance(md, dict):
        vol = _as_float(md.get("volume") or md.get("volume_traded") or md.get("oi")) or 0.0
    ik = node.get("instrument_key")
    return {
        "strike": float(strike),
        "delta_pts": pts,
        "volume": float(vol),
        "instrument_key": str(ik).strip() if ik else None,
    }


def iter_chain_strikes(chain: Any) -> List[Dict[str, Any]]:
    if chain is None:
        return []
    if isinstance(chain, list):
        return [x for x in chain if isinstance(x, dict)]
    if isinstance(chain, dict):
        for key in ("data", "strikes"):
            val = chain.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
            if isinstance(val, dict):
                inner = val.get("strikes") or val.get("data")
                if isinstance(inner, list):
                    return [x for x in inner if isinstance(x, dict)]
    return []


def legs_from_chain(chain: Any, call: bool) -> List[Dict[str, Any]]:
    key = "call_options" if call else "put_options"
    out: List[Dict[str, Any]] = []
    for row in iter_chain_strikes(chain):
        strike = _as_float(row.get("strike_price") or row.get("strike"))
        if strike is None:
            continue
        leg = _leg_from_option_node(row.get(key), strike)
        if leg:
            out.append(leg)
    return out


def instrument_key_at_strike(chain: Any, side: Optional[str], strike: Any) -> Optional[str]:
    """Exact-strike instrument_key from option chain (no greeks required)."""
    target = _as_float(strike)
    if target is None or side not in (SIDE_BEAR, SIDE_BULL):
        return None
    node_key = "call_options" if side == SIDE_BEAR else "put_options"
    for row in iter_chain_strikes(chain):
        s = _as_float(row.get("strike_price") or row.get("strike"))
        if s is None or abs(s - target) > 1e-6:
            continue
        node = row.get(node_key)
        if isinstance(node, list):
            node = node[0] if node and isinstance(node[0], dict) else None
        if not isinstance(node, dict):
            return None
        ik = node.get("instrument_key")
        if ik:
            return str(ik).strip()
        return None
    return None


def ensure_executed_option_instrument_keys(
    signal_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Resolve sell/buy instrument keys for filled Executed rows from user strikes.

    Re-fetches the option chain when keys are missing or when user strikes differ
    from the algo-suggested strikes (whose keys were stored at arm time).
    """
    ensure_stock_option_tables()
    db = SessionLocal()
    try:
        params: Dict[str, Any] = {
            "executed": STATUS_EXECUTED,
            "remarks": INVALIDATE_REMARKS,
        }
        id_clause = ""
        if signal_id is not None:
            id_clause = "AND id = :id"
            params["id"] = int(signal_id)
        rows = db.execute(
            text(
                f"""
                SELECT id, symbol, side,
                       sell_strike, buy_strike,
                       user_sell_strike, user_buy_strike,
                       sell_instrument_key, buy_instrument_key,
                       remarks
                FROM stock_option_signals
                WHERE status = :executed
                  AND date_traded IS NOT NULL
                  AND sell_cost IS NOT NULL
                  AND buy_cost IS NOT NULL
                  AND COALESCE(user_sell_strike, sell_strike) IS NOT NULL
                  AND COALESCE(user_buy_strike, buy_strike) IS NOT NULL
                  AND remarks IS DISTINCT FROM :remarks
                  {id_clause}
                ORDER BY id
                """
            ),
            params,
        ).mappings().all()
    finally:
        db.close()

    updated = 0
    cleared_remarks = 0
    for row in rows:
        sell_s = _as_float(row.get("user_sell_strike"))
        if sell_s is None:
            sell_s = _as_float(row.get("sell_strike"))
        buy_s = _as_float(row.get("user_buy_strike"))
        if buy_s is None:
            buy_s = _as_float(row.get("buy_strike"))
        if sell_s is None or buy_s is None:
            continue

        algo_sell = _as_float(row.get("sell_strike"))
        algo_buy = _as_float(row.get("buy_strike"))
        sell_key = str(row.get("sell_instrument_key") or "").strip() or None
        buy_key = str(row.get("buy_instrument_key") or "").strip() or None
        sell_mismatch = (
            algo_sell is not None and abs(algo_sell - sell_s) > 1e-6
        ) or not sell_key
        buy_mismatch = (
            algo_buy is not None and abs(algo_buy - buy_s) > 1e-6
        ) or not buy_key
        # Keep keys when user strikes match algo and keys already present.
        need_chain = sell_mismatch or buy_mismatch
        note = str(row.get("remarks") or "")
        clear_expiry = EXPIRY_REMARKS in note

        new_sell, new_buy = sell_key, buy_key
        if need_chain:
            try:
                chain = _option_chain_for_symbol(str(row.get("symbol") or ""))
            except Exception as e:
                logger.info(
                    "stock_option key resolve chain failed id=%s %s: %s",
                    row.get("id"),
                    row.get("symbol"),
                    e,
                )
                chain = None
            if chain is not None:
                if sell_mismatch:
                    resolved = instrument_key_at_strike(chain, row.get("side"), sell_s)
                    if resolved:
                        new_sell = resolved
                if buy_mismatch:
                    resolved = instrument_key_at_strike(chain, row.get("side"), buy_s)
                    if resolved:
                        new_buy = resolved

        if (
            new_sell == sell_key
            and new_buy == buy_key
            and not clear_expiry
        ):
            continue

        db = SessionLocal()
        try:
            db.execute(
                text(
                    """
                    UPDATE stock_option_signals
                    SET sell_instrument_key = COALESCE(:sell_ik, sell_instrument_key),
                        buy_instrument_key = COALESCE(:buy_ik, buy_instrument_key),
                        remarks = CASE
                            WHEN remarks LIKE :expiry_like THEN NULL
                            ELSE remarks
                        END,
                        updated_at = :ts
                    WHERE id = :id AND status = :executed
                    """
                ),
                {
                    "sell_ik": new_sell,
                    "buy_ik": new_buy,
                    "expiry_like": f"%{EXPIRY_REMARKS}%",
                    "ts": now_ist_second(),
                    "id": int(row["id"]),
                    "executed": STATUS_EXECUTED,
                },
            )
            db.commit()
            updated += 1
            if clear_expiry:
                cleared_remarks += 1
        except Exception:
            db.rollback()
            logger.exception(
                "stock_option key persist failed id=%s", row.get("id")
            )
        finally:
            db.close()

    out = {"ok": True, "rows": len(rows), "updated": updated, "cleared_expiry_remarks": cleared_remarks}
    if updated:
        logger.info("stock_option ensure option keys: %s", out)
    return out


def pick_nearest_delta(legs: Sequence[Dict[str, Any]], target: float, exclude_strike: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """Nearest |delta| to target. If several are within 2 points, prefer higher volume."""
    pool: List[Tuple[float, float, Dict[str, Any]]] = []
    for leg in legs:
        if exclude_strike is not None and abs(float(leg["strike"]) - float(exclude_strike)) < 1e-6:
            continue
        pts = leg.get("delta_pts")
        if pts is None:
            continue
        dist = abs(float(pts) - float(target))
        pool.append((dist, float(leg.get("volume") or 0.0), leg))
    if not pool:
        return None
    near = [x for x in pool if x[0] <= 2.0]
    if near:
        near.sort(key=lambda x: (-x[1], x[0]))
        return near[0][2]
    pool.sort(key=lambda x: (x[0], -x[1]))
    return pool[0][2]


def pick_credit_spread(
    chain: Any,
    side: str,
    *,
    sell_target: float = DELTA_SELL,
    buy_target: float = DELTA_BUY,
) -> Optional[Dict[str, Any]]:
    call = side == SIDE_BEAR
    if side not in (SIDE_BEAR, SIDE_BULL):
        return None
    legs = legs_from_chain(chain, call=call)
    if not legs:
        return None
    sell = pick_nearest_delta(legs, sell_target)
    if not sell:
        return None
    buy = pick_nearest_delta(legs, buy_target, exclude_strike=sell["strike"])
    if not buy:
        return None
    return {
        "sell_strike": sell["strike"],
        "sell_delta": sell["delta_pts"],
        "sell_instrument_key": sell.get("instrument_key"),
        "buy_strike": buy["strike"],
        "buy_delta": buy["delta_pts"],
        "buy_instrument_key": buy.get("instrument_key"),
    }


def option_code(side: Optional[str]) -> Optional[str]:
    if side == SIDE_BEAR:
        return "CE"
    if side == SIDE_BULL:
        return "PE"
    return None


def fmt_strike(value: Any) -> str:
    v = _as_float(value)
    if v is None:
        return ""
    if abs(v - round(v)) < 1e-6:
        return str(int(round(v)))
    return f"{v:.2f}".rstrip("0").rstrip(".")


def spread_lines(
    side: Optional[str],
    sell_strike: Any,
    buy_strike: Any,
    *,
    symbol: Any = None,
    sell_target: Optional[float] = None,
    buy_target: Optional[float] = None,
) -> Tuple[Optional[str], Optional[str]]:
    code = option_code(side)
    if not code or sell_strike is None or buy_strike is None:
        return None, None
    if sell_target is None or buy_target is None:
        sell_target, buy_target = delta_targets_for_symbol(symbol)
    sell_label = int(round(float(sell_target)))
    buy_label = int(round(float(buy_target)))
    return (
        f"Sell {code}:~{sell_label} Δ - {fmt_strike(sell_strike)}",
        f"Buy {code}~{buy_label} Δ {fmt_strike(buy_strike)}",
    )


def combined_pnl(sell_cost: Any, buy_cost: Any, sell_ltp: Any, buy_ltp: Any) -> Optional[float]:
    """Mark / realized credit-spread P&L in premium points (× lot elsewhere for ₹).

    Equivalent leg-wise form:
      buy  = (buy_ltp − buy_entry)
      sell = (sell_entry − sell_ltp)
      pnl  = buy + sell

    Same as: sell_entry − buy_entry − (sell_mark − buy_mark).
    """
    sc = _as_float(sell_cost)
    bc = _as_float(buy_cost)
    sl = _as_float(sell_ltp)
    bl = _as_float(buy_ltp)
    if sc is None or bc is None or sl is None or bl is None:
        return None
    # buy_leg + sell_leg == credit − close_cost
    return (bl - bc) + (sc - sl)


def realized_credit_pnl(
    sell_entry: Any,
    buy_entry: Any,
    sell_exit: Any,
    buy_exit: Any,
) -> Optional[float]:
    """Credit-spread P&L in premium points. Hard stop is not part of this.

    net_credit_at_entry = sell_entry − buy_entry
    cost_to_close = sell_exit − buy_exit
    pnl = net_credit_at_entry − cost_to_close
        = sell_entry − buy_entry − (sell_exit − buy_exit)

    Hard stop stays 3 × sell entry (hard_stop_price); exit prices do not change it.
    """
    return combined_pnl(sell_entry, buy_entry, sell_exit, buy_exit)


def pnl_rupees(points: Any, lot_size: Any) -> Optional[float]:
    """Premium points × option lot. Missing or non-positive lot is not treated as 1."""
    pts = _as_float(points)
    if pts is None:
        return None
    try:
        lot = int(float(lot_size))
    except (TypeError, ValueError):
        return None
    if lot <= 0:
        return None
    return round(pts * lot, 2)


def _positive_lot(value: Any) -> Optional[int]:
    try:
        n = int(float(value))
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


_LOT_CACHE: Tuple[Dict[str, int], Dict[str, int]] = ({}, {})
_LOT_CACHE_AT = 0.0
_LOT_CACHE_TTL = 600.0
_FUT_BY_UND: Dict[str, List[Dict[str, Any]]] = {}
_FUT_BY_UND_AT = 0.0
_FUT_BY_UND_TTL = 600.0


def _fut_contracts_index() -> Dict[str, List[Dict[str, Any]]]:
    """underlying → NSE_FO FUT instrument rows. Cached 10 minutes."""
    global _FUT_BY_UND, _FUT_BY_UND_AT
    now = time.monotonic()
    if _FUT_BY_UND and (now - _FUT_BY_UND_AT) < _FUT_BY_UND_TTL:
        return _FUT_BY_UND
    from backend.config import get_instruments_file_path

    path = get_instruments_file_path()
    out: Dict[str, List[Dict[str, Any]]] = {}
    if not path.is_file():
        _FUT_BY_UND = {}
        _FUT_BY_UND_AT = now
        return _FUT_BY_UND
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("stock_option contract: instruments unreadable")
        return _FUT_BY_UND or {}
    if not isinstance(data, list):
        return {}
    for inst in data:
        if not isinstance(inst, dict):
            continue
        if str(inst.get("segment") or "").upper() != "NSE_FO":
            continue
        if str(inst.get("instrument_type") or "").upper() != "FUT":
            continue
        if expiry_date_from_instrument(inst) is None:
            continue
        und = _norm_symbol(inst.get("underlying_symbol") or "")
        if not und:
            tsym = str(inst.get("trading_symbol") or "")
            if " FUT " in tsym.upper():
                und = _norm_symbol(tsym.split(" FUT ", 1)[0])
        if not und:
            continue
        out.setdefault(und, []).append(inst)
    for und in out:
        out[und].sort(key=lambda x: int(x.get("expiry") or 0))
    _FUT_BY_UND = out
    _FUT_BY_UND_AT = now
    return out


def _fut_contracts_for_symbol(symbol: str) -> List[Dict[str, Any]]:
    """NSE_FO FUT rows for underlying from instruments file."""
    sym = _norm_symbol(symbol)
    if not sym:
        return []
    return list(_fut_contracts_index().get(sym) or [])


def _instrument_lot_maps() -> Tuple[Dict[str, int], Dict[str, int]]:
    """Upstox instrument file: key → lot, and underlying → CE/PE/FUT lot. Cached 10 minutes."""
    global _LOT_CACHE, _LOT_CACHE_AT
    now = time.monotonic()
    if _LOT_CACHE[0] and (now - _LOT_CACHE_AT) < _LOT_CACHE_TTL:
        return _LOT_CACHE
    from backend.config import get_instruments_file_path

    path = get_instruments_file_path()
    if not path.is_file():
        logger.warning("stock_option lot lookup: instruments file missing at %s", path)
        return {}, {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("stock_option lot lookup: instruments file unreadable")
        return {}, {}
    if not isinstance(data, list):
        return {}, {}
    by_key: Dict[str, int] = {}
    by_und: Dict[str, int] = {}
    for inst in data:
        if not isinstance(inst, dict):
            continue
        lot = _positive_lot(inst.get("lot_size") or inst.get("lotSize"))
        if not lot:
            continue
        ik = str(inst.get("instrument_key") or "").strip()
        if ik:
            by_key[ik] = lot
        kind = str(inst.get("instrument_type") or "").strip().upper()
        und = _norm_symbol(inst.get("underlying_symbol") or "")
        if und and kind in ("CE", "PE", "FUT") and und not in by_und:
            by_und[und] = lot
    _LOT_CACHE = (by_key, by_und)
    _LOT_CACHE_AT = now
    return by_key, by_und


def lookup_option_lots(
    db,
    pairs: Sequence[Tuple[str, Optional[str]]],
) -> Dict[str, int]:
    """Map symbol → option lot. Prefer sell-leg option key, else FUT lot from arbitrage_master.

    Never substitutes 1. Missing symbols are omitted.
    """
    wanted: Dict[str, Optional[str]] = {}
    for raw_sym, option_key in pairs:
        sym = _norm_symbol(raw_sym)
        if not sym:
            continue
        prev = wanted.get(sym)
        if prev:
            continue
        wanted[sym] = str(option_key).strip() if option_key else None
    if not wanted:
        return {}
    try:
        lots, by_und = _instrument_lot_maps()
        placeholders = ", ".join(f":s{i}" for i in range(len(wanted)))
        params = {f"s{i}": sym for i, sym in enumerate(wanted)}
        fut_rows = db.execute(
            text(
                f"""
                SELECT UPPER(TRIM(stock)) AS stock, currmth_future_instrument_key AS ikey
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) IN ({placeholders})
                """
            ),
            params,
        ).mappings().all()
    except Exception:
        logger.exception("stock_option lot lookup failed")
        return {}
    fut_by_sym = {
        _norm_symbol(r.get("stock")): str(r.get("ikey") or "").strip()
        for r in fut_rows
        if _norm_symbol(r.get("stock"))
    }
    found: Dict[str, int] = {}
    for sym, option_key in wanted.items():
        lot = _positive_lot(lots.get(str(option_key or "").strip()))
        if lot is None:
            lot = _positive_lot(lots.get(fut_by_sym.get(sym) or ""))
        if lot is None:
            lot = _positive_lot(by_und.get(sym))
        if lot is not None:
            found[sym] = lot
    return found


def attach_rupee_pnl(item: Dict[str, Any], lot_size: Optional[int]) -> None:
    """Set lot_size and combined_pnl_inr. combined_pnl stays premium points."""
    lot = _positive_lot(lot_size)
    item["lot_size"] = lot
    item["combined_pnl_inr"] = pnl_rupees(item.get("combined_pnl"), lot)


def hard_stop_price(sell_cost: Any) -> Optional[float]:
    sc = _as_float(sell_cost)
    if sc is None:
        return None
    return 3.0 * sc


def _parse_candle_ts(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    if isinstance(ts, (int, float)):
        v = float(ts)
        if v > 1_000_000_000_000:
            v /= 1000.0
        return datetime.fromtimestamp(v, tz=IST)
    s = str(ts).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00").replace(" ", "T"))
        return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    except ValueError:
        return None


def session_2h_bucket_start(ts: datetime) -> Optional[datetime]:
    """2h bucket start aligned to 09:15 IST (Upstox hours/2).

    Four cash-session buckets: 09:15, 11:15, 13:15, and the short final
    15:15–15:30 bar. None outside 09:15–15:30 IST.
    """
    local = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    start = local.replace(hour=9, minute=15, second=0, microsecond=0)
    if local < start:
        return None
    elapsed_min = (local - start).total_seconds() / 60.0
    # Session ends 15:30 = 6h15m after 09:15.
    if elapsed_min >= 6 * 60 + 15:
        return None
    idx = int(elapsed_min // BAR_MINUTES)
    if idx < 0 or idx > 3:
        return None
    return start + timedelta(minutes=idx * BAR_MINUTES)


def session_2h_bucket_end(bucket: datetime) -> datetime:
    """Bucket close time: +120m, except the final 15:15 bar which ends at 15:30."""
    local = bucket.astimezone(IST) if bucket.tzinfo else IST.localize(bucket)
    if (local.hour, local.minute) == (15, 15):
        return local.replace(hour=15, minute=30, second=0, microsecond=0)
    return local + timedelta(minutes=BAR_MINUTES)


def completed_2h_bars(candles: Sequence[Dict[str, Any]], now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Keep bars whose 2h bucket has completed (bucket end <= now)."""
    now_ist = now if now is not None else datetime.now(IST)
    if now_ist.tzinfo is None:
        now_ist = IST.localize(now_ist)
    else:
        now_ist = now_ist.astimezone(IST)
    grouped: Dict[datetime, Dict[str, Any]] = {}
    for c in candles or []:
        ts = _parse_candle_ts(c.get("timestamp"))
        close = _as_float(c.get("close"))
        if ts is None or close is None:
            continue
        bucket = session_2h_bucket_start(ts)
        if bucket is None:
            continue
        if now_ist < session_2h_bucket_end(bucket):
            continue
        rec = grouped.get(bucket)
        if rec is None or ts >= rec["_ts"]:
            grouped[bucket] = {
                "timestamp": bucket.isoformat(),
                "close": close,
                "open": _as_float(c.get("open")),
                "high": _as_float(c.get("high")),
                "low": _as_float(c.get("low")),
                "volume": _as_float(c.get("volume")) or 0.0,
                "_ts": ts,
            }
    out = []
    for bucket in sorted(grouped):
        rec = grouped[bucket]
        rec.pop("_ts", None)
        out.append(rec)
    return out


def aggregate_intraday_to_2h(candles: Sequence[Dict[str, Any]], now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Aggregate finer bars (1h / 15m) into completed 09:15-aligned 2h candles."""
    now_ist = now if now is not None else datetime.now(IST)
    if now_ist.tzinfo is None:
        now_ist = IST.localize(now_ist)
    else:
        now_ist = now_ist.astimezone(IST)
    buckets: Dict[datetime, List[Dict[str, Any]]] = {}
    for c in candles or []:
        ts = _parse_candle_ts(c.get("timestamp"))
        close = _as_float(c.get("close"))
        if ts is None or close is None:
            continue
        bucket = session_2h_bucket_start(ts)
        if bucket is None:
            continue
        if now_ist < session_2h_bucket_end(bucket):
            continue
        buckets.setdefault(bucket, []).append(
            {
                "ts": ts,
                "open": _as_float(c.get("open")),
                "high": _as_float(c.get("high")),
                "low": _as_float(c.get("low")),
                "close": close,
                "volume": _as_float(c.get("volume")) or 0.0,
            }
        )
    out: List[Dict[str, Any]] = []
    for bucket in sorted(buckets):
        parts = sorted(buckets[bucket], key=lambda x: x["ts"])
        if not parts:
            continue
        highs = [p["high"] for p in parts if p["high"] is not None]
        lows = [p["low"] for p in parts if p["low"] is not None]
        out.append(
            {
                "timestamp": bucket.isoformat(),
                "open": parts[0]["open"] if parts[0]["open"] is not None else parts[0]["close"],
                "high": max(highs) if highs else parts[-1]["close"],
                "low": min(lows) if lows else parts[-1]["close"],
                "close": parts[-1]["close"],
                "volume": sum(p["volume"] for p in parts),
            }
        )
    return out


def completed_2h_ohlc(raw: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Completed 2h candles as {end, high, low, close}. end = bucket close (15:30 for 15:15)."""
    out: List[Dict[str, Any]] = []
    for bar in raw or []:
        ts = _parse_candle_ts(bar.get("timestamp"))
        high = bar.get("high")
        low = bar.get("low")
        close = bar.get("close")
        if ts is None or high is None or low is None or close is None:
            continue
        try:
            h, lo, c = float(high), float(low), float(close)
        except (TypeError, ValueError):
            continue
        out.append({"end": naive_ist(session_2h_bucket_end(ts)), "high": h, "low": lo, "close": c})
    out.sort(key=lambda b: b["end"])
    dedup: Dict[datetime, Dict[str, Any]] = {}
    for b in out:
        dedup[b["end"]] = b
    return [dedup[k] for k in sorted(dedup)]


def williams_r_at(
    bars: Sequence[Dict[str, Any]],
    asof: datetime,
    period: int = WR_PERIOD,
) -> Optional[float]:
    """WR(period) of the last completed bar with end <= asof.

    (Highest High(period) - Close) / (Highest High(period) - Lowest Low(period)) * -100
    """
    asof_n = naive_ist(asof)
    idx = -1
    for i, bar in enumerate(bars):
        end = bar.get("end")
        if not isinstance(end, datetime):
            continue
        if naive_ist(end) <= asof_n:
            idx = i
        else:
            break
    if idx < period - 1:
        return None
    window = bars[idx - period + 1 : idx + 1]
    if len(window) < period:
        return None
    hh = max(b["high"] for b in window)
    ll = min(b["low"] for b in window)
    if hh <= ll:
        return None
    close = window[-1]["close"]
    return (hh - close) / (hh - ll) * -100.0


def fetch_completed_2h_ohlc(instrument_key: str, asof: datetime) -> List[Dict[str, Any]]:
    """Completed 2h OHLC for WR(280). Prefer hours/2 chunks; early-stop at ≥280 bars.

    Typical call volume: **2–3** ``hours/2`` requests (60d windows stepped
    ``WR_CHUNK_STEP_DAYS``) when history is dense; at most ``WR_MAX_CHUNKS``.
    Falls back to ``hours/1`` chunks only if hours/2 stays short. Reuses the
    per-tick cache when present. Throttles with ``FETCH_SLEEP_SEC``.
    """
    cached = get_cached_tick_wr_bars(instrument_key)
    if cached is not None and len(cached) >= WR_PERIOD:
        return cached

    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    asof_n = naive_ist(asof)
    asof_d = asof_n.date()
    ends = [asof_d - timedelta(days=WR_CHUNK_STEP_DAYS * i) for i in range(WR_MAX_CHUNKS)]

    def _merge_chunks(interval: str) -> List[Dict[str, Any]]:
        by_ts: Dict[str, Dict[str, Any]] = {}
        for end_d in ends:
            raw = _upstox_candles_with_retry(
                u,
                instrument_key,
                interval=interval,
                days_back=WR_CHUNK_DAYS,
                range_end_date=end_d if end_d < asof_d else None,
            )
            for c in raw or []:
                key = str(c.get("timestamp") or "")
                if key:
                    by_ts[key] = c
            time.sleep(FETCH_SLEEP_SEC)
            merged_raw = [by_ts[k] for k in sorted(by_ts)]
            if interval == "hours/2":
                agg = completed_2h_bars(merged_raw, now=IST.localize(asof_n))
            else:
                agg = aggregate_intraday_to_2h(merged_raw, now=IST.localize(asof_n))
            ohlc = completed_2h_ohlc(agg)
            if len(ohlc) >= WR_PERIOD:
                return ohlc
        merged_raw = [by_ts[k] for k in sorted(by_ts)]
        if interval == "hours/2":
            agg = completed_2h_bars(merged_raw, now=IST.localize(asof_n))
        else:
            agg = aggregate_intraday_to_2h(merged_raw, now=IST.localize(asof_n))
        return completed_2h_ohlc(agg)

    ohlc = _merge_chunks("hours/2")
    if len(ohlc) < WR_PERIOD:
        logger.info(
            "stock_option WR hours/2 short for %s bars=%s; trying hours/1",
            instrument_key,
            len(ohlc),
        )
        ohlc_h1 = _merge_chunks("hours/1")
        if len(ohlc_h1) > len(ohlc):
            ohlc = ohlc_h1

    if ohlc:
        cache_tick_wr_bars(instrument_key, ohlc)
    return ohlc


def compute_williams_r_280(instrument_key: str, asof: datetime) -> Optional[float]:
    """WR(280) from Upstox completed 2h bars ending at or before ``asof``."""
    bars = fetch_completed_2h_ohlc(instrument_key, asof)
    return williams_r_at(bars, asof, WR_PERIOD)


def resolve_equity_instrument_key(symbol: str, db: Any) -> Optional[str]:
    sym = _norm_symbol(symbol)
    if not sym:
        return None
    try:
        row = db.execute(
            text(
                """
                SELECT stock_instrument_key
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) = :s
                  AND stock_instrument_key IS NOT NULL
                  AND TRIM(stock_instrument_key) <> ''
                LIMIT 1
                """
            ),
            {"s": sym},
        ).fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception as e:
        logger.debug("arbitrage_master instrument key lookup failed for %s: %s", sym, e)
    try:
        from backend.services.symbol_isin_mapping import get_instrument_key

        ik = get_instrument_key(sym)
        if ik and "|" in str(ik):
            return str(ik).strip()
    except Exception as e:
        logger.debug("nse instruments instrument key lookup failed for %s: %s", sym, e)
    return None


def resolve_currmth_fut_instrument_key(symbol: str, db: Any) -> Optional[str]:
    """currmth FUT instrument key from arbitrage_master (OHLC for index EMAs)."""
    sym = _norm_symbol(symbol)
    if not sym or db is None:
        return None
    try:
        row = db.execute(
            text(
                """
                SELECT currmth_future_instrument_key
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) = :s
                  AND currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                LIMIT 1
                """
            ),
            {"s": sym},
        ).fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception as e:
        logger.debug("arbitrage_master currmth FUT key lookup failed for %s: %s", sym, e)
    return None


def _ema_closes_ready(closes: Sequence[float]) -> bool:
    return len(closes) >= EMA_SLOW


def _fetch_2h_closes_for_ema(
    symbol: str,
    instrument_key: str,
    *,
    db: Any = None,
    now: Optional[datetime] = None,
) -> List[float]:
    """2h closes for EMA9/30/100.

    Indices keep display/equity key as NSE_INDEX on the row, but if that history is
    empty/thin, fall back to arbitrage_master currmth FUT for OHLC only.
    """
    closes = _fetch_2h_closes(instrument_key, now=now)
    if _ema_closes_ready(closes) or not is_index_symbol(symbol):
        return closes
    fut_ik: Optional[str] = None
    own_db = False
    sess = db
    try:
        if sess is None:
            sess = SessionLocal()
            own_db = True
        fut_ik = resolve_currmth_fut_instrument_key(symbol, sess)
    except Exception:
        logger.exception("stock_option FUT key resolve failed for %s", symbol)
        fut_ik = None
    finally:
        if own_db and sess is not None:
            sess.close()
    if not fut_ik or fut_ik == instrument_key:
        logger.info(
            "stock_option index OHLC thin for %s key=%s closes=%s (no FUT fallback)",
            symbol,
            instrument_key,
            len(closes),
        )
        return closes
    logger.info(
        "stock_option index OHLC fallback %s %s -> FUT %s (index closes=%s)",
        symbol,
        instrument_key,
        fut_ik,
        len(closes),
    )
    return _fetch_2h_closes(fut_ik, now=now)


def _statuses_for_symbol(db: Any, symbol: str) -> List[str]:
    rows = db.execute(
        text("SELECT status FROM stock_option_signals WHERE symbol = :s"),
        {"s": symbol},
    ).fetchall()
    return [str(r[0]) for r in rows]


def ensure_index_radar_row(
    db: Any,
    symbol: str,
    *,
    now: Optional[datetime] = None,
) -> bool:
    """Insert a fresh Radar row for NIFTY/BANKNIFTY when no Radar/Active exists.

    Side and WR stay blank until a 2h EMA cross arms the row. Returns True if inserted.
    """
    sym = _norm_symbol(symbol)
    if not is_index_symbol(sym):
        return False
    if not should_insert_new_signal(
        _statuses_for_symbol(db, sym), block_executed=False
    ):
        return False
    ts = now_ist_second() if now is None else naive_ist(now)
    ik = resolve_equity_instrument_key(sym, db)
    db.execute(
        text(
            """
            INSERT INTO stock_option_signals (
                symbol, williamsr, instrument_key, status, side,
                trigger_at, triggered_at_raw, scan_name, alert_name, raw_payload,
                hard_stop_placed, created_at, updated_at
            ) VALUES (
                :symbol, NULL, :instrument_key, :status, NULL,
                :trigger_at, NULL, :scan_name, :alert_name, NULL,
                FALSE, :created_at, :updated_at
            )
            """
        ),
        {
            "symbol": sym,
            "instrument_key": ik,
            "status": STATUS_RADAR,
            "trigger_at": ts,
            "scan_name": "index-permanent-radar",
            "alert_name": "NIFTY/BANKNIFTY permanent Radar",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    logger.info("stock_option seeded Radar for permanent index %s", sym)
    return True


def ensure_index_radar_rows(now: Optional[datetime] = None) -> int:
    """Ensure Radar availability for NIFTY and BANKNIFTY (seed if missing)."""
    ensure_stock_option_tables()
    ts = now_ist_second() if now is None else naive_ist(now)
    n = 0
    db = SessionLocal()
    try:
        for sym in INDEX_PIN_ORDER:
            if ensure_index_radar_row(db, sym, now=ts):
                n += 1
        if n:
            db.commit()
        return n
    except Exception:
        db.rollback()
        logger.exception("stock_option ensure_index_radar_rows failed")
        return 0
    finally:
        db.close()


def _closes_from_2h_bars(bars: Sequence[Dict[str, Any]], asof: datetime) -> List[float]:
    asof_n = naive_ist(asof)
    closes: List[float] = []
    for bar in bars:
        end = bar.get("end")
        close = bar.get("close")
        if not isinstance(end, datetime) or close is None:
            continue
        if naive_ist(end) <= asof_n:
            closes.append(float(close))
    return closes


def _ema_snap_at(
    instrument_key: str,
    asof: datetime,
    *,
    symbol: Optional[str] = None,
    db: Any = None,
) -> Dict[str, Optional[float]]:
    """EMA9/30/100 on completed 2h closes ending at or before ``asof``.

    For NIFTY/BANKNIFTY, fall back to currmth FUT OHLC when NSE_INDEX history is thin.
    """
    closes = _closes_from_2h_bars(fetch_completed_2h_ohlc(instrument_key, asof), asof)
    if _ema_closes_ready(closes) or not is_index_symbol(symbol):
        return ema_snapshot(closes)
    fut_ik = resolve_currmth_fut_instrument_key(symbol or "", db)
    if fut_ik and fut_ik != instrument_key:
        logger.info(
            "stock_option index EMA snap fallback %s %s -> FUT %s (index closes=%s)",
            symbol,
            instrument_key,
            fut_ik,
            len(closes),
        )
        closes = _closes_from_2h_bars(fetch_completed_2h_ohlc(fut_ik, asof), asof)
    return ema_snapshot(closes)


def record_index_executed_at(
    symbol: str,
    armed_at: datetime,
    *,
    side: str = SIDE_BEAR,
    remarks: Optional[str] = None,
    try_strikes: bool = True,
) -> Dict[str, Any]:
    """Insert an Executed history row for a permanent index (keep Radar open).

    Idempotent on (symbol, armed_at minute, status=Executed). Contract from
    currmth-as-of armed_at. Strikes ~15Δ/~2Δ from live chain when available
    (blank OK for historical dates).
    """
    ensure_stock_option_tables()
    sym = _norm_symbol(symbol)
    if not is_index_symbol(sym):
        raise ValueError(f"not an index symbol: {symbol}")
    if side not in (SIDE_BEAR, SIDE_BULL):
        raise ValueError(f"invalid side: {side}")
    armed = naive_ist(armed_at).replace(second=0, microsecond=0)
    trade_date = armed.date()
    note = remarks or (
        f"Index EMA9 cross → {side} at {armed.strftime('%Y-%m-%d %H:%M')} IST"
    )

    db = SessionLocal()
    try:
        existing = db.execute(
            text(
                """
                SELECT id FROM stock_option_signals
                WHERE symbol = :sym
                  AND status = :executed
                  AND armed_at >= :lo AND armed_at < :hi
                ORDER BY id
                LIMIT 1
                """
            ),
            {
                "sym": sym,
                "executed": STATUS_EXECUTED,
                "lo": armed,
                "hi": armed + timedelta(minutes=1),
            },
        ).fetchone()
        if existing:
            ensure_index_radar_row(db, sym, now=now_ist_second())
            db.commit()
            return {"ok": True, "id": int(existing[0]), "created": False, "symbol": sym}

        ik = resolve_equity_instrument_key(sym, db)
        snap: Dict[str, Optional[float]] = {"ema9": None, "ema30": None, "ema100": None}
        if ik:
            try:
                snap = _ema_snap_at(ik, armed, symbol=sym, db=db)
            except Exception as e:
                logger.info("stock_option index EMA snap failed for %s @ %s: %s", sym, armed, e)

        contract = resolve_contract_mmm_yyyy(sym, armed, db=db)
        sell_strike = buy_strike = sell_delta = buy_delta = None
        sell_ik = buy_ik = None
        if try_strikes:
            try:
                chain = _option_chain_for_symbol(sym)
                sell_t, buy_t = delta_targets_for_symbol(sym)
                spread = pick_credit_spread(
                    chain, side, sell_target=sell_t, buy_target=buy_t
                )
                if spread:
                    sell_strike = spread.get("sell_strike")
                    buy_strike = spread.get("buy_strike")
                    sell_delta = spread.get("sell_delta")
                    buy_delta = spread.get("buy_delta")
                    sell_ik = spread.get("sell_instrument_key")
                    buy_ik = spread.get("buy_instrument_key")
            except Exception as e:
                logger.info("stock_option index historical strikes blank for %s: %s", sym, e)

        now = now_ist_second()
        row = db.execute(
            text(
                """
                INSERT INTO stock_option_signals (
                    symbol, williamsr, instrument_key, status, side,
                    trigger_at, armed_at, date_traded, contract_mmm_yyyy,
                    ema9, ema30, ema100, ema_updated_at,
                    sell_strike, buy_strike, sell_delta, buy_delta,
                    sell_instrument_key, buy_instrument_key,
                    user_sell_strike, user_buy_strike,
                    scan_name, alert_name, remarks,
                    hard_stop_placed, created_at, updated_at
                ) VALUES (
                    :symbol, NULL, :instrument_key, :status, :side,
                    :trigger_at, :armed_at, :date_traded, :contract,
                    :ema9, :ema30, :ema100, :armed_at,
                    :sell_strike, :buy_strike, :sell_delta, :buy_delta,
                    :sell_ik, :buy_ik,
                    :sell_strike, :buy_strike,
                    :scan_name, :alert_name, :remarks,
                    FALSE, :created_at, :updated_at
                )
                RETURNING id
                """
            ),
            {
                "symbol": sym,
                "instrument_key": ik,
                "status": STATUS_EXECUTED,
                "side": side,
                "trigger_at": armed,
                "armed_at": armed,
                "date_traded": trade_date,
                "contract": contract,
                "ema9": snap.get("ema9"),
                "ema30": snap.get("ema30"),
                "ema100": snap.get("ema100"),
                "sell_strike": sell_strike,
                "buy_strike": buy_strike,
                "sell_delta": sell_delta,
                "buy_delta": buy_delta,
                "sell_ik": sell_ik,
                "buy_ik": buy_ik,
                "scan_name": "index-ema-cross-executed",
                "alert_name": "NIFTY/BANKNIFTY EMA cross (Executed)",
                "remarks": note,
                "created_at": now,
                "updated_at": now,
            },
        ).fetchone()
        ensure_index_radar_row(db, sym, now=now)
        db.commit()
        new_id = int(row[0]) if row else None
        logger.info(
            "stock_option recorded index Executed %s id=%s armed=%s contract=%s",
            sym,
            new_id,
            armed,
            contract,
        )
        return {
            "ok": True,
            "id": new_id,
            "created": True,
            "symbol": sym,
            "side": side,
            "armed_at": armed.isoformat(sep=" "),
            "contract_mmm_yyyy": contract,
            "ema9": snap.get("ema9"),
            "ema30": snap.get("ema30"),
            "ema100": snap.get("ema100"),
            "sell_strike": sell_strike,
            "buy_strike": buy_strike,
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def insert_webhook_and_signals(
    *,
    received_at: datetime,
    source_ip: Optional[str],
    parsed: Any,
    raw_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Log ChartInk (or any) webhook body; do **not** insert stock Radar rows.

    Radar entry is owned by ``run_wr_radar_scan``. Permanent NIFTY/BANKNIFTY rows
    remain scheduler-seeded. Webhook still returns 200 with parse metadata so
    ChartInk does not retry-storm.
    """
    ensure_stock_option_tables()
    status, candidates, meta = parse_chartink_symbols(parsed)
    payload_json = json.dumps(raw_payload if raw_payload is not None else {})
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                INSERT INTO stock_option_webhook_log (
                    received_at, source_ip, scan_name, alert_name, triggered_at_raw,
                    raw_payload, parse_status
                ) VALUES (
                    :received_at, :source_ip, :scan_name, :alert_name, :triggered_at_raw,
                    CAST(:raw_payload AS jsonb), :parse_status
                )
                """
            ),
            {
                "received_at": received_at,
                "source_ip": source_ip,
                "scan_name": meta.get("scan_name"),
                "alert_name": meta.get("alert_name"),
                "triggered_at_raw": meta.get("triggered_at_raw"),
                "raw_payload": payload_json,
                "parse_status": status,
            },
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    logger.info(
        "stock_option webhook logged only (ChartInk Radar ingest disabled) "
        "parse=%s candidates=%s",
        status,
        len(candidates),
    )
    return {
        "parse_status": status,
        "inserted": 0,
        "ignored": len(candidates),
        "discarded": 0,
        "candidates": len(candidates),
        "radar_source": "wr_scan",
        "chartink_inserts": False,
    }


def _upstox_candles_with_retry(
    u: Any,
    instrument_key: str,
    *,
    interval: str,
    days_back: int,
    range_end_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Fetch Upstox candles; retry transient failures a few times with pause."""
    last_err: Optional[Exception] = None
    for attempt in range(EMA_FETCH_RETRIES):
        try:
            kwargs: Dict[str, Any] = {"interval": interval, "days_back": days_back}
            if range_end_date is not None:
                kwargs["range_end_date"] = range_end_date
            raw = u.get_historical_candles_by_instrument_key(instrument_key, **kwargs)
            return list(raw or [])
        except Exception as e:
            last_err = e
            time.sleep(EMA_FETCH_RETRY_PAUSE_SEC * (attempt + 1))
    if last_err is not None:
        logger.info(
            "stock_option %s fetch failed for %s after %s tries: %s",
            interval,
            instrument_key,
            EMA_FETCH_RETRIES,
            last_err,
        )
    return []


def _cached_minutes_5(instrument_key: str, days_back: int = 6) -> List[Dict[str, Any]]:
    """Best-effort read of Kavach/market-data shared minutes/5 cache (no REST)."""
    try:
        from backend.services.market_data import candle_cache

        end_d = datetime.now(IST).date()
        from_date = (end_d - timedelta(days=max(1, int(days_back)))).strftime("%Y-%m-%d")
        cached = candle_cache.get(instrument_key, "minutes/5", from_date, max_age_sec=300)
        return list(cached or [])
    except Exception as e:
        logger.debug("stock_option minutes/5 cache miss for %s: %s", instrument_key, e)
        return []


def _closes_from_5m_via_10m(
    candles_5m: Sequence[Dict[str, Any]],
    now: Optional[datetime] = None,
) -> List[float]:
    """Kavach 5m→10m pair, then session-aligned aggregate to completed 2h closes."""
    if not candles_5m:
        return []
    try:
        from backend.services.kavach_10m import aggregate_10m_bars

        bars_10m = aggregate_10m_bars(list(candles_5m))
    except Exception as e:
        logger.info("stock_option 5m→10m aggregate failed: %s", e)
        bars_10m = []
    if not bars_10m:
        # Direct 5m→2h if Kavach pairing unavailable.
        agg = aggregate_intraday_to_2h(candles_5m, now=now)
        return [float(b["close"]) for b in agg]
    # Normalize Kavach 10m shape (may use bar_end) into aggregate_intraday_to_2h input.
    normalized: List[Dict[str, Any]] = []
    for b in bars_10m:
        ts = b.get("timestamp") or b.get("bar_end")
        if ts is None:
            continue
        if hasattr(ts, "isoformat"):
            ts = ts.isoformat()
        normalized.append(
            {
                "timestamp": ts,
                "open": b.get("open"),
                "high": b.get("high"),
                "low": b.get("low"),
                "close": b.get("close"),
                "volume": b.get("volume") or 0.0,
            }
        )
    agg = aggregate_intraday_to_2h(normalized, now=now)
    return [float(b["close"]) for b in agg]


def _fetch_2h_closes_from_10m_fallback(
    instrument_key: str,
    now: Optional[datetime] = None,
) -> List[float]:
    """Build 2h closes from 10m (via 5m) when hours/2h Upstox paths fail.

    Uses shared Kavach minutes/5 cache when present, then chunked REST minutes/5
    (and minutes/10) windows so EMA100 can warm beyond a single 31d span.
    """
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    asof_n = naive_ist(now if now is not None else datetime.now(IST))
    asof_d = asof_n.date()

    # Prefer already-warmed Kavach/market-data 5m candles when available.
    cached = _cached_minutes_5(instrument_key)
    best = _closes_from_5m_via_10m(cached, now=now)
    if _ema_closes_ready(best):
        logger.info(
            "stock_option 2h from cached 5m→10m for %s closes=%s",
            instrument_key,
            len(best),
        )
        return best

    by_ts: Dict[str, Dict[str, Any]] = {}
    ends = [asof_d - timedelta(days=28 * i) for i in range(4)]
    for end_d in ends:
        for interval in ("minutes/5", "minutes/10"):
            raw = _upstox_candles_with_retry(
                u,
                instrument_key,
                interval=interval,
                days_back=30,
                range_end_date=end_d if end_d < asof_d else None,
            )
            for c in raw:
                key = str(c.get("timestamp") or "")
                if key:
                    by_ts[key] = c
            time.sleep(FETCH_SLEEP_SEC)
            if interval == "minutes/5" and len(by_ts) >= 500:
                break
        closes_probe = _closes_from_5m_via_10m([by_ts[k] for k in sorted(by_ts)], now=now)
        if _ema_closes_ready(closes_probe):
            break
        time.sleep(FETCH_SLEEP_SEC)

    merged = [by_ts[k] for k in sorted(by_ts)]
    if not merged and cached:
        merged = cached
    closes = _closes_from_5m_via_10m(merged, now=now)
    if closes:
        logger.info(
            "stock_option 2h from 10m fallback for %s raw=%s closes=%s",
            instrument_key,
            len(merged),
            len(closes),
        )
    return closes


def _normalize_2h_bars_chronological(
    bars: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Sort 2h bars oldest→newest by parsed timestamp; drop unparseable rows.

    Upstox historical often returns newest-first. EMA9/30/100 require ascending
    closes — a reversed series tracks old price levels and can falsely arm
    BULL PUT (EMA9 appears above EMA30/100 while the chart is bearish).
    """
    dated: List[Tuple[datetime, Dict[str, Any]]] = []
    unkeyed = 0
    for b in bars or []:
        ts = _parse_candle_ts(b.get("timestamp"))
        if ts is None or _as_float(b.get("close")) is None:
            unkeyed += 1
            continue
        dated.append((naive_ist(ts), b))
    if unkeyed:
        logger.info(
            "stock_option dropped %s 2h bars without parseable timestamp/close",
            unkeyed,
        )
    if len(dated) >= 2 and dated[0][0] > dated[-1][0]:
        logger.warning(
            "stock_option 2h bars arrived newest-first; normalizing to ascending"
        )
    dated.sort(key=lambda x: x[0])
    dedup: Dict[datetime, Dict[str, Any]] = {}
    for ts, b in dated:
        dedup[ts] = b
    return [dedup[k] for k in sorted(dedup)]


def _closes_from_2h_bar_dicts(bars: Sequence[Dict[str, Any]]) -> List[float]:
    """Chronological close series for EMA (always oldest→newest)."""
    return [float(b["close"]) for b in _normalize_2h_bars_chronological(bars)]


def _merge_2h_bar_dicts(
    primary: Sequence[Dict[str, Any]],
    overlay: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Union 2h bars by bucket time. Overlay wins on conflict (fresher hours/1 / 15m).

    hours/2 historical is not merged with the intraday V3 feed, so overlay closes
    are preferred when both sources share a bucket. Bars without a parseable
    timestamp are ignored (they must not append newest-first and poison EMA).
    """
    by_ts: Dict[datetime, Dict[str, Any]] = {}
    for b in primary or []:
        ts = _parse_candle_ts(b.get("timestamp"))
        if ts is None or _as_float(b.get("close")) is None:
            continue
        by_ts[naive_ist(ts)] = b
    for b in overlay or []:
        ts = _parse_candle_ts(b.get("timestamp"))
        if ts is None or _as_float(b.get("close")) is None:
            continue
        by_ts[naive_ist(ts)] = b
    return [by_ts[k] for k in sorted(by_ts)]


def _fetch_2h_closes(instrument_key: str, now: Optional[datetime] = None) -> List[float]:
    """REST historical 2h closes for EMA (oldest→newest).

    Prefer native hours/2 (includes Upstox 15:15 final bar). When hours/2 already
    has ≥100 closes, overlay only a **short** hours/1 window
    (``EMA_OVERLAY_DAYS_WHEN_READY``) for session freshness — hours/2 is not in
    the Upstox intraday-merge set, so a full 60d hours/1 pull is wasteful.
    Full hours/1 (60d), minutes/15, then 10m/5m fallback only when still short.

    Approx call volume per symbol per attempt:
    - Healthy (≥100 from hours/2): **2** candle requests (hours/2 + short hours/1)
    - Thin history: **3+** (add full hours/1 / 15m / chunked 5m–10m)
    Each request retries up to ``EMA_FETCH_RETRIES``. Overlay wins on bucket
    conflicts; closes always normalized ascending before return.
    """
    cached = get_cached_tick_wr_bars(instrument_key)
    if cached is not None and len(cached) >= EMA_SLOW:
        return [float(b["close"]) for b in cached]

    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    best: List[float] = []

    native = _upstox_candles_with_retry(
        u, instrument_key, interval="hours/2", days_back=120
    )
    bars_h2 = completed_2h_bars(native, now=now)
    closes_h2 = _closes_from_2h_bar_dicts(bars_h2)

    if _ema_closes_ready(closes_h2):
        # Short overlay only — keep ascending merge / tip freshness without a
        # redundant full multi-month hours/1 pull.
        one = _upstox_candles_with_retry(
            u,
            instrument_key,
            interval="hours/1",
            days_back=EMA_OVERLAY_DAYS_WHEN_READY,
        )
        bars_h1 = aggregate_intraday_to_2h(one, now=now)
        merged = _merge_2h_bar_dicts(bars_h2, bars_h1)
        return _closes_from_2h_bar_dicts(merged)

    one = _upstox_candles_with_retry(
        u, instrument_key, interval="hours/1", days_back=60
    )
    bars_h1 = aggregate_intraday_to_2h(one, now=now)
    merged = _merge_2h_bar_dicts(bars_h2, bars_h1)
    closes = _closes_from_2h_bar_dicts(merged)
    if _ema_closes_ready(closes):
        return closes
    if len(closes) > len(best):
        best = closes

    fifteen = _upstox_candles_with_retry(
        u, instrument_key, interval="minutes/15", days_back=60
    )
    bars_15 = aggregate_intraday_to_2h(fifteen, now=now)
    merged15 = _merge_2h_bar_dicts(merged, bars_15)
    closes = _closes_from_2h_bar_dicts(merged15)
    if _ema_closes_ready(closes):
        return closes
    if len(closes) > len(best):
        best = closes

    # Last resort: Kavach-style 10m (5m paired) → 2h.
    from10 = _fetch_2h_closes_from_10m_fallback(instrument_key, now=now)
    if _ema_closes_ready(from10):
        return from10
    if len(from10) > len(best):
        return from10
    return best


def _mark_ema_fetch_failed(
    row_id: int,
    now_naive: datetime,
    *,
    prior_ema9: Any = None,
    prior_ema30: Any = None,
    prior_ema100: Any = None,
) -> None:
    """On fetch fail: set ema_fetch_ok=FALSE (UI ⚠); preserve last good EMAs if any."""
    has_prior = any(v is not None for v in (prior_ema9, prior_ema30, prior_ema100))
    db = SessionLocal()
    try:
        if has_prior:
            # Keep last good numbers in DB, but flag so Radar shows ⚠ until retry succeeds.
            db.execute(
                text(
                    """
                    UPDATE stock_option_signals
                    SET ema_fetch_ok = FALSE,
                        updated_at = :ts
                    WHERE id = :id
                    """
                ),
                {"ts": now_naive, "id": row_id},
            )
            logger.info(
                "stock_option EMA fetch failed id=%s; preserved prior EMAs, ema_fetch_ok=FALSE",
                row_id,
            )
        else:
            db.execute(
                text(
                    """
                    UPDATE stock_option_signals
                    SET ema9 = NULL,
                        ema30 = NULL,
                        ema100 = NULL,
                        ema_fetch_ok = FALSE,
                        updated_at = :ts
                    WHERE id = :id
                    """
                ),
                {"ts": now_naive, "id": row_id},
            )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("stock_option ema fail-mark failed for id=%s", row_id)
    finally:
        db.close()


def _option_chain_for_symbol(symbol: str) -> Any:
    from backend.config import settings
    from backend.services.iron_condor_service import option_chain_underlying
    from backend.services.upstox_service import UpstoxService

    u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    api_sym = option_chain_underlying(symbol)
    return u.get_option_chain(api_sym)


def _fill_spreads_if_blank(row: Dict[str, Any], now: datetime) -> None:
    if row.get("sell_strike") is not None and row.get("buy_strike") is not None:
        return
    try:
        chain = _option_chain_for_symbol(row["symbol"])
    except Exception as e:
        logger.info("stock_option option chain failed for %s: %s", row.get("symbol"), e)
        return
    sell_t, buy_t = delta_targets_for_symbol(row.get("symbol"))
    spread = pick_credit_spread(
        chain, row.get("side"), sell_target=sell_t, buy_target=buy_t
    )
    if not spread:
        logger.info("stock_option delta spread blank for %s (chain/greeks unavailable)", row.get("symbol"))
        return
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE stock_option_signals
                SET sell_strike = :sell_strike,
                    sell_delta = :sell_delta,
                    sell_instrument_key = :sell_instrument_key,
                    buy_strike = :buy_strike,
                    buy_delta = :buy_delta,
                    buy_instrument_key = :buy_instrument_key,
                    updated_at = :updated_at
                WHERE id = :id
                  AND status = :active
                  AND date_traded IS NULL
                """
            ),
            {**spread, "updated_at": now, "id": row["id"], "active": STATUS_ACTIVE},
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("stock_option spread update failed for %s", row.get("symbol"))
    finally:
        db.close()


def _open_rows(*, only_fetch_failed: bool = False) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        failed_clause = " AND ema_fetch_ok IS FALSE" if only_fetch_failed else ""
        rows = db.execute(
            text(
                f"""
                SELECT id, symbol, instrument_key, status, side,
                       armed_at, ema9, ema30, ema100, sell_strike, buy_strike,
                       date_traded, sell_cost, buy_cost, ema_fetch_ok
                FROM stock_option_signals
                WHERE status IN (:radar, :active)
                {failed_clause}
                ORDER BY id
                """
            ),
            {"radar": STATUS_RADAR, "active": STATUS_ACTIVE},
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def expire_stale_active(now: Optional[datetime] = None) -> int:
    """Move Active rows armed 72h+ ago to Executed. Keep strikes/side/EMAs.

    Trade date is the armed calendar date. Does not invent or clear strikes.
    Skips rows that already have a submitted trade date or costs.
    Re-seeds permanent index Radar after expiry.
    """
    ensure_stock_option_tables()
    now_naive = now_ist_second() if now is None else naive_ist(now)
    cutoff = now_naive - timedelta(hours=ACTIVE_MAX_HOURS)
    db = SessionLocal()
    reseeds: List[str] = []
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, armed_at, remarks
                FROM stock_option_signals
                WHERE status = :active
                  AND armed_at IS NOT NULL
                  AND armed_at <= :cutoff
                  AND date_traded IS NULL
                  AND sell_cost IS NULL
                  AND buy_cost IS NULL
                """
            ),
            {"active": STATUS_ACTIVE, "cutoff": cutoff},
        ).mappings().all()
        n = 0
        for row in rows:
            armed = _naive_ist(row.get("armed_at"))
            if armed is None or not active_past_max_age(armed, now_naive):
                continue
            db.execute(
                text(
                    """
                    UPDATE stock_option_signals
                    SET status = :executed,
                        date_traded = CAST(:armed_date AS DATE),
                        remarks = :remarks,
                        updated_at = :ts
                    WHERE id = :id
                      AND status = :active
                      AND date_traded IS NULL
                    """
                ),
                {
                    "executed": STATUS_EXECUTED,
                    "armed_date": armed.date().isoformat(),
                    "remarks": expiry_remarks(row.get("remarks")),
                    "ts": now_naive,
                    "id": row["id"],
                    "active": STATUS_ACTIVE,
                },
            )
            n += 1
            sym = _norm_symbol(row.get("symbol"))
            if is_index_symbol(sym):
                reseeds.append(sym)
        for sym in reseeds:
            ensure_index_radar_row(db, sym, now=now_naive)
        if n or reseeds:
            db.commit()
        return n
    except Exception:
        db.rollback()
        logger.exception("stock_option 72h active expiry failed")
        return 0
    finally:
        db.close()


def _flag_executed_arm_caution(now: Optional[datetime] = None) -> int:
    """Set arm_caution on Executed (not Completed) rows that still need review.

    Covers: (1) legacy invalidate-without-trade rows; (2) Executed with entry costs
    but no exit (not Trade Report / Completed) whose stored EMAs no longer support
    the side. Does not invent status changes.
    """
    now_naive = now_ist_second() if now is None else naive_ist(now)
    db = SessionLocal()
    n = 0
    try:
        # Legacy false-arm → Executed (invalidate remarks) without a submitted trade.
        r1 = db.execute(
            text(
                """
                UPDATE stock_option_signals
                SET arm_caution = TRUE, updated_at = :ts
                WHERE status = :executed
                  AND arm_caution IS NOT TRUE
                  AND remarks = :inv
                  AND date_traded IS NULL
                  AND sell_cost IS NULL
                  AND buy_cost IS NULL
                """
            ),
            {
                "ts": now_naive,
                "executed": STATUS_EXECUTED,
                "inv": INVALIDATE_REMARKS,
            },
        )
        n += int(r1.rowcount or 0)

        rows = db.execute(
            text(
                """
                SELECT id, side, ema9, ema30, ema100
                FROM stock_option_signals
                WHERE status = :executed
                  AND exit_date IS NULL
                  AND sell_cost IS NOT NULL
                  AND buy_cost IS NOT NULL
                  AND arm_caution IS NOT TRUE
                  AND ema9 IS NOT NULL
                  AND ema30 IS NOT NULL
                  AND ema100 IS NOT NULL
                """
            ),
            {"executed": STATUS_EXECUTED},
        ).mappings().all()
        for row in rows:
            if ema_condition_holds(
                row.get("side"), row.get("ema9"), row.get("ema30"), row.get("ema100")
            ):
                continue
            db.execute(
                text(
                    """
                    UPDATE stock_option_signals
                    SET arm_caution = TRUE, updated_at = :ts
                    WHERE id = :id AND status = :executed
                    """
                ),
                {"ts": now_naive, "id": row["id"], "executed": STATUS_EXECUTED},
            )
            n += 1
        db.commit()
        if n:
            logger.info("stock_option arm_caution flagged on %s Executed row(s)", n)
        return n
    except Exception:
        db.rollback()
        logger.exception("stock_option arm_caution flag failed")
        return 0
    finally:
        db.close()


def list_arbitrage_master_equity_universe(db: Any) -> List[Tuple[str, str]]:
    """(symbol, stock_instrument_key) from arbitrage_master — equity keys for WR scan."""
    rows = db.execute(
        text(
            """
            SELECT UPPER(TRIM(stock)) AS stock, TRIM(stock_instrument_key) AS ikey
            FROM arbitrage_master
            WHERE stock IS NOT NULL
              AND TRIM(stock) <> ''
              AND stock_instrument_key IS NOT NULL
              AND TRIM(stock_instrument_key) <> ''
            ORDER BY stock
            """
        )
    ).fetchall()
    out: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for r in rows:
        sym = _norm_symbol(r[0])
        ik = str(r[1] or "").strip()
        if not sym or not ik or sym in seen or is_index_symbol(sym):
            continue
        seen.add(sym)
        out.append((sym, ik))
    return out


def _blocked_open_lifecycle_symbols(db: Any) -> set[str]:
    """Symbols with Radar / Active / Executed — ineligible for new Radar insert."""
    rows = db.execute(
        text(
            """
            SELECT DISTINCT UPPER(TRIM(symbol))
            FROM stock_option_signals
            WHERE LOWER(TRIM(status)) IN ('radar', 'active', 'executed')
            """
        )
    ).fetchall()
    return {_norm_symbol(r[0]) for r in rows if r and r[0]}


def insert_radar_from_wr_scan(
    db: Any,
    *,
    symbol: str,
    instrument_key: str,
    williamsr: float,
    side: str,
    now: datetime,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO stock_option_signals (
                symbol, williamsr, instrument_key, status, side,
                trigger_at, triggered_at_raw, scan_name, alert_name, raw_payload,
                hard_stop_placed, created_at, updated_at
            ) VALUES (
                :symbol, :williamsr, :instrument_key, :status, :side,
                :trigger_at, :triggered_at_raw, :scan_name, :alert_name,
                CAST(:raw_payload AS jsonb),
                FALSE, :created_at, :updated_at
            )
            """
        ),
        {
            "symbol": symbol,
            "williamsr": williamsr,
            "instrument_key": instrument_key,
            "status": STATUS_RADAR,
            "side": side,
            "trigger_at": now,
            "triggered_at_raw": now.strftime("%H:%M"),
            "scan_name": SCAN_NAME_WR,
            "alert_name": SCAN_NAME_WR,
            "raw_payload": json.dumps({"source": SCAN_NAME_WR, "asof": now.isoformat()}),
            "created_at": now,
            "updated_at": now,
        },
    )


def run_wr_radar_scan(now: Optional[datetime] = None) -> Dict[str, Any]:
    """Universe WR(280) scan → Radar inserts (BEAR CALL / BULL PUT).

    Universe = arbitrage_master equities with instrument keys. Skips symbols that
    already have Radar / Active / Executed. Completed / Rejected are eligible.
    Shares OHLC via the per-tick cache for the subsequent EMA pass.
    """
    ensure_stock_option_tables()
    from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

    if defer_job_for_breakfast_exclusivity("stock_option_wr_scan"):
        return {"ok": True, "skipped": "breakfast_exclusivity"}

    now_naive = now_ist_second() if now is None else naive_ist(now)
    clear_tick_wr_bars_cache()
    scanned = inserted = ignored = discarded = failed = 0
    db = SessionLocal()
    try:
        universe = list_arbitrage_master_equity_universe(db)
        blocked = _blocked_open_lifecycle_symbols(db)
    finally:
        db.close()

    for sym, ik in universe:
        scanned += 1
        if sym in blocked:
            ignored += 1
            continue
        try:
            bars = fetch_completed_2h_ohlc(ik, now_naive)
            wr = williams_r_at(bars, now_naive, WR_PERIOD)
        except Exception as e:
            failed += 1
            logger.info("stock_option WR scan fetch failed %s: %s", sym, e)
            time.sleep(FETCH_SLEEP_SEC)
            continue
        side = side_from_williamsr(wr)
        if side is None:
            discarded += 1
            time.sleep(FETCH_SLEEP_SEC)
            continue
        db = SessionLocal()
        try:
            # Re-check under write lock path (race with concurrent arm/execute).
            if not should_insert_new_signal(_statuses_for_symbol(db, sym)):
                ignored += 1
                db.rollback()
            else:
                insert_radar_from_wr_scan(
                    db,
                    symbol=sym,
                    instrument_key=ik,
                    williamsr=float(wr),
                    side=side,
                    now=now_naive,
                )
                db.commit()
                inserted += 1
                blocked.add(sym)
                logger.info(
                    "stock_option WR scan insert %s side=%s wr=%s",
                    sym,
                    side,
                    wr,
                )
        except Exception:
            db.rollback()
            failed += 1
            logger.exception("stock_option WR scan insert failed for %s", sym)
        finally:
            db.close()
        time.sleep(FETCH_SLEEP_SEC)

    out = {
        "ok": True,
        "scanned": scanned,
        "inserted": inserted,
        "ignored": ignored,
        "discarded": discarded,
        "failed": failed,
        "asof": now_naive.isoformat(sep=" "),
    }
    logger.info("stock_option WR radar scan: %s", out)
    return out


def run_radar_eod_cleanup(now: Optional[datetime] = None) -> Dict[str, Any]:
    """Post-market: drop Radar rows whose WR has left the entry extreme.

    Only ``Radar`` (not Active / Executed). BULL PUT + WR > -5, or BEAR CALL +
    WR < -95 → DELETE so the symbol is free for a future scan.
    """
    ensure_stock_option_tables()
    from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

    if defer_job_for_breakfast_exclusivity("stock_option_radar_eod"):
        return {"ok": True, "skipped": "breakfast_exclusivity"}

    now_naive = now_ist_second() if now is None else naive_ist(now)
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, side, instrument_key, williamsr
                FROM stock_option_signals
                WHERE status = :radar
                  AND UPPER(TRIM(symbol)) NOT IN ('NIFTY', 'BANKNIFTY')
                ORDER BY id
                """
            ),
            {"radar": STATUS_RADAR},
        ).mappings().all()
        radar_rows = [dict(r) for r in rows]
    finally:
        db.close()

    checked = removed = failed = 0
    for row in radar_rows:
        checked += 1
        ik = (row.get("instrument_key") or "").strip()
        sym = _norm_symbol(row.get("symbol"))
        if not ik:
            db = SessionLocal()
            try:
                ik = resolve_equity_instrument_key(sym, db) or ""
            finally:
                db.close()
        wr = None
        if ik:
            try:
                wr = compute_williams_r_280(ik, now_naive)
            except Exception as e:
                failed += 1
                logger.info("stock_option EOD WR failed %s: %s", sym, e)
                time.sleep(FETCH_SLEEP_SEC)
                continue
        if not should_remove_radar_eod(row.get("side"), wr):
            time.sleep(FETCH_SLEEP_SEC)
            continue
        db = SessionLocal()
        try:
            db.execute(
                text(
                    """
                    DELETE FROM stock_option_signals
                    WHERE id = :id AND status = :radar
                    """
                ),
                {"id": row["id"], "radar": STATUS_RADAR},
            )
            db.commit()
            removed += 1
            logger.info(
                "stock_option EOD removed Radar id=%s %s side=%s wr=%s",
                row["id"],
                sym,
                row.get("side"),
                wr,
            )
        except Exception:
            db.rollback()
            failed += 1
            logger.exception("stock_option EOD delete failed id=%s", row.get("id"))
        finally:
            db.close()
        time.sleep(FETCH_SLEEP_SEC)

    out = {
        "ok": True,
        "checked": checked,
        "removed": removed,
        "failed": failed,
        "asof": now_naive.isoformat(sep=" "),
    }
    logger.info("stock_option Radar EOD cleanup: %s", out)
    return out


def run_ema_tick(
    now: Optional[datetime] = None,
    *,
    only_fetch_failed: bool = False,
) -> Dict[str, Any]:
    """2h job: seed index Radar, expire stale Active, update EMAs, arm, demote, fill spreads.

    NIFTY/BANKNIFTY: no WR gate; Radar→Active only on EMA9 cross vs EMA30+EMA100;
    Active (not submitted) demotes to Radar when hold fails.
    Stocks: WR side already set at Radar entry; arm when hold **and** fresh 1-candle
    cross matches that side; Active demotes to Radar when hold fails.

    EMA reliability: failed fetches set ``ema_fetch_ok=FALSE`` (UI ⚠) and preserve
    last good EMAs; scheduler schedules +10m (then chained) retries for those rows
    only. Tick order pins indices first, then fetch-failed Active, then other Active,
    then Radar — so open rows are not starved by late Upstox empties.

    When ``only_fetch_failed`` is True (off-schedule retry), only Radar/Active
    rows with ``ema_fetch_ok=FALSE`` are refreshed; seed/expire/LTP refresh are skipped.
    """
    ensure_stock_option_tables()
    from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

    job_label = "stock_option_ema_retry" if only_fetch_failed else "stock_option_ema"
    if defer_job_for_breakfast_exclusivity(job_label):
        return {"ok": True, "skipped": "breakfast_exclusivity", "retry": only_fetch_failed}

    now_naive = now_ist_second() if now is None else naive_ist(now)
    seeded = 0
    expired = 0
    if not only_fetch_failed:
        seeded = ensure_index_radar_rows(now_naive)
        expired = expire_stale_active(now_naive)
    # Indices first so permanent NIFTY/BANKNIFTY EMA/arm is not starved by late-loop
    # Upstox empty responses after many stock candle fetches.
    rows = _pin_index_rows(_open_rows(only_fetch_failed=only_fetch_failed))
    if only_fetch_failed and not rows:
        return {
            "ok": True,
            "retry": True,
            "skipped": "no_failed_rows",
            "open_rows": 0,
            "updated": 0,
            "armed": 0,
            "demoted": 0,
            "cautioned": 0,
            "ema_fetch_failed": 0,
            "expired_72h": 0,
            "index_radar_seeded": 0,
            "ltp_refreshed": 0,
        }
    updated = armed = demoted = failed = 0
    for row in rows:
        ik = (row.get("instrument_key") or "").strip()
        if not ik:
            db = SessionLocal()
            try:
                ik = resolve_equity_instrument_key(row["symbol"], db)
                if ik:
                    db.execute(
                        text(
                            "UPDATE stock_option_signals SET instrument_key = :ik, updated_at = :ts WHERE id = :id"
                        ),
                        {"ik": ik, "ts": now_naive, "id": row["id"]},
                    )
                    db.commit()
                    row["instrument_key"] = ik
            except Exception:
                db.rollback()
            finally:
                db.close()
        if not ik:
            _mark_ema_fetch_failed(
                int(row["id"]),
                now_naive,
                prior_ema9=row.get("ema9"),
                prior_ema30=row.get("ema30"),
                prior_ema100=row.get("ema100"),
            )
            failed += 1
            continue
        closes: List[float] = []
        fetch_err: Optional[Exception] = None
        for attempt in range(2):
            try:
                closes = _fetch_2h_closes_for_ema(
                    row.get("symbol") or "",
                    ik,
                    now=now or datetime.now(IST),
                )
                fetch_err = None
                if closes:
                    break
            except Exception as e:
                fetch_err = e
                logger.info(
                    "stock_option 2h candles failed for %s attempt=%s: %s",
                    row.get("symbol"),
                    attempt + 1,
                    e,
                )
            if attempt == 0:
                time.sleep(EMA_FETCH_RETRY_PAUSE_SEC)
        if fetch_err is not None or not closes:
            logger.info(
                "stock_option EMA fetch exhausted for %s key=%s err=%s closes=%s",
                row.get("symbol"),
                ik,
                fetch_err,
                len(closes),
            )
            _mark_ema_fetch_failed(
                int(row["id"]),
                now_naive,
                prior_ema9=row.get("ema9"),
                prior_ema30=row.get("ema30"),
                prior_ema100=row.get("ema100"),
            )
            failed += 1
            time.sleep(FETCH_SLEEP_SEC)
            continue
        snap = ema_snapshot(closes)
        if not _ema_closes_ready(closes):
            logger.info(
                "stock_option EMA incomplete for %s closes=%s key=%s snap=%s",
                row.get("symbol"),
                len(closes),
                ik,
                snap,
            )
        last_close = float(closes[-1]) if closes else None
        if (
            snap.get("ema9") is not None
            and last_close is not None
            and not ema_plausible_vs_last_close(snap["ema9"], last_close)
        ):
            logger.warning(
                "stock_option EMA implausible for %s ema9=%s last_close=%s closes=%s; treating as fetch fail",
                row.get("symbol"),
                snap.get("ema9"),
                last_close,
                len(closes),
            )
            _mark_ema_fetch_failed(
                int(row["id"]),
                now_naive,
                prior_ema9=None,
                prior_ema30=None,
                prior_ema100=None,
            )
            failed += 1
            time.sleep(FETCH_SLEEP_SEC)
            continue
        index_sym = is_index_symbol(row.get("symbol"))
        pair = ema_snapshot_pair(closes)
        wr_now = None
        if not index_sym:
            try:
                cached_wr = get_cached_tick_wr_bars(ik)
                if cached_wr is not None and len(cached_wr) >= WR_PERIOD:
                    wr_now = williams_r_at(cached_wr, now_naive, WR_PERIOD)
                else:
                    # Avoid a second multi-chunk WR pull on the EMA path — entry WR
                    # was set by the scan; COALESCE keeps prior when wr_now is None.
                    wr_now = None
            except Exception as e:
                logger.info("stock_option WR(280) refresh failed for %s: %s", row.get("symbol"), e)
                wr_now = None
        submitted = trade_is_submitted(row.get("date_traded"), row.get("sell_cost"), row.get("buy_cost"))
        arm_side: Optional[str] = None
        if index_sym:
            if pair is None:
                action = "hold"
            else:
                prev, curr = pair
                snap = curr
                action, arm_side = next_index_ema_action(
                    row.get("status") or "",
                    row.get("side"),
                    prev["ema9"],
                    prev["ema30"],
                    prev["ema100"],
                    curr["ema9"],
                    curr["ema30"],
                    curr["ema100"],
                    submitted,
                )
        else:
            if pair is None:
                action = "hold"
            else:
                prev, curr = pair
                snap = curr
                action = next_stock_ema_action(
                    row.get("status") or "",
                    row.get("side"),
                    prev["ema9"],
                    prev["ema30"],
                    prev["ema100"],
                    curr["ema9"],
                    curr["ema30"],
                    curr["ema100"],
                    submitted,
                )
        db = SessionLocal()
        try:
            if action == "demote":
                db.execute(
                    text(
                        """
                        UPDATE stock_option_signals
                        SET status = :radar,
                            armed_at = NULL,
                            contract_mmm_yyyy = NULL,
                            sell_strike = NULL,
                            sell_delta = NULL,
                            sell_instrument_key = NULL,
                            buy_strike = NULL,
                            buy_delta = NULL,
                            buy_instrument_key = NULL,
                            ema9 = :ema9, ema30 = :ema30, ema100 = :ema100,
                            ema_updated_at = :ts,
                            ema_fetch_ok = TRUE,
                            williamsr = COALESCE(:williamsr, williamsr),
                            arm_caution = TRUE,
                            remarks = :remarks,
                            updated_at = :ts
                        WHERE id = :id AND status = :active AND date_traded IS NULL
                          AND sell_cost IS NULL AND buy_cost IS NULL
                        """
                    ),
                    {
                        "radar": STATUS_RADAR,
                        "ema9": snap["ema9"],
                        "ema30": snap["ema30"],
                        "ema100": snap["ema100"],
                        "williamsr": wr_now,
                        "ts": now_naive,
                        "remarks": DEMOTE_REMARKS,
                        "id": row["id"],
                        "active": STATUS_ACTIVE,
                    },
                )
                demoted += 1
                row["status"] = STATUS_RADAR
            elif action == "arm":
                contract = resolve_contract_mmm_yyyy(
                    row.get("symbol"), now_naive, db=db
                )
                side_set = arm_side if arm_side else row.get("side")
                db.execute(
                    text(
                        """
                        UPDATE stock_option_signals
                        SET status = :active,
                            side = COALESCE(:side, side),
                            armed_at = COALESCE(armed_at, :ts),
                            contract_mmm_yyyy = COALESCE(contract_mmm_yyyy, :contract),
                            ema9 = :ema9, ema30 = :ema30, ema100 = :ema100,
                            ema_updated_at = :ts,
                            ema_fetch_ok = TRUE,
                            williamsr = COALESCE(:williamsr, williamsr),
                            arm_caution = FALSE,
                            remarks = NULL,
                            updated_at = :ts
                        WHERE id = :id AND status = :radar
                        """
                    ),
                    {
                        "active": STATUS_ACTIVE,
                        "side": side_set,
                        "ema9": snap["ema9"],
                        "ema30": snap["ema30"],
                        "ema100": snap["ema100"],
                        "williamsr": wr_now,
                        "ts": now_naive,
                        "contract": contract,
                        "id": row["id"],
                        "radar": STATUS_RADAR,
                    },
                )
                armed += 1
                row["status"] = STATUS_ACTIVE
                if side_set:
                    row["side"] = side_set
                if contract:
                    row["contract_mmm_yyyy"] = contract
            else:
                # Write snap as-is (including NULL). Do not COALESCE-preserve stale EMAs.
                db.execute(
                    text(
                        """
                        UPDATE stock_option_signals
                        SET ema9 = :ema9,
                            ema30 = :ema30,
                            ema100 = :ema100,
                            ema_updated_at = :ts,
                            ema_fetch_ok = TRUE,
                            williamsr = COALESCE(:williamsr, williamsr),
                            updated_at = :ts
                        WHERE id = :id
                        """
                    ),
                    {
                        "ema9": snap["ema9"],
                        "ema30": snap["ema30"],
                        "ema100": snap["ema100"],
                        "williamsr": wr_now,
                        "ts": now_naive,
                        "id": row["id"],
                    },
                )
            db.commit()
            updated += 1
        except Exception:
            db.rollback()
            logger.exception("stock_option ema update failed for %s", row.get("symbol"))
            _mark_ema_fetch_failed(
                int(row["id"]),
                now_naive,
                prior_ema9=row.get("ema9"),
                prior_ema30=row.get("ema30"),
                prior_ema100=row.get("ema100"),
            )
            failed += 1
        finally:
            db.close()

        if action != "demote" and (
            action == "arm"
            or (
                (row.get("status") or "").strip().lower() == "active"
                and (row.get("sell_strike") is None or row.get("buy_strike") is None)
                and not submitted
            )
        ):
            row["status"] = STATUS_ACTIVE
            _fill_spreads_if_blank(row, now_naive)
        time.sleep(FETCH_SLEEP_SEC)

    cautioned = 0 if only_fetch_failed else _flag_executed_arm_caution(now_naive)
    ltp_n = 0 if only_fetch_failed else refresh_executed_ltps(now_naive)
    return {
        "ok": True,
        "retry": only_fetch_failed,
        "open_rows": len(rows),
        "updated": updated,
        "armed": armed,
        "demoted": demoted,
        "cautioned": cautioned,
        "ema_fetch_failed": failed,
        "expired_72h": expired,
        "index_radar_seeded": seeded,
        "ltp_refreshed": ltp_n,
    }



def _sync_executed_ws_ltp_subscriptions() -> None:
    """Best-effort: keep shared Upstox WS subscribed to filled Executed option keys."""
    try:
        from backend.services.stock_option_ws_ltp import sync_executed_option_subscriptions

        sync_executed_option_subscriptions()
    except Exception:
        logger.debug("stock_option WS LTP sync after mutation failed", exc_info=True)


def refresh_executed_ltps(now: Optional[datetime] = None) -> int:
    """Quote LTPs for Executed rows with strikes + entry costs filled.

    Called at the end of ``run_ema_tick`` (after EMA / WR work). Prefer WS cache via
    ``ltp_map_with_fallback``; REST batch quotes fill gaps. Live ticks also push LTPs
    via ``stock_option_ws_ltp`` between 2h cycles.

    Includes auto-expired Executed rows the user later filled; excludes only
    invalidate remarks.
    """
    ensure_stock_option_tables()
    now_naive = now_ist_second() if now is None else naive_ist(now)
    try:
        ensure_executed_option_instrument_keys()
    except Exception:
        logger.debug("stock_option key ensure before LTP refresh failed", exc_info=True)
    try:
        from backend.services.stock_option_ws_ltp import list_executed_option_ltp_rows

        rows = list_executed_option_ltp_rows()
    except Exception:
        db = SessionLocal()
        try:
            rows = db.execute(
                text(
                    """
                    SELECT id, sell_instrument_key, buy_instrument_key
                    FROM stock_option_signals
                    WHERE status = :executed
                      AND date_traded IS NOT NULL
                      AND sell_cost IS NOT NULL
                      AND buy_cost IS NOT NULL
                      AND (
                        COALESCE(user_sell_strike, sell_strike) IS NOT NULL
                        AND COALESCE(user_buy_strike, buy_strike) IS NOT NULL
                      )
                      AND remarks IS DISTINCT FROM :remarks
                    """
                ),
                {
                    "executed": STATUS_EXECUTED,
                    "remarks": INVALIDATE_REMARKS,
                },
            ).mappings().all()
        finally:
            db.close()
    if not rows:
        return 0
    keys = []
    for r in rows:
        if r.get("sell_instrument_key"):
            keys.append(str(r["sell_instrument_key"]))
        if r.get("buy_instrument_key"):
            keys.append(str(r["buy_instrument_key"]))
    if not keys:
        logger.info(
            "stock_option LTP refresh: %s filled Executed rows but no option instrument keys",
            len(rows),
        )
        return 0
    try:
        from backend.services.market_data.reads import ltp_map_with_fallback

        prices = ltp_map_with_fallback(keys, allow_broker_fallback=True) or {}
    except Exception as e:
        logger.info("stock_option LTP refresh failed: %s", e)
        return 0
    n = 0
    db = SessionLocal()
    try:
        for r in rows:
            sk = str(r.get("sell_instrument_key") or "")
            bk = str(r.get("buy_instrument_key") or "")
            sl = prices.get(sk) if sk else None
            bl = prices.get(bk) if bk else None
            if sl is None and bl is None:
                # Try colon/pipe variants used by Upstox / WS normalize.
                if sk and sl is None:
                    sl = prices.get(sk.replace("|", ":")) or prices.get(sk.replace(":", "|"))
                if bk and bl is None:
                    bl = prices.get(bk.replace("|", ":")) or prices.get(bk.replace(":", "|"))
            if sl is None and bl is None:
                continue
            db.execute(
                text(
                    """
                    UPDATE stock_option_signals
                    SET sell_ltp = COALESCE(:sell_ltp, sell_ltp),
                        buy_ltp = COALESCE(:buy_ltp, buy_ltp),
                        ltp_updated_at = :ts,
                        updated_at = :ts
                    WHERE id = :id
                    """
                ),
                {
                    "sell_ltp": sl,
                    "buy_ltp": bl,
                    "ts": now_naive,
                    "id": r["id"],
                },
            )
            n += 1
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("stock_option LTP persist failed")
        return 0
    finally:
        db.close()
    return n


def _fmt_ts(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _row_public(r: Dict[str, Any]) -> Dict[str, Any]:
    sell_line, buy_line = spread_lines(
        r.get("side"), r.get("sell_strike"), r.get("buy_strike"), symbol=r.get("symbol")
    )
    user_sell = r.get("user_sell_strike") if r.get("user_sell_strike") is not None else r.get("sell_strike")
    user_buy = r.get("user_buy_strike") if r.get("user_buy_strike") is not None else r.get("buy_strike")
    completed = (r.get("status") or "").strip().lower() == "completed"
    if completed:
        pnl = realized_credit_pnl(
            r.get("sell_cost"), r.get("buy_cost"), r.get("sell_exit_price"), r.get("buy_exit_price")
        )
        if pnl is None:
            pnl = _as_float(r.get("realized_pnl"))
    else:
        pnl = combined_pnl(r.get("sell_cost"), r.get("buy_cost"), r.get("sell_ltp"), r.get("buy_ltp"))
    hs = hard_stop_price(r.get("sell_cost"))
    sym = r.get("symbol")
    fetch_ok = r.get("ema_fetch_ok")
    if fetch_ok is None:
        ema_stale = False
    else:
        ema_stale = not bool(fetch_ok)
    return {
        "id": r.get("id"),
        "symbol": sym,
        "is_index": is_index_symbol(sym),
        "index_fut": is_index_symbol(sym),
        "williamsr": r.get("williamsr"),
        "instrument_key": r.get("instrument_key"),
        "status": r.get("status"),
        "side": r.get("side"),
        "trigger_at": _fmt_ts(r.get("trigger_at")),
        "armed_at": _fmt_ts(r.get("armed_at")),
        "contract_mmm_yyyy": r.get("contract_mmm_yyyy"),
        "ema9": None if ema_stale else r.get("ema9"),
        "ema30": None if ema_stale else r.get("ema30"),
        "ema100": None if ema_stale else r.get("ema100"),
        "ema_updated_at": _fmt_ts(r.get("ema_updated_at")),
        "ema_fetch_ok": None if fetch_ok is None else bool(fetch_ok),
        "ema_stale": ema_stale,
        "sell_strike": r.get("sell_strike"),
        "buy_strike": r.get("buy_strike"),
        "sell_delta": r.get("sell_delta"),
        "buy_delta": r.get("buy_delta"),
        "spread_sell": sell_line,
        "spread_buy": buy_line,
        "date_traded": _fmt_ts(r.get("date_traded")),
        "sell_cost": r.get("sell_cost"),
        "buy_cost": r.get("buy_cost"),
        "user_sell_strike": user_sell,
        "user_buy_strike": user_buy,
        "sell_ltp": r.get("sell_ltp"),
        "buy_ltp": r.get("buy_ltp"),
        "exit_date": _fmt_ts(r.get("exit_date")),
        "sell_exit_price": r.get("sell_exit_price"),
        "buy_exit_price": r.get("buy_exit_price"),
        "trade_mode": normalize_trade_mode(r.get("trade_mode")),
        "combined_pnl": pnl,
        "hard_stop": hs,
        "hard_stop_placed": bool(r.get("hard_stop_placed")),
        "remarks": r.get("remarks"),
        "arm_caution": bool(r.get("arm_caution")),
    }


def _pin_index_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """NIFTY/BANKNIFTY first, then Active (esp. fetch-failed), then remaining Radar."""
    order = {s: i for i, s in enumerate(INDEX_PIN_ORDER)}
    pinned: List[Dict[str, Any]] = []
    active_failed: List[Dict[str, Any]] = []
    active_ok: List[Dict[str, Any]] = []
    rest: List[Dict[str, Any]] = []
    for item in rows:
        sym = _norm_symbol(item.get("symbol"))
        if sym in order:
            pinned.append(item)
            continue
        st = (item.get("status") or "").strip().lower()
        if st == "active":
            # Fetch-failed Active first so poisoned EMAs are refreshed before Radar.
            if item.get("ema_fetch_ok") is False:
                active_failed.append(item)
            else:
                active_ok.append(item)
        else:
            rest.append(item)
    pinned.sort(key=lambda r: order.get(_norm_symbol(r.get("symbol")), 99))
    return pinned + active_failed + active_ok + rest


def list_workspace() -> Dict[str, Any]:
    ensure_stock_option_tables()
    ensure_index_radar_rows()
    expire_stale_active()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, williamsr, instrument_key, status, side,
                       trigger_at, armed_at, contract_mmm_yyyy, ema9, ema30, ema100,
                       ema_updated_at, ema_fetch_ok, arm_caution,
                       sell_strike, sell_delta, buy_strike, buy_delta,
                       date_traded, sell_cost, buy_cost,
                       user_sell_strike, user_buy_strike, hard_stop_placed,
                       remarks, sell_ltp, buy_ltp, sell_instrument_key,
                       exit_date, sell_exit_price, buy_exit_price, realized_pnl,
                       trade_mode
                FROM stock_option_signals
                WHERE status IS DISTINCT FROM :rejected
                ORDER BY id DESC
                """
            ),
            {"rejected": STATUS_REJECTED},
        ).mappings().all()
        raws = [dict(r) for r in rows]
        need = [
            (r.get("symbol"), r.get("sell_instrument_key"))
            for r in raws
            if (r.get("status") or "").strip().lower() in ("executed", "completed")
        ]
        lots_by_sym = lookup_option_lots(db, need) if need else {}
    finally:
        db.close()
    radar, active, executed, completed = [], [], [], []
    for raw in raws:
        item = _row_public(raw)
        st = (item.get("status") or "").strip().lower()
        if st in ("executed", "completed"):
            sym = _norm_symbol(item.get("symbol"))
            lot = lots_by_sym.get(sym)
            attach_rupee_pnl(item, lot)
            if item.get("combined_pnl") is not None and item.get("lot_size") is None:
                logger.warning(
                    "stock_option lot size missing for %s; P&L shown as premium points, not ₹",
                    sym or item.get("symbol"),
                )
        if st == "radar":
            radar.append(item)
        elif st == "active":
            active.append(item)
        elif st == "executed":
            executed.append(item)
        elif st == "completed":
            completed.append(item)
    return {
        "radar": _pin_index_rows(radar),
        "active": _pin_index_rows(active),
        "executed": _pin_index_rows(executed),
        "completed": _pin_index_rows(completed),
    }


def submit_trade(
    signal_id: int,
    *,
    date_traded: str,
    buy_strike: float,
    buy_cost: float,
    sell_strike: float,
    sell_cost: float,
) -> Dict[str, Any]:
    ensure_stock_option_tables()
    try:
        traded = date.fromisoformat(str(date_traded)[:10])
    except ValueError as e:
        raise ValueError("date_traded must be YYYY-MM-DD") from e
    now = now_ist_second()
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT id, symbol, status, date_traded FROM stock_option_signals WHERE id = :id"),
            {"id": signal_id},
        ).mappings().first()
        if not row:
            raise LookupError("signal not found")
        if (row.get("status") or "").strip().lower() != "active":
            raise ValueError("only Active rows can be submitted")
        if row.get("date_traded") is not None:
            raise ValueError("trade already submitted")
        db.execute(
            text(
                """
                UPDATE stock_option_signals
                SET status = :executed,
                    date_traded = :date_traded,
                    user_buy_strike = :buy_strike,
                    user_sell_strike = :sell_strike,
                    buy_cost = :buy_cost,
                    sell_cost = :sell_cost,
                    remarks = NULL,
                    updated_at = :ts
                WHERE id = :id
                """
            ),
            {
                "executed": STATUS_EXECUTED,
                "date_traded": traded,
                "buy_strike": float(buy_strike),
                "sell_strike": float(sell_strike),
                "buy_cost": float(buy_cost),
                "sell_cost": float(sell_cost),
                "ts": now,
                "id": signal_id,
            },
        )
        # Permanent indices: immediately start a fresh Radar cycle.
        ensure_index_radar_row(db, row.get("symbol") or "", now=now)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    try:
        ensure_executed_option_instrument_keys(signal_id)
    except Exception:
        logger.debug("stock_option key resolve after submit failed", exc_info=True)
    _sync_executed_ws_ltp_subscriptions()
    return {"ok": True, "id": signal_id, "status": STATUS_EXECUTED}


def set_hard_stop_placed(signal_id: int, placed: bool) -> Dict[str, Any]:
    ensure_stock_option_tables()
    now = now_ist_second()
    db = SessionLocal()
    try:
        res = db.execute(
            text(
                """
                UPDATE stock_option_signals
                SET hard_stop_placed = :placed, updated_at = :ts
                WHERE id = :id AND status = :executed
                """
            ),
            {"placed": bool(placed), "ts": now, "id": signal_id, "executed": STATUS_EXECUTED},
        )
        db.commit()
        if res.rowcount == 0:
            raise LookupError("executed signal not found")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return {"ok": True, "id": signal_id, "hard_stop_placed": bool(placed)}


def _parse_iso_date(value: Any, field: str) -> date:
    """Date-only helper (YYYY-MM-DD). Prefer ``_parse_iso_datetime`` for trade fields."""
    return _parse_iso_datetime(value, field).date()


def _parse_iso_datetime(value: Any, field: str) -> datetime:
    """Parse YYYY-MM-DD or YYYY-MM-DD[ T]HH:MM[:SS] as naive IST datetime.

    Date-only values become midnight (legacy DATE rows / date-only clients).
    """
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"{field} must be YYYY-MM-DD or YYYY-MM-DD HH:MM")
    s = raw.replace("T", " ", 1)
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1].strip()
    # Drop trailing timezone offset (+05:30 / -04:00) after the time portion.
    if len(s) > 10:
        tail = s[10:]
        for i, ch in enumerate(tail):
            if ch in "+-" and i > 0:
                s = (s[:10] + tail[:i]).strip()
                break
    s = " ".join(s.split())
    try:
        if len(s) >= 19 and s[10] == " ":
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        if len(s) >= 16 and s[10] == " ":
            return datetime.strptime(s[:16], "%Y-%m-%d %H:%M")
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"{field} must be YYYY-MM-DD or YYYY-MM-DD HH:MM") from e


def quote_exit_ltps(signal_id: int) -> Dict[str, Any]:
    """Live LTP for an Executed row at exit-popup open. Does not complete the trade."""
    ensure_stock_option_tables()
    now = now_ist_second()
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT id, status, date_traded, sell_cost, buy_cost,
                       sell_instrument_key, buy_instrument_key, sell_ltp, buy_ltp
                FROM stock_option_signals
                WHERE id = :id
                """
            ),
            {"id": signal_id},
        ).mappings().first()
        if not row:
            raise LookupError("signal not found")
        if (row.get("status") or "").strip().lower() != "executed":
            raise ValueError("only Executed rows can be quoted for exit")
        sell_key = str(row.get("sell_instrument_key") or "").strip()
        buy_key = str(row.get("buy_instrument_key") or "").strip()
        sell_ltp = _as_float(row.get("sell_ltp"))
        buy_ltp = _as_float(row.get("buy_ltp"))
        keys = [k for k in (sell_key, buy_key) if k]
        if keys:
            try:
                from backend.services.market_data.reads import ltp_map_with_fallback

                prices = ltp_map_with_fallback(keys, allow_broker_fallback=True) or {}
                if sell_key and prices.get(sell_key) is not None:
                    sell_ltp = _as_float(prices.get(sell_key))
                if buy_key and prices.get(buy_key) is not None:
                    buy_ltp = _as_float(prices.get(buy_key))
                if sell_ltp is not None or buy_ltp is not None:
                    db.execute(
                        text(
                            """
                            UPDATE stock_option_signals
                            SET sell_ltp = COALESCE(:sell_ltp, sell_ltp),
                                buy_ltp = COALESCE(:buy_ltp, buy_ltp),
                                ltp_updated_at = :ts,
                                updated_at = :ts
                            WHERE id = :id AND status = :executed
                            """
                        ),
                        {
                            "sell_ltp": sell_ltp,
                            "buy_ltp": buy_ltp,
                            "ts": now,
                            "id": signal_id,
                            "executed": STATUS_EXECUTED,
                        },
                    )
                    db.commit()
            except Exception:
                db.rollback()
                logger.info("stock_option exit quote failed for %s", signal_id)
        return {
            "ok": True,
            "id": signal_id,
            "sell_ltp": sell_ltp,
            "buy_ltp": buy_ltp,
            "quoted_at": _fmt_ts(now),
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def submit_exit(
    signal_id: int,
    *,
    date_traded: str,
    buy_strike: float,
    buy_cost: float,
    sell_strike: float,
    sell_cost: float,
    exit_date: str,
    sell_exit: float,
    buy_exit: float,
    trade_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Complete an Executed trade. Does not change status unless this Submit runs."""
    ensure_stock_option_tables()
    traded = _parse_iso_datetime(date_traded, "date_traded")
    exited = _parse_iso_datetime(exit_date, "exit_date")
    mode = normalize_trade_mode(trade_mode) if trade_mode is not None else TRADE_MODE_PAPER
    pnl = realized_credit_pnl(sell_cost, buy_cost, sell_exit, buy_exit)
    if pnl is None:
        raise ValueError("entry and exit prices are required")
    now = now_ist_second()
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT id, status, date_traded, sell_cost, buy_cost, trade_mode
                FROM stock_option_signals
                WHERE id = :id
                """
            ),
            {"id": signal_id},
        ).mappings().first()
        if not row:
            raise LookupError("signal not found")
        if (row.get("status") or "").strip().lower() != "executed":
            raise ValueError("only Executed rows can be completed")
        if trade_mode is None:
            mode = normalize_trade_mode(row.get("trade_mode"))
        db.execute(
            text(
                """
                UPDATE stock_option_signals
                SET status = :completed,
                    date_traded = :date_traded,
                    user_buy_strike = :buy_strike,
                    user_sell_strike = :sell_strike,
                    buy_cost = :buy_cost,
                    sell_cost = :sell_cost,
                    exit_date = :exit_date,
                    sell_exit_price = :sell_exit,
                    buy_exit_price = :buy_exit,
                    realized_pnl = :pnl,
                    trade_mode = :trade_mode,
                    updated_at = :ts
                WHERE id = :id AND status = :executed
                """
            ),
            {
                "completed": STATUS_COMPLETED,
                "date_traded": traded,
                "buy_strike": float(buy_strike),
                "sell_strike": float(sell_strike),
                "buy_cost": float(buy_cost),
                "sell_cost": float(sell_cost),
                "exit_date": exited,
                "sell_exit": float(sell_exit),
                "buy_exit": float(buy_exit),
                "pnl": float(pnl),
                "trade_mode": mode,
                "ts": now,
                "id": signal_id,
                "executed": STATUS_EXECUTED,
            },
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    _sync_executed_ws_ltp_subscriptions()
    return {
        "ok": True,
        "id": signal_id,
        "status": STATUS_COMPLETED,
        "combined_pnl": pnl,
        "trade_mode": mode,
        "hard_stop": hard_stop_price(sell_cost),
    }


def _optional_exit_bundle(
    exit_date: Any,
    sell_exit: Any,
    buy_exit: Any,
) -> Optional[Tuple[datetime, float, float]]:
    """All-or-nothing optional exit fields. Empty trio → None; partial → ValueError."""
    date_raw = str(exit_date or "").strip()
    sell_f = _as_float(sell_exit)
    buy_f = _as_float(buy_exit)
    any_set = bool(date_raw) or sell_f is not None or buy_f is not None
    if not any_set:
        return None
    if not date_raw or sell_f is None or buy_f is None:
        raise ValueError("exit date and both exit prices are required together")
    if sell_f < 0 or buy_f < 0:
        raise ValueError("exit prices must be >= 0")
    return (_parse_iso_datetime(date_raw, "exit_date"), float(sell_f), float(buy_f))


def update_trade(
    signal_id: int,
    *,
    date_traded: Optional[str] = None,
    buy_strike: Optional[float] = None,
    buy_cost: Optional[float] = None,
    sell_strike: Optional[float] = None,
    sell_cost: Optional[float] = None,
    exit_date: Optional[str] = None,
    sell_exit: Optional[float] = None,
    buy_exit: Optional[float] = None,
    trade_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Edit Executed/Completed fields without changing status. Partial updates OK.

    Omitted fields keep their existing DB values. Provided fields replace. Status never
    changes (Exit Submit is the path to Completed).
    """
    ensure_stock_option_tables()
    provided = {
        "date_traded": date_traded,
        "buy_strike": buy_strike,
        "buy_cost": buy_cost,
        "sell_strike": sell_strike,
        "sell_cost": sell_cost,
        "exit_date": exit_date,
        "sell_exit": sell_exit,
        "buy_exit": buy_exit,
        "trade_mode": trade_mode,
    }
    if all(v is None or (isinstance(v, str) and not str(v).strip()) for v in provided.values()):
        raise ValueError("provide at least one field to update")

    now = now_ist_second()
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT id, status, date_traded, user_buy_strike, user_sell_strike,
                       buy_cost, sell_cost, exit_date, sell_exit_price, buy_exit_price,
                       realized_pnl, trade_mode
                FROM stock_option_signals
                WHERE id = :id
                """
            ),
            {"id": signal_id},
        ).mappings().first()
        if not row:
            raise LookupError("signal not found")
        st = (row.get("status") or "").strip().lower()
        if st not in ("executed", "completed"):
            raise ValueError("only Executed or Completed rows can be edited")

        # Merge: provided wins; blank string for exit_date treated as omit.
        date_raw = date_traded if date_traded is not None and str(date_traded).strip() else None
        if date_raw is not None:
            traded = _parse_iso_datetime(date_raw, "date_traded")
        else:
            traded = row.get("date_traded")
            if isinstance(traded, date) and not isinstance(traded, datetime):
                traded = datetime.combine(traded, dt_time(0, 0, 0))
            if traded is None:
                raise ValueError("date_traded is required")

        buy_s = float(buy_strike) if buy_strike is not None else _as_float(row.get("user_buy_strike"))
        sell_s = float(sell_strike) if sell_strike is not None else _as_float(row.get("user_sell_strike"))
        buy_c = float(buy_cost) if buy_cost is not None else _as_float(row.get("buy_cost"))
        sell_c = float(sell_cost) if sell_cost is not None else _as_float(row.get("sell_cost"))

        if buy_s is None or sell_s is None or not (buy_s > 0 and sell_s > 0):
            raise ValueError("strikes must be > 0")
        if buy_c is None or sell_c is None or buy_c < 0 or sell_c < 0:
            raise ValueError("entry costs must be >= 0")

        exit_date_raw = exit_date if exit_date is not None and str(exit_date).strip() else None
        if exit_date_raw is not None:
            exited = _parse_iso_datetime(exit_date_raw, "exit_date")
        else:
            exited = row.get("exit_date")
            if isinstance(exited, date) and not isinstance(exited, datetime):
                exited = datetime.combine(exited, dt_time(0, 0, 0))

        sell_x = float(sell_exit) if sell_exit is not None else _as_float(row.get("sell_exit_price"))
        buy_x = float(buy_exit) if buy_exit is not None else _as_float(row.get("buy_exit_price"))
        if sell_exit is not None and sell_exit < 0:
            raise ValueError("exit prices must be >= 0")
        if buy_exit is not None and buy_exit < 0:
            raise ValueError("exit prices must be >= 0")

        if trade_mode is not None and str(trade_mode).strip():
            mode = normalize_trade_mode(trade_mode)
        else:
            mode = normalize_trade_mode(row.get("trade_mode"))

        pnl = realized_credit_pnl(sell_c, buy_c, sell_x, buy_x)
        if pnl is None:
            pnl = row.get("realized_pnl")

        status_const = STATUS_COMPLETED if st == "completed" else STATUS_EXECUTED
        db.execute(
            text(
                """
                UPDATE stock_option_signals
                SET date_traded = :date_traded,
                    user_buy_strike = :buy_strike,
                    user_sell_strike = :sell_strike,
                    buy_cost = :buy_cost,
                    sell_cost = :sell_cost,
                    exit_date = :exit_date,
                    sell_exit_price = :sell_exit,
                    buy_exit_price = :buy_exit,
                    realized_pnl = :pnl,
                    trade_mode = :trade_mode,
                    remarks = CASE
                        WHEN status = :executed_status
                             AND remarks LIKE :expiry_like THEN NULL
                        ELSE remarks
                    END,
                    updated_at = :ts
                WHERE id = :id AND status = :status
                """
            ),
            {
                "date_traded": traded,
                "buy_strike": float(buy_s),
                "sell_strike": float(sell_s),
                "buy_cost": float(buy_c),
                "sell_cost": float(sell_c),
                "exit_date": exited,
                "sell_exit": float(sell_x) if sell_x is not None else None,
                "buy_exit": float(buy_x) if buy_x is not None else None,
                "pnl": float(pnl) if pnl is not None else None,
                "trade_mode": mode,
                "ts": now,
                "id": signal_id,
                "status": status_const,
                "executed_status": STATUS_EXECUTED,
                "expiry_like": f"%{EXPIRY_REMARKS}%",
            },
        )
        db.commit()
        if st == "executed":
            try:
                ensure_executed_option_instrument_keys(signal_id)
            except Exception:
                logger.debug("stock_option key resolve after update failed", exc_info=True)
            _sync_executed_ws_ltp_subscriptions()
        return {
            "ok": True,
            "id": signal_id,
            "status": status_const,
            "combined_pnl": float(pnl) if pnl is not None else None,
            "trade_mode": mode,
            "hard_stop": hard_stop_price(sell_c),
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
