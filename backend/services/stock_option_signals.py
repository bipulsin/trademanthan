"""Stock Options algo: ChartInk webhook ingest, Upstox WR(280), 2h EMA arm/invalidate.

Permanent indices NIFTY/BANKNIFTY (arbitrage_master): always on Radar, no WR gate,
arm only on EMA9 cross vs EMA30+EMA100, ~15Δ/~2Δ spreads, re-seed Radar after Executed.
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
_ENSURED = False

STATUS_RADAR = "Radar"
STATUS_ACTIVE = "Active"
STATUS_EXECUTED = "Executed"
STATUS_COMPLETED = "Completed"
STATUS_REJECTED = "Rejected"
SIDE_BEAR = "BEAR CALL"
SIDE_BULL = "BULL PUT"
INVALIDATE_REMARKS = "Trade not executed for this symbol"
ACTIVE_MAX_HOURS = 72
EXPIRY_REMARKS = "Auto-expired after 72 hours from armed time"

_MONTH_NUM = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_MONTH_ABBR = {v: k for k, v in _MONTH_NUM.items()}

WR_PERIOD = 280
WR_BEAR_GT = -3.0
WR_BULL_LT = -97.0
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
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS exit_date DATE",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS sell_exit_price DOUBLE PRECISION",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS buy_exit_price DOUBLE PRECISION",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS realized_pnl DOUBLE PRECISION",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS contract_mmm_yyyy TEXT",
            "ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS ema_fetch_ok BOOLEAN",
        ):
            conn.execute(text(stmt))
        if _CONTRACT_MIGRATION.is_file():
            conn.execute(text(_CONTRACT_MIGRATION.read_text(encoding="utf-8")))
    backfill_contract_mmm_yyyy()
    _ENSURED = True
    logger.info("stock_option tables ensured")


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
    """> -3 → BEAR CALL; < -97 → BULL PUT. -3 and -97 themselves do not qualify."""
    try:
        wr = float(williamsr)
    except (TypeError, ValueError):
        return None
    if wr > WR_BEAR_GT:
        return SIDE_BEAR
    if wr < WR_BULL_LT:
        return SIDE_BULL
    return None


def should_insert_new_signal(existing_statuses: Sequence[str]) -> bool:
    """New Radar only when no Radar/Active row exists for the symbol.

    First trigger (no rows) inserts. A later trigger inserts only when every
    existing row is Executed. Any Radar or Active row ignores the new trigger.
    """
    for raw in existing_statuses or []:
        st = (raw or "").strip().lower()
        if st in ("radar", "active"):
            return False
    return True


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
    """Index arm side from EMA9 cross vs EMA30+EMA100 on consecutive completed 2h bars.

    BULL PUT: previous bar EMA9 was *not* above both; current bar EMA9 is *strictly*
    above both EMA30 and EMA100.
    BEAR CALL: previous bar EMA9 was *not* below both; current is *strictly* below both.
    Hold (condition already true on previous bar) does not qualify — that is stocks only.
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


def next_ema_action(
    status: str,
    side: Optional[str],
    ema9: Any,
    ema30: Any,
    ema100: Any,
    trade_submitted: bool,
) -> str:
    """Stock path: arm | invalidate | hold when EMA *condition holds* (not cross).

    Incomplete EMAs never arm or invalidate. Indices use ``next_index_ema_action``.
    """
    st = (status or "").strip().lower()
    incomplete = ema9 is None or ema30 is None or ema100 is None
    holds = (not incomplete) and ema_condition_holds(side, ema9, ema30, ema100)
    if st == "radar":
        if holds:
            return "arm"
        return "hold"
    if st == "active":
        if trade_submitted or incomplete:
            return "hold"
        if not holds:
            return "invalidate"
        return "hold"
    return "hold"


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
    """Index path: Radar arms only on EMA *cross*; Active invalidates when hold fails.

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
            return "invalidate", None
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
    """Credit received (sell − buy) minus current close cost (sell LTP − buy LTP).

    Same formula as realized_credit_pnl: sell_entry − buy_entry − (sell_mark − buy_mark).
    """
    sc = _as_float(sell_cost)
    bc = _as_float(buy_cost)
    sl = _as_float(sell_ltp)
    bl = _as_float(buy_ltp)
    if sc is None or bc is None or sl is None or bl is None:
        return None
    credit = sc - bc
    close_cost = sl - bl
    return credit - close_cost


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
    """2h bucket start aligned to 09:15 IST. None outside the cash session buckets."""
    local = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    start = local.replace(hour=9, minute=15, second=0, microsecond=0)
    if local < start:
        return None
    elapsed_min = (local - start).total_seconds() / 60.0
    if elapsed_min >= 6 * 60:
        return None
    idx = int(elapsed_min // BAR_MINUTES)
    if idx < 0 or idx > 2:
        return None
    return start + timedelta(minutes=idx * BAR_MINUTES)


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
        bucket_end = bucket + timedelta(minutes=BAR_MINUTES)
        if now_ist < bucket_end:
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
        if now_ist < bucket + timedelta(minutes=BAR_MINUTES):
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
    """Completed 2h candles as {end, high, low, close}. end = bucket start + 120m."""
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
        out.append({"end": naive_ist(ts + timedelta(minutes=BAR_MINUTES)), "high": h, "low": lo, "close": c})
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
    """hours/1 chunks aggregated to completed 09:15 2h bars, same window as WR(280) backfill."""
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    asof_n = naive_ist(asof)
    asof_d = asof_n.date()
    ends = [asof_d - timedelta(days=55 * i) for i in range(5)]
    chunks: List[List[Dict[str, Any]]] = []
    for end_d in ends:
        kwargs: Dict[str, Any] = {"interval": "hours/1", "days_back": 60}
        if end_d < asof_d:
            kwargs["range_end_date"] = end_d
        raw = None
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                raw = u.get_historical_candles_by_instrument_key(instrument_key, **kwargs)
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(1.5 * (attempt + 1))
        if last_err is not None:
            logger.info("stock_option hours/1 end=%s failed %s: %s", end_d, instrument_key, last_err)
        chunks.append(raw or [])
        time.sleep(FETCH_SLEEP_SEC)

    by_ts: Dict[str, Dict[str, Any]] = {}
    for chunk in chunks:
        for c in chunk or []:
            key = str(c.get("timestamp") or "")
            if key:
                by_ts[key] = c
    merged = [by_ts[k] for k in sorted(by_ts)]
    agg = aggregate_intraday_to_2h(merged, now=IST.localize(asof_n))
    return completed_2h_ohlc(agg)


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
    if not should_insert_new_signal(_statuses_for_symbol(db, sym)):
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
    ensure_stock_option_tables()
    status, candidates, meta = parse_chartink_symbols(parsed)
    payload_json = json.dumps(raw_payload if raw_payload is not None else {})
    inserted = 0
    ignored = 0
    discarded = 0
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
        seen: set[str] = set()
        for cand in candidates:
            sym = cand["symbol"]
            if sym in seen:
                continue
            seen.add(sym)
            # Permanent indices are seeded/managed by the 2h job — never ChartInk.
            if is_index_symbol(sym):
                ignored += 1
                continue
            statuses = _statuses_for_symbol(db, sym)
            if not should_insert_new_signal(statuses):
                ignored += 1
                continue
            ik = resolve_equity_instrument_key(sym, db)
            wr = None
            if ik:
                try:
                    wr = compute_williams_r_280(ik, received_at)
                except Exception as e:
                    logger.info("stock_option WR(280) fetch failed for %s: %s", sym, e)
                    wr = None
            side = side_from_williamsr(wr)
            if side is None:
                discarded += 1
                logger.info(
                    "stock_option rejected %s: computed WR(280)=%s (webhook williamsr ignored)",
                    sym,
                    wr,
                )
                continue
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
                    "symbol": sym,
                    "williamsr": wr,
                    "instrument_key": ik,
                    "status": STATUS_RADAR,
                    "side": side,
                    "trigger_at": received_at,
                    "triggered_at_raw": meta.get("triggered_at_raw"),
                    "scan_name": meta.get("scan_name"),
                    "alert_name": meta.get("alert_name"),
                    "raw_payload": payload_json,
                    "created_at": received_at,
                    "updated_at": received_at,
                },
            )
            inserted += 1
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return {
        "parse_status": status,
        "inserted": inserted,
        "ignored": ignored,
        "discarded": discarded,
        "candidates": len(candidates),
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


def _fetch_2h_closes(instrument_key: str, now: Optional[datetime] = None) -> List[float]:
    """REST historical only. Prefer hours/2; else hours/1, minutes/15, then 10m/5m."""
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    native = _upstox_candles_with_retry(
        u, instrument_key, interval="hours/2", days_back=120
    )
    bars = completed_2h_bars(native, now=now)
    if len(bars) >= EMA_SLOW:
        return [float(b["close"]) for b in bars]

    one = _upstox_candles_with_retry(
        u, instrument_key, interval="hours/1", days_back=60
    )
    agg = aggregate_intraday_to_2h(one, now=now)
    if len(agg) >= EMA_SLOW:
        return [float(b["close"]) for b in agg]
    if len(agg) >= EMA_MID:
        return [float(b["close"]) for b in agg]

    fifteen = _upstox_candles_with_retry(
        u, instrument_key, interval="minutes/15", days_back=60
    )
    agg15 = aggregate_intraday_to_2h(fifteen, now=now)
    best = agg15 if len(agg15) > len(agg) else agg
    if len(best) >= EMA_SLOW:
        return [float(b["close"]) for b in best]

    # Last resort: Kavach-style 10m (5m paired) → 2h.
    from10 = _fetch_2h_closes_from_10m_fallback(instrument_key, now=now)
    if len(from10) > len(best):
        return from10
    if best:
        return [float(b["close"]) for b in best]
    if bars:
        return [float(b["close"]) for b in bars]
    if from10:
        return from10
    return [float(b["close"]) for b in agg15]


def _mark_ema_fetch_failed(row_id: int, now_naive: datetime) -> None:
    """Clear EMA display values and flag fetch failure for this cycle."""
    db = SessionLocal()
    try:
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


def _open_rows() -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, instrument_key, status, side,
                       armed_at, ema9, ema30, ema100, sell_strike, buy_strike,
                       date_traded, sell_cost, buy_cost
                FROM stock_option_signals
                WHERE status IN (:radar, :active)
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


def run_ema_tick(now: Optional[datetime] = None) -> Dict[str, Any]:
    """2h job: seed index Radar, expire stale Active, update EMAs, arm, invalidate, fill spreads.

    NIFTY/BANKNIFTY: no WR gate; Radar→Active only on EMA9 cross vs EMA30+EMA100.
    Stocks: WR side already set; arm when EMA condition holds.
    """
    ensure_stock_option_tables()
    from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

    if defer_job_for_breakfast_exclusivity("stock_option_ema"):
        return {"ok": True, "skipped": "breakfast_exclusivity"}

    now_naive = now_ist_second() if now is None else naive_ist(now)
    seeded = ensure_index_radar_rows(now_naive)
    expired = expire_stale_active(now_naive)
    # Indices first so permanent NIFTY/BANKNIFTY EMA/arm is not starved by late-loop
    # Upstox empty responses after many stock candle fetches.
    rows = _pin_index_rows(_open_rows())
    updated = armed = invalidated = failed = 0
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
            _mark_ema_fetch_failed(int(row["id"]), now_naive)
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
            _mark_ema_fetch_failed(int(row["id"]), now_naive)
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
        index_sym = is_index_symbol(row.get("symbol"))
        pair = ema_snapshot_pair(closes) if index_sym else None
        wr_now = None
        if not index_sym:
            try:
                wr_now = compute_williams_r_280(ik, now_naive)
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
            action = next_ema_action(
                row.get("status") or "",
                row.get("side"),
                snap["ema9"],
                snap["ema30"],
                snap["ema100"],
                submitted,
            )
        db = SessionLocal()
        try:
            if action == "invalidate":
                dest = invalidate_outcome(
                    row.get("armed_at"),
                    row.get("sell_strike"),
                    row.get("buy_strike"),
                )
                # Keep suggested strikes when both arm date and strikes exist.
                # Never wipe them, and never invent replacements. No LTP snapshot
                # unless one was already stored at arm time.
                db.execute(
                    text(
                        """
                        UPDATE stock_option_signals
                        SET status = :dest,
                            ema9 = :ema9, ema30 = :ema30, ema100 = :ema100,
                            ema_updated_at = :ts,
                            ema_fetch_ok = TRUE,
                            williamsr = COALESCE(:williamsr, williamsr),
                            remarks = :remarks,
                            updated_at = :ts
                        WHERE id = :id AND status = :active AND date_traded IS NULL
                        """
                    ),
                    {
                        "dest": dest,
                        "ema9": snap["ema9"],
                        "ema30": snap["ema30"],
                        "ema100": snap["ema100"],
                        "williamsr": wr_now,
                        "ts": now_naive,
                        "remarks": INVALIDATE_REMARKS,
                        "id": row["id"],
                        "active": STATUS_ACTIVE,
                    },
                )
                invalidated += 1
                if index_sym:
                    ensure_index_radar_row(db, row.get("symbol") or "", now=now_naive)
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
            _mark_ema_fetch_failed(int(row["id"]), now_naive)
            failed += 1
        finally:
            db.close()

        if action != "invalidate" and (
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

    ltp_n = refresh_executed_ltps(now_naive)
    return {
        "ok": True,
        "open_rows": len(rows),
        "updated": updated,
        "armed": armed,
        "invalidated": invalidated,
        "ema_fetch_failed": failed,
        "expired_72h": expired,
        "index_radar_seeded": seeded,
        "ltp_refreshed": ltp_n,
    }


def refresh_executed_ltps(now: Optional[datetime] = None) -> int:
    """Quote LTPs for submitted Executed legs. No websocket."""
    ensure_stock_option_tables()
    now_naive = now_ist_second() if now is None else naive_ist(now)
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
                  AND remarks IS DISTINCT FROM :remarks
                  AND COALESCE(remarks, '') NOT LIKE :expiry
                """
            ),
            {
                "executed": STATUS_EXECUTED,
                "remarks": INVALIDATE_REMARKS,
                "expiry": f"%{EXPIRY_REMARKS}%",
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
            sl = prices.get(sk)
            bl = prices.get(bk)
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
        "combined_pnl": pnl,
        "hard_stop": hs,
        "hard_stop_placed": bool(r.get("hard_stop_placed")),
        "remarks": r.get("remarks"),
    }


def _pin_index_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """NIFTY then BANKNIFTY first; remaining rows keep relative order."""
    order = {s: i for i, s in enumerate(INDEX_PIN_ORDER)}
    pinned: List[Dict[str, Any]] = []
    rest: List[Dict[str, Any]] = []
    for item in rows:
        sym = _norm_symbol(item.get("symbol"))
        if sym in order:
            pinned.append(item)
        else:
            rest.append(item)
    pinned.sort(key=lambda r: order.get(_norm_symbol(r.get("symbol")), 99))
    return pinned + rest


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
                       ema_updated_at, ema_fetch_ok,
                       sell_strike, sell_delta, buy_strike, buy_delta,
                       date_traded, sell_cost, buy_cost,
                       user_sell_strike, user_buy_strike, hard_stop_placed,
                       remarks, sell_ltp, buy_ltp, sell_instrument_key,
                       exit_date, sell_exit_price, buy_exit_price, realized_pnl
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
    try:
        return date.fromisoformat(str(value or "").strip()[:10])
    except (ValueError, TypeError) as e:
        raise ValueError(f"{field} must be YYYY-MM-DD") from e


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
) -> Dict[str, Any]:
    """Complete an Executed trade. Does not change status unless this Submit runs."""
    ensure_stock_option_tables()
    traded = _parse_iso_date(date_traded, "date_traded")
    exited = _parse_iso_date(exit_date, "exit_date")
    pnl = realized_credit_pnl(sell_cost, buy_cost, sell_exit, buy_exit)
    if pnl is None:
        raise ValueError("entry and exit prices are required")
    now = now_ist_second()
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT id, status, date_traded, sell_cost, buy_cost
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
    return {
        "ok": True,
        "id": signal_id,
        "status": STATUS_COMPLETED,
        "combined_pnl": pnl,
        "hard_stop": hard_stop_price(sell_cost),
    }


def _optional_exit_bundle(
    exit_date: Any,
    sell_exit: Any,
    buy_exit: Any,
) -> Optional[Tuple[date, float, float]]:
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
    return (_parse_iso_date(date_raw, "exit_date"), float(sell_f), float(buy_f))


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
                       realized_pnl
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
            traded = _parse_iso_date(date_raw, "date_traded")
        else:
            traded = row.get("date_traded")
            if isinstance(traded, datetime):
                traded = traded.date()
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
            exited = _parse_iso_date(exit_date_raw, "exit_date")
        else:
            exited = row.get("exit_date")
            if isinstance(exited, datetime):
                exited = exited.date()

        sell_x = float(sell_exit) if sell_exit is not None else _as_float(row.get("sell_exit_price"))
        buy_x = float(buy_exit) if buy_exit is not None else _as_float(row.get("buy_exit_price"))
        if sell_exit is not None and sell_exit < 0:
            raise ValueError("exit prices must be >= 0")
        if buy_exit is not None and buy_exit < 0:
            raise ValueError("exit prices must be >= 0")

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
                "ts": now,
                "id": signal_id,
                "status": status_const,
            },
        )
        db.commit()
        return {
            "ok": True,
            "id": signal_id,
            "status": status_const,
            "combined_pnl": float(pnl) if pnl is not None else None,
            "hard_stop": hard_stop_price(sell_c),
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
