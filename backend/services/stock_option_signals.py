"""Stock Options algo: ChartInk webhook ingest, 2h EMA arm/invalidate, credit spreads."""
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
_ENSURED = False

STATUS_RADAR = "Radar"
STATUS_ACTIVE = "Active"
STATUS_EXECUTED = "Executed"
SIDE_BEAR = "BEAR CALL"
SIDE_BULL = "BULL PUT"
INVALIDATE_REMARKS = "Trade not executed for this symbol"

WR_BEAR_GT = -3.0
WR_BULL_LT = -97.0
DELTA_SELL = 28.0
DELTA_BUY = 18.0
EMA_FAST = 9
EMA_MID = 30
EMA_SLOW = 100
SESSION_OPEN = dt_time(9, 15)
BAR_MINUTES = 120
FETCH_SLEEP_SEC = 0.4

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
COLUMN_KEYS = ("symbol", "williamsr")
WR_ALIASES = ("williamsr", "williams_r", "williamsR", "williams_R")


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
    _ENSURED = True
    logger.info("stock_option tables ensured")


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


def _wr_from_column(col: Dict[str, Any]) -> Any:
    for key in WR_ALIASES:
        if key in col and col.get(key) is not None and str(col.get(key)).strip() != "":
            return col.get(key)
    return None


def parse_chartink_symbols(parsed: Any) -> Tuple[str, List[Dict[str, Any]], Dict[str, Optional[str]]]:
    """Return (parse_status, candidates, meta). candidates have symbol + williamsr."""
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
            rows.append({"symbol": sym, "williamsr": _wr_from_column(col)})
    else:
        stocks = parsed.get("stocks")
        parts: List[str] = []
        if isinstance(stocks, list):
            parts = [_norm_symbol(x) for x in stocks if _norm_symbol(x)]
        elif stocks is not None:
            parts = [_norm_symbol(p) for p in str(stocks).split(",") if _norm_symbol(p)]
        top_wr = parsed.get("williamsr", parsed.get("williams_r"))
        for sym in parts:
            rows.append({"symbol": sym, "williamsr": top_wr})

    if not rows:
        return "failed", [], meta
    usable = [r for r in rows if side_from_williamsr(r.get("williamsr")) is not None]
    if not usable:
        return "partial", rows, meta
    if len(usable) < len(rows):
        return "partial", rows, meta
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


def next_ema_action(
    status: str,
    side: Optional[str],
    ema9: Any,
    ema30: Any,
    ema100: Any,
    trade_submitted: bool,
) -> str:
    """arm | invalidate | hold. Incomplete EMAs never arm or invalidate."""
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


def pick_credit_spread(chain: Any, side: str) -> Optional[Dict[str, Any]]:
    call = side == SIDE_BEAR
    if side not in (SIDE_BEAR, SIDE_BULL):
        return None
    legs = legs_from_chain(chain, call=call)
    if not legs:
        return None
    sell = pick_nearest_delta(legs, DELTA_SELL)
    if not sell:
        return None
    buy = pick_nearest_delta(legs, DELTA_BUY, exclude_strike=sell["strike"])
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


def spread_lines(side: Optional[str], sell_strike: Any, buy_strike: Any) -> Tuple[Optional[str], Optional[str]]:
    code = option_code(side)
    if not code or sell_strike is None or buy_strike is None:
        return None, None
    return (
        f"Sell {code}:~28 Δ - {fmt_strike(sell_strike)}",
        f"Buy {code}~18 Δ {fmt_strike(buy_strike)}",
    )


def combined_pnl(sell_cost: Any, buy_cost: Any, sell_ltp: Any, buy_ltp: Any) -> Optional[float]:
    """Credit received (sell − buy) minus current close cost (sell LTP − buy LTP)."""
    sc = _as_float(sell_cost)
    bc = _as_float(buy_cost)
    sl = _as_float(sell_ltp)
    bl = _as_float(buy_ltp)
    if sc is None or bc is None or sl is None or bl is None:
        return None
    credit = sc - bc
    close_cost = sl - bl
    return credit - close_cost


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


def _statuses_for_symbol(db: Any, symbol: str) -> List[str]:
    rows = db.execute(
        text("SELECT status FROM stock_option_signals WHERE symbol = :s"),
        {"s": symbol},
    ).fetchall()
    return [str(r[0]) for r in rows]


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
            side = side_from_williamsr(cand.get("williamsr"))
            if side is None:
                discarded += 1
                continue
            statuses = _statuses_for_symbol(db, sym)
            if not should_insert_new_signal(statuses):
                ignored += 1
                continue
            wr = _as_float(cand.get("williamsr"))
            ik = resolve_equity_instrument_key(sym, db)
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


def _fetch_2h_closes(instrument_key: str, now: Optional[datetime] = None) -> List[float]:
    """REST historical only. Prefer hours/2; else aggregate hours/1, then minutes/15."""
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    native = None
    try:
        native = u.get_historical_candles_by_instrument_key(
            instrument_key, interval="hours/2", days_back=120
        )
    except Exception as e:
        logger.info("stock_option hours/2 fetch failed for %s: %s", instrument_key, e)
        native = None
    bars = completed_2h_bars(native or [], now=now)
    if len(bars) >= EMA_SLOW:
        return [float(b["close"]) for b in bars]

    one = None
    try:
        one = u.get_historical_candles_by_instrument_key(
            instrument_key, interval="hours/1", days_back=60
        )
    except Exception as e:
        logger.info("stock_option hours/1 fetch failed for %s: %s", instrument_key, e)
        one = None
    agg = aggregate_intraday_to_2h(one or [], now=now)
    if len(agg) >= EMA_SLOW:
        return [float(b["close"]) for b in agg]
    if len(agg) >= EMA_MID:
        return [float(b["close"]) for b in agg]

    fifteen = None
    try:
        fifteen = u.get_historical_candles_by_instrument_key(
            instrument_key, interval="minutes/15", days_back=60
        )
    except Exception as e:
        logger.info("stock_option minutes/15 fetch failed for %s: %s", instrument_key, e)
        fifteen = None
    agg15 = aggregate_intraday_to_2h(fifteen or [], now=now)
    if len(agg15) > len(agg):
        return [float(b["close"]) for b in agg15]
    if agg:
        return [float(b["close"]) for b in agg]
    if bars:
        return [float(b["close"]) for b in bars]
    return [float(b["close"]) for b in agg15]


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
    spread = pick_credit_spread(chain, row.get("side"))
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
                       ema9, ema30, ema100, sell_strike, buy_strike,
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


def run_ema_tick(now: Optional[datetime] = None) -> Dict[str, Any]:
    """2h job: update EMAs, arm, invalidate, fill blank Active spreads. REST only."""
    ensure_stock_option_tables()
    from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

    if defer_job_for_breakfast_exclusivity("stock_option_ema"):
        return {"ok": True, "skipped": "breakfast_exclusivity"}

    now_naive = now_ist_second() if now is None else naive_ist(now)
    rows = _open_rows()
    updated = armed = invalidated = 0
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
            continue
        try:
            closes = _fetch_2h_closes(ik, now=now or datetime.now(IST))
        except Exception as e:
            logger.info("stock_option 2h candles failed for %s: %s", row.get("symbol"), e)
            time.sleep(FETCH_SLEEP_SEC)
            continue
        snap = ema_snapshot(closes)
        submitted = trade_is_submitted(row.get("date_traded"), row.get("sell_cost"), row.get("buy_cost"))
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
                db.execute(
                    text(
                        """
                        UPDATE stock_option_signals
                        SET status = :executed,
                            ema9 = :ema9, ema30 = :ema30, ema100 = :ema100,
                            ema_updated_at = :ts,
                            sell_strike = NULL, sell_delta = NULL, sell_instrument_key = NULL,
                            buy_strike = NULL, buy_delta = NULL, buy_instrument_key = NULL,
                            sell_cost = NULL, buy_cost = NULL,
                            user_sell_strike = NULL, user_buy_strike = NULL,
                            remarks = :remarks,
                            updated_at = :ts
                        WHERE id = :id AND status = :active AND date_traded IS NULL
                        """
                    ),
                    {
                        "executed": STATUS_EXECUTED,
                        "ema9": snap["ema9"],
                        "ema30": snap["ema30"],
                        "ema100": snap["ema100"],
                        "ts": now_naive,
                        "remarks": INVALIDATE_REMARKS,
                        "id": row["id"],
                        "active": STATUS_ACTIVE,
                    },
                )
                invalidated += 1
            elif action == "arm":
                db.execute(
                    text(
                        """
                        UPDATE stock_option_signals
                        SET status = :active,
                            armed_at = COALESCE(armed_at, :ts),
                            ema9 = :ema9, ema30 = :ema30, ema100 = :ema100,
                            ema_updated_at = :ts,
                            updated_at = :ts
                        WHERE id = :id AND status = :radar
                        """
                    ),
                    {
                        "active": STATUS_ACTIVE,
                        "ema9": snap["ema9"],
                        "ema30": snap["ema30"],
                        "ema100": snap["ema100"],
                        "ts": now_naive,
                        "id": row["id"],
                        "radar": STATUS_RADAR,
                    },
                )
                armed += 1
                row["status"] = STATUS_ACTIVE
            else:
                db.execute(
                    text(
                        """
                        UPDATE stock_option_signals
                        SET ema9 = COALESCE(:ema9, ema9),
                            ema30 = COALESCE(:ema30, ema30),
                            ema100 = COALESCE(:ema100, ema100),
                            ema_updated_at = :ts,
                            updated_at = :ts
                        WHERE id = :id
                        """
                    ),
                    {
                        "ema9": snap["ema9"],
                        "ema30": snap["ema30"],
                        "ema100": snap["ema100"],
                        "ts": now_naive,
                        "id": row["id"],
                    },
                )
            db.commit()
            updated += 1
        except Exception:
            db.rollback()
            logger.exception("stock_option ema update failed for %s", row.get("symbol"))
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
                  AND remarks IS DISTINCT FROM :remarks
                """
            ),
            {"executed": STATUS_EXECUTED, "remarks": INVALIDATE_REMARKS},
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
    sell_line, buy_line = spread_lines(r.get("side"), r.get("sell_strike"), r.get("buy_strike"))
    user_sell = r.get("user_sell_strike") if r.get("user_sell_strike") is not None else r.get("sell_strike")
    user_buy = r.get("user_buy_strike") if r.get("user_buy_strike") is not None else r.get("buy_strike")
    pnl = combined_pnl(r.get("sell_cost"), r.get("buy_cost"), r.get("sell_ltp"), r.get("buy_ltp"))
    hs = hard_stop_price(r.get("sell_cost"))
    return {
        "id": r.get("id"),
        "symbol": r.get("symbol"),
        "williamsr": r.get("williamsr"),
        "instrument_key": r.get("instrument_key"),
        "status": r.get("status"),
        "side": r.get("side"),
        "trigger_at": _fmt_ts(r.get("trigger_at")),
        "armed_at": _fmt_ts(r.get("armed_at")),
        "ema9": r.get("ema9"),
        "ema30": r.get("ema30"),
        "ema100": r.get("ema100"),
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
        "combined_pnl": pnl,
        "hard_stop": hs,
        "hard_stop_placed": bool(r.get("hard_stop_placed")),
        "remarks": r.get("remarks"),
    }


def list_workspace() -> Dict[str, Any]:
    ensure_stock_option_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, williamsr, instrument_key, status, side,
                       trigger_at, armed_at, ema9, ema30, ema100,
                       sell_strike, sell_delta, buy_strike, buy_delta,
                       date_traded, sell_cost, buy_cost,
                       user_sell_strike, user_buy_strike, hard_stop_placed,
                       remarks, sell_ltp, buy_ltp
                FROM stock_option_signals
                ORDER BY id DESC
                """
            )
        ).mappings().all()
    finally:
        db.close()
    radar, active, executed = [], [], []
    for raw in rows:
        item = _row_public(dict(raw))
        st = (item.get("status") or "").strip().lower()
        if st == "radar":
            radar.append(item)
        elif st == "active":
            active.append(item)
        elif st == "executed":
            executed.append(item)
    return {"radar": radar, "active": active, "executed": executed}


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
            text("SELECT id, status, date_traded FROM stock_option_signals WHERE id = :id"),
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
