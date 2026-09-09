"""TradingView ingest for Premium Futures (dailyfutures.html) — persist + ChartInk-parity picks."""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.ist_datetime import naive_ist

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
_MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "add_premium_futures_tv_webhook_log.sql"
_ENSURED = False

PARSE_SUCCESS = "success"
PARSE_UNMATCHED = "unmatched"
PARSE_FAILED = "failed"

TV_PICK_LIMIT_PER_SIDE = 8
# Premium Futures chart/workspace uses 15m; alert candle is the 15m bar that contains received_at.
TV_ALERT_CANDLE_INTERVAL = "minutes/15"

RECOMMENDED_ALERT_JSON = (
    '{"ticker":"{{ticker}}","action":"{{strategy.order.action}}",'
    '"side":"{{strategy.market_position}}","message":"BULLISH"}'
)

_EXCHANGE_PREFIX = re.compile(
    r"^(NSE|BSE|NFO|MCX|BINANCE|BYBIT|COINBASE|NYSE|NASDAQ|AMEX)\s*:\s*",
    re.I,
)
_CONT_FUT = re.compile(r"\d+!$")
_WORD_BULL = re.compile(r"\b(BULLISH|LONG|BUY)\b", re.I)
_WORD_BEAR = re.compile(r"\b(BEARISH|SHORT|SELL)\b", re.I)


def now_ist_second() -> datetime:
    return naive_ist(datetime.now(IST))


def ensure_premium_futures_tv_webhook_table() -> None:
    global _ENSURED
    if _ENSURED:
        return
    if not _MIGRATION.is_file():
        raise FileNotFoundError(f"migration missing: {_MIGRATION}")
    sql = _MIGRATION.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
        conn.execute(
            text(
                "ALTER TABLE premium_futures_tv_webhook_log "
                "ADD COLUMN IF NOT EXISTS promoted_at TIMESTAMP WITHOUT TIME ZONE"
            )
        )
    _ENSURED = True
    logger.info("premium_futures_tv_webhook_log table ensured")


def decode_raw_payload(body: bytes) -> Tuple[Any, Dict[str, Any], str]:
    text_body = body.decode("utf-8", errors="replace") if body else ""
    if not text_body.strip():
        return None, {}, ""
    try:
        parsed = json.loads(text_body)
        if isinstance(parsed, dict):
            return parsed, parsed, text_body
        return parsed, {"_json": parsed}, text_body
    except (json.JSONDecodeError, ValueError):
        return None, {"_raw": text_body}, text_body


def normalize_tv_ticker(raw: Optional[str]) -> Optional[str]:
    """NSE:RELIANCE / RELIANCE / RELIANCE1! → RELIANCE. Empty / unexpanded templates → None."""
    if raw is None:
        return None
    s = str(raw).strip().strip('"').strip("'").upper()
    if not s:
        return None
    if "{{" in s and "}}" in s:
        return None
    s = _EXCHANGE_PREFIX.sub("", s).strip()
    if ":" in s:
        s = s.split(":")[-1].strip()
    s = _CONT_FUT.sub("", s).replace("!", "").strip()
    if " FUT" in s:
        s = s.split(" FUT", 1)[0].strip()
    s = re.sub(r"\s+", "", s)
    if s.endswith("-EQ"):
        s = s[:-3]
    s = s.strip()
    return s or None


def _blob_text(*parts: Any) -> str:
    chunks: List[str] = []
    for p in parts:
        if p is None:
            continue
        if isinstance(p, dict):
            try:
                chunks.append(json.dumps(p))
            except (TypeError, ValueError):
                chunks.append(str(p))
        else:
            chunks.append(str(p))
    return " ".join(chunks)


def _side_from_token(token: Optional[str]) -> Optional[str]:
    if token is None:
        return None
    t = str(token).strip().lower()
    if t in ("bullish", "long", "buy", "bull"):
        return "bullish"
    if t in ("bearish", "short", "sell", "bear"):
        return "bearish"
    return None


def parse_tv_side(parsed: Any, raw_text: str = "") -> Optional[str]:
    """bullish vs bearish from TV JSON fields and/or message text."""
    d: Dict[str, Any] = parsed if isinstance(parsed, dict) else {}
    for key in (
        "side",
        "direction",
        "bias",
        "action",
        "order_action",
        "strategy.order.action",
        "market_position",
        "strategy.market_position",
    ):
        if key in d:
            got = _side_from_token(d.get(key))
            if got:
                return got
    nested = d.get("strategy")
    if isinstance(nested, dict):
        order = nested.get("order") if isinstance(nested.get("order"), dict) else {}
        got = _side_from_token(order.get("action")) or _side_from_token(nested.get("market_position"))
        if got:
            return got
    blob = _blob_text(d.get("message"), d.get("text"), d.get("comment"), raw_text, d)
    bull = bool(_WORD_BULL.search(blob))
    bear = bool(_WORD_BEAR.search(blob))
    if bull and not bear:
        return "bullish"
    if bear and not bull:
        return "bearish"
    return None


def extract_ticker_raw(parsed: Any, raw_text: str = "") -> Optional[str]:
    d: Dict[str, Any] = parsed if isinstance(parsed, dict) else {}
    for key in ("ticker", "symbol", "tickerid", "sym", "stock"):
        v = d.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    stocks = d.get("stocks")
    if isinstance(stocks, str) and stocks.strip():
        return stocks.split(",")[0].strip()
    if isinstance(stocks, list) and stocks:
        return str(stocks[0]).strip()
    blob = raw_text or (d.get("_raw") if isinstance(d.get("_raw"), str) else "")
    m = re.search(r"\b(?:NSE|NFO|BSE)\s*:\s*([A-Z0-9&._-]+)", blob, re.I)
    if m:
        return m.group(0)
    m2 = re.search(r"\b([A-Z][A-Z0-9.&-]{1,24})\b", blob.upper())
    if m2 and m2.group(1) not in ("BULLISH", "BEARISH", "LONG", "SHORT", "BUY", "SELL"):
        return m2.group(1)
    return None


def _try_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s or "{{" in s:
            return None
        v = s
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def extract_tv_ohlc(parsed: Any, raw_payload: Optional[Dict[str, Any]] = None) -> Tuple[Optional[float], Optional[float]]:
    """High/low from TV JSON when the alert message includes them."""
    d: Dict[str, Any] = {}
    if isinstance(parsed, dict):
        d.update(parsed)
    if isinstance(raw_payload, dict):
        d.update(raw_payload)
    nested = d.get("bar") or d.get("candle") or d.get("ohlc")
    if isinstance(nested, dict):
        d = {**nested, **d}
    hi = None
    lo = None
    for k in ("high", "alert_high", "candle_high", "h"):
        hi = _try_float(d.get(k))
        if hi is not None:
            break
    for k in ("low", "alert_low", "candle_low", "l"):
        lo = _try_float(d.get(k))
        if lo is not None:
            break
    return hi, lo


def _aware_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def floor_received_to_15m(received_at: datetime) -> datetime:
    dt = _aware_ist(received_at)
    mm = (dt.minute // 15) * 15
    return dt.replace(minute=mm, second=0, microsecond=0)


def _bar_ts_ist(c: Dict[str, Any]) -> Optional[datetime]:
    ts = c.get("timestamp")
    if isinstance(ts, datetime):
        return _aware_ist(ts)
    if ts is None:
        return None
    try:
        from backend.services.daily_futures_service import _parse_iso_ist

        parsed = _parse_iso_ist(ts if isinstance(ts, str) else str(ts))
        return parsed
    except Exception:
        return None


def _match_15m_slot(rows: List[Dict[str, Any]], slot: datetime) -> Optional[Dict[str, Any]]:
    slot_n = _aware_ist(slot).replace(second=0, microsecond=0)
    for c in rows or []:
        ts = _bar_ts_ist(c)
        if ts is None:
            continue
        ts_n = ts.replace(second=0, microsecond=0)
        if ts_n == slot_n:
            return c
    return None


def fetch_15m_bar_containing(
    instrument_key: str,
    received_at: datetime,
) -> Optional[Dict[str, Any]]:
    """15m bar whose open equals floor(received_at). Includes in-progress bar (intraday fallback)."""
    ik = str(instrument_key or "").strip()
    if not ik:
        return None
    slot = floor_received_to_15m(received_at)
    session_date = slot.date()
    try:
        from backend.config import settings
        from backend.services.upstox_service import UpstoxService

        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        raw = (
            ux.get_historical_candles_by_instrument_key(
                ik, interval=TV_ALERT_CANDLE_INTERVAL, days_back=3, range_end_date=session_date
            )
            or []
        )
        match = _match_15m_slot(raw, slot)
        if match:
            return match
        intra = ux._fetch_intraday_candles_v3(ik, TV_ALERT_CANDLE_INTERVAL) or []
        return _match_15m_slot(intra, slot)
    except Exception as e:
        logger.warning("premium_futures TV: 15m alert candle fetch failed ik=%s: %s", ik, e)
    return None


def resolve_alert_candle_hl(
    *,
    parsed: Any,
    raw_payload: Dict[str, Any],
    instrument_key: Optional[str],
    received_at: datetime,
) -> Tuple[Optional[float], Optional[float], str]:
    hi, lo = extract_tv_ohlc(parsed, raw_payload)
    if hi is not None or lo is not None:
        return hi, lo, "payload"
    if not instrument_key:
        return None, None, "none"
    bar = fetch_15m_bar_containing(instrument_key, received_at)
    if not bar:
        return None, None, "none"
    return _try_float(bar.get("high")), _try_float(bar.get("low")), "upstox_15m"


def parse_tv_alert(parsed: Any, raw_text: str = "") -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    """
    Returns (parse_status_if_no_master, ticker_raw, symbol, side).
    parse_status here is failed when symbol or side missing; success/unmatched after master lookup.
    """
    ticker_raw = extract_ticker_raw(parsed, raw_text)
    symbol = normalize_tv_ticker(ticker_raw)
    side = parse_tv_side(parsed, raw_text)
    if not symbol or not side:
        return PARSE_FAILED, ticker_raw, symbol, side
    return PARSE_SUCCESS, ticker_raw, symbol, side


def resolve_currmth_future(symbol: str) -> Optional[Dict[str, str]]:
    """Match equity / FUT on arbitrage_master. None if no row or empty current-month FUT key."""
    sym_u = str(symbol or "").strip().upper()
    if not sym_u:
        return None
    base_u = sym_u.split("FUT")[0].strip() or sym_u
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT stock,
                       COALESCE(TRIM(currmth_future_symbol), '') AS cfs,
                       COALESCE(TRIM(currmth_future_instrument_key), '') AS cfi
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) IN (:sym, :base)
                   OR UPPER(TRIM(currmth_future_symbol)) = :sym
                LIMIT 1
                """
            ),
            {"sym": sym_u, "base": base_u},
        ).mappings().first()
    except Exception as e:
        logger.warning("premium_futures TV: arbitrage_master lookup failed symbol=%s: %s", sym_u, e)
        return None
    finally:
        db.close()
    if not row:
        logger.info("premium_futures TV: no arbitrage_master row for symbol=%s", sym_u)
        return None
    ikey = str(row.get("cfi") or "").strip()
    if not ikey:
        logger.info("premium_futures TV: master row without FUT key symbol=%s", sym_u)
        return None
    cfs = str(row.get("cfs") or "").strip()
    stk = str(row.get("stock") or sym_u).strip()
    return {
        "underlying": stk,
        "fut_symbol": cfs or stk,
        "fut_instrument_key": ikey,
    }


def insert_tv_webhook_row(
    *,
    received_at: datetime,
    source_ip: Optional[str],
    parsed: Any,
    raw_payload: Dict[str, Any],
    raw_body: str,
) -> Dict[str, Any]:
    ensure_premium_futures_tv_webhook_table()
    status, ticker_raw, symbol, side = parse_tv_alert(parsed, raw_body)
    note: Optional[str] = None
    fut: Optional[Dict[str, str]] = None
    if status == PARSE_FAILED:
        note = "missing symbol or side"
        logger.info(
            "premium_futures TV parse failed ticker_raw=%s symbol=%s side=%s",
            ticker_raw,
            symbol,
            side,
        )
    else:
        fut = resolve_currmth_future(symbol or "")
        if not fut:
            status = PARSE_UNMATCHED
            note = "no arbitrage_master row or empty currmth FUT"
            logger.info("premium_futures TV unmatched symbol=%s side=%s", symbol, side)
        else:
            note = None
            logger.info(
                "premium_futures TV matched symbol=%s side=%s fut=%s",
                fut["underlying"],
                side,
                fut["fut_symbol"],
            )

    payload_json = json.dumps(raw_payload)
    db = SessionLocal()
    try:
        rid = db.execute(
            text(
                """
                INSERT INTO premium_futures_tv_webhook_log (
                    received_at, source_ip, raw_body, raw_payload, parse_status, parse_note,
                    ticker_raw, symbol, side, resolved_fut_symbol, resolved_fut_instrument_key,
                    underlying
                ) VALUES (
                    :received_at, :source_ip, :raw_body, CAST(:raw_payload AS jsonb),
                    :parse_status, :parse_note, :ticker_raw, :symbol, :side,
                    :resolved_fut_symbol, :resolved_fut_instrument_key, :underlying
                )
                RETURNING id
                """
            ),
            {
                "received_at": received_at,
                "source_ip": source_ip,
                "raw_body": (raw_body or "")[:8000],
                "raw_payload": payload_json,
                "parse_status": status,
                "parse_note": note,
                "ticker_raw": ticker_raw,
                "symbol": symbol,
                "side": side,
                "resolved_fut_symbol": (fut or {}).get("fut_symbol"),
                "resolved_fut_instrument_key": (fut or {}).get("fut_instrument_key"),
                "underlying": (fut or {}).get("underlying"),
            },
        ).scalar()
        db.commit()
        log_id = int(rid) if rid is not None else None
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    promoted: Optional[int] = None
    if status == PARSE_SUCCESS and fut and log_id is not None:
        try:
            promoted = promote_tv_log_to_screening(
                log_id=log_id,
                received_at=received_at,
                parsed=parsed,
                raw_payload=raw_payload,
                side=side or "",
                fut=fut,
            )
        except Exception as e:
            logger.warning("premium_futures TV: screening promote failed log_id=%s: %s", log_id, e)
    return {
        "parse_status": status,
        "parse_note": note,
        "symbol": symbol,
        "side": side,
        "resolved_fut": (fut or {}).get("fut_symbol") if fut else None,
        "underlying": (fut or {}).get("underlying") if fut else None,
        "screening_id": promoted,
    }


def _iso_hit(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return _aware_ist(dt).isoformat()
    return str(dt)


def _mark_log_promoted(log_id: int) -> None:
    db = SessionLocal()
    try:
        db.execute(
            text("UPDATE premium_futures_tv_webhook_log SET promoted_at = :ts WHERE id = :id"),
            {"ts": now_ist_second(), "id": int(log_id)},
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def promote_tv_log_to_screening(
    *,
    log_id: int,
    received_at: datetime,
    parsed: Any,
    raw_payload: Dict[str, Any],
    side: str,
    fut: Dict[str, str],
) -> Optional[int]:
    """Ingest like ChartInk (quotes, conviction, LTP, rel. str.) then stamp alert-candle H/L."""
    from backend.services.daily_futures_service import _ingest_df_webhook, fut_lot_for_key

    und = str(fut.get("underlying") or "").strip().upper()
    ik = str(fut.get("fut_instrument_key") or "").strip()
    direction = "LONG" if str(side).strip().lower() == "bullish" else "SHORT"
    summary = _ingest_df_webhook([und], direction)
    sid: Optional[int] = None
    touched = summary.get("touched_screening_ids") or []
    if touched:
        sid = int(touched[-1])
    hi, lo, src = resolve_alert_candle_hl(
        parsed=parsed,
        raw_payload=raw_payload if isinstance(raw_payload, dict) else {},
        instrument_key=ik,
        received_at=received_at,
    )
    lot = fut_lot_for_key(ik) if ik else None
    db = SessionLocal()
    try:
        if sid is None:
            row = db.execute(
                text(
                    """
                    SELECT id FROM daily_futures_screening
                    WHERE trade_date = CAST(:d AS DATE) AND UPPER(TRIM(underlying)) = :u
                    ORDER BY id DESC LIMIT 1
                    """
                ),
                {"d": _aware_ist(received_at).date().isoformat(), "u": und},
            ).fetchone()
            if row:
                sid = int(row[0])
        if sid is None:
            return None
        rowj = db.execute(
            text("SELECT conviction_breakdown_json FROM daily_futures_screening WHERE id = :id"),
            {"id": sid},
        ).fetchone()
        breakdown: Dict[str, Any] = {}
        if rowj and rowj[0]:
            rawj = rowj[0]
            if isinstance(rawj, dict):
                breakdown = dict(rawj)
            elif isinstance(rawj, str):
                try:
                    breakdown = json.loads(rawj)
                except (TypeError, ValueError):
                    breakdown = {}
        breakdown["source"] = "tradingview_webhook"
        breakdown["tv_alert_candle_tf"] = TV_ALERT_CANDLE_INTERVAL
        breakdown["tv_alert_hl_source"] = src
        if hi is not None:
            breakdown["alert_candle_high"] = hi
        if lo is not None:
            breakdown["alert_candle_low"] = lo
        db.execute(
            text(
                """
                UPDATE daily_futures_screening SET
                  last_hit_at = :lh,
                  first_hit_at = COALESCE(first_hit_at, :lh),
                  trigger_candle_high = COALESCE(:th, trigger_candle_high),
                  trigger_candle_low = COALESCE(:tl, trigger_candle_low),
                  lot_size = COALESCE(:lot, lot_size),
                  conviction_breakdown_json = CAST(:cbj AS JSONB),
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """
            ),
            {
                "lh": received_at,
                "th": hi,
                "tl": lo,
                "lot": lot,
                "cbj": json.dumps(breakdown),
                "id": sid,
            },
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    _mark_log_promoted(log_id)
    logger.info(
        "premium_futures TV promoted screening_id=%s und=%s hl_src=%s hl=%s/%s",
        sid,
        und,
        src,
        hi,
        lo,
    )
    return sid



def backfill_unpromoted_tv_logs(session_date: date) -> int:
    """Promote leftover success rows for the session (display-only logs from earlier today)."""
    ensure_premium_futures_tv_webhook_table()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, received_at, raw_payload, side, resolved_fut_symbol,
                       resolved_fut_instrument_key, underlying
                FROM premium_futures_tv_webhook_log
                WHERE CAST(received_at AS date) = CAST(:d AS date)
                  AND parse_status = 'success'
                  AND promoted_at IS NULL
                  AND side IN ('bullish', 'bearish')
                  AND underlying IS NOT NULL
                  AND TRIM(COALESCE(resolved_fut_instrument_key, '')) <> ''
                ORDER BY received_at ASC, id ASC
                """
            ),
            {"d": session_date.isoformat()},
        ).mappings().all()
    finally:
        db.close()
    n = 0
    for r in rows:
        fut = {
            "underlying": str(r["underlying"]).strip(),
            "fut_symbol": str(r.get("resolved_fut_symbol") or "").strip(),
            "fut_instrument_key": str(r.get("resolved_fut_instrument_key") or "").strip(),
        }
        payload = r.get("raw_payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (TypeError, ValueError):
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        try:
            sid = promote_tv_log_to_screening(
                log_id=int(r["id"]),
                received_at=r["received_at"],
                parsed=payload,
                raw_payload=payload,
                side=str(r["side"]),
                fut=fut,
            )
            if sid:
                n += 1
        except Exception as e:
            logger.warning("premium_futures TV backfill log_id=%s: %s", r.get("id"), e)
    return n


def session_tv_hit_map(session_date: date) -> Dict[Tuple[str, str], Dict[str, Any]]:
    ensure_premium_futures_tv_webhook_table()
    db = SessionLocal()
    try:
        rows_raw = db.execute(
            text(
                """
                SELECT id, received_at, side, underlying, resolved_fut_symbol,
                       resolved_fut_instrument_key, raw_payload
                FROM premium_futures_tv_webhook_log
                WHERE CAST(received_at AS date) = CAST(:d AS date)
                  AND parse_status = 'success'
                  AND side IN ('bullish', 'bearish')
                  AND underlying IS NOT NULL
                  AND TRIM(COALESCE(resolved_fut_instrument_key, '')) <> ''
                ORDER BY received_at ASC, id ASC
                """
            ),
            {"d": session_date.isoformat()},
        ).mappings().all()
    finally:
        db.close()
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows_raw:
        side = str(r.get("side") or "").strip().lower()
        und = str(r.get("underlying") or "").strip().upper()
        if not und or side not in ("bullish", "bearish"):
            continue
        key = (und, side)
        payload = r.get("raw_payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (TypeError, ValueError):
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        hi, lo = extract_tv_ohlc(payload, payload)
        rec = r.get("received_at")
        if key not in out:
            out[key] = {
                "underlying": und,
                "side": side,
                "scan_count": 1,
                "first_hit_at": rec,
                "last_hit_at": rec,
                "future_symbol": r.get("resolved_fut_symbol"),
                "instrument_key": r.get("resolved_fut_instrument_key"),
                "alert_candle_high": hi,
                "alert_candle_low": lo,
            }
        else:
            out[key]["scan_count"] = int(out[key]["scan_count"]) + 1
            out[key]["last_hit_at"] = rec
            out[key]["future_symbol"] = r.get("resolved_fut_symbol")
            out[key]["instrument_key"] = r.get("resolved_fut_instrument_key")
            if hi is not None:
                out[key]["alert_candle_high"] = hi
            if lo is not None:
                out[key]["alert_candle_low"] = lo
    return out


def _tv_key_for_pick(p: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    und = str(p.get("underlying") or "").strip().upper()
    if not und:
        return None
    dtp = str(p.get("direction_type") or "LONG").strip().upper()
    side = "bearish" if dtp == "SHORT" else "bullish"
    return (und, side)


def apply_tv_hit_to_pick(p: Dict[str, Any], hit: Dict[str, Any], *, featured: bool) -> None:
    p["tv_webhook"] = True
    p["source"] = "tradingview_webhook"
    p["tv_featured"] = featured
    p["scan_count"] = int(hit.get("scan_count") or 1)
    p["first_hit_at"] = _iso_hit(hit.get("first_hit_at"))
    p["last_hit_at"] = _iso_hit(hit.get("last_hit_at"))
    if hit.get("future_symbol"):
        p["future_symbol"] = hit.get("future_symbol")
    if hit.get("instrument_key"):
        p["instrument_key"] = hit.get("instrument_key")
    hi = hit.get("alert_candle_high")
    lo = hit.get("alert_candle_low")
    br = p.get("conviction_breakdown_json")
    if isinstance(br, dict):
        hi = hi if hi is not None else br.get("alert_candle_high")
        lo = lo if lo is not None else br.get("alert_candle_low")
    if hi is None:
        hi = p.get("trigger_candle_high")
    if lo is None:
        lo = p.get("trigger_candle_low")
    p["alert_candle_high"] = hi
    p["alert_candle_low"] = lo
    dtp = str(p.get("direction_type") or "LONG").strip().upper()
    if dtp == "SHORT" and lo is not None:
        p["target_entry_price"] = round(float(lo), 4)
    elif dtp != "SHORT" and hi is not None:
        p["target_entry_price"] = round(float(hi), 4)


def tagging_tv_on_screening_rows(
    rows: List[Dict[str, Any]],
    tv_map: Dict[Tuple[str, str], Dict[str, Any]],
    *,
    side: str,
) -> None:
    featured_und = None
    for key, _hit in tv_map.items():
        if key[1] != side:
            continue
        featured_und = key[0]
        break
    for p in rows:
        k = _tv_key_for_pick(p)
        if not k or k not in tv_map:
            continue
        apply_tv_hit_to_pick(p, tv_map[k], featured=(k[0] == featured_und))


def merge_tv_picks_into_workspace(
    picks_bull: List[Dict[str, Any]],
    picks_bearish: List[Dict[str, Any]],
    session_date: date,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    tv_map = session_tv_hit_map(session_date)
    tagging_tv_on_screening_rows(picks_bull, tv_map, side="bullish")
    tagging_tv_on_screening_rows(picks_bearish, tv_map, side="bearish")
    return picks_bull, picks_bearish


def fetch_session_tv_workspace_rows(session_date: date) -> Dict[str, List[Dict[str, Any]]]:
    tv_map = session_tv_hit_map(session_date)
    out: Dict[str, List[Dict[str, Any]]] = {"bullish": [], "bearish": []}
    for (und, side), hit in tv_map.items():
        if len(out[side]) >= TV_PICK_LIMIT_PER_SIDE:
            continue
        direction = "LONG" if side == "bullish" else "SHORT"
        featured = len(out[side]) == 0
        row = {
            "screening_id": None,
            "underlying": und,
            "direction_type": direction,
            "future_symbol": hit.get("future_symbol"),
            "instrument_key": hit.get("instrument_key"),
            "lot_size": None,
            "source": "tradingview_webhook",
            "tv_webhook": True,
        }
        apply_tv_hit_to_pick(row, hit, featured=featured)
        out[side].append(row)
    return out

