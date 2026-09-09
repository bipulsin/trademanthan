"""TradingView ingest for Premium Futures (dailyfutures.html) — persist + display only."""
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
        db.execute(
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
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return {
        "parse_status": status,
        "parse_note": note,
        "symbol": symbol,
        "side": side,
        "resolved_fut": (fut or {}).get("fut_symbol") if fut else None,
        "underlying": (fut or {}).get("underlying") if fut else None,
    }


def _workspace_row_from_log(r: Dict[str, Any], *, featured: bool) -> Dict[str, Any]:
    side = str(r.get("side") or "").strip().lower()
    direction = "LONG" if side == "bullish" else "SHORT"
    received = r.get("received_at")
    return {
        "screening_id": None,
        "underlying": r.get("underlying"),
        "direction_type": direction,
        "future_symbol": r.get("resolved_fut_symbol"),
        "instrument_key": r.get("resolved_fut_instrument_key"),
        "lot_size": None,
        "scan_count": None,
        "first_hit_at": received,
        "last_hit_at": received,
        "conviction_score": None,
        "ltp": None,
        "order_eligible": False,
        "order_block_reason": "TradingView alert — display only (no ChartInk Enter)",
        "source": "tradingview_webhook",
        "tv_webhook": True,
        "tv_featured": featured,
    }


def fetch_session_tv_workspace_rows(session_date: date) -> Dict[str, List[Dict[str, Any]]]:
    ensure_premium_futures_tv_webhook_table()
    db = SessionLocal()
    try:
        rows_raw = db.execute(
            text(
                """
                SELECT received_at, side, underlying, resolved_fut_symbol, resolved_fut_instrument_key
                FROM premium_futures_tv_webhook_log
                WHERE CAST(received_at AS date) = CAST(:d AS date)
                  AND parse_status = 'success'
                  AND side IN ('bullish', 'bearish')
                  AND underlying IS NOT NULL
                  AND TRIM(COALESCE(resolved_fut_instrument_key, '')) <> ''
                ORDER BY received_at DESC, id DESC
                """
            ),
            {"d": session_date.isoformat()},
        ).mappings().all()
    finally:
        db.close()

    out: Dict[str, List[Dict[str, Any]]] = {"bullish": [], "bearish": []}
    seen: Dict[str, set] = {"bullish": set(), "bearish": set()}
    for r in rows_raw:
        side = str(r.get("side") or "").strip().lower()
        if side not in out:
            continue
        und = str(r.get("underlying") or "").strip().upper()
        if not und or und in seen[side]:
            continue
        if len(out[side]) >= TV_PICK_LIMIT_PER_SIDE:
            continue
        seen[side].add(und)
        featured = len(out[side]) == 0
        out[side].append(_workspace_row_from_log(dict(r), featured=featured))
    return out


def merge_tv_picks_into_workspace(
    picks_bull: List[Dict[str, Any]],
    picks_bearish: List[Dict[str, Any]],
    session_date: date,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    tv = fetch_session_tv_workspace_rows(session_date)

    def _prepend(tv_rows: List[Dict[str, Any]], existing: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = {str(t.get("underlying") or "").strip().upper() for t in tv_rows}
        rest = [
            p
            for p in existing
            if str(p.get("underlying") or "").strip().upper() not in seen
        ]
        return list(tv_rows) + rest

    return _prepend(tv["bullish"], picks_bull), _prepend(tv["bearish"], picks_bearish)
