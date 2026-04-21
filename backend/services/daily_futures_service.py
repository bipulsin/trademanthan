"""
Daily Futures — ChartInk webhook → arbitrage_master front-month future, Upstox LTP + conviction.
"""
from __future__ import annotations

import logging
import os
import re
import time
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import engine
from backend.services.fno_bullish_backtest import TradeRow, _fill_conviction_raw_metrics, finalize_conviction_scores
from backend.services.nks_intraday_backtest import (
    _bucket_candles_by_hhmm,
    _index_instruments,
    _load_instruments,
    fetch_intraday_1m_candles,
)
from backend.services.upstox_service import UpstoxService, _candles_rows_to_structured

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

_INSTRUMENT_CACHE: Optional[Tuple[Any, Any]] = None
_DF_INTRADAY_1M_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}


def _instruments_index():
    global _INSTRUMENT_CACHE
    if _INSTRUMENT_CACHE is None:
        instruments = _load_instruments()
        _INSTRUMENT_CACHE = _index_instruments(instruments)
    return _INSTRUMENT_CACHE


def fut_lot_for_key(instrument_key: str) -> Optional[int]:
    fut_by_und, _eq = _instruments_index()
    for _sym, lst in fut_by_und.items():
        for inst in lst or []:
            if (inst.get("instrument_key") or "").strip() == instrument_key:
                ls = inst.get("lot_size")
                try:
                    return int(ls) if ls is not None else None
                except (TypeError, ValueError):
                    return None
    return None


def ensure_daily_futures_tables() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS daily_futures_screening (
        id SERIAL PRIMARY KEY,
        trade_date DATE NOT NULL,
        underlying VARCHAR(64) NOT NULL,
        future_symbol TEXT,
        instrument_key TEXT NOT NULL,
        lot_size INTEGER,
        scan_count INTEGER NOT NULL DEFAULT 1,
        first_hit_at TIMESTAMPTZ,
        last_hit_at TIMESTAMPTZ,
        conviction_score NUMERIC(8,2),
        ltp NUMERIC(18,4),
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (trade_date, underlying)
    );
    CREATE INDEX IF NOT EXISTS idx_dfs_trade_date ON daily_futures_screening (trade_date);

    CREATE TABLE IF NOT EXISTS daily_futures_user_trade (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        screening_id INTEGER NOT NULL REFERENCES daily_futures_screening(id) ON DELETE CASCADE,
        underlying VARCHAR(64) NOT NULL,
        future_symbol TEXT,
        instrument_key TEXT,
        lot_size INTEGER,
        order_status VARCHAR(16) NOT NULL DEFAULT 'bought',
        entry_time VARCHAR(16),
        entry_price NUMERIC(18,4),
        exit_time VARCHAR(16),
        exit_price NUMERIC(18,4),
        pnl_points NUMERIC(18,4),
        pnl_rupees NUMERIC(18,4),
        consecutive_webhook_misses INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_dfut_user_status ON daily_futures_user_trade (user_id, order_status);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def ist_today() -> date:
    return datetime.now(IST).date()


def is_daily_futures_session_open_ist(now: Optional[datetime] = None) -> bool:
    """
    Workspace shows only the current IST calendar session from 09:00 onward.
    Before 09:00 IST all sections are empty (clean slate until the day session starts).
    """
    dt = now or datetime.now(IST)
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    else:
        dt = dt.astimezone(IST)
    return dt.time() >= dt_time(9, 0)


def _empty_daily_futures_workspace(trade_date: date, *, session_before_open: bool) -> Dict[str, Any]:
    msg = (
        "Daily Futures shows only the current IST session from 09:00 onward. "
        "Sections stay empty before 09:00 IST."
    )
    return {
        "trade_date": str(trade_date),
        "session_before_open": session_before_open,
        "session_message": msg if session_before_open else None,
        "picks": [],
        "running": [],
        "closed": [],
        "trade_if_could_have_done": [],
        "summary": {
            "cumulative_pnl_rupees": 0.0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": None,
        },
    }


def _parse_iso_ist(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        raw = str(ts).strip()
        if raw.isdigit():
            v = float(raw)
            if v > 1_000_000_000_000:
                v /= 1000.0
            return datetime.fromtimestamp(v, tz=IST)
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    except Exception:
        return None


def _fmt_hm(dt: Optional[datetime]) -> str:
    return dt.strftime("%H:%M") if dt else "—"


def _floor_to_15m_ist(dt: datetime) -> datetime:
    """Normalize to scan slot boundary: 11:00, 11:15, 11:30, 11:45, ..."""
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    else:
        dt = dt.astimezone(IST)
    mm = (dt.minute // 15) * 15
    return dt.replace(minute=mm, second=0, microsecond=0)


def _fetch_intraday_1m_cached(upstox: UpstoxService, instrument_key: str, trade_date: date) -> List[Dict[str, Any]]:
    if not instrument_key:
        return []
    ck = (instrument_key.strip(), str(trade_date))
    now = time.time()
    hit = _DF_INTRADAY_1M_CACHE.get(ck)
    if hit and (now - float(hit.get("ts") or 0)) < 180.0:
        return list(hit.get("candles") or [])
    try:
        candles = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="minutes/1",
            days_back=2,
            range_end_date=trade_date,
        ) or []
    except Exception:
        candles = []
    # Fallback for current-day snapshots: historical endpoint can be empty intraday.
    # Use Upstox intraday-candle endpoint for minute bars when needed.
    if not candles and trade_date == ist_today():
        try:
            key_enc = quote(instrument_key, safe="")
            intraday_url = f"{upstox.base_url}/historical-candle/intraday/{key_enc}/minutes/1"
            raw = upstox.make_api_request(intraday_url, method="GET", timeout=15, max_retries=2) or {}
            if isinstance(raw, dict) and raw.get("status") == "success":
                rows = ((raw.get("data") or {}).get("candles")) or []
                candles = _candles_rows_to_structured(rows) or []
        except Exception:
            candles = candles or []
    _DF_INTRADAY_1M_CACHE[ck] = {"ts": now, "candles": candles}
    return list(candles)


def _ltp_asof_ist(candles: List[Dict[str, Any]], target_dt_ist: datetime) -> Optional[float]:
    best_ts: Optional[datetime] = None
    best_close: Optional[float] = None
    for c in candles or []:
        ts = c.get("timestamp")
        dt = None
        try:
            if isinstance(ts, (int, float)):
                v = float(ts)
                if v > 1_000_000_000_000:
                    v /= 1000.0
                dt = datetime.fromtimestamp(v, tz=IST)
            else:
                dt = _parse_iso_ist(str(ts))
        except Exception:
            dt = None
        if not dt:
            continue
        if dt > target_dt_ist:
            continue
        close_px = c.get("close")
        try:
            cp = float(close_px)
        except (TypeError, ValueError):
            continue
        if cp <= 0:
            continue
        if best_ts is None or dt > best_ts:
            best_ts = dt
            best_close = cp
    return round(float(best_close), 4) if best_close is not None else None


def _build_trade_if_could_rows(
    picks: List[Dict[str, Any]],
    closed: List[Dict[str, Any]],
    trade_date: date,
) -> List[Dict[str, Any]]:
    closed_sids = {
        int(r.get("screening_id"))
        for r in (closed or [])
        if r.get("screening_id") is not None
    }
    candidates = [
        p for p in (picks or [])
        if p.get("screening_id") is not None and int(p.get("screening_id")) not in closed_sids
    ]
    if not candidates:
        return []

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    out: List[Dict[str, Any]] = []
    close_1515 = IST.localize(datetime.combine(trade_date, datetime.min.time()).replace(hour=15, minute=15))

    for p in candidates:
        first_hit = _parse_iso_ist(p.get("first_hit_at"))
        if not first_hit:
            continue
        last_hit = _parse_iso_ist(p.get("last_hit_at")) or first_hit
        last_hit_slot = _floor_to_15m_ist(last_hit)
        entry_dt = first_hit + timedelta(minutes=5)
        ikey = (p.get("instrument_key") or "").strip()
        candles = _fetch_intraday_1m_cached(upstox, ikey, trade_date)
        entry_ltp = _ltp_asof_ist(candles, entry_dt)
        scan_ltp = _ltp_asof_ist(candles, last_hit_slot)
        if scan_ltp is None:
            try:
                pv = p.get("ltp")
                if pv is not None:
                    x = float(pv)
                    if x > 0:
                        scan_ltp = round(x, 4)
            except Exception:
                scan_ltp = None
        qty = p.get("lot_size")
        try:
            qty_num = float(qty) if qty is not None else None
        except Exception:
            qty_num = None

        row: Dict[str, Any] = {
            "screening_id": p.get("screening_id"),
            "underlying": p.get("underlying"),
            "future_symbol": p.get("future_symbol"),
            "instrument_key": ikey,
            "qty": int(qty_num) if qty_num is not None else None,
            "first_scan_time": _fmt_hm(first_hit),
            "entry_time": _fmt_hm(entry_dt),
            "entry_ltp": entry_ltp,
            "exit_scan_time": _fmt_hm(last_hit_slot),
            "exit_scan_ltp": scan_ltp,
            "current_ltp": None,
            "pnl_scan_rupees": None,
            "exit_1515_time": "15:15",
            "exit_1515_ltp": None,
            "pnl_1515_rupees": None,
        }

        try:
            pv = p.get("ltp")
            if pv is not None:
                cur = float(pv)
                if cur > 0:
                    row["current_ltp"] = round(cur, 4)
        except Exception:
            row["current_ltp"] = None

        pnl_ref_ltp = row.get("current_ltp")
        if pnl_ref_ltp is None:
            pnl_ref_ltp = scan_ltp
        if entry_ltp is not None and pnl_ref_ltp is not None and qty_num is not None:
            row["pnl_scan_rupees"] = round((float(pnl_ref_ltp) - entry_ltp) * qty_num, 2)

        ltp_1515 = _ltp_asof_ist(candles, close_1515)
        row["exit_1515_ltp"] = ltp_1515
        if entry_ltp is not None and ltp_1515 is not None and qty_num is not None:
            row["pnl_1515_rupees"] = round((ltp_1515 - entry_ltp) * qty_num, 2)

        out.append(row)

    out.sort(key=lambda r: (r.get("future_symbol") or r.get("underlying") or ""))
    return out


def normalize_symbols_from_payload(payload: Any) -> List[str]:
    out: List[str] = []
    if payload is None:
        return out
    if isinstance(payload, str):
        return sorted({s.strip().upper() for s in re.split(r"[\s,;\n]+", payload) if s.strip()})
    if isinstance(payload, list):
        return sorted({str(x).strip().upper() for x in payload if str(x).strip()})
    if isinstance(payload, dict):
        for key in ("symbols", "symbol", "stocks", "tickers", "data", "alert_symbols"):
            v = payload.get(key)
            if isinstance(v, list):
                out.extend(str(x).strip().upper() for x in v if x)
            elif isinstance(v, str):
                out.extend(normalize_symbols_from_payload(v))
        if not out and payload.get("text"):
            out.extend(normalize_symbols_from_payload(str(payload["text"])))
    return sorted(set(out))


def load_arbitrage_future_row(conn, underlying: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        text(
            """
            SELECT stock,
                   currmth_future_symbol,
                   currmth_future_instrument_key
            FROM arbitrage_master
            WHERE UPPER(TRIM(stock)) = :u
              AND currmth_future_instrument_key IS NOT NULL
              AND TRIM(currmth_future_instrument_key) <> ''
            LIMIT 1
            """
        ),
        {"u": underlying.strip().upper()},
    ).fetchone()
    if not row:
        return None
    return {
        "underlying": row[0],
        "future_symbol": row[1],
        "instrument_key": str(row[2]).strip(),
    }


def _recompute_conviction_all_today(upstox: UpstoxService, trade_date: date) -> None:
    """Recompute conviction_score + LTP for every screening row on trade_date."""
    cand_cache: Dict[str, Dict[Tuple[int, int], Dict[str, Any]]] = {}

    with engine.connect() as conn:
        rows_db = conn.execute(
            text(
                """
                SELECT id, underlying, instrument_key FROM daily_futures_screening
                WHERE trade_date = CAST(:d AS DATE)
                """
            ),
            {"d": str(trade_date)},
        ).fetchall()

    finals: List[Dict[str, Any]] = []
    row_ids: List[int] = []

    for rid, und, ik in rows_db:
        ik = str(ik).strip()
        if ik not in cand_cache:
            cand_cache[ik] = fetch_intraday_1m_candles(upstox, ik, trade_date) or {}
        buckets = _bucket_candles_by_hhmm(cand_cache[ik])
        row_ids.append(int(rid))
        if not buckets:
            finals.append(
                {
                    "trade_date": str(trade_date),
                    "symbol": str(und),
                    "underlying": str(und),
                    "instrument_key": ik,
                    "conviction_oi_change_pct": None,
                    "conviction_price_vs_vwap_pct": None,
                    "_rid": rid,
                }
            )
            continue
        keys = sorted(buckets.keys())
        tr = TradeRow(trade_date=str(trade_date), symbol=str(und))
        _fill_conviction_raw_metrics(tr, buckets, [keys[-1]], conviction_scan_index=0)
        d = tr.to_dict()
        d["trade_date"] = str(trade_date)
        d["underlying"] = str(und)
        d["instrument_key"] = ik
        d["_rid"] = rid
        finals.append(d)

    finalize_conviction_scores(finals)

    with engine.begin() as conn:
        for d in finals:
            rid = int(d["_rid"])
            cs = d.get("conviction_score")
            ik = str(d.get("instrument_key") or "").strip()
            q = upstox.get_market_quote_by_key(ik) if ik else {}
            lp = (q or {}).get("last_price") or (q or {}).get("close")
            try:
                ltp = float(lp) if lp is not None else None
            except (TypeError, ValueError):
                ltp = None
            conn.execute(
                text(
                    """
                    UPDATE daily_futures_screening SET
                      conviction_score = :cs,
                      ltp = COALESCE(:ltp, ltp),
                      updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                    """
                ),
                {"cs": cs, "ltp": ltp, "id": rid},
            )


def process_chartink_webhook(symbols: List[str]) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    trade_date = ist_today()
    sym_set: Set[str] = {s.strip().upper() for s in symbols if s and str(s).strip()}

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    summary: Dict[str, Any] = {"trade_date": str(trade_date), "processed": 0, "skipped": []}

    touched_ids: List[int] = []

    with engine.begin() as conn:
        for u in sorted(sym_set):
            row = load_arbitrage_future_row(conn, u)
            if not row:
                summary["skipped"].append({"underlying": u, "reason": "not_in_arbitrage_master"})
                continue
            ik = row["instrument_key"]
            lot = fut_lot_for_key(ik)
            now_ist = datetime.now(IST)
            ex = conn.execute(
                text(
                    """
                    SELECT id FROM daily_futures_screening
                    WHERE trade_date = CAST(:d AS DATE) AND UPPER(TRIM(underlying)) = :u
                    """
                ),
                {"d": str(trade_date), "u": u},
            ).fetchone()
            if ex:
                sid = int(ex[0])
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_screening SET
                          scan_count = scan_count + 1,
                          last_hit_at = :lh,
                          lot_size = COALESCE(:lot, lot_size),
                          future_symbol = :fs,
                          instrument_key = :ik,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": sid,
                        "lh": now_ist,
                        "lot": lot,
                        "fs": row["future_symbol"],
                        "ik": ik,
                    },
                )
                touched_ids.append(sid)
            else:
                r = conn.execute(
                    text(
                        """
                        INSERT INTO daily_futures_screening (
                          trade_date, underlying, future_symbol, instrument_key, lot_size,
                          scan_count, first_hit_at, last_hit_at
                        ) VALUES (
                          CAST(:d AS DATE), :u, :fs, :ik, :lot, 1, :fh, :lh
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "d": str(trade_date),
                        "u": u,
                        "fs": row["future_symbol"],
                        "ik": ik,
                        "lot": lot,
                        "fh": now_ist,
                        "lh": now_ist,
                    },
                ).fetchone()
                touched_ids.append(int(r[0]))
            summary["processed"] += 1

        miss_rows = conn.execute(
            text(
                """
                SELECT id, UPPER(TRIM(underlying)) FROM daily_futures_user_trade
                WHERE order_status = 'bought'
                """
            ),
        ).fetchall()

        present = set(sym_set)
        for mid, und in miss_rows:
            und = (und or "").strip().upper()
            if und in present:
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_user_trade SET
                          consecutive_webhook_misses = 0,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {"id": mid},
                )
            else:
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_user_trade SET
                          consecutive_webhook_misses = consecutive_webhook_misses + 1,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {"id": mid},
                )

    try:
        _recompute_conviction_all_today(upstox, trade_date)
    except Exception as e:
        logger.warning("daily_futures: conviction recompute failed: %s", e, exc_info=True)

    summary["touched_screening_ids"] = touched_ids
    return summary


def _fetch_screening_dicts(conn: Any, trade_date: date) -> List[Dict[str, Any]]:
    res = conn.execute(
        text(
            """
            SELECT id, underlying, future_symbol, instrument_key, lot_size,
                   scan_count, first_hit_at, last_hit_at, conviction_score, ltp
            FROM daily_futures_screening
            WHERE trade_date = CAST(:d AS DATE)
            ORDER BY conviction_score DESC NULLS LAST, underlying
            """
        ),
        {"d": str(trade_date)},
    ).fetchall()
    out = []
    for row in res:
        out.append(
            {
                "screening_id": row[0],
                "underlying": row[1],
                "future_symbol": row[2],
                "instrument_key": row[3],
                "lot_size": int(row[4]) if row[4] is not None else None,
                "scan_count": int(row[5] or 0),
                "first_hit_at": row[6].isoformat() if row[6] else None,
                "last_hit_at": row[7].isoformat() if row[7] else None,
                "conviction_score": float(row[8]) if row[8] is not None else None,
                "ltp": float(row[9]) if row[9] is not None else None,
            }
        )
    return out


def _apply_live_ltps_to_picks_and_running(
    picks: List[Dict[str, Any]],
    running: List[Dict[str, Any]],
    closed: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Refresh LTP from Upstox for every row (batch + per-key fallback), update dicts in place,
    and persist ltp on daily_futures_screening so 15-min webhook runs and page loads stay aligned.
    """
    combined: List[Dict[str, Any]] = list(picks) + list(running) + list(closed or [])
    uniq_keys: List[str] = []
    seen: Set[str] = set()
    for r in combined:
        ik = (r.get("instrument_key") or "").strip()
        if ik and ik not in seen:
            seen.add(ik)
            uniq_keys.append(ik)
    if not uniq_keys:
        return

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    batch: Dict[str, float] = {}
    try:
        batch = upstox.get_market_quotes_batch_by_keys(uniq_keys)
    except Exception as e:
        logger.warning("daily_futures: batch LTP failed: %s", e)

    def _norm(k: str) -> str:
        return k.replace(":", "|").replace(" ", "").upper()

    def _resolve_ltp(ik: str) -> Optional[float]:
        if not ik:
            return None
        if ik in batch:
            lp = batch[ik]
            if lp and float(lp) > 0:
                return float(lp)
        nk = _norm(ik)
        for bk, lp in batch.items():
            if _norm(bk) == nk and lp and float(lp) > 0:
                return float(lp)
        try:
            q = upstox.get_market_quote_by_key(ik)
            if q and q.get("last_price"):
                v = float(q["last_price"])
                return v if v > 0 else None
        except Exception as ex:
            logger.debug("daily_futures: single-quote LTP failed for %s: %s", ik, ex)
        return None

    by_screening: Dict[int, float] = {}
    for r in combined:
        ik = (r.get("instrument_key") or "").strip()
        lp = _resolve_ltp(ik)
        if lp is None:
            continue
        lp_r = round(lp, 4)
        r["ltp"] = lp_r
        sid = r.get("screening_id")
        if sid is not None:
            by_screening[int(sid)] = lp_r

    if not by_screening:
        return
    try:
        with engine.begin() as conn:
            for sid, ltp in by_screening.items():
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_screening SET
                          ltp = :ltp,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {"ltp": ltp, "id": sid},
                )
    except Exception as e:
        logger.warning("daily_futures: persist LTP to screening failed: %s", e)


def get_workspace(db: Session, user_id: int) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    td = ist_today()
    if not is_daily_futures_session_open_ist():
        return _empty_daily_futures_workspace(td, session_before_open=True)

    with engine.connect() as conn:
        screenings = _fetch_screening_dicts(conn, td)

    br = db.execute(
        text(
            """
            SELECT t.screening_id
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'bought'
              AND s.trade_date = CAST(:td AS DATE)
            """
        ),
        {"u": user_id, "td": str(td)},
    ).fetchall()
    bought_sids = {int(r[0]) for r in br}

    picks = [s for s in screenings if s["screening_id"] not in bought_sids]

    running_rows = db.execute(
        text(
            """
            SELECT t.id, t.screening_id, t.underlying, t.future_symbol, t.instrument_key,
                   t.lot_size, t.entry_time, t.entry_price, t.consecutive_webhook_misses,
                   s.scan_count, s.first_hit_at, s.last_hit_at, s.conviction_score, s.ltp
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'bought'
              AND s.trade_date = CAST(:td AS DATE)
            ORDER BY t.updated_at DESC
            """
        ),
        {"u": user_id, "td": str(td)},
    ).fetchall()

    running = []
    for row in running_rows:
        miss = int(row[8] or 0)
        running.append(
            {
                "trade_id": row[0],
                "screening_id": row[1],
                "underlying": row[2],
                "future_symbol": row[3],
                "instrument_key": row[4],
                "lot_size": int(row[5]) if row[5] is not None else None,
                "entry_time": row[6],
                "entry_price": float(row[7]) if row[7] is not None else None,
                "consecutive_webhook_misses": miss,
                "scan_count": int(row[9] or 0),
                "first_hit_at": row[10].isoformat() if row[10] else None,
                "last_hit_at": row[11].isoformat() if row[11] else None,
                "conviction_score": float(row[12]) if row[12] is not None else None,
                "ltp": float(row[13]) if row[13] is not None else None,
                "warn_two_misses": miss >= 2,
            }
        )

    closed_rows = db.execute(
        text(
            """
            SELECT t.id, t.screening_id, t.underlying, t.future_symbol, t.instrument_key, t.lot_size,
                   t.entry_time, t.entry_price, t.exit_time, t.exit_price, t.pnl_points, t.pnl_rupees
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'sold'
              AND s.trade_date = CAST(:td AS DATE)
            ORDER BY t.updated_at DESC
            LIMIT 200
            """
        ),
        {"u": user_id, "td": str(td)},
    ).fetchall()

    closed = []
    total_pnl = 0.0
    wins = 0
    losses = 0
    for row in closed_rows:
        pnl_pts = float(row[10]) if row[10] is not None else None
        pnl_rs = float(row[11]) if row[11] is not None else None
        wl = None
        if pnl_rs is not None:
            total_pnl += pnl_rs
            if pnl_rs > 0:
                wins += 1
                wl = "Win"
            elif pnl_rs < 0:
                losses += 1
                wl = "Loss"
            else:
                wl = "Flat"
        closed.append(
            {
                "trade_id": row[0],
                "screening_id": row[1],
                "underlying": row[2],
                "future_symbol": row[3],
                "instrument_key": row[4],
                "lot_size": int(row[5]) if row[5] is not None else None,
                "entry_time": row[6],
                "entry_price": float(row[7]) if row[7] is not None else None,
                "exit_time": row[8],
                "exit_price": float(row[9]) if row[9] is not None else None,
                "pnl_points": pnl_pts,
                "pnl_rupees": pnl_rs,
                "win_loss": wl,
            }
        )

    denom = wins + losses
    win_rate = round(100.0 * wins / denom, 1) if denom else None

    try:
        _apply_live_ltps_to_picks_and_running(picks, running, closed)
    except Exception as e:
        logger.warning("daily_futures: live LTP refresh failed: %s", e, exc_info=True)

    return {
        "trade_date": str(td),
        "session_before_open": False,
        "session_message": None,
        "picks": picks,
        "running": running,
        "closed": closed,
        "trade_if_could_have_done": _build_trade_if_could_rows(picks, closed, td),
        "summary": {
            "cumulative_pnl_rupees": round(total_pnl, 2),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": win_rate,
        },
    }


def confirm_buy(db: Session, user_id: int, screening_id: int, entry_time: str, entry_price: float) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    if not is_daily_futures_session_open_ist():
        raise ValueError("Daily Futures session opens at 09:00 IST. Orders are not accepted before that.")
    row = db.execute(
        text(
            """
            SELECT id, underlying, future_symbol, instrument_key, lot_size
            FROM daily_futures_screening WHERE id = :sid AND trade_date = CAST(:d AS DATE)
            """
        ),
        {"sid": screening_id, "d": str(ist_today())},
    ).fetchone()
    if not row:
        raise ValueError("Screening row not found for today")

    exists = db.execute(
        text(
            """
            SELECT id FROM daily_futures_user_trade
            WHERE user_id = :u AND screening_id = :sid AND order_status = 'bought'
            """
        ),
        {"u": user_id, "sid": screening_id},
    ).fetchone()
    if exists:
        raise ValueError("Already bought this pick")

    db.execute(
        text(
            """
            INSERT INTO daily_futures_user_trade (
              user_id, screening_id, underlying, future_symbol, instrument_key, lot_size,
              order_status, entry_time, entry_price, consecutive_webhook_misses
            ) VALUES (
              :u, :sid, :und, :fs, :ik, :lot, 'bought', :et, :ep, 0
            )
            """
        ),
        {
            "u": user_id,
            "sid": screening_id,
            "und": row[1],
            "fs": row[2],
            "ik": row[3],
            "lot": row[4],
            "et": entry_time.strip(),
            "ep": entry_price,
        },
    )
    db.commit()
    return {"success": True}


def confirm_sell(db: Session, user_id: int, trade_id: int, exit_time: str, exit_price: float) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    row = db.execute(
        text(
            """
            SELECT id, screening_id, underlying, entry_price, lot_size, instrument_key
            FROM daily_futures_user_trade
            WHERE id = :id AND user_id = :u AND order_status = 'bought'
            """
        ),
        {"id": trade_id, "u": user_id},
    ).fetchone()
    if not row:
        raise ValueError("Open trade not found")

    entry_px = float(row[3]) if row[3] is not None else None
    lot = int(row[4]) if row[4] is not None else None
    pts = None
    pnl_rs = None
    if entry_px is not None:
        pts = round(float(exit_price) - entry_px, 4)
        if lot:
            pnl_rs = round(pts * lot, 2)

    db.execute(
        text(
            """
            UPDATE daily_futures_user_trade SET
              order_status = 'sold',
              exit_time = :xt,
              exit_price = :xp,
              pnl_points = :pts,
              pnl_rupees = :pnl,
              updated_at = CURRENT_TIMESTAMP
            WHERE id = :id AND user_id = :u
            """
        ),
        {
            "xt": exit_time.strip(),
            "xp": exit_price,
            "pts": pts,
            "pnl": pnl_rs,
            "id": trade_id,
            "u": user_id,
        },
    )
    db.commit()
    return {"success": True, "pnl_points": pts, "pnl_rupees": pnl_rs}


def webhook_secret_ok(provided: Optional[str]) -> bool:
    default_secret = "tradewithctodailyfuture"
    expected = (os.getenv("CHARTINK_DAILY_FUTURES_SECRET") or default_secret).strip()
    return bool(provided and provided.strip() == expected)
