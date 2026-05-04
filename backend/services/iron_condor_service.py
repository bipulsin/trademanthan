"""
Iron Condor advisory: strike math, hedge validation, sizing, persisted positions/alerts.
Advisory-only — no broker orders.
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
import copy
from datetime import datetime, date, time as dt_time
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import engine
from backend.services.iron_condor_universe import IRON_CONDOR_UNIVERSE, sector_for_symbol
from backend.services.upstox_service import upstox_service as vwap_service
from backend.services.iron_condor_snapshot_cache import (
    ensure_iron_condor_snapshot_tables,
    read_underlying_atr_closes_session,
)
from backend.services import market_holiday as mh_ic

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Process-wide cache: batched universe quotes (symbols × one Upstox call per TTL)
_ic_uq_cache_deadline: float = 0.0
_ic_uq_cache_rows: List[Dict[str, Any]] = []

# In-process cache for equity instrument_key lookups (get_instrument_key can load large instruments JSON).
_ic_equity_ik_cache: Dict[str, str] = {}

# DDL + extended migrations once per worker — they were doubling per /universe-symbol-quote (~tens of seconds).
_ic_tables_lock = threading.Lock()
_iron_condor_tables_ready_flag = False

# One-time-ish cache: symbol, sector, equity instrument_key (master for picker / static APIs).
_ic_master_universe_lock = threading.Lock()
_ic_master_universe_rows: Optional[List[Dict[str, Any]]] = None


def _sanitize_ic_quote_numbers(ltp_v: Any, pct_v: Any) -> Tuple[Optional[float], Optional[float]]:
    """Strip NaN/Inf and coerce to JSON-safe floats (avoids 500 during response serialization)."""
    ltp_o: Optional[float] = None
    if ltp_v is not None:
        try:
            x = float(ltp_v)
            if math.isfinite(x) and x > 0:
                ltp_o = round(x, 6)
        except (TypeError, ValueError):
            pass
    pct_o: Optional[float] = None
    if pct_v is not None:
        try:
            y = float(pct_v)
            if math.isfinite(y):
                pct_o = round(y, 4)
        except (TypeError, ValueError):
            pass
    return ltp_o, pct_o


def normalize_ic_picker_row_for_api(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row
    r = dict(row)
    lt, cg = _sanitize_ic_quote_numbers(r.get("ltp"), r.get("change_pct_day"))
    r["ltp"] = lt
    r["change_pct_day"] = cg
    r["symbol"] = str(r.get("symbol") or "").strip().upper()
    r["sector"] = str(r.get("sector") or "").strip()
    r["active_position"] = bool(r.get("active_position"))
    qs = r.get("quote_source")
    if qs is not None:
        r["quote_source"] = str(qs)
    ik = r.get("instrument_key")
    r["instrument_key"] = str(ik).strip() if ik else ""
    return r


def normalize_ic_universe_row_for_api(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row
    r = dict(row)
    lt, cg = _sanitize_ic_quote_numbers(r.get("ltp"), r.get("change_pct_day"))
    r["symbol"] = str(r.get("symbol") or "").strip().upper()
    r["sector"] = str(r.get("sector") or "").strip()
    r["ltp"] = lt
    r["change_pct_day"] = cg
    iku = r.get("instrument_key")
    r["instrument_key"] = str(iku).strip() if iku else ""
    return r


def get_iron_condor_universe_master_rows() -> List[Dict[str, Any]]:
    """
    Symbol, sector, equity instrument_key for the approved universe — used only by /universe and
    /approved-underlyings (lazy first call per worker). Hot paths resolve one key via
    `_instrument_key_for_ic_equity` instead of building this list.
    """
    global _ic_master_universe_rows
    if _ic_master_universe_rows is not None:
        return [{**r} for r in _ic_master_universe_rows]
    with _ic_master_universe_lock:
        if _ic_master_universe_rows is None:
            rows: List[Dict[str, Any]] = []
            for sym_u, sector in sorted(IRON_CONDOR_UNIVERSE.items()):
                api = option_chain_underlying(str(sym_u))
                ik = _instrument_key_for_ic_equity(api) or ""
                rows.append({"symbol": str(sym_u), "sector": str(sector), "instrument_key": ik})
            _ic_master_universe_rows = rows
        return [{**r} for r in _ic_master_universe_rows]


def _ic_prev_day_close_from_ohlc(nested: Any) -> float:
    """Prefer previous session close keys from Upstox ohlc; fall back to close."""
    if not isinstance(nested, dict):
        return 0.0
    for key in ("previous_close", "prev_close", "close"):
        v = nested.get(key)
        if v is not None and v != "":
            try:
                f = float(v)
                if f > 0:
                    return f
            except (TypeError, ValueError):
                continue
    return 0.0


def _normalize_ik_key(k: str) -> str:
    return k.replace(" ", "").replace(":", "|").upper()


def _batch_lookup(batch: Dict[str, Dict[str, Any]], ik: str) -> Optional[Dict[str, Any]]:
    if not ik or not batch:
        return None
    if ik in batch:
        return batch[ik]
    a1, a2 = ik.replace("|", ":"), ik.replace(":", "|")
    if a1 in batch:
        return batch[a1]
    if a2 in batch:
        return batch[a2]
    nreq = _normalize_ik_key(ik)
    for rk, qd in batch.items():
        if _normalize_ik_key(str(rk)) == nreq:
            return qd if isinstance(qd, dict) else None
    return None


def build_universe_picker_rows_with_quotes_cached() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Build universe picker rows (symbol, sector, ltp, change_pct_day) in one batched
    Upstox request. Cached briefly to keep the UI snappy.

    Returns:
        (rows, quotes_error). Rows omit active_position — merge in the router.
        quotes_error is human-readable when broker/auth is unavailable.
    """
    global _ic_uq_cache_deadline, _ic_uq_cache_rows

    ttl = float(os.getenv("IRON_CONDOR_UNIVERSE_QUOTES_CACHE_SEC", "55") or "55")
    now = time.monotonic()
    if _ic_uq_cache_rows and now < _ic_uq_cache_deadline:
        return ([copy.deepcopy(r) for r in _ic_uq_cache_rows], None)

    pairs: List[Tuple[str, str, str]] = []
    for sym_u, sector in sorted(IRON_CONDOR_UNIVERSE.items()):
        api = option_chain_underlying(str(sym_u))
        ik_u = _instrument_key_for_ic_equity(api) or ""
        pairs.append((str(sym_u), str(sector), ik_u))

    tok = (getattr(vwap_service, "access_token", None) or "").strip()
    quotes_error: Optional[str] = None
    batch: Dict[str, Dict[str, Any]] = {}
    keys = [ik for _, _, ik in pairs if ik]

    if not tok:
        quotes_error = (
            "Upstox is not connected (no access token). Sign in to Upstox from "
            "Settings or use the OAuth link, then reload this page."
        )
    elif not keys:
        quotes_error = (
            "Could not resolve equity instrument keys for the universe (instruments data missing?)."
        )
    else:
        try:
            snap = vwap_service.get_market_quote_snapshots_batch(keys, max_per_request=100)
            batch = snap if snap else {}
            if not batch:
                quotes_error = (
                    "Upstox returned no quote data (session may be expired). Reconnect Upstox and retry."
                )
        except Exception as ex:
            logger.warning("Iron Condor universe batch quotes failed: %s", ex)
            quotes_error = "Upstox quotes request failed — check broker connection and token."

    rows: List[Dict[str, Any]] = []
    for sym, sec, ik in pairs:
        ltp_f: Optional[float] = None
        chg: Optional[float] = None
        qd = _batch_lookup(batch, ik) if ik else None
        if qd:
            try:
                lp = float(qd.get("last_price") or 0)
                ltp_f = lp if lp > 0 else None
                nested = qd.get("ohlc") if isinstance(qd.get("ohlc"), dict) else {}
                prev = _ic_prev_day_close_from_ohlc(nested)
                if ltp_f and prev > 0:
                    chg = round((ltp_f - prev) / prev * 100.0, 2)
            except (TypeError, ValueError):
                pass
        rows.append(
            normalize_ic_universe_row_for_api(
                {
                    "symbol": sym,
                    "sector": sec,
                    "instrument_key": ik or "",
                    "ltp": ltp_f,
                    "change_pct_day": chg,
                }
            )
        )

    _ic_uq_cache_rows = [copy.deepcopy(r) for r in rows]
    _ic_uq_cache_deadline = now + max(5.0, ttl)

    return ([copy.deepcopy(r) for r in rows], quotes_error)

# Same mappings as scan router for rare symbol variants (option chain underlying).
OPTION_CHAIN_API_SYMBOL = {
    "LTIMIND": "LTIM",
    "LTIMINDTREE": "LTIM",
    "HPCL": "HINDPETRO",
    "PERSISTENTSYS": "PERSISTENT",
}


def option_chain_underlying(symbol: str) -> str:
    key = symbol.strip().upper()
    return OPTION_CHAIN_API_SYMBOL.get(key, key)


def _instrument_key_for_ic_equity(api_symbol: str) -> Optional[str]:
    """Cached wrapper — avoids repeated cold loads from instruments_downloader for picker clicks."""
    k = (api_symbol or "").strip().upper()
    if not k:
        return None
    if k in _ic_equity_ik_cache:
        return _ic_equity_ik_cache[k]
    ik = vwap_service.get_instrument_key(api_symbol)
    if ik:
        _ic_equity_ik_cache[k] = ik
    return ik


def iron_condor_position_table_exists(db: Session) -> bool:
    """Fast check — avoids ensure_iron_condor_tables() when the table is already there."""
    try:
        return bool(db.execute(text("SELECT to_regclass('public.iron_condor_position')")).scalar())
    except Exception:
        return False


def user_has_ic_active_open_position(db: Session, user_id: int, underlying: str) -> bool:
    """Active / open / adjusted row for this underlying; no DDL if table missing."""
    if not iron_condor_position_table_exists(db):
        return False
    sym = (underlying or "").strip()
    if not sym:
        return False
    try:
        row_ct = db.execute(
            text(
                """
                SELECT COUNT(*) FROM iron_condor_position
                WHERE user_id = :uid AND UPPER(TRIM(underlying)) = UPPER(:sy)
                  AND UPPER(status) IN ('ACTIVE', 'OPEN', 'ADJUSTED')
                """
            ),
            {"uid": user_id, "sy": sym},
        ).scalar()
        return int(row_ct or 0) >= 1
    except Exception:
        return False


def universe_picker_snapshot_row_only(db: Session, user_id: int, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    DB-only picker row (today's pre-market daily closes). Skips ensure_iron_condor_tables and Upstox.
    Used to unblock the UI when the full quote route is slow (migrations / broker).
    """
    ensure_iron_condor_snapshot_tables()
    sym = (symbol or "").strip().upper()
    sec = sector_for_symbol(sym)
    if not sec:
        return {}, "Symbol is not in the Iron Condor universe."

    active = user_has_ic_active_open_position(db, user_id, sym)

    trade_date = mh_ic._normalize_ist(None).date()
    _, closes = read_underlying_atr_closes_session(db, trade_date, sym)

    ltp_f: Optional[float] = None
    chg: Optional[float] = None
    if closes and len(closes) >= 1:
        try:
            last = float(closes[-1])
            if last > 0:
                ltp_f = last
                if len(closes) >= 2:
                    prev_d = float(closes[-2])
                    if prev_d > 0:
                        chg = round((last - prev_d) / prev_d * 100.0, 2)
        except (TypeError, ValueError):
            pass

    quotes_error: Optional[str] = None
    if ltp_f is None or ltp_f <= 0:
        quotes_error = (
            "No snapshot close for today yet — run refresh daily cache or wait for the pre-market job."
        )

    ik_snap = _instrument_key_for_ic_equity(option_chain_underlying(sym)) or ""

    return (
        normalize_ic_picker_row_for_api(
            {
                "symbol": sym,
                "sector": sec,
                "instrument_key": ik_snap,
                "ltp": ltp_f,
                "change_pct_day": chg,
                "active_position": active,
                "quote_source": "daily_cache",
            }
        ),
        quotes_error,
    )


def warm_iron_condor_startup() -> None:
    """
    Run once per worker at process boot (see main.py lifespan).
    Pays DDL/migrations once so checklist/positions paths are ready. Does not pre-build the universe
    instrument list (that is lazy on first /universe or /approved-underlyings).
    """
    ensure_iron_condor_tables()


def universe_picker_row_for_symbol(db: Session, user_id: int, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Hot path: DB active check (no DDL) + one Upstox snapshot for the picked symbol only.
    Instrument key comes from the per-symbol resolver cache, not the full universe master list.
    """
    sym = (symbol or "").strip().upper()
    sec = sector_for_symbol(sym)
    if not sec:
        return {}, "Symbol is not in the Iron Condor universe."

    active = user_has_ic_active_open_position(db, user_id, sym)
    api = option_chain_underlying(sym)
    ik = _instrument_key_for_ic_equity(api) or ""

    tok = (getattr(vwap_service, "access_token", None) or "").strip()
    quotes_error: Optional[str] = None
    ltp_f: Optional[float] = None
    chg: Optional[float] = None
    quote_source = "live"

    if not tok:
        quotes_error = (
            "Upstox is not connected (no access token). Sign in to Upstox from "
            "Settings or use the OAuth link, then reload this page."
        )
    elif not ik:
        quotes_error = "Could not resolve equity instrument key for this symbol."
    else:
        batch: Dict[str, Any] = {}
        try:
            batch = vwap_service.get_market_quote_snapshots_batch(
                [ik],
                max_per_request=10,
                request_timeout=4,
                max_retries=1,
            )
        except Exception as ex:
            logger.warning("Iron Condor single-symbol quote failed: %s", ex)
            quotes_error = "Upstox quotes request failed — check broker connection and token."
        qd = _batch_lookup(batch, ik) if batch else None
        if qd:
            try:
                lp = float(qd.get("last_price") or 0)
                live_ltp = lp if lp > 0 else None
                nested = qd.get("ohlc") if isinstance(qd.get("ohlc"), dict) else {}
                prev = _ic_prev_day_close_from_ohlc(nested)
                if live_ltp and prev > 0:
                    chg = round((live_ltp - prev) / prev * 100.0, 2)
                if live_ltp is not None and live_ltp > 0:
                    ltp_f = live_ltp
                    quotes_error = None
            except (TypeError, ValueError):
                pass
        if ltp_f is None or ltp_f <= 0:
            quotes_error = quotes_error or (
                "Upstox returned no quote for this symbol (session may be expired)."
            )

    return (
        normalize_ic_picker_row_for_api(
            {
                "symbol": sym,
                "sector": sec,
                "instrument_key": ik,
                "ltp": ltp_f,
                "change_pct_day": chg,
                "active_position": active,
                "quote_source": quote_source,
            }
        ),
        quotes_error,
    )


def ensure_iron_condor_tables() -> None:
    global _iron_condor_tables_ready_flag
    if engine is None:
        return
    if _iron_condor_tables_ready_flag:
        return
    with _ic_tables_lock:
        if _iron_condor_tables_ready_flag:
            return
        ddl = """
    CREATE TABLE IF NOT EXISTS iron_condor_user_settings (
        user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
        trading_capital NUMERIC(18,2) NOT NULL DEFAULT 0,
        max_simultaneous_positions INTEGER NOT NULL DEFAULT 3,
        target_position_slots INTEGER NOT NULL DEFAULT 5,
        profit_target_pct_of_credit NUMERIC(8,2),
        stop_loss_pct_of_credit NUMERIC(8,2),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS iron_condor_position (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        underlying VARCHAR(32) NOT NULL,
        sector VARCHAR(64) NOT NULL,
        status VARCHAR(16) NOT NULL DEFAULT 'OPEN',
        expiry_date DATE NOT NULL,
        monthly_atr NUMERIC(18,4),
        strike_distance NUMERIC(18,4),
        spot_at_snapshot NUMERIC(18,4),
        strike_interval NUMERIC(18,6),
        sell_call_strike NUMERIC(18,4) NOT NULL,
        buy_call_strike NUMERIC(18,4) NOT NULL,
        sell_put_strike NUMERIC(18,4) NOT NULL,
        buy_put_strike NUMERIC(18,4) NOT NULL,
        premium_sell_call NUMERIC(18,4),
        premium_buy_call NUMERIC(18,4),
        premium_sell_put NUMERIC(18,4),
        premium_buy_put NUMERIC(18,4),
        premium_collected NUMERIC(18,4),
        hedge_cost NUMERIC(18,4),
        hedge_ratio NUMERIC(18,6),
        hedge_gate VARCHAR(16) NOT NULL,
        allocation_pct NUMERIC(8,4),
        suggested_capital_rupees NUMERIC(18,2),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        closed_at TIMESTAMP WITH TIME ZONE,
        UNIQUE (user_id, underlying, expiry_date)
    );
    CREATE INDEX IF NOT EXISTS idx_ic_pos_user_status ON iron_condor_position(user_id, status);
    CREATE TABLE IF NOT EXISTS iron_condor_alert (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        position_id INTEGER REFERENCES iron_condor_position(id) ON DELETE CASCADE,
        rule_code VARCHAR(64) NOT NULL,
        message TEXT NOT NULL,
        payload_json JSONB,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        acknowledged BOOLEAN NOT NULL DEFAULT FALSE
    );
    CREATE INDEX IF NOT EXISTS idx_ic_alert_user_created ON iron_condor_alert(user_id, created_at DESC);
    """
        with engine.begin() as conn:
            for stmt in ddl.split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
        try:
            from backend.services.iron_condor_extended import iron_condor_migrations_v2

            iron_condor_migrations_v2()
        except Exception as e:
            logger.warning("iron_condor_extended migrations: %s", e)
        try:
            from backend.services.iron_condor_snapshot_cache import ensure_iron_condor_snapshot_tables

            ensure_iron_condor_snapshot_tables()
        except Exception as e:
            logger.warning("iron_condor snapshot tables: %s", e)
        _iron_condor_tables_ready_flag = True


def _strike_ladder_price_list(spot: float, step: float, *, half_width: int = 60) -> List[float]:
    if step <= 0:
        return []
    mid = round(float(spot) / step) * step
    out: List[float] = []
    for i in range(-half_width, half_width + 1):
        px = mid + float(i) * step
        if px > 0:
            out.append(round(px, 6))
    return sorted(set(out))


def _align_geometric_long_wing(sell_strike: float, step: float, *, long_call: bool, wing_steps: int = 5) -> float:
    raw = (
        float(sell_strike) + float(wing_steps) * float(step)
        if long_call
        else float(sell_strike) - float(wing_steps) * float(step)
    )
    aligned = round(raw / step) * step
    if long_call and aligned <= float(sell_strike):
        aligned += step
    if not long_call and aligned >= float(sell_strike):
        aligned -= step
    return float(aligned)


def _leg_dict_from_quote(strike: float, ce_side: bool, quote: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(quote, dict):
        return {
            "strike": strike,
            "side": "CE" if ce_side else "PE",
            "ltp": None,
            "bid": None,
            "ask": None,
            "oi": 0,
            "iv": None,
        }
    lp = float(quote.get("last_price") or quote.get("ltp") or 0.0)
    oi_raw = quote.get("oi")
    try:
        oi = int(float(oi_raw)) if oi_raw is not None else 0
    except (TypeError, ValueError):
        oi = 0
    ohlc = quote.get("ohlc") if isinstance(quote.get("ohlc"), dict) else {}
    bid = ohlc.get("close") if isinstance(ohlc, dict) else None  # placeholders; vendor often omits NBBO depth
    return {
        "strike": strike,
        "side": "CE" if ce_side else "PE",
        "ltp": round(lp, 4) if lp else None,
        "bid": bid,
        "ask": bid,
        "oi": max(oi, 0),
        "iv": None,
    }


def batch_legs_quote_for_strikes(api_sym: str, expiry_date: date, strikes: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    """
    Four targeted option quotes via instrument keys + batch market-quote (no full chain download).
    """
    exp_dt = IST.localize(datetime.combine(expiry_date, dt_time(12, 0)))
    spec: List[Tuple[str, float, str]] = [
        ("sell_call", float(strikes["sell_call"]), "CE"),
        ("buy_call", float(strikes["buy_call"]), "CE"),
        ("sell_put", float(strikes["sell_put"]), "PE"),
        ("buy_put", float(strikes["buy_put"]), "PE"),
    ]
    ik_list: List[Optional[str]] = [
        vwap_service.get_option_instrument_key(api_sym, exp_dt, st, ot) for _nm, st, ot in spec
    ]
    valid_keys = [k for k in ik_list if k]
    snaps: Dict[str, Dict[str, Any]] = {}
    if valid_keys:
        snaps = vwap_service.get_market_quote_snapshots_batch(valid_keys, request_timeout=12, max_retries=2) or {}
    out: Dict[str, Dict[str, Any]] = {}
    for j, (nm, st, ot) in enumerate(spec):
        ik = ik_list[j]
        ce = ot.upper() == "CE"
        qd = None
        if ik:
            qd = snaps.get(ik) or snaps.get(ik.replace("|", ":"))
        out[nm] = _leg_dict_from_quote(st, ce, qd)
    return out


def _monthly_atr_wilder_14(monthly_rows: List[Dict[str, Any]]) -> Optional[float]:
    """ATR(14) on MONTHLY candles; last candle's ATR (Wilder smoothing)."""
    if not monthly_rows or len(monthly_rows) < 16:
        return None
    bars = sorted(monthly_rows, key=lambda r: r.get("timestamp") or "")
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    closes = [float(b["close"]) for b in bars]
    tr: List[float] = []
    for i in range(1, len(bars)):
        tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    if len(tr) < 14:
        return None
    p = 14
    atr = sum(tr[:p]) / float(p)
    for i in range(p, len(tr)):
        atr = (atr * (p - 1) + tr[i]) / float(p)
    return float(atr)


def _extract_chain_strike_list(chain_payload: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    raw = chain_payload
    strike_list = None
    if isinstance(raw, dict):
        if isinstance(raw.get("strikes"), list):
            strike_list = raw["strikes"]
        elif isinstance(raw.get("data"), dict) and isinstance(raw["data"].get("strikes"), list):
            strike_list = raw["data"]["strikes"]
    elif isinstance(raw, list):
        strike_list = raw
    if not strike_list:
        return None
    return strike_list


def _strike_row_ltp_oi(strike_data: Dict[str, Any], ce: bool) -> Tuple[float, float]:
    """Return (ltp, oi) for CE or PE leg."""
    key = "call_options" if ce else "put_options"
    node = strike_data.get(key)
    option_data = None
    if isinstance(node, dict):
        option_data = node.get("market_data", node)
    elif isinstance(node, list) and node and isinstance(node[0], dict):
        option_data = node[0]
    if not option_data or not isinstance(option_data, dict):
        return 0.0, 0.0
    ltp = float(option_data.get("ltp") or option_data.get("last_price") or 0.0)
    oi = float(option_data.get("oi") or option_data.get("open_interest") or 0.0)
    return ltp, oi


def _build_strike_aggregate(strike_list: List[Dict[str, Any]]) -> Dict[float, Dict[str, Any]]:
    out: Dict[float, Dict[str, Any]] = {}
    for sd in strike_list:
        if not isinstance(sd, dict):
            continue
        sp_raw = sd.get("strike_price")
        if sp_raw is None:
            continue
        sp = float(sp_raw)
        cel, ceoi = _strike_row_ltp_oi(sd, True)
        pel, peoi = _strike_row_ltp_oi(sd, False)
        out[sp] = {"strike": sp, "ce_ltp": cel, "ce_oi": ceoi, "pe_ltp": pel, "pe_oi": peoi}
    return out


def _nearest_strike_ge(sorted_strikes: List[float], threshold: float) -> Optional[float]:
    for sp in sorted_strikes:
        if sp >= threshold:
            return sp
    return None


def _nearest_strike_le(sorted_strikes: List[float], threshold: float) -> Optional[float]:
    for sp in reversed(sorted_strikes):
        if sp <= threshold:
            return sp
    return None


def _pick_buy_wing(
    sorted_strikes: List[float],
    agg: Dict[float, Dict[str, Any]],
    sell_strike: float,
    step: float,
    long_call: bool,
) -> Optional[Tuple[float, float, float, List[str]]]:
    """
    Buy wing ~5–6 strikes from sell; if exact step not on chain, widen search and pick
    closest strike with best OI. Returns (strike, ltp, oi, warnings) or None.
    """
    if not sorted_strikes or step <= 0:
        return None
    warnings: List[str] = []

    def collect_for_steps(ks: Tuple[int, ...], tol_mult: float, min_oi: float) -> List[Tuple[float, float, float, int]]:
        out: List[Tuple[float, float, float, int]] = []
        for k in ks:
            target = sell_strike + k * step if long_call else sell_strike - k * step
            best_sp = None
            best_dist = None
            for sp in sorted_strikes:
                d = abs(sp - target)
                if best_sp is None or d < best_dist:
                    best_sp, best_dist = sp, d
            if best_sp is None or best_dist is None or best_dist > step * tol_mult:
                continue
            row = agg.get(best_sp)
            if not row:
                continue
            if long_call:
                ltp, oi = row["ce_ltp"], row["ce_oi"]
            else:
                ltp, oi = row["pe_ltp"], row["pe_oi"]
            m_oi = max(oi, 0.0)
            if m_oi < min_oi:
                continue
            out.append((best_sp, float(ltp), m_oi, k))
        return out

    # Prefer ideal 5–6 step with tight chain fit; then widen; lower OI floor last.
    plans: List[Tuple[Tuple[int, ...], float, float]] = [
        ((5, 6), 0.55, 500.0),
        ((5, 6), 0.55, 0.0),
        ((4, 7), 1.05, 500.0),
        ((4, 7), 1.05, 0.0),
        ((3, 8), 1.55, 0.0),
        ((2, 9), 2.05, 0.0),
    ]
    for i, (ks, tol, min_oi) in enumerate(plans):
        bucket = collect_for_steps(ks, tol, min_oi)
        if not bucket:
            continue
        if i == 1:
            warnings.append(
                "Ideal hedge distance had OI under 500 on chain snapshot; using strike with best available OI."
            )
        elif i >= 2:
            warnings.append(
                "Hedge strike mapped to closest available strike (spacing widened vs ideal 5–6 steps). "
                "Confirm bid/ask and OI in Upstox."
            )
        best = max(bucket, key=lambda t: (t[2], -abs(t[3] - 5)))
        return (best[0], best[1], best[2], warnings)

    return None


def classify_hedge_ratio(ratio: float) -> Tuple[str, str]:
    if ratio <= 0 or ratio != ratio:
        return "BLOCK", "Invalid hedge ratio."
    if 0.25 <= ratio <= 0.35:
        return "VALID", "Hedge ratio within 25%–35%."
    if ratio < 0.25:
        return "WARN", "Hedge ratio below 25%; hedges may be too far (lower protection)."
    return "BLOCK", "Hedge ratio above 35%; strikes too tight — recompute monthly ATR and strikes."


def compute_position_suggestion(
    *,
    trading_capital: float,
    target_slots: int,
    new_sector: str,
    open_sectors: List[str],
) -> Tuple[float, Optional[str]]:
    """Returns (allocation_pct as 0–100, reject_reason). Max 3 open; one active condor per sector."""
    if trading_capital <= 0:
        return 0.0, "Trading capital must be positive."
    su = [s.upper() for s in open_sectors]
    if new_sector.upper() in su:
        return 0.0, "You already have an open Iron Condor in this sector."
    distinct = sorted(set(su))
    if len(distinct) >= 3:
        return 0.0, "Maximum 3 concurrent positions (one per sector)."
    pct = 3.0 if int(target_slots) >= 5 else 5.0
    return pct, None


def _iron_condor_capital_slots_from_env() -> Tuple[float, int]:
    from backend.config import settings as _settings

    return (
        float(getattr(_settings, "IRON_CONDOR_TRADING_CAPITAL_DEFAULT", 500_000.0)),
        int(getattr(_settings, "IRON_CONDOR_TARGET_POSITION_SLOTS", 5)),
    )


def get_or_create_settings(db: Session, user_id: int) -> Dict[str, Any]:
    ensure_iron_condor_tables()
    env_tc, env_slots = _iron_condor_capital_slots_from_env()
    row = db.execute(
        text("SELECT * FROM iron_condor_user_settings WHERE user_id = :uid LIMIT 1"),
        {"uid": user_id},
    ).mappings().first()
    if row:
        d = dict(row)
        d["trading_capital"] = env_tc
        d["target_position_slots"] = max(1, min(10, env_slots))
        return d
    db.execute(
        text(
            """
            INSERT INTO iron_condor_user_settings (user_id, trading_capital, max_simultaneous_positions, target_position_slots)
            VALUES (:uid, :tc, 3, :ts)
            """
        ),
        {"uid": user_id, "tc": env_tc, "ts": max(1, min(10, env_slots))},
    )
    db.commit()
    row2 = db.execute(
        text("SELECT * FROM iron_condor_user_settings WHERE user_id = :uid LIMIT 1"),
        {"uid": user_id},
    ).mappings().first()
    d2 = dict(row2 or {})
    d2["trading_capital"] = env_tc
    d2["target_position_slots"] = max(1, min(10, env_slots))
    return d2


def update_settings(db: Session, user_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
    get_or_create_settings(db, user_id)
    patches = []
    params: Dict[str, Any] = {"uid": user_id}
    # trading_capital / target_position_slots come from IRON_CONDOR_* env vars only
    if "max_simultaneous_positions" in body:
        patches.append("max_simultaneous_positions = :mx")
        params["mx"] = max(1, min(12, int(body["max_simultaneous_positions"])))
    if "profit_target_pct_of_credit" in body:
        patches.append("profit_target_pct_of_credit = :pp")
        params["pp"] = body["profit_target_pct_of_credit"]
    if "stop_loss_pct_of_credit" in body:
        patches.append("stop_loss_pct_of_credit = :sl")
        params["sl"] = body["stop_loss_pct_of_credit"]
    if patches:
        patches.append("updated_at = CURRENT_TIMESTAMP")
        db.execute(
            text(f"UPDATE iron_condor_user_settings SET {', '.join(patches)} WHERE user_id = :uid"),
            params,
        )
        db.commit()
    return get_or_create_settings(db, user_id)


def _open_sectors(db: Session, user_id: int) -> List[str]:
    rows = db.execute(
        text(
            """
            SELECT sector FROM iron_condor_position
            WHERE user_id = :uid AND UPPER(status) IN ('OPEN', 'ACTIVE', 'ADJUSTED')
            """
        ),
        {"uid": user_id},
    ).fetchall()
    return [r[0] for r in rows]


def analyze_iron_condor(symbol: str, db: Session, user_id: int) -> Dict[str, Any]:
    ensure_iron_condor_tables()
    und = (symbol or "").strip().upper()
    sec = sector_for_symbol(und)
    if not sec:
        raise ValueError(f"Symbol {und} is not in the approved Iron Condor universe.")

    settings = get_or_create_settings(db, user_id)
    tc = float(settings.get("trading_capital") or 0.0)
    target_slots = int(settings.get("target_position_slots") or 5)
    os_secs = _open_sectors(db, user_id)
    alloc_pct, sizing_err = compute_position_suggestion(
        trading_capital=tc,
        target_slots=target_slots,
        new_sector=sec,
        open_sectors=os_secs,
    )
    suggested_cap = round(tc * (alloc_pct / 100.0), 2) if tc > 0 and alloc_pct else None

    api_sym = option_chain_underlying(und)
    eq_key = vwap_service.get_instrument_key(api_sym)
    if not eq_key:
        raise RuntimeError(f"No equity instrument key for {api_sym}")

    ohlc_live = vwap_service.get_ohlc_data(eq_key) or {}
    spot = float(ohlc_live.get("last_price") or ohlc_live.get("close") or 0.0)
    nested_lc = ohlc_live.get("ohlc") if isinstance(ohlc_live.get("ohlc"), dict) else {}
    prev_close_live = float(
        nested_lc.get("close") or ohlc_live.get("close_price") or nested_lc.get("previous_close") or 0.0
    )
    pct_day_live: Optional[float] = None
    if spot > 0 and prev_close_live > 0:
        pct_day_live = round((spot - prev_close_live) / prev_close_live * 100.0, 2)

    if spot <= 0:
        spot_try = vwap_service.get_candle_vwap_by_instrument_key(eq_key, interval="days/1", days_back=3)
        if spot_try is not None and spot_try > 0:
            spot = float(spot_try)
    if spot <= 0:
        mq = vwap_service.get_market_quote(api_sym)
        if isinstance(mq, dict) and mq.get("last_price"):
            spot = float(mq["last_price"])
    if spot <= 0:
        raise RuntimeError(f"Could not resolve spot for {api_sym}")

    td_ic = datetime.now(IST).date()
    from backend.services.iron_condor_snapshot_cache import read_underlying_atr_closes_session

    cached_atr, _ = read_underlying_atr_closes_session(db, td_ic, und)
    atr_from_prefetch = cached_atr is not None and float(cached_atr) > 0
    monthly_atr: Optional[float] = float(cached_atr) if atr_from_prefetch else None
    if monthly_atr is None or monthly_atr <= 0:
        months_live = vwap_service.get_monthly_candles_by_instrument_key(eq_key, months_back=48) or []
        monthly_atr = _monthly_atr_wilder_14(months_live)
    if monthly_atr is None or monthly_atr <= 0:
        raise RuntimeError("Insufficient monthly candles for ATR(14).")

    strike_distance = 1.25 * float(monthly_atr)
    step = float(vwap_service.calculate_strike_interval(float(spot)))
    ladder = _strike_ladder_price_list(float(spot), step)

    sell_call_thr = float(spot) + strike_distance
    sell_put_thr = float(spot) - strike_distance
    sell_ce = _nearest_strike_ge(ladder, sell_call_thr)
    sell_pe = _nearest_strike_le(ladder, sell_put_thr)
    if sell_ce is None or sell_pe is None:
        raise RuntimeError("Could not place short strikes relative to strike distance.")

    buy_ce_strike = _align_geometric_long_wing(float(sell_ce), step, long_call=True, wing_steps=5)
    buy_pe_strike = _align_geometric_long_wing(float(sell_pe), step, long_call=False, wing_steps=5)

    exp = vwap_service.get_monthly_expiry()
    exp_d = exp.date()

    strikes_plan = {
        "sell_call": float(sell_ce),
        "buy_call": float(buy_ce_strike),
        "sell_put": float(sell_pe),
        "buy_put": float(buy_pe_strike),
    }
    legs_quote = batch_legs_quote_for_strikes(api_sym, exp_d, strikes_plan)

    def _ltp(nm: str) -> float:
        return float(((legs_quote.get(nm) or {}).get("ltp")) or 0.0)

    prem_sell_ce = _ltp("sell_call")
    prem_sell_pe = _ltp("sell_put")
    prem_buy_ce = _ltp("buy_call")
    prem_buy_pe = _ltp("buy_put")

    prem_collected = prem_sell_ce + prem_sell_pe
    hedge_cost = prem_buy_ce + prem_buy_pe
    hedge_ratio = (hedge_cost / prem_collected) if prem_collected > 0 else 0.0
    gate, gate_msg = classify_hedge_ratio(hedge_ratio)

    strike_warnings = [
        "Strikes: symmetric ladder for shorts + geometric 5‑step hedges — four targeted broker quotes "
        "(no full chain scrape). Confirm bid/ask and OI manually in Upstox."
    ]
    if atr_from_prefetch:
        strike_warnings.insert(0, "Monthly ATR(14) reused from today's Iron Condor pre‑market cache when available.")

    expiry_note = (
        "Cycle: hold from day 1 of new monthly expiry until profit target / stop loss / 10 days before expiry (advisory)."
    )

    return {
        "underlying": und,
        "sector": sec,
        "option_chain_symbol": api_sym,
        "spot": round(float(spot), 4),
        "monthly_atr_14": round(float(monthly_atr), 4),
        "strike_distance": round(strike_distance, 4),
        "strike_interval": step,
        "expiry_date": exp.strftime("%Y-%m-%d"),
        "strikes": {
            "sell_call": sell_ce,
            "buy_call": buy_ce_strike,
            "sell_put": sell_pe,
            "buy_put": buy_pe_strike,
        },
        "premiums": {
            "sell_call": round(prem_sell_ce, 4),
            "buy_call": round(prem_buy_ce, 4),
            "sell_put": round(prem_sell_pe, 4),
            "buy_put": round(prem_buy_pe, 4),
        },
        "premium_collected": round(prem_collected, 4),
        "hedge_cost": round(hedge_cost, 4),
        "hedge_ratio": round(hedge_ratio, 4),
        "hedge_gate": gate,
        "hedge_gate_message": gate_msg,
        "legs_quote": legs_quote,
        "underlying_change_pct_today": pct_day_live,
        "position_sizing": {
            "trading_capital": tc,
            "target_position_slots": target_slots,
            "suggested_allocation_pct": alloc_pct if sizing_err is None else 0.0,
            "suggested_capital_rupees": suggested_cap,
            "reject_reason": sizing_err,
            "max_concurrent_sectors_rule": "At most 3 open positions across different sectors.",
        },
        "disclaimer": "Advisory only — no orders placed. Execute manually on Upstox.",
        "notes": expiry_note,
        "strike_selection_warnings": strike_warnings,
    }


def persist_position_from_analysis(db: Session, user_id: int, snapshot: Dict[str, Any]) -> int:
    ensure_iron_condor_tables()
    und = snapshot["underlying"]
    sec = snapshot["sector"]
    exp_d = datetime.strptime(snapshot["expiry_date"], "%Y-%m-%d").date()
    st = snapshot["strikes"]
    pr = snapshot["premiums"]
    ps = snapshot["position_sizing"]
    db.execute(
        text(
            """
            INSERT INTO iron_condor_position (
                user_id, underlying, sector, status, expiry_date,
                monthly_atr, strike_distance, spot_at_snapshot, strike_interval,
                sell_call_strike, buy_call_strike, sell_put_strike, buy_put_strike,
                premium_sell_call, premium_buy_call, premium_sell_put, premium_buy_put,
                premium_collected, hedge_cost, hedge_ratio, hedge_gate,
                allocation_pct, suggested_capital_rupees
            ) VALUES (
                :uid, :und, :sec, 'ACTIVE', :exp,
                :matr, :sdist, :spot, :sint,
                :sce, :bce, :spe, :bpe,
                :psce, :pbce, :pspe, :pbpe,
                :pcol, :hcost, :hrat, :hgate,
                :apct, :scap
            )
            ON CONFLICT (user_id, underlying, expiry_date)
            DO UPDATE SET
                status = 'ACTIVE',
                monthly_atr = EXCLUDED.monthly_atr,
                strike_distance = EXCLUDED.strike_distance,
                spot_at_snapshot = EXCLUDED.spot_at_snapshot,
                strike_interval = EXCLUDED.strike_interval,
                sell_call_strike = EXCLUDED.sell_call_strike,
                buy_call_strike = EXCLUDED.buy_call_strike,
                sell_put_strike = EXCLUDED.sell_put_strike,
                buy_put_strike = EXCLUDED.buy_put_strike,
                premium_sell_call = EXCLUDED.premium_sell_call,
                premium_buy_call = EXCLUDED.premium_buy_call,
                premium_sell_put = EXCLUDED.premium_sell_put,
                premium_buy_put = EXCLUDED.premium_buy_put,
                premium_collected = EXCLUDED.premium_collected,
                hedge_cost = EXCLUDED.hedge_cost,
                hedge_ratio = EXCLUDED.hedge_ratio,
                hedge_gate = EXCLUDED.hedge_gate,
                allocation_pct = EXCLUDED.allocation_pct,
                suggested_capital_rupees = EXCLUDED.suggested_capital_rupees
            """
        ),
        {
            "uid": user_id,
            "und": und,
            "sec": sec,
            "exp": exp_d,
            "matr": snapshot.get("monthly_atr_14"),
            "sdist": snapshot.get("strike_distance"),
            "spot": snapshot.get("spot"),
            "sint": snapshot.get("strike_interval"),
            "sce": st["sell_call"],
            "bce": st["buy_call"],
            "spe": st["sell_put"],
            "bpe": st["buy_put"],
            "psce": pr["sell_call"],
            "pbce": pr["buy_call"],
            "pspe": pr["sell_put"],
            "pbpe": pr["buy_put"],
            "pcol": snapshot.get("premium_collected"),
            "hcost": snapshot.get("hedge_cost"),
            "hrat": snapshot.get("hedge_ratio"),
            "hgate": snapshot.get("hedge_gate"),
            "apct": ps.get("suggested_allocation_pct"),
            "scap": ps.get("suggested_capital_rupees"),
        },
    )
    db.commit()
    rid = db.execute(
        text(
            """
            SELECT id FROM iron_condor_position
            WHERE user_id = :uid AND underlying = :und AND expiry_date = :exp
            """
        ),
        {"uid": user_id, "und": und, "exp": exp_d},
    ).scalar()
    return int(rid or 0)


def list_positions(db: Session, user_id: int) -> List[Dict[str, Any]]:
    ensure_iron_condor_tables()
    rows = db.execute(
        text(
            """
            SELECT * FROM iron_condor_position
            WHERE user_id = :uid
            ORDER BY created_at DESC
            LIMIT 100
            """
        ),
        {"uid": user_id},
    ).mappings().all()
    return [dict(r) for r in rows]


def close_position(db: Session, user_id: int, position_id: int) -> bool:
    ensure_iron_condor_tables()
    res = db.execute(
        text(
            """
            UPDATE iron_condor_position
            SET status = 'CLOSED', closed_at = CURRENT_TIMESTAMP
            WHERE id = :pid AND user_id = :uid AND UPPER(status) IN ('OPEN','ACTIVE','ADJUSTED')
            """
        ),
        {"pid": position_id, "uid": user_id},
    )
    db.commit()
    return res.rowcount > 0


def _fresh_chain_quotes(underlying: str, strikes_pack: Dict[str, float]) -> Optional[Dict[str, float]]:
    api_sym = option_chain_underlying(underlying)
    try:
        exp_d = vwap_service.get_monthly_expiry().date()
    except Exception:
        return None
    sp = {
        "sell_call": float(strikes_pack["sell_call"]),
        "buy_call": float(strikes_pack["buy_call"]),
        "sell_put": float(strikes_pack["sell_put"]),
        "buy_put": float(strikes_pack["buy_put"]),
    }
    lq = batch_legs_quote_for_strikes(api_sym, exp_d, sp)

    def _lq(nm: str) -> float:
        return float(((lq.get(nm) or {}).get("ltp")) or 0.0)

    sc, bc, spp, bp = _lq("sell_call"), _lq("buy_call"), _lq("sell_put"), _lq("buy_put")
    if sc <= 0 or bc <= 0 or spp <= 0 or bp <= 0:
        return None
    return {"sc_ltp": sc, "bc_ltp": bc, "sp_ltp": spp, "bp_ltp": bp}


def evaluate_position_alerts(db: Session, user_id: int, position_id: int) -> List[Dict[str, Any]]:
    """Mark-to-mid PnL vs entry + 10d-to-expiry reminder. Inserts rows into iron_condor_alert."""
    ensure_iron_condor_tables()
    row = db.execute(
        text("SELECT * FROM iron_condor_position WHERE id = :pid AND user_id = :uid LIMIT 1"),
        {"pid": position_id, "uid": user_id},
    ).mappings().first()
    if not row:
        return []
    if str(row.get("status") or "").upper() not in ("OPEN", "ACTIVE", "ADJUSTED"):
        return []

    st = {
        "sell_call": float(row["sell_call_strike"]),
        "buy_call": float(row["buy_call_strike"]),
        "sell_put": float(row["sell_put_strike"]),
        "buy_put": float(row["buy_put_strike"]),
    }
    q = _fresh_chain_quotes(str(row["underlying"]), st)
    alerts: List[Dict[str, Any]] = []
    if not q:
        return alerts

    entry_net = float(row["premium_collected"] or 0) - float(row["hedge_cost"] or 0)
    cost_to_close = (q["sc_ltp"] - q["bc_ltp"]) + (q["sp_ltp"] - q["bp_ltp"])
    mtm = entry_net - cost_to_close

    sets = get_or_create_settings(db, user_id)
    pt = sets.get("profit_target_pct_of_credit")
    sl = sets.get("stop_loss_pct_of_credit")
    if entry_net > 0 and pt is not None:
        try:
            ptv = float(pt)
            if mtm >= entry_net * (ptv / 100.0):
                alerts.append(
                    {
                        "rule_code": "PROFIT_TARGET_ADVISORY",
                        "message": f"Mark-to-mid PnL ₹{mtm:.2f} reached advisory profit threshold ({ptv}% of entry net ₹{entry_net:.2f}). Review for manual exit.",
                    }
                )
        except Exception:
            pass
    if entry_net > 0 and sl is not None:
        try:
            slv = float(sl)
            if mtm <= -abs(entry_net * (slv / 100.0)):
                alerts.append(
                    {
                        "rule_code": "STOP_LOSS_ADVISORY",
                        "message": f"Mark-to-mid loss depth suggests stop advisory ({slv}% of entry net). Review wings.",
                    }
                )
        except Exception:
            pass

    today = datetime.now(IST).date()
    days_left = (row["expiry_date"] - today).days if row.get("expiry_date") else 999
    if days_left <= 10:
        alerts.append(
            {
                "rule_code": "EXPIRY_NEAR_10D",
                "message": f"Expiry in {days_left} day(s): consider closing or rolling manually (policy: 10 days before expiry advisory).",
            }
        )

    out_rows: List[Dict[str, Any]] = []
    for a in alerts:
        exists = db.execute(
            text(
                """
                SELECT 1 FROM iron_condor_alert
                WHERE user_id = :uid AND position_id = :pid AND rule_code = :rc
                  AND created_at >= (CURRENT_TIMESTAMP - interval '36 hours')
                LIMIT 1
                """
            ),
            {"uid": user_id, "pid": position_id, "rc": a["rule_code"]},
        ).scalar()
        if exists:
            continue
        db.execute(
            text(
                """
                INSERT INTO iron_condor_alert (user_id, position_id, rule_code, message, payload_json)
                VALUES (:uid, :pid, :rc, :msg, CAST(:payload AS JSONB))
                """
            ),
            {
                "uid": user_id,
                "pid": position_id,
                "rc": a["rule_code"],
                "msg": a["message"],
                "payload": json.dumps({"mtm_estimate": mtm, "entry_net": entry_net}),
            },
        )
        db.commit()
        out_rows.append(a)
    return out_rows


def recent_alerts(db: Session, user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    ensure_iron_condor_tables()
    rows = db.execute(
        text(
            """
            SELECT * FROM iron_condor_alert
            WHERE user_id = :uid
            ORDER BY created_at DESC
            LIMIT :lim
            """
        ),
        {"uid": user_id, "lim": limit},
    ).mappings().all()
    return [dict(r) for r in rows]
