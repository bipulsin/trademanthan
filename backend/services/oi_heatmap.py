"""
Live OI heatmap (Top ~200 NSE stock futures) — Upstox only.

1) Instruments: same official daily file as ``InstrumentsDownloader``
   (https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz).
2) Filter: NSE_FO + instrument_type FUT + equity underlyings (exclude index / commodity FO).
3) Liquidity: one near-month future per underlying, then batch market quotes → top N by volume.
4) Refresh: in-memory cache + optional DB table ``oi_heatmap_latest``; persist only rows with non-zero raw heat score; stored ``score`` is normalized to 0–100 (batch median → 50); scheduler interval from config.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError

from backend.config import settings
from backend.database import SessionLocal

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Index / macro underlyings — not stock futures (NSE_FO index contracts)
_NSE_INDEX_FUT_UNDERLYINGS = frozenset(
    {
        "NIFTY",
        "BANKNIFTY",
        "FINNIFTY",
        "MIDCPNIFTY",
        "NIFTYNXT50",
        "SENSEX",
        "BANKEX",
    }
)

_cache_lock = threading.Lock()
_rows_cache: List[Dict[str, Any]] = []
_cache_updated_at_mono: float = 0.0
_cache_updated_at_iso: str = ""
_cache_source: str = "none"  # "live" (Upstox refresh) | "snapshot" (DB) | "none"
_last_error: Optional[str] = None
_underlying_rank: Dict[str, int] = {}
_prev_signal_by_instrument: Dict[str, str] = {}
_prev_signal_by_underlying: Dict[str, str] = {}
_api_refresh_lock = threading.Lock()
_last_api_refresh_attempt_mono: float = 0.0

# Daily universe of instrument_keys (rebuilt when instruments file mtime changes)
_universe_date: Optional[date] = None
_universe_keys: List[str] = []


def _instruments_path():
    from backend.config import get_instruments_file_path

    return get_instruments_file_path()


def load_nse_instruments_json() -> List[Dict[str, Any]]:
    """Load raw Upstox NSE instruments list from local JSON (daily download)."""
    path = _instruments_path()
    if not path.is_file():
        logger.warning("oi_heatmap: instruments file missing: %s", path)
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.error("oi_heatmap: failed to read instruments: %s", e)
        return []


def _expiry_sort_key(inst: Dict[str, Any]) -> int:
    ex = inst.get("expiry")
    try:
        v = int(ex)
        if v > 1_000_000_000_000:
            v //= 1000
        return v
    except (TypeError, ValueError):
        return 2**62


def is_stock_future_contract(inst: Dict[str, Any]) -> bool:
    seg = str(inst.get("segment") or "").upper()
    if "NSE_FO" not in seg and "NFO" not in seg:
        return False
    if str(inst.get("instrument_type") or "").upper() != "FUT":
        return False
    u = (inst.get("underlying_symbol") or inst.get("name") or "").strip().upper()
    if not u or u in _NSE_INDEX_FUT_UNDERLYINGS:
        return False
    return True


def filter_stock_futures_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [r for r in rows if isinstance(r, dict) and is_stock_future_contract(r)]


def pick_nearest_expiry_future_per_underlying(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One FUT row per underlying — nearest expiry (smallest expiry timestamp)."""
    best: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    for r in rows:
        u = (r.get("underlying_symbol") or "").strip().upper()
        if not u:
            continue
        ik = (r.get("instrument_key") or "").strip()
        if not ik:
            continue
        ek = _expiry_sort_key(r)
        prev = best.get(u)
        if prev is None or ek < prev[0]:
            best[u] = (ek, r)
    return [t[1] for t in best.values()]


def build_liquidity_universe_instrument_keys(top_n: int) -> List[str]:
    """
    After filtering + one contract per underlying, batch-quote all keys and keep top ``top_n`` by volume.
    """
    raw = load_nse_instruments_json()
    fut_rows = filter_stock_futures_rows(raw)
    per_u = pick_nearest_expiry_future_per_underlying(fut_rows)
    keys = [(r.get("instrument_key") or "").strip() for r in per_u if (r.get("instrument_key") or "").strip()]
    if not keys:
        return []

    try:
        from backend.services.upstox_service import UpstoxService

        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        logger.error("oi_heatmap: Upstox init failed: %s", e)
        return keys[:top_n]

    chunk = max(10, min(int(getattr(settings, "OI_BATCH_CHUNK_SIZE", 100)), 500))
    vol_pairs: List[Tuple[float, str]] = []
    for i in range(0, len(keys), chunk):
        batch = keys[i : i + chunk]
        snap = ux.get_market_quote_snapshots_batch(batch, max_per_request=len(batch))
        for ik in batch:
            s = snap.get(ik) or {}
            vol = float(s.get("volume") or 0)
            vol_pairs.append((vol, ik))

    vol_pairs.sort(key=lambda x: x[0], reverse=True)
    return [ik for _, ik in vol_pairs[:top_n]]


def ensure_daily_universe_cached() -> List[str]:
    """Rebuild universe list when instruments file changes (IST calendar day)."""
    global _universe_date, _universe_keys
    path = _instruments_path()
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0
    day = datetime.fromtimestamp(mtime, IST).date()
    with _cache_lock:
        if _universe_date == day and _universe_keys:
            return _universe_keys
        n = int(getattr(settings, "OI_HEATMAP_TOP_N", 200))
        _universe_keys = build_liquidity_universe_instrument_keys(n)
        _universe_date = day
        logger.info("oi_heatmap: universe size=%s (file day=%s)", len(_universe_keys), day)
        return _universe_keys


def _normalize_signal(sig: str) -> str:
    m = {
        "LONG_UNWINDING": "LONG_UNWIND",
        "SHORT_BUILDUP": "SHORT_BUILDUP",
        "LONG_BUILDUP": "LONG_BUILDUP",
        "SHORT_COVERING": "SHORT_COVER",
        "NEUTRAL": "NEUTRAL",
    }
    return m.get(sig, sig or "NEUTRAL")


def _interpret_signal(price_dp: float, oi_chg: float) -> str:
    from backend.services.oi_integration import interpret_oi_signal

    return _normalize_signal(interpret_oi_signal(float(price_dp), float(oi_chg)))


def _score_row(oi_chg: int, chg_pct: float) -> float:
    return abs(float(oi_chg)) + abs(chg_pct) * 0.01


def _row_has_nonzero_score(row: Dict[str, Any]) -> bool:
    try:
        return float(row.get("score") or 0) != 0.0
    except (TypeError, ValueError):
        return False


def _normalize_scores_to_0_100_median_50(rows: List[Dict[str, Any]]) -> None:
    """
    In-place: set each row's ``score`` to [0, 100], with the batch **median raw score** mapping to **50**
    (piecewise linear from min→0 through median→50 to max→100). Single-row batches become 50.
    """
    if not rows:
        return
    vals: List[float] = []
    for r in rows:
        try:
            vals.append(float(r.get("score") or 0.0))
        except (TypeError, ValueError):
            vals.append(0.0)
    n = len(vals)
    sv = sorted(vals)
    min_v = sv[0]
    max_v = sv[-1]
    if n % 2 == 1:
        med = float(sv[n // 2])
    else:
        med = (float(sv[n // 2 - 1]) + float(sv[n // 2])) / 2.0

    for i, r in enumerate(rows):
        v = vals[i]
        if max_v <= min_v or (max_v - min_v) < 1e-15:
            norm = 50.0
        elif v <= med:
            if med <= min_v:
                norm = 50.0
            else:
                norm = (v - min_v) / (med - min_v) * 50.0
        else:
            if max_v <= med:
                norm = 50.0
            else:
                norm = 50.0 + (v - med) / (max_v - med) * 50.0
        r["score"] = round(max(0.0, min(100.0, float(norm))), 4)


def finalize_heatmap_rows_for_store(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Drop rows with raw heat score == 0, sort by |oi_chg| descending, assign rank 1..n,
    then normalize ``score`` to 0–100 with batch median at 50.
    Used for live refresh and historical replay so ``oi_heatmap_latest`` has no zero-raw-score rows.
    """
    out = [r for r in rows if _row_has_nonzero_score(r)]
    out.sort(key=lambda r: abs(int(r.get("oi_chg") or 0)), reverse=True)
    for i, r in enumerate(out, start=1):
        r["rank"] = i
    _normalize_scores_to_0_100_median_50(out)
    return out


def _set_prev_signal_maps_from_rows(rows: List[Dict[str, Any]]) -> None:
    """Snapshot previous-scan signals from rows for API-derived ``prev_oi_signal`` output."""
    global _prev_signal_by_instrument, _prev_signal_by_underlying
    by_i: Dict[str, str] = {}
    by_u: Dict[str, str] = {}
    for r in rows or []:
        sig = str(r.get("oi_signal") or "").strip()
        if not sig:
            continue
        ik = str(r.get("instrument_key") or "").strip()
        und = str(r.get("underlying_symbol") or "").strip().upper()
        if ik:
            by_i[ik] = sig
        if und:
            by_u[und] = sig
    with _cache_lock:
        _prev_signal_by_instrument = by_i
        _prev_signal_by_underlying = by_u


def _attach_prev_signal_for_api(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return API rows with derived ``prev_oi_signal`` from previous scan snapshot maps."""
    with _cache_lock:
        by_i = dict(_prev_signal_by_instrument)
        by_u = dict(_prev_signal_by_underlying)
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        d = dict(r)
        ik = str(d.get("instrument_key") or "").strip()
        und = str(d.get("underlying_symbol") or "").strip().upper()
        prev = by_i.get(ik) or by_u.get(und)
        d["prev_oi_signal"] = prev
        out.append(d)
    return out


def refresh_oi_heatmap_live() -> Dict[str, Any]:
    """
    Fetch batch quotes for universe keys, sort by |oi_change|, update memory cache + DB.
    """
    global _rows_cache, _cache_updated_at_mono, _cache_updated_at_iso, _last_error, _underlying_rank, _cache_source

    if not getattr(settings, "UPSTOX_OI_ENABLED", True):
        return {"success": False, "skipped": "UPSTOX_OI_ENABLED false"}

    keys = ensure_daily_universe_cached()
    if not keys:
        _last_error = "empty_universe"
        return {"success": False, "error": _last_error}

    try:
        from backend.services.upstox_service import UpstoxService

        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        _last_error = str(e)
        logger.error("oi_heatmap: Upstox: %s", e)
        return {"success": False, "error": _last_error}

    chunk = max(10, min(int(getattr(settings, "OI_BATCH_CHUNK_SIZE", 100)), 500))
    merged: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(keys), chunk):
        batch = keys[i : i + chunk]
        part = ux.get_market_quote_snapshots_batch(batch, max_per_request=len(batch))
        merged.update(part)

    # Reload instrument meta for underlying / symbol labels
    raw = load_nse_instruments_json()
    ik_meta = {((r.get("instrument_key") or "").strip()): r for r in raw if isinstance(r, dict)}

    rows: List[Dict[str, Any]] = []
    for ik in keys:
        s = merged.get(ik) or {}
        lp = float(s.get("last_price") or 0)
        vol = float(s.get("volume") or 0)
        oi = int(s.get("oi") or 0)
        oi_chg = int(s.get("change_in_oi") or 0)
        net_chg = float(s.get("net_change") or 0)
        ohlc = s.get("ohlc") if isinstance(s.get("ohlc"), dict) else {}
        open_ = float(ohlc.get("open") or 0)
        if abs(net_chg) > 1e-9:
            prev = lp - net_chg
        elif open_ > 1e-9:
            prev = open_
        else:
            prev = float(ohlc.get("close") or 0)
        chg_pct = ((lp - prev) / prev * 100.0) if prev > 1e-9 else 0.0
        oi_chg_pct = (oi_chg / max(1, oi - oi_chg) * 100.0) if oi else 0.0
        price_dp = lp - prev
        sig = _interpret_signal(price_dp, float(oi_chg))
        meta = ik_meta.get(ik) or {}
        und = (meta.get("underlying_symbol") or "").strip().upper()
        tsym = (meta.get("trading_symbol") or meta.get("tradingsymbol") or "").strip()
        exp = meta.get("expiry")
        rows.append(
            {
                "instrument_key": ik,
                "underlying_symbol": und,
                "trading_symbol": tsym,
                "expiry": exp,
                "ltp": round(lp, 2),
                "chg_pct": round(chg_pct, 3),
                "oi": oi,
                "oi_chg": oi_chg,
                "oi_chg_pct": round(oi_chg_pct, 3),
                "oi_signal": sig,
                "volume": int(vol),
                "score": round(_score_row(oi_chg, chg_pct), 4),
            }
        )

    with _cache_lock:
        prev_rows = list(_rows_cache)
    if not prev_rows:
        prev_rows, _ = load_oi_heatmap_snapshot_from_db()
    _set_prev_signal_maps_from_rows(prev_rows)

    raw_n = len(rows)
    rows = finalize_heatmap_rows_for_store(rows)
    if raw_n != len(rows):
        logger.info(
            "oi_heatmap: persisting %s rows (dropped %s zero-score)",
            len(rows),
            raw_n - len(rows),
        )
    _underlying_rank = {str(r.get("underlying_symbol") or "").upper(): int(r["rank"]) for r in rows if r.get("underlying_symbol")}

    now_dt = datetime.now(IST)
    now_iso = now_dt.isoformat()
    with _cache_lock:
        _rows_cache = rows
        _cache_updated_at_mono = time.monotonic()
        _cache_updated_at_iso = now_iso
        _cache_source = "live"
        _last_error = None

    _persist_snapshot(rows, now_dt)
    return {"success": True, "count": len(rows), "updated_at": now_iso}


def _persist_snapshot(rows: List[Dict[str, Any]], updated_at: datetime) -> None:
    db = None
    try:
        db = SessionLocal()
        db.execute(text("DELETE FROM oi_heatmap_latest"))
        for r in rows:
            db.execute(
                text(
                    """
                    INSERT INTO oi_heatmap_latest (
                        rank, instrument_key, underlying_symbol, trading_symbol, expiry,
                        ltp, chg_pct, oi, oi_chg, oi_chg_pct, oi_signal, volume, score, updated_at
                    ) VALUES (
                        :rank, :instrument_key, :underlying_symbol, :trading_symbol, :expiry,
                        :ltp, :chg_pct, :oi, :oi_chg, :oi_chg_pct, :oi_signal, :volume, :score, :updated_at
                    )
                    """
                ),
                {
                    "rank": int(r["rank"]),
                    "instrument_key": r.get("instrument_key"),
                    "underlying_symbol": r.get("underlying_symbol"),
                    "trading_symbol": r.get("trading_symbol"),
                    "expiry": str(r.get("expiry") or ""),
                    "ltp": float(r["ltp"]),
                    "chg_pct": float(r["chg_pct"]),
                    "oi": int(r["oi"]),
                    "oi_chg": int(r["oi_chg"]),
                    "oi_chg_pct": float(r["oi_chg_pct"]),
                    "oi_signal": r.get("oi_signal"),
                    "volume": int(r.get("volume") or 0),
                    "score": float(r.get("score") or 0),
                    "updated_at": updated_at,
                },
            )
        db.commit()
    except Exception as e:
        logger.warning("oi_heatmap: persist skipped: %s", e)
    finally:
        if db is not None:
            db.close()


def load_oi_heatmap_snapshot_from_db() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Last persisted snapshot (PostgreSQL ``oi_heatmap_latest``). Used when process memory is empty
    (e.g. after restart, or before the first scheduler tick; weekends skip live refresh).
    """
    db = None
    try:
        db = SessionLocal()
        r = db.execute(
            text(
                """
                SELECT rank, instrument_key, underlying_symbol, trading_symbol, expiry,
                       ltp, chg_pct, oi, oi_chg, oi_chg_pct, oi_signal, volume, score, updated_at
                FROM oi_heatmap_latest
                ORDER BY rank ASC
                """
            )
        )
        rows_out: List[Dict[str, Any]] = []
        updated_iso: Optional[str] = None
        for row in r.mappings():
            d = dict(row)
            uat = d.pop("updated_at", None)
            if updated_iso is None and uat is not None:
                updated_iso = uat.isoformat() if hasattr(uat, "isoformat") else str(uat)
            exp = d.get("expiry")
            if exp is not None and not isinstance(exp, (str, int, float)):
                d["expiry"] = str(exp)
            # Normalize for JSON (PostgreSQL may return Decimal)
            for _k in ("ltp", "chg_pct", "oi_chg_pct", "score"):
                if _k in d and d[_k] is not None:
                    d[_k] = float(d[_k])
            for _k in ("oi", "oi_chg", "volume", "rank"):
                if _k in d and d[_k] is not None:
                    try:
                        d[_k] = int(d[_k])
                    except (TypeError, ValueError):
                        pass
            rows_out.append(d)
        return rows_out, updated_iso
    except (ProgrammingError, OperationalError) as e:
        logger.debug("oi_heatmap: DB snapshot unavailable: %s", e)
        return [], None
    except Exception as e:
        logger.warning("oi_heatmap: read oi_heatmap_latest failed: %s", e)
        return [], None
    finally:
        if db is not None:
            db.close()


def replace_cache_with_rows(
    rows: List[Dict[str, Any]], updated_at_iso: str, *, source: str = "snapshot"
) -> None:
    """
    Replace in-memory heatmap cache (e.g. after loading a historical replay into ``oi_heatmap_latest``).
    """
    global _cache_updated_at_mono, _cache_updated_at_iso, _cache_source, _underlying_rank
    snap = [dict(r) for r in rows]
    with _cache_lock:
        _rows_cache[:] = snap
        _cache_updated_at_mono = time.monotonic()
        _cache_updated_at_iso = updated_at_iso or ""
        _cache_source = source if source in ("live", "snapshot") else "snapshot"
        _underlying_rank = {
            str(r.get("underlying_symbol") or "").upper(): int(r["rank"])
            for r in snap
            if r.get("underlying_symbol") is not None and r.get("rank") is not None
        }
    logger.info("oi_heatmap: replaced memory cache (%s rows, source=%s)", len(snap), _cache_source)


def _hydrate_cache_from_db_rows(rows: List[Dict[str, Any]], updated_iso: Optional[str]) -> None:
    """Fill in-memory cache from DB if still empty (single writer under lock)."""
    global _cache_updated_at_mono, _cache_updated_at_iso, _cache_source, _underlying_rank
    if not rows:
        return
    snap = [dict(r) for r in rows]
    with _cache_lock:
        if _rows_cache:
            return
        _rows_cache[:] = snap
        _cache_updated_at_mono = time.monotonic()
        _cache_updated_at_iso = updated_iso or ""
        _cache_source = "snapshot"
        _underlying_rank = {
            str(r.get("underlying_symbol") or "").upper(): int(r["rank"])
            for r in snap
            if r.get("underlying_symbol") is not None and r.get("rank") is not None
        }
    logger.info("oi_heatmap: hydrated memory from oi_heatmap_latest (%s rows)", len(snap))


def maybe_trigger_refresh_if_empty() -> None:
    """
    When both memory and DB are empty, kick a one-off Upstox refresh (debounced) so the first
    dashboard load after deploy can populate without waiting for the interval job.
    """
    global _last_api_refresh_attempt_mono
    if not getattr(settings, "UPSTOX_OI_ENABLED", True):
        return
    with _api_refresh_lock:
        now = time.monotonic()
        if now - _last_api_refresh_attempt_mono < 120.0:
            return
        _last_api_refresh_attempt_mono = now

    def _run() -> None:
        try:
            refresh_oi_heatmap_live()
        except Exception as e:
            logger.warning("oi_heatmap: on-demand refresh failed: %s", e)

    threading.Thread(target=_run, daemon=True, name="oi-heatmap-on-demand-refresh").start()


def get_live_oi_heatmap_json(force_reload_from_db: bool = False) -> Dict[str, Any]:
    """API payload for GET /scan/dashboard/oi-heatmap."""
    if force_reload_from_db:
        db_rows, db_ts = load_oi_heatmap_snapshot_from_db()
        if db_rows:
            replace_cache_with_rows(db_rows, db_ts or "", source="snapshot")

    with _cache_lock:
        rows = list(_rows_cache)
        ts = _cache_updated_at_iso
        err = _last_error
        origin = _cache_source

    if not rows:
        db_rows, db_ts = load_oi_heatmap_snapshot_from_db()
        if db_rows:
            _hydrate_cache_from_db_rows(db_rows, db_ts)
            with _cache_lock:
                rows = list(_rows_cache)
                ts = _cache_updated_at_iso
                err = _last_error
                origin = _cache_source
        else:
            maybe_trigger_refresh_if_empty()

    empty_help = (
        "No heatmap rows yet. Weekday sessions refresh from Upstox on a timer; after a server "
        "restart the last snapshot is loaded from the database when available. If this persists, "
        "check the Upstox instruments file and API credentials."
    )
    snapshot_note = None
    if rows and origin == "snapshot":
        snapshot_note = (
            "Showing last saved snapshot from the database until live data is fetched "
            "(scheduler: every 15 min, 9:15–15:15 IST on trading days)."
        )
    return {
        "success": True,
        "source": "upstox",
        "data_origin": origin if rows else "none",
        "updated_at": ts or None,
        "error": err,
        "rows": _attach_prev_signal_for_api(rows),
        "message": None if rows else empty_help,
        "snapshot_note": snapshot_note,
    }


def oi_heat_rank_for_underlying(symbol: str) -> Optional[int]:
    """1-based rank in last refresh, or None if not in top universe."""
    s = (symbol or "").strip().upper()
    if not s:
        return None
    with _cache_lock:
        return _underlying_rank.get(s)


def get_snapshot_row_for_underlying(symbol: str) -> Optional[Dict[str, Any]]:
    """CMS / OI gate: lookup cached row by equity symbol."""
    s = (symbol or "").strip().upper()
    if not s:
        return None
    with _cache_lock:
        for r in _rows_cache:
            if str(r.get("underlying_symbol") or "").upper() == s:
                return dict(r)
    return None


def try_oiquote_from_heatmap_for_gate(stock: str):
    """
    Build ``OIQuote`` from live Upstox heatmap cache (preferred when ``UPSTOX_OI_ENABLED``).
    Returns None if cache has no row for this underlying.
    """
    import time as time_mod

    from backend.services.oi_integration import OIQuote

    row = get_snapshot_row_for_underlying(stock)
    if not row:
        return None
    ltp = float(row.get("ltp") or 0)
    chg_pct = float(row.get("chg_pct") or 0)
    if abs(chg_pct) > 1e-12:
        prev = ltp / (1.0 + chg_pct / 100.0)
    else:
        prev = ltp
    oi = int(row.get("oi") or 0)
    chg = int(row.get("oi_chg") or 0)
    prev_oi = max(0, oi - chg)
    return OIQuote(
        symbol=(stock or "").strip().upper(),
        oi=oi,
        change_in_oi=chg,
        last_price=ltp,
        prev_close=float(prev),
        prev_oi=int(prev_oi),
        fetched_at=time_mod.time(),
    )


def premkt_rank_for_stock(stock: str, session_d: date) -> Optional[int]:
    """Today's premarket_watchlist rank (1–10), if present."""
    from backend.services.premarket_watchlist_job import fetch_premarket_watchlist_for_date

    sym = (stock or "").strip().upper()
    if not sym:
        return None
    try:
        rows = fetch_premarket_watchlist_for_date(session_d)
    except Exception:
        return None
    for r in rows:
        if str(r.get("stock") or "").strip().upper() == sym:
            try:
                return int(r.get("rank"))
            except (TypeError, ValueError):
                return None
    return None
