"""Account ticker settings, revocable app tokens, and one read of live desks.

Live prices come from each desk's existing mark (Upstox cache or stored LTP).
This module does not open a broker feed.
"""
from __future__ import annotations

import concurrent.futures
import hashlib
import logging
import secrets
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

ALGOS = (
    "commdiv",
    "stock_options",
    "kavach",
    "breakfast",
    "premium_futures",
    "tarang",
)
ALGO_LABELS = {
    "commdiv": "CommDiv",
    "stock_options": "Stock Options",
    "kavach": "Kavach",
    "breakfast": "Breakfast",
    "premium_futures": "Premium Futures",
    "tarang": "Kosmic Tarang",
}
TOKEN_PREFIX = "twt_"
# Mac menu-bar poll, and how long a desk read may be reused.
TICKER_REFRESH_SEC = 120.0

# Active-signal windows are inclusive of the closing minute (09:15–15:30, 09:00–23:30).
NSE_SIGNAL_OPEN_MIN = 9 * 60 + 15
NSE_SIGNAL_CLOSE_MIN = 15 * 60 + 30
MCX_SIGNAL_OPEN_MIN = 9 * 60
MCX_SIGNAL_CLOSE_MIN = 23 * 60 + 30
NSE_SIGNAL_ALGOS = frozenset({"stock_options", "premium_futures", "kavach", "breakfast"})
MCX_SIGNAL_ALGOS = frozenset({"commdiv"})
_BLANK_SYMBOLS = {"", "—", "-", "–"}

_CACHE: Dict[str, tuple] = {}
_tables_ready = False
_tables_lock = threading.Lock()
_COLLECT_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="ticker")
_INFLIGHT: Dict[str, concurrent.futures.Future] = {}
_INFLIGHT_LOCK = threading.Lock()
# Mac client drops the connection at 8s (nginx 499 → "Waiting for TradeWithCTO…").
# Keep the whole snapshot inside that window even if one desk never returns.
SNAPSHOT_BUDGET_SEC = 4.0


def _now() -> datetime:
    return datetime.now(IST)


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def ensure_ticker_tables(db: Optional[Session] = None) -> None:
    """Create ticker tables once, on the caller's connection.

    Opening a second pooled connection (engine.begin) while the request
    already holds a session deadlocks the pool: each /api/ticker/live poll
    keeps its connection and waits for another, until Create app token
    cannot get a connection at all.
    """
    global _tables_ready
    if _tables_ready:
        return
    own = False
    if db is None:
        from backend.database import SessionLocal

        if SessionLocal is None:
            return
        db = SessionLocal()
        own = True
    try:
        with _tables_lock:
            if _tables_ready:
                return
            db.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ticker_settings (
                        user_id INTEGER PRIMARY KEY,
                        enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        algos JSONB NOT NULL DEFAULT '{}'::jsonb,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            db.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ticker_tokens (
                        id BIGSERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        token_hash TEXT NOT NULL UNIQUE,
                        token_hint TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        revoked_at TIMESTAMPTZ
                    )
                    """
                )
            )
            db.commit()
            _tables_ready = True
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            db.close()


def default_algos() -> Dict[str, bool]:
    return {k: True for k in ALGOS}


def _merge_algos(raw: Any) -> Dict[str, bool]:
    if isinstance(raw, str):
        try:
            raw = __import__("json").loads(raw)
        except Exception:
            raw = {}
    out = default_algos()
    if isinstance(raw, dict):
        for k in ALGOS:
            if k in raw:
                out[k] = bool(raw[k])
    return out


def get_settings(db: Session, user_id: int) -> Dict[str, Any]:
    ensure_ticker_tables(db)
    row = db.execute(
        text("SELECT enabled, algos FROM ticker_settings WHERE user_id = :u"),
        {"u": user_id},
    ).mappings().first()
    if not row:
        return {"enabled": True, "algos": default_algos(), "algos_meta": ALGO_LABELS}
    return {
        "enabled": bool(row["enabled"]),
        "algos": _merge_algos(row["algos"]),
        "algos_meta": ALGO_LABELS,
    }


def save_settings(db: Session, user_id: int, enabled: bool, algos: Dict[str, bool]) -> Dict[str, Any]:
    ensure_ticker_tables(db)
    merged = _merge_algos(algos)
    db.execute(
        text(
            """
            INSERT INTO ticker_settings (user_id, enabled, algos, updated_at)
            VALUES (:u, :en, CAST(:algos AS jsonb), NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET enabled = EXCLUDED.enabled,
                  algos = EXCLUDED.algos,
                  updated_at = NOW()
            """
        ),
        {"u": user_id, "en": bool(enabled), "algos": __import__("json").dumps(merged)},
    )
    return get_settings(db, user_id)


def issue_token(db: Session, user_id: int) -> Dict[str, Any]:
    ensure_ticker_tables(db)
    raw = TOKEN_PREFIX + secrets.token_urlsafe(32)
    db.execute(
        text(
            """
            INSERT INTO ticker_tokens (user_id, token_hash, token_hint)
            VALUES (:u, :h, :hint)
            """
        ),
        {"u": user_id, "h": _hash_token(raw), "hint": raw[-4:]},
    )
    return {"token": raw, "hint": raw[-4:]}


def revoke_tokens(db: Session, user_id: int) -> int:
    ensure_ticker_tables(db)
    res = db.execute(
        text(
            """
            UPDATE ticker_tokens
               SET revoked_at = NOW()
             WHERE user_id = :u AND revoked_at IS NULL
            """
        ),
        {"u": user_id},
    )
    return int(res.rowcount or 0)


def user_id_for_ticker_token(db: Session, raw: str) -> Optional[int]:
    if not raw or not raw.startswith(TOKEN_PREFIX):
        return None
    ensure_ticker_tables(db)
    row = db.execute(
        text(
            """
            SELECT user_id FROM ticker_tokens
             WHERE token_hash = :h AND revoked_at IS NULL
            """
        ),
        {"h": _hash_token(raw)},
    ).first()
    return int(row[0]) if row else None


def _num(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


def _row(algo: str, symbol: str, *, pnl: Any = None, label: str = "") -> Dict[str, Any]:
    return {
        "algo": algo,
        "algo_label": ALGO_LABELS.get(algo, algo),
        "symbol": symbol or "—",
        "pnl": _num(pnl),
        "label": label,
    }


def _symbol_ok(symbol: Any) -> bool:
    return str(symbol or "").strip() not in _BLANK_SYMBOLS


def _as_ist(now: Optional[datetime] = None) -> datetime:
    now = now or _now()
    if now.tzinfo is None:
        return IST.localize(now)
    return now.astimezone(IST)


def _nse_closed_day(now: datetime) -> bool:
    """Weekends and the project's NSE ``holiday`` calendar. Weekdays if that table is down."""
    try:
        from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

        return bool(should_skip_scheduled_market_jobs_ist(now))
    except Exception:
        logger.debug("ticker nse calendar unavailable", exc_info=True)
        return _as_ist(now).weekday() >= 5


def _mcx_closed_day(now: datetime) -> bool:
    """Weekends and the same holiday calendar MCX already uses. Weekdays if that lookup fails."""
    try:
        from backend.services.tarang.calendar import mcx_is_holiday_or_weekend

        return bool(mcx_is_holiday_or_weekend(now))
    except Exception:
        logger.debug("ticker mcx calendar unavailable", exc_info=True)
        return _as_ist(now).weekday() >= 5


def signal_visible(algo: str, now: Optional[datetime] = None) -> bool:
    """Whether this desk's active signals may appear at ``now`` (IST).

    Live trades are not gated here — they stay up around the clock.
    Stock Options, Premium Futures, and the other NSE desks: 09:15–15:30.
    CommDiv: 09:00–23:30. Closed days drop the desk entirely.
    """
    ist = _as_ist(now)
    minute = ist.hour * 60 + ist.minute
    if algo in MCX_SIGNAL_ALGOS:
        if _mcx_closed_day(ist):
            return False
        return MCX_SIGNAL_OPEN_MIN <= minute <= MCX_SIGNAL_CLOSE_MIN
    if algo in NSE_SIGNAL_ALGOS:
        if _nse_closed_day(ist):
            return False
        return NSE_SIGNAL_OPEN_MIN <= minute <= NSE_SIGNAL_CLOSE_MIN
    return True


def _for_client(row: Dict[str, Any]) -> Dict[str, Any]:
    """Line is the script name (and PnL or status). Desk labels stay out of the payload."""
    out = dict(row)
    out["symbol"] = str(out.get("symbol") or "").strip()
    out["algo_label"] = ""
    return out


def _live_trades(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Open rows that have a script and a current PnL. Blanks never reach the ticker."""
    out = []
    for row in rows:
        if row.get("pnl") is None or not _symbol_ok(row.get("symbol")):
            continue
        out.append(_for_client(row))
    return out


def _active_signals(rows: List[Dict[str, Any]], now: datetime) -> List[Dict[str, Any]]:
    out = []
    for row in rows:
        if not signal_visible(str(row.get("algo") or ""), now):
            continue
        if not _symbol_ok(row.get("symbol")):
            continue
        out.append(_for_client(row))
    return out


def _submit_collector(key: str, fn: Callable[[], Any]) -> concurrent.futures.Future:
    """One in-flight read per desk. A hung call is not started again until it ends."""
    with _INFLIGHT_LOCK:
        fut = _INFLIGHT.get(key)
        if fut is None or fut.done():
            fut = _COLLECT_POOL.submit(fn)
            _INFLIGHT[key] = fut
        return fut


def _collect(
    jobs: List[Tuple[str, float, Callable[[], Any], Any]],
    budget: Optional[float] = None,
) -> Dict[str, Any]:
    """Read desks together. A stall returns the previous value for that desk only."""
    if budget is None:
        budget = SNAPSHOT_BUDGET_SEC
    now = time.monotonic()
    ready: Dict[str, Any] = {}
    pending: List[Tuple[str, concurrent.futures.Future, Any]] = []
    for key, ttl, fn, empty in jobs:
        hit = _CACHE.get(key)
        if hit and now - hit[0] < ttl:
            ready[key] = hit[1]
            continue
        pending.append((key, _submit_collector(key, fn), empty))
    deadline = time.monotonic() + budget
    for key, fut, empty in pending:
        left = deadline - time.monotonic()
        hit = _CACHE.get(key)
        if not fut.done() and left <= 0:
            logger.warning("ticker collector %s skipped; snapshot budget spent", key)
            ready[key] = hit[1] if hit else empty
            continue
        try:
            val = fut.result(timeout=0 if fut.done() else left)
        except concurrent.futures.TimeoutError:
            logger.warning("ticker collector %s still running after %.1fs", key, budget)
            ready[key] = hit[1] if hit else empty
            continue
        except Exception:
            logger.exception("ticker collector %s failed", key)
            val = hit[1] if hit else empty
            _CACHE[key] = (time.monotonic(), val)
            ready[key] = val
            continue
        _CACHE[key] = (time.monotonic(), val)
        ready[key] = val
    return ready


def _commdiv_trades() -> List[Dict[str, Any]]:
    """Open CommDiv In-Trade rows for both PAPER and LIVE.

    History and Exit Trade are not in this list. Rows without a script or a
    current PnL are dropped so the ticker never shows a dash.
    """
    from backend.services.commodities_div.actions import list_in_trade_signals

    out = []
    for r in list_in_trade_signals() or []:
        sym = str(r.get("display_symbol") or r.get("symbol_mapped") or r.get("symbol_raw") or "").strip()
        if not _symbol_ok(sym) or r.get("pnl") is None:
            continue
        out.append(_row("commdiv", sym, pnl=r.get("pnl"), label="In-Trade"))
    return out


def _commdiv_signals() -> List[Dict[str, Any]]:
    from backend.services.commodities_div.actions import list_active_tab_signals

    out = []
    for r in list_active_tab_signals() or []:
        status = str(r.get("status") or "")
        if status not in ("Divergence", "Activated"):
            continue
        sym = r.get("display_symbol") or r.get("symbol_mapped") or r.get("symbol_raw") or "—"
        out.append(_row("commdiv", str(sym), label=status))
    return out


def _stock_options() -> tuple:
    """Live trades are LIVE Executed rows that still have a rupee PnL.

    Radar, Active (untraded), PAPER, and the rest of the Executed tab are not
    open live trades. Completed / history never enter this read. Active-tab
    rows are signals, and the snapshot hides them outside 09:15–15:30 IST.
    """
    from backend.services.stock_option_signals import list_workspace

    ws = list_workspace() or {}
    trades, signals = [], []
    for r in ws.get("executed") or []:
        if str(r.get("trade_mode") or "").strip().upper() != "LIVE":
            continue
        pnl = r.get("combined_pnl_inr")
        sym = str(r.get("symbol") or "").strip()
        if pnl is None or not _symbol_ok(sym):
            continue
        trades.append(_row("stock_options", sym, pnl=pnl, label="LIVE"))
    for r in ws.get("active") or []:
        sym = str(r.get("symbol") or "").strip()
        if not _symbol_ok(sym):
            continue
        signals.append(_row("stock_options", sym, label="Active"))
    return trades, signals


def _kavach_trades() -> List[Dict[str, Any]]:
    from backend.database import SessionLocal
    from backend.services.kavach_open_trades import (
        _row_to_dict,
        enrich_trade_live,
        ensure_tables,
        today_ist,
    )

    ensure_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT * FROM kavach_checklist_trades
                WHERE session_date = CAST(:d AS date) AND status = 'OPEN'
                ORDER BY entry_time
                """
            ),
            {"d": today_ist()},
        ).fetchall()
        out = []
        for r in rows:
            t = enrich_trade_live(_row_to_dict(r), db)
            sym = t.get("future_symbol") or t.get("symbol") or "—"
            out.append(
                _row(
                    "kavach",
                    str(sym),
                    pnl=t.get("unrealized_pnl_inr"),
                    label=str(t.get("direction") or "OPEN"),
                )
            )
        return out
    finally:
        db.close()


def _kavach_ready() -> List[Dict[str, Any]]:
    """Latest READY badges from the consistency log.

    daily_checklist.get_state() rebuilds the whole checklist page (radar,
    ignition, go-board) and was hanging /api/ticker/live until the Mac client
    gave up. This read is the recent rendered state only.
    """
    from backend.database import SessionLocal

    db = SessionLocal()
    try:
        db.execute(text("SET LOCAL statement_timeout = '1200ms'"))
        rows = db.execute(
            text(
                """
                SELECT symbol, rendered_state
                FROM (
                    SELECT DISTINCT ON (symbol) symbol, rendered_state
                    FROM kavach_ready_consistency_log
                    WHERE session_date = CAST(:d AS date)
                      AND logged_at > NOW() - INTERVAL '20 minutes'
                    ORDER BY symbol, logged_at DESC
                ) latest
                WHERE rendered_state LIKE 'READY%'
                ORDER BY symbol
                """
            ),
            {"d": _now().date().isoformat()},
        ).mappings().all()
        return [
            _row("kavach", str(r["symbol"]), label=str(r["rendered_state"] or "READY"))
            for r in rows
            if r.get("symbol")
        ]
    except Exception:
        logger.exception("ticker kavach ready lookup failed")
        return []
    finally:
        db.close()


def _breakfast_signals() -> List[Dict[str, Any]]:
    from backend.services.breakfast_strategy.live import build_live_state

    state = build_live_state() or {}
    if str(state.get("phase") or "") not in ("locked", "frozen"):
        return []
    out = []
    for sector in state.get("sectors") or []:
        direction = sector.get("direction") or ""
        for stk in sector.get("stocks") or []:
            sym = stk.get("display_symbol") or stk.get("symbol")
            if not sym:
                continue
            label = stk.get("rank_label") or "Frozen"
            if direction:
                label = f"{label} · {direction}"
            out.append(_row("breakfast", str(sym), label=str(label)))
    return out


def _premium(user_id: int) -> tuple:
    from backend.database import SessionLocal

    db = SessionLocal()
    try:
        running = db.execute(
            text(
                """
                SELECT COALESCE(NULLIF(TRIM(t.future_symbol), ''), t.underlying) AS sym,
                       COALESCE(t.direction_type, 'LONG') AS direction_type,
                       t.entry_price, t.lot_size, s.ltp
                FROM daily_futures_user_trade t
                LEFT JOIN daily_futures_screening s ON s.id = t.screening_id
                WHERE t.user_id = :u AND t.order_status = 'bought'
                ORDER BY t.updated_at DESC
                """
            ),
            {"u": user_id},
        ).mappings().all()
        trades = []
        for r in running:
            entry = _num(r["entry_price"])
            ltp = _num(r["ltp"])
            lot = int(r["lot_size"] or 0)
            pnl = None
            if entry is not None and ltp is not None and lot > 0:
                pts = ltp - entry
                if str(r["direction_type"] or "").upper() == "SHORT":
                    pts = entry - ltp
                pnl = round(pts * lot, 2)
            sym = str(r["sym"] or "").strip()
            if pnl is None or not _symbol_ok(sym):
                continue
            trades.append(
                _row(
                    "premium_futures",
                    sym,
                    pnl=pnl,
                    label=str(r["direction_type"] or "Running"),
                )
            )
        picks = db.execute(
            text(
                """
                SELECT COALESCE(NULLIF(TRIM(s.future_symbol), ''), s.underlying) AS sym,
                       COALESCE(s.direction_type, 'LONG') AS direction_type,
                       COALESCE(s.effective_conviction, s.conviction_score) AS conv
                FROM daily_futures_screening s
                WHERE s.trade_date = (NOW() AT TIME ZONE 'Asia/Kolkata')::date
                  AND COALESCE(s.effective_conviction, s.conviction_score) >= 50
                  AND NOT EXISTS (
                      SELECT 1 FROM daily_futures_user_trade t
                      WHERE t.screening_id = s.id
                        AND t.user_id = :u
                        AND t.order_status IN ('bought', 'sold')
                  )
                ORDER BY conv DESC NULLS LAST
                LIMIT 12
                """
            ),
            {"u": user_id},
        ).mappings().all()
        signals = []
        for r in picks:
            sym = str(r["sym"] or "").strip()
            if not _symbol_ok(sym):
                continue
            conv = _num(r["conv"])
            label = f"Pick · {int(conv)}" if conv is not None else "Pick"
            signals.append(_row("premium_futures", sym, label=label))
        return trades, signals
    finally:
        db.close()


def _tarang_trades() -> List[Dict[str, Any]]:
    from backend.services.tarang.lifecycle import build_in_trade_view, list_open_trades

    out = []
    for t in (list_open_trades() or [])[:12]:
        sym = t.get("display_symbol") or t.get("profile_id") or t.get("underlying") or "—"
        label = str(t.get("status") or "IN_TRADE")
        pnl = None
        try:
            view = build_in_trade_view(int(t["id"]), refresh_quotes=False)
            pnl = (view.get("mtm") or {}).get("unrealized_pnl_inr")
            if view.get("pnl_inr") is not None:
                pnl = view.get("pnl_inr")
        except Exception:
            logger.debug("tarang ticker mtm skipped", exc_info=True)
        out.append(_row("tarang", str(sym), pnl=pnl, label=label))
    return out


def build_snapshot(db: Session, user_id: int) -> Dict[str, Any]:
    settings = get_settings(db, user_id)
    try:
        db.close()
    except Exception:
        logger.debug("ticker snapshot session close skipped", exc_info=True)
    enabled = bool(settings["enabled"])
    algos = settings["algos"]
    if not enabled:
        return {
            "ok": True,
            "enabled": False,
            "server_time": _now().isoformat(),
            "trades": [],
            "signals": [],
            "settings": settings,
        }

    trades: List[Dict[str, Any]] = []
    signals: List[Dict[str, Any]] = []
    jobs: List[Tuple[str, float, Callable[[], Any], Any]] = []
    if algos.get("commdiv"):
        jobs.append(("commdiv_trades", TICKER_REFRESH_SEC, _commdiv_trades, []))
        jobs.append(("commdiv_signals", TICKER_REFRESH_SEC, _commdiv_signals, []))
    if algos.get("stock_options"):
        jobs.append(("stock_options", TICKER_REFRESH_SEC, _stock_options, ([], [])))
    if algos.get("kavach"):
        jobs.append(("kavach_trades", TICKER_REFRESH_SEC, _kavach_trades, []))
        jobs.append(("kavach_ready", TICKER_REFRESH_SEC, _kavach_ready, []))
    if algos.get("breakfast"):
        jobs.append(("breakfast", TICKER_REFRESH_SEC, _breakfast_signals, []))
    if algos.get("premium_futures"):
        jobs.append((f"premium:{user_id}", TICKER_REFRESH_SEC, lambda: _premium(user_id), ([], [])))
    if algos.get("tarang"):
        jobs.append(("tarang", TICKER_REFRESH_SEC, _tarang_trades, []))

    got = _collect(jobs)
    if algos.get("commdiv"):
        trades.extend(got.get("commdiv_trades") or [])
        signals.extend(got.get("commdiv_signals") or [])
    if algos.get("stock_options"):
        so_t, so_s = got.get("stock_options") or ([], [])
        trades.extend(so_t)
        signals.extend(so_s)
    if algos.get("kavach"):
        trades.extend(got.get("kavach_trades") or [])
        signals.extend(got.get("kavach_ready") or [])
    if algos.get("breakfast"):
        signals.extend(got.get("breakfast") or [])
    if algos.get("premium_futures"):
        pf_t, pf_s = got.get(f"premium:{user_id}") or ([], [])
        trades.extend(pf_t)
        signals.extend(pf_s)
    if algos.get("tarang"):
        trades.extend(got.get("tarang") or [])

    # Gate signals at publish time so a cached desk read cannot outlive its window.
    now = _now()
    return {
        "ok": True,
        "enabled": True,
        "server_time": now.isoformat(),
        "trades": _live_trades(trades),
        "signals": _active_signals(signals, now),
        "settings": settings,
    }
