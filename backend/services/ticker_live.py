"""Account ticker settings, revocable app tokens, and one read of live desks.

Live prices come from each desk's existing mark (Upstox cache or stored LTP).
This module does not open a broker feed.
"""
from __future__ import annotations

import hashlib
import logging
import secrets
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

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

_CACHE: Dict[str, tuple] = {}
_tables_ready = False
_tables_lock = threading.Lock()


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


def _cached(key: str, ttl: float, fn):
    hit = _CACHE.get(key)
    now = time.monotonic()
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        val = fn()
    except Exception:
        logger.exception("ticker collector %s failed", key)
        val = hit[1] if hit else []
    _CACHE[key] = (now, val)
    return val


def _commdiv_trades() -> List[Dict[str, Any]]:
    from backend.services.commodities_div.actions import list_in_trade_signals

    out = []
    for r in list_in_trade_signals() or []:
        sym = r.get("display_symbol") or r.get("symbol_mapped") or r.get("symbol_raw") or "—"
        out.append(_row("commdiv", str(sym), pnl=r.get("pnl"), label="In-Trade"))
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
    from backend.services.stock_option_signals import list_workspace

    ws = list_workspace() or {}
    trades, signals = [], []
    for r in ws.get("executed") or []:
        sym = r.get("symbol") or "—"
        side = r.get("side") or ""
        pnl = r.get("combined_pnl_inr")
        if pnl is None:
            pnl = r.get("combined_pnl")
        trades.append(_row("stock_options", str(sym), pnl=pnl, label=str(side or "Executed")))
    for r in ws.get("active") or []:
        signals.append(_row("stock_options", str(r.get("symbol") or "—"), label="Active"))
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
    from backend.services.daily_checklist import get_state

    state = get_state()
    out = []
    seen = set()
    for s in state.get("stocks") or []:
        ts = str(s.get("trade_state") or "")
        if not ts.startswith("READY"):
            continue
        sym = str(s.get("symbol") or "")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(_row("kavach", sym, label=ts))
    return out


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
            trades.append(
                _row(
                    "premium_futures",
                    str(r["sym"] or "—"),
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
            conv = _num(r["conv"])
            label = f"Pick · {int(conv)}" if conv is not None else "Pick"
            signals.append(_row("premium_futures", str(r["sym"] or "—"), label=label))
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

    if algos.get("commdiv"):
        trades.extend(_cached("commdiv_trades", 2, _commdiv_trades))
        signals.extend(_cached("commdiv_signals", 2, _commdiv_signals))
    if algos.get("stock_options"):
        so_t, so_s = _cached("stock_options", 3, _stock_options)
        trades.extend(so_t)
        signals.extend(so_s)
    if algos.get("kavach"):
        trades.extend(_cached("kavach_trades", 3, _kavach_trades))
        signals.extend(_cached("kavach_ready", 15, _kavach_ready))
    if algos.get("breakfast"):
        signals.extend(_cached("breakfast", 15, _breakfast_signals))
    if algos.get("premium_futures"):
        pf_t, pf_s = _cached(f"premium:{user_id}", 3, lambda: _premium(user_id))
        trades.extend(pf_t)
        signals.extend(pf_s)
    if algos.get("tarang"):
        trades.extend(_cached("tarang", 5, _tarang_trades))

    return {
        "ok": True,
        "enabled": True,
        "server_time": _now().isoformat(),
        "trades": trades,
        "signals": signals,
        "settings": settings,
    }
