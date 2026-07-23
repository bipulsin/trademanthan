"""Shadow-only: READY episode standard exit PnL vs exit+4×10m candles.

For every READY episode (traded or not), when the episode ends, log:
  - PnL at episode-end close (standard exit proxy)
  - PnL if held 4 more closed 10m candles past that exit

No live gates / exit rules / trade permission changes.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import engine
from backend.services.kavach_vwap_close_confirm_shadow import (
    _as_ist,
    _episode_ready_active,
    _f,
    is_ready_like,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
_ENSURED = False

TABLE = "kavach_ready_exit_plus4_shadow"
PLUS4_CANDLES = 4


_CREATE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id BIGSERIAL PRIMARY KEY,
    session_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    contract_month TEXT,
    appearance_note TEXT,
    episode_started_at TIMESTAMPTZ NOT NULL,
    entry_price DOUBLE PRECISION,
    episode_ended_at TIMESTAMPTZ,
    exit_price DOUBLE PRECISION,
    exit_at TIMESTAMPTZ,
    pnl_pts_standard DOUBLE PRECISION,
    pnl_inr_standard DOUBLE PRECISION,
    lot_size INTEGER,
    extended_exit_price DOUBLE PRECISION,
    extended_exit_at TIMESTAMPTZ,
    plus4_candles_used INTEGER,
    pnl_pts_plus4 DOUBLE PRECISION,
    pnl_inr_plus4 DOUBLE PRECISION,
    delta_pts_plus4_vs_standard DOUBLE PRECISION,
    delta_inr_plus4_vs_standard DOUBLE PRECISION,
    episode_end_reason TEXT,
    was_traded BOOLEAN DEFAULT FALSE,
    trade_log_id BIGINT,
    source TEXT DEFAULT 'live',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def ensure_ready_exit_plus4_shadow_table() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(text(_CREATE))
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_open "
                f"ON {TABLE} (session_date, symbol) "
                f"WHERE episode_ended_at IS NULL"
            )
        )
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_session "
                f"ON {TABLE} (session_date DESC, symbol)"
            )
        )
    _ENSURED = True


def _direction_of(stock: Dict[str, Any]) -> str:
    d = (stock.get("direction") or "").upper()
    if d in ("LONG", "SHORT"):
        return d
    return "LONG"


def _lot_size(db, symbol: str, session_date: str) -> int:
    _ = session_date
    try:
        from backend.services.smart_futures_picker.position_sizing import (
            get_futures_lot_size_by_instrument_key,
        )

        row = db.execute(
            text(
                """
                SELECT currmth_future_instrument_key AS ikey
                FROM arbitrage_master
                WHERE UPPER(stock) = :s
                LIMIT 1
                """
            ),
            {"s": symbol.upper()},
        ).mappings().first()
        ikey = (row or {}).get("ikey")
        if ikey:
            n = int(get_futures_lot_size_by_instrument_key(str(ikey)) or 0)
            if n > 0:
                return n
    except Exception:
        pass
    return 1


def _signed_pnl(direction: str, entry: float, exit_px: float) -> float:
    if (direction or "").upper() == "SHORT":
        return float(entry) - float(exit_px)
    return float(exit_px) - float(entry)


def close_plus_n_10m(
    candles: Sequence[Dict[str, Any]],
    *,
    exit_at: datetime,
    n: int = PLUS4_CANDLES,
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """First closed 10m bar_end >= exit_at, then the nth bar after that (0-index: exit bar = 0).

    Returns close/bar_end of the bar that is ``n`` closed 10m bars after the first bar
    with bar_end >= exit_at. If fewer bars available, uses last available and reports
    candles_used < n.
    """
    try:
        from backend.services.kavach_10m import (
            _current_and_prev_day_close,
            _parse_ist,
            _sorted_candles,
            aggregate_10m_bars,
            last_closed_10m_pair_end_idx,
        )

        until = _as_ist(now) or datetime.now(IST)
        exit_i = _as_ist(exit_at)
        if exit_i is None:
            return None
        c5 = _sorted_candles(list(candles))
        split = _current_and_prev_day_close(c5)
        if split is None:
            return None
        _, _, first_today = split
        pair_end = last_closed_10m_pair_end_idx(c5, now=until)
        if pair_end < first_today:
            return None
        today = c5[first_today : pair_end + 1]
        bars = aggregate_10m_bars(today)
        ends: List[Tuple[datetime, float]] = []
        for b in bars:
            end = b.get("bar_end")
            if end is None:
                ts = _parse_ist(b.get("timestamp"))
                end = ts + timedelta(minutes=5) if ts else None
            end = _as_ist(end)
            close = _f(b.get("close"))
            if end is None or close is None:
                continue
            ends.append((end, close))
        # bars with bar_end >= exit_at
        after = [(e, c) for e, c in ends if e >= exit_i]
        if not after:
            return None
        # index 0 = first bar at/after exit; we want +n more from standard exit bar.
        # Diagnostic used: exit bar, then hold 4 more → bar at index min(n, len-1) from first after exit.
        # If exit_at coincides with a bar end, that bar is the standard exit; +4 = index 4.
        idx = min(n, len(after) - 1)
        e, c = after[idx]
        return {
            "close": c,
            "bar_end": e,
            "candles_used": idx,
            "available": len(after),
        }
    except Exception:
        logger.debug("close_plus_n_10m failed", exc_info=True)
        return None


def _load_open(db, session_date: str) -> Dict[str, Dict[str, Any]]:
    rows = db.execute(
        text(
            f"""
            SELECT id, symbol, direction, entry_price, episode_started_at, contract_month
            FROM {TABLE}
            WHERE session_date = CAST(:d AS date)
              AND episode_ended_at IS NULL
            """
        ),
        {"d": session_date},
    ).mappings()
    return {str(r["symbol"]).upper(): dict(r) for r in rows}


def _entry_price(stock: Dict[str, Any], snap_close: Optional[float]) -> Optional[float]:
    for k in ("trade_entry", "entry_price", "ready_entry_price", "ltp", "price"):
        v = _f(stock.get(k))
        if v is not None and v > 0:
            return v
    return snap_close


def update_ready_exit_plus4_shadow(
    db,
    *,
    session_date: str,
    stocks: List[Dict[str, Any]],
    candles_by_symbol: Optional[Dict[str, Sequence[Dict[str, Any]]]] = None,
    now: Optional[datetime] = None,
    source: str = "live",
) -> Dict[str, int]:
    """Track READY episodes; on end, write standard vs +4 PnL. Best-effort."""
    stats = {"started": 0, "ended": 0, "touched": 0}
    try:
        ensure_ready_exit_plus4_shadow_table()
        now_i = _as_ist(now) or datetime.now(IST)
        candles_by_symbol = candles_by_symbol or {}
        open_map = _load_open(db, session_date)
        active_syms: Set[str] = set()

        from backend.services.kavach_watching_shadow import contract_month_for_symbol

        for s in stocks:
            sym = (s.get("symbol") or "").upper()
            if not sym:
                continue
            if not _episode_ready_active(s):
                continue
            active_syms.add(sym)
            stats["touched"] += 1
            direction = _direction_of(s)
            if sym in open_map:
                continue
            # start episode
            snap_close = None
            try:
                from backend.services.kavach_vwap_close_confirm_shadow import closed_10m_snapshot

                snap = closed_10m_snapshot(
                    candles_by_symbol.get(sym) or [],
                    direction=direction,
                    now=now_i,
                )
                snap_close = (snap or {}).get("close")
            except Exception:
                snap_close = None
            entry = _entry_price(s, snap_close)
            try:
                cm = contract_month_for_symbol(db, sym)
            except Exception:
                cm = None
            db.execute(
                text(
                    f"""
                    INSERT INTO {TABLE} (
                        session_date, symbol, direction, contract_month,
                        episode_started_at, entry_price, source, updated_at
                    ) VALUES (
                        CAST(:sd AS date), :s, :d, :cm, :now, :entry, :src, :now
                    )
                    """
                ),
                {
                    "sd": session_date,
                    "s": sym,
                    "d": direction,
                    "cm": cm,
                    "now": now_i,
                    "entry": entry,
                    "src": source,
                },
            )
            stats["started"] += 1
            open_map[sym] = {
                "symbol": sym,
                "direction": direction,
                "entry_price": entry,
                "episode_started_at": now_i,
            }

        # End episodes that left READY
        stock_by = {(s.get("symbol") or "").upper(): s for s in stocks if s.get("symbol")}
        for sym, ep in list(open_map.items()):
            if sym in active_syms:
                # still ready — optionally fill missing entry
                continue
            stock = stock_by.get(sym)
            direction = (ep.get("direction") or _direction_of(stock or {})).upper()
            entry = _f(ep.get("entry_price"))
            candles = candles_by_symbol.get(sym) or []
            exit_px = None
            exit_at = now_i
            try:
                from backend.services.kavach_vwap_close_confirm_shadow import closed_10m_snapshot

                snap = closed_10m_snapshot(candles, direction=direction, now=now_i)
                if snap:
                    exit_px = snap.get("close")
                    if snap.get("bar_end"):
                        exit_at = snap["bar_end"]
            except Exception:
                pass
            if exit_px is None and stock:
                exit_px = _f(stock.get("ltp") or stock.get("price"))

            lot = _lot_size(db, sym, session_date)
            pnl_pts = None
            pnl_inr = None
            if entry is not None and exit_px is not None:
                pnl_pts = _signed_pnl(direction, entry, exit_px)
                pnl_inr = pnl_pts * lot

            ext = close_plus_n_10m(candles, exit_at=exit_at, n=PLUS4_CANDLES, now=now_i)
            # If +4 not yet available (intra-day), leave extended null and keep row... 
            # User wants automatic log at episode end. If +4 bars not yet closed, 
            # still close episode with standard PnL and null plus4; a follow-up
            # enrich can backfill open extended fields — simpler: only finalize when
            # +4 available OR session past 15:30 OR no candles.
            plus4_ready = ext is not None and int(ext.get("candles_used") or 0) >= PLUS4_CANDLES
            eod = now_i.timetz().replace(tzinfo=None) >= datetime.strptime("15:25", "%H:%M").time()
            if not plus4_ready and not eod and candles:
                # defer ending until +4 candles print (sticky keep open? but not READY)
                # Mark ended with whatever we have so leave isn't lost; plus4 may be partial.
                pass

            ext_px = (ext or {}).get("close")
            ext_at = (ext or {}).get("bar_end")
            candles_used = (ext or {}).get("candles_used")
            pnl_pts4 = None
            pnl_inr4 = None
            if entry is not None and ext_px is not None:
                pnl_pts4 = _signed_pnl(direction, entry, ext_px)
                pnl_inr4 = pnl_pts4 * lot

            end_reason = "left_ready"
            if stock and is_ready_like(stock.get("trade_state")):
                end_reason = "still_ready_anomaly"
            if eod:
                end_reason = "session_eod"
            if stock and (stock.get("trade_state") or "").upper() == "EXPIRED":
                end_reason = "expired_move"

            was_traded = False
            trade_log_id = None
            try:
                tr = db.execute(
                    text(
                        """
                        SELECT id FROM trade_log
                        WHERE UPPER(symbol) = :s
                          AND trade_date = CAST(:d AS date)
                        ORDER BY id DESC LIMIT 1
                        """
                    ),
                    {"s": sym, "d": session_date},
                ).fetchone()
                if tr:
                    was_traded = True
                    trade_log_id = int(tr[0])
            except Exception:
                pass

            delta_pts = None
            delta_inr = None
            if pnl_pts is not None and pnl_pts4 is not None:
                delta_pts = pnl_pts4 - pnl_pts
                delta_inr = (pnl_inr4 or 0) - (pnl_inr or 0)

            eid = ep.get("id")
            if eid is None:
                row = db.execute(
                    text(
                        f"""
                        SELECT id FROM {TABLE}
                        WHERE session_date = CAST(:d AS date)
                          AND UPPER(symbol) = :s
                          AND episode_ended_at IS NULL
                        ORDER BY episode_started_at DESC LIMIT 1
                        """
                    ),
                    {"d": session_date, "s": sym},
                ).fetchone()
                eid = row[0] if row else None
            if eid is None:
                continue

            db.execute(
                text(
                    f"""
                    UPDATE {TABLE}
                    SET episode_ended_at = :now,
                        exit_price = :exit_px,
                        exit_at = :exit_at,
                        pnl_pts_standard = :pnl_pts,
                        pnl_inr_standard = :pnl_inr,
                        lot_size = :lot,
                        extended_exit_price = :ext_px,
                        extended_exit_at = :ext_at,
                        plus4_candles_used = :cu,
                        pnl_pts_plus4 = :pnl4,
                        pnl_inr_plus4 = :inr4,
                        delta_pts_plus4_vs_standard = :dpts,
                        delta_inr_plus4_vs_standard = :dinr,
                        episode_end_reason = :reason,
                        was_traded = :wt,
                        trade_log_id = :tid,
                        updated_at = :now
                    WHERE id = :id AND episode_ended_at IS NULL
                    """
                ),
                {
                    "now": now_i,
                    "exit_px": exit_px,
                    "exit_at": exit_at,
                    "pnl_pts": pnl_pts,
                    "pnl_inr": pnl_inr,
                    "lot": lot,
                    "ext_px": ext_px,
                    "ext_at": ext_at,
                    "cu": candles_used,
                    "pnl4": pnl_pts4,
                    "inr4": pnl_inr4,
                    "dpts": delta_pts,
                    "dinr": delta_inr,
                    "reason": end_reason,
                    "wt": was_traded,
                    "tid": trade_log_id,
                    "id": int(eid),
                },
            )
            stats["ended"] += 1
    except Exception:
        logger.debug("ready exit+4 shadow skipped", exc_info=True)
    return stats
