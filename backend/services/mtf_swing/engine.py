"""Bar-by-bar MTF Swing execution. Pure: no I/O. Month batches call this with carried state."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from backend.services.mtf_swing.bars import (
    Bar,
    bars_for_week,
    filter_session_bars,
    iso,
    last_index_by_date,
    last_index_by_week,
    roll_gap,
    week_key,
)
from backend.services.mtf_swing.config import MtfSwingConfig
from backend.services.mtf_swing.levels import Levels, adjusted_pair, build_levels, candidate_side


@dataclass
class Setup:
    side: str
    screen_date: str
    valid_week: Tuple[int, int]
    sl: float
    t1: float
    trigger: float
    basis_contract: str
    phase: str = "watch"  # watch | next_open | limit
    limit_price: Optional[float] = None
    defer_fill: bool = False

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["valid_week"] = list(self.valid_week)
        return d

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "Setup":
        vw = raw["valid_week"]
        return cls(
            side=raw["side"],
            screen_date=raw["screen_date"],
            valid_week=(int(vw[0]), int(vw[1])),
            sl=float(raw["sl"]),
            t1=float(raw["t1"]),
            trigger=float(raw["trigger"]),
            basis_contract=str(raw.get("basis_contract") or ""),
            phase=raw.get("phase") or "watch",
            limit_price=None if raw.get("limit_price") in (None, "") else float(raw["limit_price"]),
            defer_fill=bool(raw.get("defer_fill")),
        )


@dataclass
class Position:
    side: str
    screen_date: str
    entry_time: str
    entry_price: float
    entry_type: str
    sl: float
    t1: float
    lot_size: int
    basis_contract: str
    entry_month: str
    sessions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "Position":
        return cls(
            side=raw["side"],
            screen_date=raw["screen_date"],
            entry_time=raw["entry_time"],
            entry_price=float(raw["entry_price"]),
            entry_type=raw["entry_type"],
            sl=float(raw["sl"]),
            t1=float(raw["t1"]),
            lot_size=int(raw.get("lot_size") or 0),
            basis_contract=str(raw.get("basis_contract") or ""),
            entry_month=raw["entry_month"],
            sessions=list(raw.get("sessions") or []),
        )


def _blank_state() -> Dict[str, Any]:
    return {
        "setup": None,
        "queued": None,
        "position": None,
        "last_ts": "",
        "last_close": None,
        "last_contract": "",
    }


def _shift_setup(setup: Setup, gap: float, contract: str) -> None:
    if gap:
        setup.sl += gap
        setup.t1 += gap
        setup.trigger += gap
        if setup.limit_price is not None:
            setup.limit_price += gap
    setup.basis_contract = contract


def _shift_pos(pos: Position, gap: float, contract: str) -> None:
    if gap:
        pos.entry_price += gap
        pos.sl += gap
        pos.t1 += gap
    pos.basis_contract = contract


def _fmt(n: Optional[float], nd: int = 4) -> str:
    if n is None:
        return ""
    return f"{n:.{nd}f}"


def _pnl(side: str, entry: float, exit_px: float, sl: float, lot: int, cfg: MtfSwingConfig) -> Tuple[str, str, str, str]:
    slip = float(cfg.slippage_points or 0)
    if side == "LONG":
        en = entry + slip
        ex = exit_px - slip
        points = ex - en
        risk = entry - sl
        raw_points = exit_px - entry
    else:
        en = entry - slip
        ex = exit_px + slip
        points = en - ex
        risk = sl - entry
        raw_points = entry - exit_px
    gross = points * int(lot) * int(cfg.qty_lots)
    charges = cfg.charge_inr()
    net = gross - charges
    r = "" if risk == 0 else f"{(raw_points / risk):.4f}"
    return _fmt(gross, 2), _fmt(charges, 2), _fmt(net, 2), r


def _trade_row(
    *,
    symbol: str,
    cfg: MtfSwingConfig,
    pos: Position,
    exit_time: str,
    exit_price: Optional[float],
    exit_reason: str,
    run_ts: str,
) -> Dict[str, str]:
    tid = f"{symbol}|{cfg.timeframe}|{pos.side}|{pos.entry_time}"
    if exit_reason == "OPEN":
        gross = charges = net = r = ""
        exit_px_s = ""
    else:
        gross, charges, net, r = _pnl(pos.side, pos.entry_price, float(exit_price), pos.sl, pos.lot_size, cfg)
        exit_px_s = _fmt(exit_price)
    return {
        "trade_id": tid,
        "timeframe": cfg.timeframe,
        "month": pos.entry_month,
        "symbol": symbol,
        "side": pos.side,
        "screen_date": pos.screen_date,
        "entry_time": pos.entry_time,
        "entry_price": _fmt(pos.entry_price),
        "entry_type": pos.entry_type,
        "lot_size": str(int(pos.lot_size)),
        "qty_lots": str(int(cfg.qty_lots)),
        "sl": _fmt(pos.sl),
        "t1": _fmt(pos.t1),
        "exit_time": exit_time,
        "exit_price": exit_px_s,
        "exit_reason": exit_reason,
        "sessions_held": str(len(pos.sessions)),
        "gross_pnl": gross,
        "charges": charges,
        "net_pnl": net,
        "r_multiple": r,
        "run_ts": run_ts,
    }


def _discard_row(
    *,
    symbol: str,
    cfg: MtfSwingConfig,
    side: str,
    screen_date: str,
    reason: str,
    detail: str,
    event_time: str,
    run_ts: str,
) -> Dict[str, str]:
    month = (screen_date or event_time or "")[:7]
    did = f"{symbol}|{cfg.timeframe}|{screen_date}|{side}|{reason}|{event_time}"
    return {
        "discard_id": did,
        "timeframe": cfg.timeframe,
        "month": month,
        "symbol": symbol,
        "side": side,
        "screen_date": screen_date,
        "reason": reason,
        "detail": detail,
        "event_time": event_time,
        "run_ts": run_ts,
    }


def _try_exit(pos: Position, bar: Bar, is_last_session_bar: bool, cfg: MtfSwingConfig) -> Optional[Tuple[float, str]]:
    """Return (exit_price, reason) or None. SL wins when the same bar touches SL and T1."""
    sess = bar.session_date.isoformat()
    if sess not in pos.sessions:
        pos.sessions.append(sess)
    if pos.side == "LONG":
        if bar.open <= pos.sl:
            return bar.open, "SL"
        if bar.open >= pos.t1:
            return bar.open, "T1"
        hit_sl = bar.low <= pos.sl
        hit_t1 = bar.high >= pos.t1
        if hit_sl and hit_t1:
            return pos.sl, "SL"
        if hit_sl:
            return pos.sl, "SL"
        if hit_t1:
            return pos.t1, "T1"
    else:
        if bar.open >= pos.sl:
            return bar.open, "SL"
        if bar.open <= pos.t1:
            return bar.open, "T1"
        hit_sl = bar.high >= pos.sl
        hit_t1 = bar.low <= pos.t1
        if hit_sl and hit_t1:
            return pos.sl, "SL"
        if hit_sl:
            return pos.sl, "SL"
        if hit_t1:
            return pos.t1, "T1"
    if len(pos.sessions) >= int(cfg.max_sessions) and is_last_session_bar:
        return bar.close, "TIME"
    return None


def _cancel_long_before(setup: Setup, bar: Bar) -> Optional[str]:
    if bar.low < setup.sl:
        return "SL_BEFORE_ENTRY"
    if bar.high >= setup.t1:
        return "T1_BEFORE_ENTRY"
    return None


def _cancel_short_before(setup: Setup, bar: Bar) -> Optional[str]:
    if bar.high > setup.sl:
        return "SL_BEFORE_ENTRY"
    if bar.low <= setup.t1:
        return "T1_BEFORE_ENTRY"
    return None


def _arm_or_enter_from_signal(
    setup: Setup,
    close: float,
    ts: datetime,
    cfg: MtfSwingConfig,
    lot_size: int,
) -> Tuple[str, Optional[Position], Optional[str]]:
    """After a breakout/breakdown close. Returns (action, position, cancel_reason)."""
    if setup.side == "LONG":
        if close <= setup.sl:
            return "cancel", None, "RR_UNDEFINED"
        rr = (setup.t1 - close) / (close - setup.sl)
    else:
        if close >= setup.sl:
            return "cancel", None, "RR_UNDEFINED"
        rr = (close - setup.t1) / (setup.sl - close)
    if rr >= 1.0:
        if cfg.entry_timing == "close":
            pos = Position(
                side=setup.side,
                screen_date=setup.screen_date,
                entry_time=iso(ts),
                entry_price=close,
                entry_type="direct",
                sl=setup.sl,
                t1=setup.t1,
                lot_size=int(lot_size),
                basis_contract=setup.basis_contract,
                entry_month=ts.date().strftime("%Y-%m"),
                sessions=[ts.date().isoformat()],
            )
            return "enter", pos, None
        setup.phase = "next_open"
        setup.defer_fill = False
        return "pending", None, None
    setup.phase = "limit"
    setup.limit_price = (setup.t1 + setup.sl) / 2.0
    setup.defer_fill = False
    return "pending", None, None


def _enter_at(
    setup: Setup,
    ts: datetime,
    price: float,
    entry_type: str,
    lot_size: int,
) -> Position:
    return Position(
        side=setup.side,
        screen_date=setup.screen_date,
        entry_time=iso(ts),
        entry_price=price,
        entry_type=entry_type,
        sl=setup.sl,
        t1=setup.t1,
        lot_size=int(lot_size),
        basis_contract=setup.basis_contract,
        entry_month=ts.date().strftime("%Y-%m"),
        sessions=[ts.date().isoformat()],
    )


def _step_setup(
    setup: Setup,
    bar: Bar,
    cfg: MtfSwingConfig,
    lot_size: int,
) -> Tuple[Optional[Setup], Optional[Position], Optional[str]]:
    """Advance a setup on one bar.

    Returns (still_setup, new_position, cancel_reason).
    Limit orders are live starting the bar AFTER the signal bar.
    A next_open armed on this bar is not filled on this bar.
    """
    if setup.phase == "next_open":
        if setup.side == "LONG":
            if bar.open < setup.sl:
                return None, None, "GAP_THROUGH_SL"
            if bar.open >= setup.t1:
                return None, None, "GAP_THROUGH_T1"
        else:
            if bar.open > setup.sl:
                return None, None, "GAP_THROUGH_SL"
            if bar.open <= setup.t1:
                return None, None, "GAP_THROUGH_T1"
        return None, _enter_at(setup, bar.ts, bar.open, "direct", lot_size), None

    if setup.phase == "limit":
        limit_px = float(setup.limit_price if setup.limit_price is not None else (setup.t1 + setup.sl) / 2.0)
        if setup.side == "LONG":
            if bar.open <= setup.sl:
                return None, None, "SL_BEFORE_ENTRY"
            if bar.open >= setup.t1:
                return None, None, "T1_BEFORE_ENTRY"
            if bar.open <= limit_px:
                return None, _enter_at(setup, bar.ts, bar.open, "limit", lot_size), None
            if bar.low < setup.sl:
                return None, None, "SL_BEFORE_ENTRY"
            if bar.high >= setup.t1:
                return None, None, "T1_BEFORE_ENTRY"
            if bar.low <= limit_px:
                return None, _enter_at(setup, bar.ts, limit_px, "limit", lot_size), None
        else:
            if bar.open >= setup.sl:
                return None, None, "SL_BEFORE_ENTRY"
            if bar.open <= setup.t1:
                return None, None, "T1_BEFORE_ENTRY"
            if bar.open >= limit_px:
                return None, _enter_at(setup, bar.ts, bar.open, "limit", lot_size), None
            if bar.high > setup.sl:
                return None, None, "SL_BEFORE_ENTRY"
            if bar.low <= setup.t1:
                return None, None, "T1_BEFORE_ENTRY"
            if bar.high >= limit_px:
                return None, _enter_at(setup, bar.ts, limit_px, "limit", lot_size), None
        return setup, None, None

    # watch
    if setup.side == "LONG":
        reason = _cancel_long_before(setup, bar)
        if reason:
            return None, None, reason
        if bar.close > setup.trigger:
            action, pos, creason = _arm_or_enter_from_signal(setup, bar.close, bar.ts, cfg, lot_size)
            if action == "cancel":
                return None, None, creason
            if action == "enter":
                return None, pos, None
            return setup, None, None
    else:
        reason = _cancel_short_before(setup, bar)
        if reason:
            return None, None, reason
        if bar.close < setup.trigger:
            action, pos, creason = _arm_or_enter_from_signal(setup, bar.close, bar.ts, cfg, lot_size)
            if action == "cancel":
                return None, None, creason
            if action == "enter":
                return None, pos, None
            return setup, None, None
    return setup, None, None


def _log_screen_discard(discard, bar: Bar, note) -> None:
    """Log structural screen failures. A week that is simply not a candidate is silent."""
    if not discard:
        return
    side, reason, detail = discard
    if reason in ("NO_SWING", "LEVELS_ILLOGICAL") and side:
        note(side, bar.session_date.isoformat(), reason, detail, bar.ts)
    elif reason == "NO_W2":
        note(side or "", bar.session_date.isoformat(), "NO_W2", detail, bar.ts)


def _screen(
    bars: Sequence[Bar],
    week_end_index: int,
    cfg: MtfSwingConfig,
) -> Tuple[Optional[Setup], Optional[Tuple[str, str, str]]]:
    """Screen the week that ends at ``week_end_index``. Discard tuple is (side, reason, detail)."""
    end = bars[week_end_index]
    w1_key = week_key(end.session_date)
    from datetime import timedelta

    prev_day = end.session_date - timedelta(days=7)
    w2_key = week_key(prev_day)
    # previous calendar week: walk back 7 days from the Monday of W1, not from Friday,
    # so a short holiday week still maps to the prior ISO week.
    # week_key(last_date - 7 days) is the previous ISO week when last_date is inside the week.
    w1_raw = bars_for_week(bars, w1_key)
    w2_raw = bars_for_week(bars, w2_key)
    if not w2_raw:
        return None, ("", "NO_W2", f"no bars in week {w2_key[0]}-W{w2_key[1]:02d}")
    w2, w1 = adjusted_pair(w2_raw, w1_raw)
    if w1 is None or w2 is None:
        return None, ("", "NO_W2", "empty week")
    side = candidate_side(w1, w2)
    if side is None:
        return None, None
    levels, reason = build_levels(side, w1, w2, cluster_pct=cfg.cluster_pct)
    if reason or levels is None:
        return None, (side, reason or "NO_SWING", "")
    return (
        Setup(
            side=levels.side,
            screen_date=levels.screen_date.isoformat(),
            valid_week=levels.valid_week,
            sl=levels.sl,
            t1=levels.t1,
            trigger=levels.trigger,
            basis_contract=levels.basis_contract,
            phase="watch",
        ),
        None,
    )


def run_symbol(
    bars: Sequence[Bar],
    *,
    symbol: str,
    holidays: Optional[Set[date]] = None,
    active_from: date,
    active_to: date,
    state: Optional[Dict[str, Any]] = None,
    config: Optional[MtfSwingConfig] = None,
    lot_size: int = 1,
    lot_by_contract: Optional[Dict[str, int]] = None,
    run_ts: str = "",
    finalize_open: bool = False,
    exit_only_after: Optional[date] = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], Dict[str, Any]]:
    """Run one symbol.

    Bars may include warm-up before ``active_from`` and a tail after ``active_to``.
    Bars at or before ``state['last_ts']`` are history (already processed).
    ``exit_only_after`` (default ``active_to``) still manages exits and an armed
    next-open fill, but does not take new breakouts or screen new weeks.
    Open positions are emitted only when ``finalize_open`` is set.
    """
    cfg = config or MtfSwingConfig()
    hol = holidays or set()
    lots = lot_by_contract or {}

    def _lot(contract: str) -> int:
        try:
            n = int(lots.get(contract) or 0)
        except (TypeError, ValueError):
            n = 0
        return n or int(lot_size)
    series = filter_session_bars(bars, hol)
    st = _blank_state()
    if state:
        st.update(state)
        if st.get("setup"):
            st["setup"] = st["setup"] if isinstance(st["setup"], Setup) else Setup.from_dict(st["setup"])
        if st.get("queued"):
            st["queued"] = st["queued"] if isinstance(st["queued"], Setup) else Setup.from_dict(st["queued"])
        if st.get("position"):
            st["position"] = st["position"] if isinstance(st["position"], Position) else Position.from_dict(st["position"])

    trades: List[Dict[str, str]] = []
    discards: List[Dict[str, str]] = []
    if not series:
        return trades, discards, _export_state(st)

    last_session = last_index_by_date(series)
    last_week = last_index_by_week(series)
    cutoff = exit_only_after or active_to
    watermark = str(st.get("last_ts") or "")

    def _note_discard(side: str, screen_date: str, reason: str, detail: str, when: datetime) -> None:
        discards.append(
            _discard_row(
                symbol=symbol,
                cfg=cfg,
                side=side,
                screen_date=screen_date,
                reason=reason,
                detail=detail,
                event_time=iso(when),
                run_ts=run_ts,
            )
        )

    def _close(pos: Position, when: datetime, price: float, reason: str) -> None:
        trades.append(
            _trade_row(
                symbol=symbol,
                cfg=cfg,
                pos=pos,
                exit_time=iso(when),
                exit_price=price,
                exit_reason=reason,
                run_ts=run_ts,
            )
        )

    for i, bar in enumerate(series):
        prev = series[i - 1] if i else None
        if watermark and iso(bar.ts) <= watermark:
            continue
        # History before the active window only tracks the roll reference,
        # unless we still owe a deferred next-open fill on the first bar after watermark.
        before_window = bar.session_date < active_from
        after_cutoff = bar.session_date > cutoff
        manage_only = after_cutoff or (before_window and not (st.get("setup") and st["setup"].defer_fill))
        if before_window and not (st.get("setup") and getattr(st["setup"], "defer_fill", False)):
            st["last_ts"] = iso(bar.ts)
            st["last_close"] = bar.close
            st["last_contract"] = bar.contract
            continue

        gap = 0.0
        if prev is not None and (not watermark or iso(prev.ts) <= watermark or True):
            gap = roll_gap(prev, bar)
        elif st.get("last_contract") and bar.contract and st["last_contract"] != bar.contract and st.get("last_close") is not None:
            gap = bar.open - float(st["last_close"])
        if gap and st.get("setup"):
            _shift_setup(st["setup"], gap, bar.contract)
        if gap and st.get("queued"):
            _shift_setup(st["queued"], gap, bar.contract)
        if gap and st.get("position"):
            _shift_pos(st["position"], gap, bar.contract)
        if st.get("setup"):
            st["setup"].basis_contract = bar.contract
        if st.get("position"):
            st["position"].basis_contract = bar.contract

        # 1. Exit an open position (includes same-bar exit after an open entry below).
        if st.get("position") is not None:
            hit = _try_exit(st["position"], bar, last_session.get(bar.session_date) == i, cfg)
            if hit:
                px, reason = hit
                _close(st["position"], bar.ts, px, reason)
                st["position"] = None

        allow_entries = (not manage_only) and (not after_cutoff) and bar.session_date <= cutoff
        # A deferred next-open may fill on the first bar after the valid week,
        # including a bar after the monthly cutoff (the entry was signaled inside the window).
        allow_defer_fill = bool(st.get("setup") and st["setup"].defer_fill)

        if st.get("position") is None and st.get("setup") is not None and (allow_entries or allow_defer_fill):
            setup: Setup = st["setup"]
            in_week = week_key(bar.session_date) == tuple(setup.valid_week)
            consumed_defer = False
            if setup.phase == "next_open" and setup.defer_fill:
                st["setup"], pos, reason = _step_setup(setup, bar, cfg, _lot(bar.contract))
                consumed_defer = True
                if reason:
                    _note_discard(setup.side, setup.screen_date, reason, "", bar.ts)
                if pos is not None:
                    st["position"] = pos
                    st["queued"] = None
                    # Same-bar exit after an open fill.
                    hit = _try_exit(pos, bar, last_session.get(bar.session_date) == i, cfg)
                    if hit:
                        px, er = hit
                        _close(pos, bar.ts, px, er)
                        st["position"] = None
                if st.get("position") is None and st.get("queued") is not None and allow_entries:
                    st["setup"] = st["queued"]
                    st["queued"] = None
                    setup = st["setup"]
                    in_week = week_key(bar.session_date) == tuple(setup.valid_week)
                    if in_week and setup.phase == "watch":
                        st["setup"], pos, reason = _step_setup(setup, bar, cfg, _lot(bar.contract))
                        if reason:
                            _note_discard(setup.side, setup.screen_date, reason, "", bar.ts)
                        if pos is not None:
                            st["position"] = pos
                            if cfg.entry_timing == "next_open" or pos.entry_type == "direct" and pos.entry_time == iso(bar.ts) and abs(pos.entry_price - bar.open) < 1e-9:
                                hit = _try_exit(pos, bar, last_session.get(bar.session_date) == i, cfg)
                                if hit and pos.entry_time == iso(bar.ts) and abs(pos.entry_price - bar.close) > 1e-9:
                                    px, er = hit
                                    _close(pos, bar.ts, px, er)
                                    st["position"] = None
                elif consumed_defer:
                    st["setup"] = None
            elif in_week and allow_entries:
                st["setup"], pos, reason = _step_setup(setup, bar, cfg, _lot(bar.contract))
                if reason:
                    _note_discard(setup.side, setup.screen_date, reason, "", bar.ts)
                if pos is not None:
                    st["position"] = pos
                    # Exit on the entry bar only when the fill is the open, not the close.
                    filled_at_open = abs(pos.entry_price - bar.open) < 1e-6 or pos.entry_type == "limit"
                    filled_at_close = cfg.entry_timing == "close" and pos.entry_type == "direct" and abs(pos.entry_price - bar.close) < 1e-6
                    if filled_at_open and not filled_at_close:
                        hit = _try_exit(pos, bar, last_session.get(bar.session_date) == i, cfg)
                        if hit:
                            px, er = hit
                            _close(pos, bar.ts, px, er)
                            st["position"] = None

        # Week-end housekeeping only inside the entry window.
        # Re-read setup after a possible cancel: the local reference would still
        # point at the cancelled object and skip the new week's screen.
        if allow_entries and last_week.get(week_key(bar.session_date)) == i:
            setup = st.get("setup")
            if setup is not None and week_key(bar.session_date) == tuple(setup.valid_week) and st.get("position") is None:
                if setup.phase in ("watch", "limit"):
                    _note_discard(setup.side, setup.screen_date, "WEEK_EXPIRED", setup.phase, bar.ts)
                    st["setup"] = None
                elif setup.phase == "next_open":
                    setup.defer_fill = True
            setup = st.get("setup")
            if st.get("position") is None and setup is None:
                new_setup, discard = _screen(series, i, cfg)
                _log_screen_discard(discard, bar, _note_discard)
                if new_setup is not None:
                    st["setup"] = new_setup
            elif (
                st.get("position") is None
                and setup is not None
                and setup.phase == "next_open"
                and setup.defer_fill
            ):
                new_setup, discard = _screen(series, i, cfg)
                _log_screen_discard(discard, bar, _note_discard)
                if new_setup is not None and st.get("position") is not None:
                    _note_discard(new_setup.side, new_setup.screen_date, "SKIP_OPEN_POSITION", "", bar.ts)
                    new_setup = None
                st["queued"] = new_setup

        if after_cutoff and st.get("setup") is not None and not st["setup"].defer_fill:
            setup = st["setup"]
            _note_discard(setup.side, setup.screen_date, "CUTOFF", "past backtest end", bar.ts)
            st["setup"] = None
            st["queued"] = None

        st["last_ts"] = iso(bar.ts)
        st["last_close"] = bar.close
        st["last_contract"] = bar.contract

    if finalize_open and st.get("position") is not None:
        pos = st["position"]
        last_bar = series[-1]
        trades.append(
            _trade_row(
                symbol=symbol,
                cfg=cfg,
                pos=pos,
                exit_time="",
                exit_price=None,
                exit_reason="OPEN",
                run_ts=run_ts,
            )
        )
        st["position"] = None

    return trades, discards, _export_state(st)


def _export_state(st: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "setup": st["setup"].to_dict() if isinstance(st.get("setup"), Setup) else st.get("setup"),
        "queued": st["queued"].to_dict() if isinstance(st.get("queued"), Setup) else st.get("queued"),
        "position": st["position"].to_dict() if isinstance(st.get("position"), Position) else st.get("position"),
        "last_ts": st.get("last_ts") or "",
        "last_close": st.get("last_close"),
        "last_contract": st.get("last_contract") or "",
    }
    return out
