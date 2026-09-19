"""Replay Screener / Sizing / ExitEngine over stored full-chain snapshots or MCX EOD."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.chain_snapshots import chain_coverage, unpack_chain_payload
from backend.services.tarang.config import get_profiles, get_risk
from backend.services.tarang.data_gaps import gaps_overlapping, recent_gaps, snapshot_in_gap
from backend.services.tarang.domain.types import OptionChain, OptionQuote
from backend.services.tarang.exit_engine import evaluate_exits
from backend.services.tarang.fee_gate import gate_fee_and_limits
from backend.services.tarang.paper_broker import simulated_fill_price
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.structures import build_structure, max_loss_per_unit_inr, floor_units

logger = logging.getLogger(__name__)

MIN_WIDE_DAYS_FOR_RUN = 5


def _quote_from_row(row: Dict[str, Any], underlying: str, expiry: str) -> OptionQuote:
    return OptionQuote(
        instrument_key=str(row.get("instrument_key") or row.get("symbol") or ""),
        symbol=str(row.get("symbol") or ""),
        underlying=underlying,
        expiry=expiry or str(row.get("expiry") or ""),
        strike=float(row.get("strike") or 0),
        right=str(row.get("right") or ""),
        bid=row.get("bid"),
        ask=row.get("ask"),
        mid=row.get("mark") or row.get("mid"),
        last=row.get("last"),
        oi=row.get("oi"),
        volume=row.get("volume"),
        iv=row.get("iv"),
        delta=row.get("delta"),
        greeks_source=row.get("greeks_source"),
        meta={"two_sided": row.get("two_sided"), "bid_qty": row.get("bid_qty"), "ask_qty": row.get("ask_qty")},
    )


def _chain_from_snapshot(row: Dict[str, Any]) -> Optional[OptionChain]:
    blob = row.get("payload_gzip")
    if not blob:
        return None
    try:
        payload = unpack_chain_payload(bytes(blob))
    except Exception:
        return None
    expiry = str(row.get("expiry_date") or payload.get("expiry") or "")[:10]
    und = row.get("underlying_symbol") or ""
    quotes = [_quote_from_row(q, und, expiry) for q in (payload.get("quotes") or [])]
    return OptionChain(
        profile_id=row.get("profile_id") or "",
        venue=row.get("venue") or "",
        underlying=und,
        expiry=expiry,
        futures_or_spot=row.get("futures_or_spot") or payload.get("underlying_price"),
        lot_size=payload.get("lot_size"),
        strike_step=payload.get("strike_step"),
        quotes=quotes,
        built_at=payload.get("captured_at"),
        meta={"window": (row.get("meta") if isinstance(row.get("meta"), dict) else {}), "from_snapshot": True},
    )


def _load_snapshots(db, *, wide_only: bool = True) -> List[Dict[str, Any]]:
    q = """
        SELECT id, captured_at, profile_id, venue, underlying_symbol, expiry_date,
               futures_or_spot, atm_iv, n_quotes, payload_gzip, meta
        FROM tarang_chain_snapshots
        ORDER BY captured_at ASC, expiry_date
    """
    rows = db.execute(text(q)).mappings().all()
    out = []
    for r in rows:
        d = dict(r)
        meta = d.get("meta") or {}
        if isinstance(meta, str):
            import json

            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        d["meta"] = meta
        kind = str(meta.get("window_kind") or "")
        if wide_only and kind == "narrow_window":
            continue
        out.append(d)
    return out


LOT_SIZE = {
    "CRUDEOILM": 10,
    "CRUDEOIL": 100,
    "NATGASMINI": 250,
    "NATURALGAS": 1250,
}

MODELLED_SPREAD_FRAC_OF_MID = 0.02
GO_LIVE_GATE_MIN_TRADES = 40
INTRADAY_EOD_MSG = "INTRADAY cannot be tested on EOD — this path is positional daily decisions only."


def _dte(expiry: date, today: date) -> int:
    return (expiry - today).days


def mcx_entry_dte_ok(dte: int, min_dte: int = 7, max_dte: int = 35) -> bool:
    return min_dte <= int(dte) <= max_dte


def _model_bid_ask(close: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if close is None or float(close) <= 0:
        return None, None, None
    c = float(close)
    spr = max(0.05, c * MODELLED_SPREAD_FRAC_OF_MID)
    return c - spr / 2.0, c + spr / 2.0, c


def _eod_quote(row: Dict[str, Any], fill_close: Optional[float] = None) -> OptionQuote:
    close = fill_close if fill_close is not None else row.get("close")
    bid, ask, mid = _model_bid_ask(close)
    return OptionQuote(
        instrument_key=f"{row.get('symbol')}-{row.get('expiry_date')}-{row.get('option_type')}-{row.get('strike')}",
        symbol=str(row.get("symbol") or ""),
        underlying=str(row.get("symbol") or ""),
        expiry=str(row.get("expiry_date"))[:10],
        strike=float(row.get("strike") or 0),
        right=str(row.get("option_type") or ""),
        bid=bid,
        ask=ask,
        mid=mid,
        last=close,
        oi=row.get("oi_lots"),
        volume=row.get("volume_lots"),
        iv=row.get("iv"),
        delta=row.get("delta"),
        greeks_source=row.get("greeks_source") or "black76_otm",
        lot_size=LOT_SIZE.get(str(row.get("symbol") or "").upper()),
        meta={
            "modelled_fills": True,
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "traded": row.get("traded"),
        },
    )


def _strike_step(strikes: List[float]) -> Optional[float]:
    uniq = sorted({float(s) for s in strikes if s is not None})
    diffs = [round(uniq[i + 1] - uniq[i], 6) for i in range(len(uniq) - 1) if uniq[i + 1] > uniq[i]]
    return min(diffs) if diffs else None


def _load_eod_rows(db) -> List[Dict[str, Any]]:
    q = """
        SELECT symbol, expiry_date, option_type, strike, trade_date,
               open, high, low, close, volume_lots, oi_lots, traded,
               iv, delta, greeks_source, underlying_price, underlying_source
        FROM tarang_hist_eod
        WHERE option_type IN ('CE','PE','FUT')
        ORDER BY trade_date, symbol, expiry_date, option_type, strike
    """
    return [dict(r) for r in db.execute(text(q)).mappings().all()]


def _chain_for_day(
    rows: List[Dict[str, Any]],
    *,
    profile_id: str,
    close_override: Optional[Dict[Tuple[str, float], float]] = None,
    require_traded: bool = True,
) -> Optional[OptionChain]:
    if not rows:
        return None
    opts = [r for r in rows if r.get("option_type") in ("CE", "PE")]
    if require_traded:
        opts = [r for r in opts if r.get("traded")]
    if not opts:
        return None
    quotes = []
    for r in opts:
        key = (str(r["option_type"]), float(r["strike"]))
        fill = (close_override or {}).get(key)
        quotes.append(_eod_quote(r, fill_close=fill))
    fut = next((r for r in rows if r.get("option_type") == "FUT"), None)
    F = None
    if fut and fut.get("close"):
        F = float(fut["close"])
    else:
        F = next((float(r["underlying_price"]) for r in opts if r.get("underlying_price")), None)
    step = _strike_step([q.strike for q in quotes])
    und = str(opts[0]["symbol"])
    return OptionChain(
        profile_id=profile_id,
        venue="upstox_mcx",
        underlying=und,
        expiry=str(opts[0]["expiry_date"])[:10],
        futures_or_spot=F,
        lot_size=LOT_SIZE.get(und.upper()),
        strike_step=step,
        quotes=quotes,
        built_at=str(opts[0]["trade_date"]),
        meta={"modelled_fills": True, "source": "mcx_eod"},
    )


def _credit_from_legs(legs: List[Dict[str, Any]], *, spread_fraction: float) -> Optional[float]:
    credit = 0.0
    for lg in legs:
        px = simulated_fill_price(lg.get("side"), lg.get("bid"), lg.get("ask"), lg.get("mid"), spread_fraction=spread_fraction)
        if px is None:
            return None
        if str(lg.get("side")).upper() == "SELL":
            credit += px
        else:
            credit -= px
    return credit


def _debit_from_chain(chain: OptionChain, legs: List[Dict[str, Any]], *, spread_fraction: float, pessimistic_hl: bool = False) -> Optional[float]:
    debit = 0.0
    for lg in legs:
        match = next(
            (q for q in chain.quotes if q.right == lg.get("right") and abs(float(q.strike) - float(lg["strike"])) < 1e-6),
            None,
        )
        if not match:
            return None
        if pessimistic_hl:
            hi = (match.meta or {}).get("high")
            lo = (match.meta or {}).get("low")
            if str(lg.get("side")).upper() == "SELL":
                px = float(hi) if hi else match.last
            else:
                px = float(lo) if lo else match.last
            if px is None:
                return None
        else:
            px = simulated_fill_price(
                "BUY" if str(lg.get("side")).upper() == "SELL" else "SELL",
                match.bid,
                match.ask,
                match.mid,
                spread_fraction=spread_fraction,
            )
            if px is None:
                return None
        if str(lg.get("side")).upper() == "SELL":
            debit += px
        else:
            debit -= px
    return debit


def run_eod_backtest(*, fill_mode: str = "base") -> Dict[str, Any]:
    """Positional daily decisions on reconstructed MCX EOD. fill_mode: base | pessimistic."""
    ensure_tarang_tables()
    profiles = get_profiles().get("profiles") or {}
    risk = get_risk()
    frac = float((risk.get("paper_fills") or {}).get("spread_fraction") or 0.40)
    db = SessionLocal()
    try:
        all_rows = _load_eod_rows(db)
    finally:
        db.close()
    by_day_exp: Dict[Tuple[Any, Any, date], List[Dict[str, Any]]] = {}
    dates_by_sym: Dict[str, List[date]] = {}
    for r in all_rows:
        td = r["trade_date"]
        if not isinstance(td, date):
            td = date.fromisoformat(str(td)[:10])
            r["trade_date"] = td
        exp = r["expiry_date"]
        if not isinstance(exp, date):
            exp = date.fromisoformat(str(exp)[:10])
            r["expiry_date"] = exp
        key = (str(r["symbol"]).upper(), exp, td)
        by_day_exp.setdefault(key, []).append(r)
        dates_by_sym.setdefault(str(r["symbol"]).upper(), []).append(td)

    paper_trades: List[Dict[str, Any]] = []
    cycles = set()
    pid_by_und = {}
    for pid, prof in profiles.items():
        if not prof.get("enabled"):
            continue
        if prof.get("venue") != "upstox_mcx":
            continue
        pid_by_und[str(prof.get("underlying_symbol") or "").upper()] = (pid, prof)

    for und, (pid, prof) in pid_by_und.items():
        d_min = float(prof.get("short_delta_min") or 0.10)
        d_max = float(prof.get("short_delta_max") or 0.16)
        width_steps = int(prof.get("width_steps_min") or 2)
        min_dte = int(prof.get("expiry_min_dte") or prof.get("expiry_dte_min") or 7)
        max_dte = int(prof.get("expiry_dte_max") or 35)
        dates = sorted(set(dates_by_sym.get(und, [])))
        expiries = sorted({k[1] for k in by_day_exp if k[0] == und})
        for exp in expiries:
            cycles.add(f"{und}|{exp}")
            open_trade = None
            for i, td in enumerate(dates):
                slice_rows = by_day_exp.get((und, exp, td)) or []
                dte = _dte(exp, td)
                chain = _chain_for_day(slice_rows, profile_id=pid)
                if open_trade is None:
                    if dte < min_dte or dte > max_dte:
                        continue
                    if not chain:
                        continue
                    built = build_structure(chain, "iron_condor", d_min, d_max, width_steps)
                    if not built.get("ok"):
                        continue
                    fill_chain = chain
                    if fill_mode == "pessimistic" and i + 1 < len(dates):
                        nxt = dates[i + 1]
                        nxt_rows = by_day_exp.get((und, exp, nxt)) or []
                        nxt_chain = _chain_for_day(nxt_rows, profile_id=pid, require_traded=False)
                        if nxt_chain:
                            fill_chain = nxt_chain
                    for lg in built["legs"]:
                        mq = next(
                            (q for q in fill_chain.quotes if q.right == lg["right"] and abs(q.strike - lg["strike"]) < 1e-6),
                            None,
                        )
                        if mq:
                            lg["bid"], lg["ask"], lg["mid"] = mq.bid, mq.ask, mq.mid
                    credit = _credit_from_legs(built["legs"], spread_fraction=frac)
                    if credit is None or credit <= 0:
                        continue
                    built["net_credit"] = credit
                    ml = max_loss_per_unit_inr(
                        float(built["width"]),
                        float(credit),
                        lot_size=built.get("lot_size") or chain.lot_size,
                        venue="upstox_mcx",
                    )
                    budget = float(((risk.get("buckets") or {}).get("ENERGY") or {}).get("per_trade_budget_inr") or 0)
                    units = floor_units(budget, ml)
                    if units < 1:
                        continue
                    fg = gate_fee_and_limits(
                        venue="upstox_mcx",
                        legs=built["legs"],
                        net_credit_pts=credit,
                        units=units,
                        lot_size=built.get("lot_size") or chain.lot_size,
                        contract_value=None,
                        underlying_price=chain.futures_or_spot,
                    )
                    if not fg["gate"].passed:
                        continue
                    open_trade = {
                        "profile_id": pid,
                        "expiry": str(exp),
                        "entry_date": td.isoformat(),
                        "entry_credit": credit,
                        "legs": built["legs"],
                        "units": units,
                        "max_loss": ml,
                        "budget_inr": budget,
                        "venue": "upstox_mcx",
                        "holding_mode": "POSITIONAL",
                        "fill_mode": fill_mode,
                        "fills_label": "MCX EOD-reconstructed, modelled fills",
                        "atm_iv": None,
                    }
                    continue
                if not chain:
                    continue
                debit = _debit_from_chain(
                    chain,
                    open_trade["legs"],
                    spread_fraction=frac,
                    pessimistic_hl=(fill_mode == "pessimistic"),
                )
                if debit is None:
                    continue
                now = datetime.combine(td, datetime.min.time()).replace(tzinfo=timezone.utc)
                ev = evaluate_exits(
                    entry_credit_pts=float(open_trade["entry_credit"]),
                    debit_to_close_pts=float(debit),
                    unrealized_pnl_inr=0.0,
                    max_profit_inr=None,
                    max_loss_inr=open_trade.get("max_loss"),
                    budget_inr=open_trade.get("budget_inr"),
                    short_deltas=[],
                    entry_atm_iv=None,
                    current_atm_iv=None,
                    venue="upstox_mcx",
                    profile_id=pid,
                    holding_mode="POSITIONAL",
                    expiry=open_trade["expiry"],
                    now=now,
                )
                if ev.should_exit:
                    open_trade["exit_date"] = td.isoformat()
                    open_trade["exit_reason"] = ev.reason
                    open_trade["origin"] = "BACKTEST_EOD"
                    paper_trades.append(open_trade)
                    open_trade = None
            if open_trade:
                open_trade["exit_reason"] = "OPEN"
                open_trade["origin"] = "BACKTEST_EOD"
                paper_trades.append(open_trade)

    n = len(paper_trades)
    return {
        "product": "Kosmic Tarang",
        "source": "mcx_eod",
        "label": "MCX EOD-reconstructed, modelled fills",
        "fill_mode": fill_mode,
        "intraday_note": INTRADAY_EOD_MSG,
        "expiry_cycles": len(cycles),
        "trades": paper_trades,
        "metrics": {"count": n, "expiry_cycles": len(cycles)},
        "show_go_live_gate": n >= GO_LIVE_GATE_MIN_TRADES,
        "go_live_gate_hidden_until_trades": GO_LIVE_GATE_MIN_TRADES,
        "note": (
            "EOD positional replay uses Screener structure rules, SizingService floor_units, fee gate 15% of credit, "
            "and ExitEngine. Base fills: signal and fill at day-t close with modelled bid-ask haircut. "
            "Pessimistic: fill at t+1 close; stops use short-leg High / long-leg Low. "
            + INTRADAY_EOD_MSG
        ),
    }


def run_backtest(*, asof: Optional[datetime] = None, source: str = "snapshots", fill_mode: str = "base") -> Dict[str, Any]:
    """Replay available wide snapshots, or MCX EOD reconstruction when source=eod."""
    if str(source).lower() in ("eod", "mcx_eod", "bhavcopy"):
        return run_eod_backtest(fill_mode=fill_mode)
    ensure_tarang_tables()
    cov = chain_coverage()
    wide_days = max((u.get("days_wide") or 0) for u in (cov.get("underlyings") or [])) if cov.get("underlyings") else 0
    timeline = cov.get("underlyings") or []
    insufficient = wide_days < MIN_WIDE_DAYS_FOR_RUN
    out: Dict[str, Any] = {
        "product": "Kosmic Tarang",
        "source": "snapshots",
        "label": "Live snapshot replay (not EOD)",
        "show_go_live_gate": False,
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "coverage": cov,
        "wide_days": wide_days,
        "min_days_required": MIN_WIDE_DAYS_FOR_RUN,
        "insufficient_data": insufficient,
        "trades": [],
        "metrics": {"count": 0},
        "data_gaps": [],
        "note": (
            "Replay uses Screener structure rules, SizingService floor_units, and ExitEngine. "
            "narrow_window (ATM±10) rows are excluded from short/wing validation. "
            "data_gap periods are skipped."
        ),
    }
    if insufficient:
        out["message"] = (
            f"Insufficient data: {wide_days} wide-chain day(s), need {MIN_WIDE_DAYS_FOR_RUN} "
            "to run a meaningful replay. Coverage timeline is below. Phase 4 is held."
        )
        out["timeline"] = timeline
        out["data_gaps"] = recent_gaps(40)
        return out

    db = SessionLocal()
    try:
        snaps = _load_snapshots(db, wide_only=True)
        if not snaps:
            out["insufficient_data"] = True
            out["message"] = "No delta-window snapshots yet."
            out["timeline"] = timeline
            return out
        start = snaps[0]["captured_at"]
        end = snaps[-1]["captured_at"]
        gaps = gaps_overlapping(start, end)
        profiles = get_profiles().get("profiles") or {}
        risk = get_risk()
        # Group by profile+expiry, walk time
        by_key: Dict[str, List[Dict[str, Any]]] = {}
        for s in snaps:
            if snapshot_in_gap(s["captured_at"], gaps):
                continue
            key = f"{s.get('profile_id')}|{s.get('expiry_date')}"
            by_key.setdefault(key, []).append(s)

        paper_trades: List[Dict[str, Any]] = []
        for key, series in by_key.items():
            pid = (series[0].get("profile_id") or "").upper()
            prof = profiles.get(pid) or {}
            if not prof.get("enabled"):
                continue
            d_min = float(prof.get("short_delta_min") or 0.10)
            d_max = float(prof.get("short_delta_max") or 0.16)
            width_steps = int(prof.get("width_steps_min") or 2)
            structure = "iron_condor"
            open_trade = None
            for snap in series:
                chain = _chain_from_snapshot(snap)
                if not chain or not chain.quotes:
                    continue
                if open_trade is None:
                    built = build_structure(chain, structure, d_min, d_max, width_steps)
                    if not built.get("ok"):
                        continue
                    two_sided = all(
                        (lg.get("bid") and lg.get("ask")) for lg in built.get("legs") or []
                    )
                    if not two_sided:
                        continue
                    ml = max_loss_per_unit_inr(
                        float(built["width"]),
                        float(built["net_credit"]),
                        lot_size=built.get("lot_size") or chain.lot_size,
                        contract_value=built.get("contract_value"),
                        venue=chain.venue,
                    )
                    bucket = "CRYPTO" if chain.venue == "delta_india" else "ENERGY"
                    budget = float(((risk.get("buckets") or {}).get(bucket) or {}).get("per_trade_budget_inr") or 0)
                    units = floor_units(budget, ml)
                    if units < 1:
                        continue
                    open_trade = {
                        "profile_id": pid,
                        "expiry": chain.expiry,
                        "entry_at": snap["captured_at"].isoformat() if hasattr(snap["captured_at"], "isoformat") else str(snap["captured_at"]),
                        "entry_credit": built["net_credit"],
                        "legs": built["legs"],
                        "units": units,
                        "max_loss": ml,
                        "budget_inr": budget,
                        "venue": chain.venue,
                        "holding_mode": "POSITIONAL",
                        "atm_iv": snap.get("atm_iv"),
                    }
                    continue
                # Evaluate exit on later snapshot
                debit = 0.0
                short_d = []
                for lg in open_trade["legs"]:
                    match = next(
                        (
                            q
                            for q in chain.quotes
                            if q.right == lg.get("right") and abs(float(q.strike) - float(lg["strike"])) < 1e-6
                        ),
                        None,
                    )
                    if not match or match.bid is None or match.ask is None:
                        debit = None
                        break
                    mid = 0.5 * (float(match.bid) + float(match.ask))
                    if str(lg.get("side")).upper() == "SELL":
                        debit += mid
                        short_d.append(match.delta)
                    else:
                        debit -= mid
                if debit is None:
                    continue
                now = snap["captured_at"]
                if now.tzinfo is None:
                    now = now.replace(tzinfo=timezone.utc)
                ev = evaluate_exits(
                    entry_credit_pts=float(open_trade["entry_credit"]),
                    debit_to_close_pts=float(debit),
                    unrealized_pnl_inr=0.0,
                    max_profit_inr=None,
                    max_loss_inr=open_trade.get("max_loss"),
                    budget_inr=open_trade.get("budget_inr"),
                    short_deltas=short_d,
                    entry_atm_iv=open_trade.get("atm_iv"),
                    current_atm_iv=snap.get("atm_iv"),
                    venue=open_trade["venue"],
                    profile_id=pid,
                    holding_mode="POSITIONAL",
                    expiry=open_trade["expiry"],
                    now=now,
                )
                if ev.should_exit:
                    open_trade["exit_at"] = now.isoformat()
                    open_trade["exit_reason"] = ev.reason
                    open_trade["origin"] = "BACKTEST"
                    paper_trades.append(open_trade)
                    open_trade = None
        out["trades"] = paper_trades
        out["metrics"] = {"count": len(paper_trades)}
        out["timeline"] = timeline
        out["data_gaps"] = gaps
        return out
    finally:
        db.close()
