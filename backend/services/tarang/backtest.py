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
IV_WARMUP_DAYS = 60
INTRADAY_EOD_MSG = "INTRADAY cannot be tested on EOD — this path is positional daily decisions only."


def _as_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def ist_today() -> date:
    from backend.services.tarang.calendar import to_ist

    return to_ist().date()


def cycle_is_expired(expiry: date, today: Optional[date] = None) -> bool:
    """Independent cycle: option expiry is strictly before today IST."""
    t = today or ist_today()
    return _as_date(expiry) is not None and _as_date(expiry) < t


def _dte(expiry: date, today: date) -> int:
    return (expiry - today).days


def mcx_entry_dte_ok(dte: int, min_dte: int = 7, max_dte: int = 35) -> bool:
    return min_dte <= int(dte) <= max_dte


def iv_history_warmup(n_iv_days: int, min_days: int = IV_WARMUP_DAYS) -> bool:
    return int(n_iv_days) < int(min_days)


def realized_vol_from_closes(closes: List[float], *, min_returns: int = 10, window: int = 20) -> Optional[float]:
    import math

    if len(closes) < min_returns + 1:
        return None
    seq = [c for c in closes if c is not None and float(c) > 0]
    seq = seq[-(window + 1) :]
    rets = []
    for i in range(1, len(seq)):
        if seq[i - 1] > 0 and seq[i] > 0:
            rets.append(math.log(float(seq[i]) / float(seq[i - 1])))
    if len(rets) < min_returns:
        return None
    mean = sum(rets) / len(rets)
    var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1) if len(rets) > 1 else 0.0
    return (var * 252) ** 0.5


def eod_iv_gate(
    atm_iv: Optional[float],
    rv: Optional[float],
    iv_history: List[float],
    *,
    min_percentile: float = 50.0,
    min_snapshots: int = IV_WARMUP_DAYS,
    relative_min: float = 0.10,
) -> Dict[str, Any]:
    """Warm-up: IV-vs-RV only. After ≥min_snapshots IV days, percentile + IV-vs-RV as live."""
    from backend.services.tarang.gates import compute_iv_percentile, gate_iv_percentile, gate_iv_vs_rv

    warmup = iv_history_warmup(len(iv_history), min_snapshots)
    rv_g = gate_iv_vs_rv(atm_iv, rv, relative_min=relative_min)
    if warmup:
        return {
            "passed": rv_g.passed,
            "warmup": True,
            "mode": "warmup_iv_vs_rv",
            "detail": rv_g.detail,
            "gate": "iv_gate" if not rv_g.passed else None,
        }
    pct = compute_iv_percentile(iv_history, atm_iv)
    pct_g = gate_iv_percentile(pct, len(iv_history), min_percentile=min_percentile, min_snapshots=min_snapshots)
    if not pct_g.passed:
        return {
            "passed": False,
            "warmup": False,
            "mode": "percentile",
            "detail": pct_g.detail,
            "gate": "iv_gate",
        }
    return {
        "passed": rv_g.passed,
        "warmup": False,
        "mode": "full",
        "detail": rv_g.detail,
        "gate": "iv_gate" if not rv_g.passed else None,
    }


def map_structure_error(err: Optional[str]) -> Optional[str]:
    if not err:
        return None
    e = str(err)
    if "short" in e:
        return "liquidity"
    if "long" in e:
        return "traded_wings"
    if "strike_step" in e or "mids" in e:
        return "data"
    return "liquidity"


def first_entry_reject(
    *,
    has_underlying: bool,
    n_quotes: int,
    dte: int,
    min_dte: int = 7,
    max_dte: int = 35,
    iv_passed: Optional[bool] = None,
    structure_error: Optional[str] = None,
    credit: Optional[float] = None,
    width: Optional[float] = None,
    min_credit_frac: float = 0.20,
    units: int = 0,
    fee_ok: Optional[bool] = None,
) -> Optional[str]:
    """First rejecting gate, in the documented order. None = accepted."""
    if not has_underlying or int(n_quotes) <= 0:
        return "data"
    if not mcx_entry_dte_ok(dte, min_dte, max_dte):
        return "dte_window"
    if iv_passed is False:
        return "iv_gate"
    mapped = map_structure_error(structure_error)
    if mapped:
        return mapped
    if credit is None or credit <= 0:
        return "min_credit"
    if width is not None and width > 0 and (credit / width) < min_credit_frac:
        return "min_credit"
    if int(units) < 1:
        return "budget"
    if fee_ok is False:
        return "fees"
    return None


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
            "underlying_estimated": bool(row.get("underlying_estimated")),
            "underlying_fut_expiry": str(row.get("underlying_fut_expiry") or "")[:10] or None,
            "underlying_fut_symbol": row.get("underlying_fut_symbol"),
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
               iv, delta, greeks_source, underlying_price, underlying_source,
               underlying_fut_symbol, underlying_fut_expiry, underlying_estimated
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
    F = next((float(r["underlying_price"]) for r in opts if r.get("underlying_price")), None)
    if F is None:
        fut = next((r for r in rows if r.get("option_type") == "FUT" and r.get("close")), None)
        if fut:
            F = float(fut["close"])
    step = _strike_step([q.strike for q in quotes])
    und = str(opts[0]["symbol"])
    estimated = any(bool(r.get("underlying_estimated")) for r in opts)
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
        meta={
            "modelled_fills": True,
            "source": "mcx_eod",
            "estimated_underlying": estimated,
            "underlying_fut_expiry": str(opts[0].get("underlying_fut_expiry") or "")[:10] or None,
            "underlying_fut_symbol": opts[0].get("underlying_fut_symbol"),
            "underlying_source": opts[0].get("underlying_source"),
        },
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


def _atm_iv(rows: List[Dict[str, Any]]) -> Optional[float]:
    ivs = [float(r["iv"]) for r in rows if r.get("iv") is not None]
    if not ivs:
        return None
    ivs.sort()
    return ivs[len(ivs) // 2]


def _run_eod_once(
    *,
    fill_mode: str,
    all_rows: List[Dict[str, Any]],
    today: date,
    record_rejections: bool,
) -> Dict[str, Any]:
    from collections import Counter

    profiles = get_profiles().get("profiles") or {}
    risk = get_risk()
    frac = float((risk.get("paper_fills") or {}).get("spread_fraction") or 0.40)
    min_credit_frac = float(risk.get("min_credit_fraction_of_width") or 0.20)
    min_pct = float(risk.get("iv_percentile_min") or 50)
    min_snaps = int(risk.get("iv_percentile_min_snapshots") or IV_WARMUP_DAYS)
    rel_min = float(risk.get("iv_vs_rv_relative_min") or 0.10)

    by_day_exp: Dict[Tuple[str, date, date], List[Dict[str, Any]]] = {}
    dates_by_sym: Dict[str, List[date]] = {}
    F_by_sym_date: Dict[Tuple[str, date], float] = {}
    iv_by_sym_date: Dict[Tuple[str, date], float] = {}
    for r in all_rows:
        td = _as_date(r["trade_date"])
        exp = _as_date(r["expiry_date"])
        r["trade_date"] = td
        r["expiry_date"] = exp
        if td is None or exp is None:
            continue
        und = str(r["symbol"]).upper()
        by_day_exp.setdefault((und, exp, td), []).append(r)
        dates_by_sym.setdefault(und, []).append(td)
        if r.get("underlying_price"):
            F_by_sym_date.setdefault((und, td), float(r["underlying_price"]))
        elif r.get("option_type") == "FUT" and r.get("close"):
            F_by_sym_date.setdefault((und, td), float(r["close"]))
        if r.get("iv") is not None:
            iv_by_sym_date.setdefault((und, td), float(r["iv"]))

    # median IV per und+date from collected first-write; recompute properly
    iv_lists: Dict[Tuple[str, date], List[float]] = {}
    for r in all_rows:
        if r.get("iv") is None:
            continue
        und = str(r["symbol"]).upper()
        td = r["trade_date"]
        if isinstance(td, date):
            iv_lists.setdefault((und, td), []).append(float(r["iv"]))
    iv_by_sym_date = {k: sorted(v)[len(v) // 2] for k, v in iv_lists.items()}

    paper_trades: List[Dict[str, Any]] = []
    independent: List[Dict[str, Any]] = []
    open_cycles: List[Dict[str, Any]] = []
    rejections: List[Dict[str, Any]] = []
    pid_by_und = {}
    for pid, prof in profiles.items():
        if not prof.get("enabled") or prof.get("venue") != "upstox_mcx":
            continue
        pid_by_und[str(prof.get("underlying_symbol") or "").upper()] = (pid, prof)

    for und, (pid, prof) in pid_by_und.items():
        d_min = float(prof.get("short_delta_min") or 0.10)
        d_max = float(prof.get("short_delta_max") or 0.16)
        width_steps = int(prof.get("width_steps_min") or 2)
        min_dte = int(prof.get("expiry_min_dte") or prof.get("expiry_dte_min") or 7)
        max_dte = int(prof.get("expiry_dte_max") or 35)
        dates = sorted(set(dates_by_sym.get(und, [])))
        F_series_dates = sorted({td for (s, td) in F_by_sym_date if s == und})
        expiries = sorted({k[1] for k in by_day_exp if k[0] == und})
        for exp in expiries:
            label = {"symbol": und, "expiry": exp.isoformat()}
            if not cycle_is_expired(exp, today):
                open_cycles.append({**label, "status": "open", "excluded": True})
                continue
            independent.append({**label, "status": "expired"})
            open_trade = None
            for i, td in enumerate(dates):
                slice_rows = by_day_exp.get((und, exp, td)) or []
                dte = _dte(exp, td)
                chain = _chain_for_day(slice_rows, profile_id=pid)
                if open_trade is not None:
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
                        entry_atm_iv=open_trade.get("atm_iv"),
                        current_atm_iv=_atm_iv(slice_rows),
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
                    continue

                opts = [r for r in slice_rows if r.get("option_type") in ("CE", "PE")]
                F = None
                if chain and chain.futures_or_spot:
                    F = chain.futures_or_spot
                elif slice_rows:
                    F = next((float(r["underlying_price"]) for r in slice_rows if r.get("underlying_price")), None)
                n_quotes = len([r for r in opts if r.get("traded")])
                has_und = F is not None

                prior_F = [F_by_sym_date[(und, d)] for d in F_series_dates if d <= td]
                rv = realized_vol_from_closes(prior_F)
                prior_iv = [iv_by_sym_date[(und, d)] for d in dates if d < td and (und, d) in iv_by_sym_date]
                atm = _atm_iv(opts)
                iv_res = eod_iv_gate(
                    atm,
                    rv,
                    prior_iv,
                    min_percentile=min_pct,
                    min_snapshots=min_snaps,
                    relative_min=rel_min,
                )

                structure_error = None
                built = None
                credit = None
                width = None
                units = 0
                fee_ok = None
                if has_und and n_quotes > 0 and mcx_entry_dte_ok(dte, min_dte, max_dte) and iv_res["passed"] and chain:
                    built = build_structure(chain, "iron_condor", d_min, d_max, width_steps)
                    if not built.get("ok"):
                        structure_error = str(built.get("error") or "structure")
                    else:
                        fill_chain = chain
                        if fill_mode == "pessimistic" and i + 1 < len(dates):
                            nxt_rows = by_day_exp.get((und, exp, dates[i + 1])) or []
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
                        width = float(built.get("width") or 0)
                        ml = max_loss_per_unit_inr(
                            width,
                            float(credit or 0),
                            lot_size=built.get("lot_size") or chain.lot_size,
                            venue="upstox_mcx",
                        )
                        budget = float(((risk.get("buckets") or {}).get("ENERGY") or {}).get("per_trade_budget_inr") or 0)
                        units = floor_units(budget, ml) if credit and credit > 0 else 0
                        if units >= 1 and credit and credit > 0:
                            fg = gate_fee_and_limits(
                                venue="upstox_mcx",
                                legs=built["legs"],
                                net_credit_pts=credit,
                                units=units,
                                lot_size=built.get("lot_size") or chain.lot_size,
                                contract_value=None,
                                underlying_price=chain.futures_or_spot,
                            )
                            fee_ok = bool(fg["gate"].passed)
                        elif credit and credit > 0:
                            fee_ok = None

                gate = first_entry_reject(
                    has_underlying=has_und,
                    n_quotes=n_quotes,
                    dte=dte,
                    min_dte=min_dte,
                    max_dte=max_dte,
                    iv_passed=iv_res["passed"] if has_und and n_quotes > 0 and mcx_entry_dte_ok(dte, min_dte, max_dte) else None,
                    structure_error=structure_error,
                    credit=credit,
                    width=width,
                    min_credit_frac=min_credit_frac,
                    units=units,
                    fee_ok=fee_ok,
                )
                # first_entry_reject treats iv_passed None as skip IV. Force iv after data+dte:
                if gate is None and not (built and built.get("ok") and units >= 1 and fee_ok):
                    gate = "data"

                if gate:
                    if record_rejections:
                        rejections.append(
                            {
                                "trade_date": td.isoformat(),
                                "symbol": und,
                                "expiry": exp.isoformat(),
                                "gate": gate,
                                "dte": dte,
                                "detail": iv_res.get("detail") if gate == "iv_gate" else (structure_error or gate),
                                "warmup_iv": iv_res.get("warmup"),
                            }
                        )
                    continue

                estimated = bool((chain.meta or {}).get("estimated_underlying")) if chain else True
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
                    "estimated_underlying": estimated,
                    "warmup_mode": bool(iv_res.get("warmup")),
                    "atm_iv": atm,
                    "underlying_fut_expiry": (chain.meta or {}).get("underlying_fut_expiry") if chain else None,
                    "underlying_source": (chain.meta or {}).get("underlying_source") if chain else None,
                }
            if open_trade:
                open_trade["exit_reason"] = "OPEN"
                open_trade["origin"] = "BACKTEST_EOD"
                paper_trades.append(open_trade)

    counts = Counter(r["gate"] for r in rejections)
    return {
        "trades": paper_trades,
        "independent_cycles": independent,
        "open_cycles_excluded": open_cycles,
        "rejections": rejections,
        "rejection_summary": dict(counts),
        "metrics": {
            "count": len(paper_trades),
            "independent_cycles": len(independent),
            "open_cycles_excluded": len(open_cycles),
        },
    }


def run_eod_backtest(*, fill_mode: str = "both", today: Optional[date] = None) -> Dict[str, Any]:
    """Positional daily decisions on reconstructed MCX EOD. Independent cycles = expired only."""
    ensure_tarang_tables()
    today = today or ist_today()
    db = SessionLocal()
    try:
        all_rows = _load_eod_rows(db)
    finally:
        db.close()

    modes = ["base", "pessimistic"] if str(fill_mode).lower() in ("both", "all") else [fill_mode]
    by_mode = {}
    shared_rej = None
    for m in modes:
        once = _run_eod_once(fill_mode=m, all_rows=all_rows, today=today, record_rejections=(shared_rej is None))
        by_mode[m] = once
        if shared_rej is None:
            shared_rej = once

    base = by_mode.get("base") or by_mode[modes[0]]
    pess = by_mode.get("pessimistic")
    n = int(base["metrics"]["count"])
    n_pess = int(pess["metrics"]["count"]) if pess else None
    return {
        "product": "Kosmic Tarang",
        "source": "mcx_eod",
        "label": "MCX EOD-reconstructed, modelled fills",
        "fill_mode": fill_mode,
        "asof_ist": today.isoformat(),
        "intraday_note": INTRADAY_EOD_MSG,
        "parity_formula": "F ≈ K + (C − P)  (Black-76 with DF=1, undiscounted)",
        "independent_cycles": base["independent_cycles"],
        "open_cycles_excluded": base["open_cycles_excluded"],
        "expiry_cycles": len(base["independent_cycles"]),
        "trades": base["trades"],
        "trades_pessimistic": (pess or {}).get("trades") if pess else [],
        "rejection_log": (shared_rej or base)["rejections"],
        "rejection_summary": (shared_rej or base)["rejection_summary"],
        "metrics": {
            "count": n,
            "count_base": n,
            "count_pessimistic": n_pess,
            "independent_cycles": len(base["independent_cycles"]),
            "open_cycles_excluded": len(base["open_cycles_excluded"]),
            "estimated_underlying_trades": sum(1 for t in base["trades"] if t.get("estimated_underlying")),
            "warmup_mode_trades": sum(1 for t in base["trades"] if t.get("warmup_mode")),
        },
        "show_go_live_gate": n >= GO_LIVE_GATE_MIN_TRADES,
        "go_live_gate_hidden_until_trades": GO_LIVE_GATE_MIN_TRADES,
        "note": (
            "Independent cycles = expired option contracts only (expiry_date < today IST). "
            "Open contracts (e.g. 15 Oct 2026, 17 Nov 2026 as of 19 Sep 2026) are excluded. "
            "EOD positional replay uses Screener structure rules, SizingService floor_units, fee gate 15% of credit, "
            "and ExitEngine. IV percentile waits for 60 days of IV history; until then IV-vs-RV only (warm-up-mode). "
            "Base fills: signal and fill at day-t close with modelled bid-ask haircut. "
            "Pessimistic: fill at t+1 close; stops use short-leg High / long-leg Low. "
            + INTRADAY_EOD_MSG
        ),
    }


def run_backtest(*, asof: Optional[datetime] = None, source: str = "snapshots", fill_mode: str = "both") -> Dict[str, Any]:
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
