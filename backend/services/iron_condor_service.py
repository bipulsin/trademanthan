"""
Iron Condor advisory: strike math, hedge validation, sizing, persisted positions/alerts.
Advisory-only — no broker orders.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import engine
from backend.services.iron_condor_universe import sector_for_symbol
from backend.services.upstox_service import upstox_service as vwap_service

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

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


def ensure_iron_condor_tables() -> None:
    if engine is None:
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


def get_or_create_settings(db: Session, user_id: int) -> Dict[str, Any]:
    ensure_iron_condor_tables()
    row = db.execute(
        text("SELECT * FROM iron_condor_user_settings WHERE user_id = :uid LIMIT 1"),
        {"uid": user_id},
    ).mappings().first()
    if row:
        return dict(row)
    db.execute(
        text(
            """
            INSERT INTO iron_condor_user_settings (user_id, trading_capital, max_simultaneous_positions, target_position_slots)
            VALUES (:uid, 0, 3, 5)
            """
        ),
        {"uid": user_id},
    )
    db.commit()
    row2 = db.execute(
        text("SELECT * FROM iron_condor_user_settings WHERE user_id = :uid LIMIT 1"),
        {"uid": user_id},
    ).mappings().first()
    return dict(row2 or {})


def update_settings(db: Session, user_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
    get_or_create_settings(db, user_id)
    patches = []
    params: Dict[str, Any] = {"uid": user_id}
    if "trading_capital" in body:
        patches.append("trading_capital = :tc")
        params["tc"] = float(body["trading_capital"])
    if "max_simultaneous_positions" in body:
        patches.append("max_simultaneous_positions = :mx")
        params["mx"] = max(1, min(12, int(body["max_simultaneous_positions"])))
    if "target_position_slots" in body:
        patches.append("target_position_slots = :ts")
        params["ts"] = max(1, min(10, int(body["target_position_slots"])))
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

    spot = vwap_service.get_candle_vwap_by_instrument_key(eq_key, interval="days/1", days_back=3)
    if spot is None or spot <= 0:
        ohlc = vwap_service.get_ohlc_data(eq_key)
        if isinstance(ohlc, dict):
            spot = float(ohlc.get("last_price") or ohlc.get("close") or 0)
        if spot is None or spot <= 0:
            mq = vwap_service.get_market_quote(api_sym)
            if isinstance(mq, dict) and mq.get("last_price"):
                spot = float(mq["last_price"])
    if spot is None or spot <= 0:
        raise RuntimeError(f"Could not resolve spot for {api_sym}")

    months = vwap_service.get_monthly_candles_by_instrument_key(eq_key, months_back=48) or []
    monthly_atr = _monthly_atr_wilder_14(months)
    if monthly_atr is None or monthly_atr <= 0:
        raise RuntimeError("Insufficient monthly candles for ATR(14).")

    strike_distance = 1.25 * float(monthly_atr)
    step = float(vwap_service.calculate_strike_interval(float(spot)))

    chain_wrapped = vwap_service.get_option_chain(api_sym)
    if not chain_wrapped:
        raise RuntimeError("Option chain request failed (no data).")
    payload = chain_wrapped
    if isinstance(chain_wrapped, dict) and chain_wrapped.get("status") == "success":
        payload = chain_wrapped.get("data") or chain_wrapped
    strike_list = _extract_chain_strike_list(payload if isinstance(payload, dict) else {})
    if not strike_list:
        raise RuntimeError("Option chain unavailable or empty.")

    agg = _build_strike_aggregate(strike_list)
    strikes_sorted = sorted(agg.keys())
    if len(strikes_sorted) < 4:
        raise RuntimeError("Not enough strikes on chain.")

    sell_call_thr = float(spot) + strike_distance
    sell_put_thr = float(spot) - strike_distance
    sell_ce = _nearest_strike_ge(strikes_sorted, sell_call_thr)
    sell_pe = _nearest_strike_le(strikes_sorted, sell_put_thr)
    if sell_ce is None or sell_pe is None:
        raise RuntimeError("Could not place short strikes relative to strike distance.")

    wing_ce = _pick_buy_wing(strikes_sorted, agg, sell_ce, step, long_call=True)
    wing_pe = _pick_buy_wing(strikes_sorted, agg, sell_pe, step, long_call=False)
    if wing_ce is None or wing_pe is None:
        raise RuntimeError("Could not resolve long wings (5–6 strikes away).")

    buy_ce_strike, prem_buy_ce, _, wcall = wing_ce
    buy_pe_strike, prem_buy_pe, _, wput = wing_pe
    strike_warnings = list(dict.fromkeys([x for x in (wcall + wput) if x]))

    sr = agg[sell_ce]
    pr = agg[sell_pe]
    prem_sell_ce = sr["ce_ltp"]
    prem_sell_pe = pr["pe_ltp"]

    prem_collected = prem_sell_ce + prem_sell_pe
    hedge_cost = prem_buy_ce + prem_buy_pe
    hedge_ratio = (hedge_cost / prem_collected) if prem_collected > 0 else 0.0
    gate, gate_msg = classify_hedge_ratio(hedge_ratio)

    exp = vwap_service.get_monthly_expiry()

    expiry_note = (
        "Cycle: hold from day 1 of new monthly expiry until profit target / stop loss / 10 days before expiry (advisory)."
    )

    return {
        "underlying": und,
        "sector": sec,
        "option_chain_symbol": api_sym,
        "spot": round(float(spot), 4),
        "monthly_atr_14": round(monthly_atr, 4),
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
    chain_wrapped = vwap_service.get_option_chain(api_sym)
    payload = chain_wrapped
    if isinstance(chain_wrapped, dict) and chain_wrapped.get("status"):
        payload = chain_wrapped.get("data") or chain_wrapped
    strike_list = _extract_chain_strike_list(payload if isinstance(payload, dict) else {})
    if not strike_list:
        return None
    agg = _build_strike_aggregate(strike_list)
    sce = agg.get(float(strikes_pack["sell_call"]))
    bce = agg.get(float(strikes_pack["buy_call"]))
    spe = agg.get(float(strikes_pack["sell_put"]))
    bpe = agg.get(float(strikes_pack["buy_put"]))
    if not all((sce, bce, spe, bpe)):
        return None
    return {
        "sc_ltp": sce["ce_ltp"],
        "bc_ltp": bce["ce_ltp"],
        "sp_ltp": spe["pe_ltp"],
        "bp_ltp": bpe["pe_ltp"],
    }


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
