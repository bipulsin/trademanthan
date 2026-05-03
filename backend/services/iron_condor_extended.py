"""
Iron Condor v2 flow: migrations, detailed analysis economics, confirm entry, polling & alerts.
Advisory-only.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import engine
from backend.services.upstox_service import upstox_service as vwap_service
from backend.services import iron_condor_service as ic
from backend.services.iron_condor_checklist import fetch_india_vix
from backend.services import market_holiday as mh

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

LAST_POLL_CACHE: Dict[str, Any] = {"ts": 0.0, "spot_by_sym": {}, "ttl": 60.0}

SEVERITY_RANK = {
    "CRITICAL_RED": 0,
    "RED": 1,
    "ORANGE": 2,
    "YELLOW": 3,
    "GREEN": 4,
    "BLUE": 5,
    "INFO": 6,
    "WARN": 7,
}


def _severity_rank(sev: Optional[str]) -> int:
    if not sev:
        return 99
    return SEVERITY_RANK.get(str(sev).strip().upper(), 15)


def merge_positions_peak_alert_severity(
    positions: List[Dict[str, Any]], alerts: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Attach card_peak_severity (worst unacked per position) for UI border color."""
    by_pid: Dict[int, List[str]] = {}
    for a in alerts or []:
        if a.get("acknowledged"):
            continue
        pid = a.get("position_id")
        if pid is None:
            continue
        try:
            pi = int(pid)
        except (TypeError, ValueError):
            continue
        by_pid.setdefault(pi, []).append(str(a.get("severity") or "INFO"))
    out: List[Dict[str, Any]] = []
    for p in positions:
        d = dict(p)
        sevs = by_pid.get(int(d.get("id") or 0), [])
        if not sevs:
            d["card_peak_severity"] = None
            out.append(d)
            continue
        best = sorted(sevs, key=_severity_rank)[0]
        d["card_peak_severity"] = best
        out.append(d)
    return out


def equity_curve_realized(db: Session, user_id: int, limit_months: int = 36) -> List[Dict[str, Any]]:
    """Cumulative realized P&amp;L from closed journal rows by calendar month."""
    iron_condor_migrations_v2()
    lim = max(1, min(limit_months, 120))
    rows = db.execute(
        text(
            """
            SELECT * FROM (
              SELECT date_trunc('month', exit_date)::date AS m,
                     COALESCE(SUM(realized_pnl_rupees), 0)::float AS s
              FROM iron_condor_trade_journal
              WHERE user_id = :uid AND realized_pnl_rupees IS NOT NULL
              GROUP BY 1
              ORDER BY 1 DESC
              LIMIT :lim
            ) q ORDER BY 1 ASC
            """
        ),
        {"uid": user_id, "lim": lim},
    ).fetchall()
    cum = 0.0
    pts: List[Dict[str, Any]] = []
    for m, s in rows or []:
        cum += float(s or 0)
        pts.append({"month": str(m) if m else None, "month_pnl": float(s or 0), "cumulative": round(cum, 2)})
    return pts


def _update_quote_feed_streak(db: Session, user_id: int, cycle_had_any_quote_ok: bool) -> Dict[str, Any]:
    """Track consecutive poll cycles with no successful option quote refresh."""
    iron_condor_migrations_v2()
    st = ic.get_or_create_settings(db, user_id)
    streak = int(st.get("ic_poll_fail_streak") or 0)
    if cycle_had_any_quote_ok:
        streak = 0
        db.execute(
            text(
                """
                UPDATE iron_condor_user_settings SET
                  ic_poll_fail_streak = 0,
                  ic_last_quote_success_at = CURRENT_TIMESTAMP
                WHERE user_id = :u
                """
            ),
            {"u": user_id},
        )
    else:
        streak += 1
        db.execute(
            text("UPDATE iron_condor_user_settings SET ic_poll_fail_streak = :s WHERE user_id = :u"),
            {"s": streak, "u": user_id},
        )
    db.commit()
    st2 = ic.get_or_create_settings(db, user_id)
    last_ok = st2.get("ic_last_quote_success_at")
    last_ok_s = last_ok.isoformat() if hasattr(last_ok, "isoformat") else (str(last_ok) if last_ok else None)
    return {
        "poll_fail_streak": streak,
        "last_quote_success_at": last_ok_s,
        "data_feed_lost": streak >= 3,
    }


def mark_positions_verified_for_session(db: Session, user_id: int) -> None:
    iron_condor_migrations_v2()
    db.execute(
        text(
            """
            UPDATE iron_condor_user_settings
            SET ic_position_verify_date = CAST(:d AS DATE)
            WHERE user_id = :u
            """
        ),
        {"u": user_id, "d": str(datetime.now(IST).date())},
    )
    db.commit()


def session_position_verify_needed(db: Session, user_id: int) -> bool:
    """True on first market window of the day when user still has open condors."""
    iron_condor_migrations_v2()
    if not is_iron_condor_poll_window_ist():
        return False
    n = db.execute(
        text(
            """
            SELECT COUNT(*) FROM iron_condor_position
            WHERE user_id=:u AND UPPER(status) IN ('ACTIVE','OPEN','ADJUSTED')
            """
        ),
        {"u": user_id},
    ).scalar()
    if not int(n or 0):
        return False
    st = ic.get_or_create_settings(db, user_id)
    vd = st.get("ic_position_verify_date")
    today = datetime.now(IST).date()
    if hasattr(vd, "date"):
        vd = vd.date()
    if vd is None:
        return True
    return vd != today


def log_adjustment_iron_condor(db: Session, user_id: int, position_id: int, payload: Dict[str, Any]) -> bool:
    """After manual roll: new strikes + marks; recompute thresholds (advisory)."""
    iron_condor_migrations_v2()
    row = db.execute(
        text("SELECT * FROM iron_condor_position WHERE id=:p AND user_id=:u LIMIT 1"),
        {"p": position_id, "u": user_id},
    ).mappings().first()
    if not row:
        return False
    r = dict(row)
    if str(r.get("status") or "").upper() not in ("ACTIVE", "OPEN", "ADJUSTED"):
        return False

    st_in = payload.get("strikes") or {}
    fl = payload.get("fills") or {}
    need_st = {"sell_call", "buy_call", "sell_put", "buy_put"}
    need_fl = {"sell_call_fill", "buy_call_fill", "sell_put_fill", "buy_put_fill"}
    if not need_st.issubset(st_in) or not need_fl.issubset(fl):
        raise ValueError("strikes and fills must include all four legs.")

    st = {k: float(st_in[k]) for k in need_st}
    scf = float(fl["sell_call_fill"])
    bpf = float(fl["buy_call_fill"])
    spf = float(fl["sell_put_fill"])
    bpgf = float(fl["buy_put_fill"])

    prem_c = scf + spf
    hedge_c = bpf + bpgf
    net_pts = prem_c - hedge_c
    w_ce = float(st["buy_call"]) - float(st["sell_call"])
    w_pe = float(st["sell_put"]) - float(st["buy_put"])
    spread_width = w_ce + w_pe
    max_loss_pts = max(0.0, spread_width - net_pts)
    lot = int(r.get("lot_size") or 1)
    nlot = int(r.get("num_lots") or 1)
    qty_mult = float(lot * nlot)
    max_loss_rupees = round(max_loss_pts * qty_mult, 2)
    max_profit_rupees = round(max(0.0, net_pts) * qty_mult, 2)
    stop_c = round(2.0 * scf, 4)
    stop_p = round(2.0 * spf, 4)
    adj_c = round(1.5 * scf, 4)
    adj_p = round(1.5 * spf, 4)
    profit_tgt = round(0.5 * max_profit_rupees, 2)
    be_u = float(st["sell_call"]) + net_pts
    be_l = float(st["sell_put"]) - net_pts
    hedge_ratio = (hedge_c / prem_c) if prem_c > 0 else 0.0
    gate, _ = ic.classify_hedge_ratio(hedge_ratio)

    hist = r.get("adjustments_history")
    if isinstance(hist, str):
        try:
            hist = json.loads(hist)
        except Exception:
            hist = []
    if not isinstance(hist, list):
        hist = []
    hist.append(
        {
            "at": datetime.now(IST).isoformat(),
            "notes": payload.get("notes"),
            "strikes": st,
            "fills": fl,
        }
    )

    db.execute(
        text(
            """
            UPDATE iron_condor_position SET
              status = 'ADJUSTED',
              sell_call_strike=:sce, buy_call_strike=:bce, sell_put_strike=:spe, buy_put_strike=:bpe,
              premium_sell_call=:scf, premium_buy_call=:bcf, premium_sell_put=:spf, premium_buy_put=:bpf,
              premium_collected=:pcol, hedge_cost=:hcost, hedge_ratio=:hrat, hedge_gate=:hg,
              sell_call_entry_fill=:scf, buy_call_entry_fill=:bcf, sell_put_entry_fill=:spf, buy_put_entry_fill=:bpf,
              sell_call_current=:scf, buy_call_current=:bcf, sell_put_current=:spf, buy_put_current=:bpf,
              net_credit_pts=:nc, max_profit_rupees=:maxp, max_loss_rupees=:maxl,
              breakeven_lower=:bel, breakeven_upper=:beu,
              stop_sl_call_px=:stc, stop_sl_put_px=:stp, adjust_call_px=:adc, adjust_put_px=:adp,
              profit_target_rupees=:ptgt,
              adjustments_history = CAST(:adj AS JSONB)
            WHERE id=:pid AND user_id=:uid
            """
        ),
        {
            "sce": st["sell_call"],
            "bce": st["buy_call"],
            "spe": st["sell_put"],
            "bpe": st["buy_put"],
            "scf": scf,
            "bcf": bpf,
            "spf": spf,
            "bpf": bpgf,
            "pcol": prem_c,
            "hcost": hedge_c,
            "hrat": hedge_ratio,
            "hg": gate,
            "nc": round(net_pts, 6),
            "maxp": max_profit_rupees,
            "maxl": max_loss_rupees,
            "bel": round(be_l, 4),
            "beu": round(be_u, 4),
            "stc": stop_c,
            "stp": stop_p,
            "adc": adj_c,
            "adp": adj_p,
            "ptgt": profit_tgt,
            "adj": json.dumps(hist),
            "pid": position_id,
            "uid": user_id,
        },
    )
    db.commit()
    return True

ALERT_KEYS = (
    ("ALERT_PROFIT_50", "GREEN"),
    ("ALERT_ADJUST_150", "ORANGE"),
    ("ALERT_STOP_200", "RED"),
    ("ALERT_TOTAL_LOSS_2X", "RED"),
    ("ALERT_HARD_CAPITAL", "CRITICAL_RED"),
    ("ALERT_VIX_SPIKE", "ORANGE"),
    ("ALERT_EXPIRY_10D", "BLUE"),
    ("ALERT_EXPIRY_MONDAY", "ORANGE"),
)


def iron_condor_migrations_v2() -> None:
    if engine is None:
        return
    ddl_macros = """
    CREATE TABLE IF NOT EXISTS iron_condor_macro_calendar (
        id SERIAL PRIMARY KEY,
        event_date DATE NOT NULL,
        event_type VARCHAR(64) NOT NULL,
        description TEXT,
        UNIQUE (event_date, event_type)
    );
    CREATE TABLE IF NOT EXISTS iron_condor_trade_journal (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        position_id INTEGER NOT NULL REFERENCES iron_condor_position(id) ON DELETE CASCADE,
        exit_date DATE NOT NULL,
        exit_reason VARCHAR(64) NOT NULL,
        emotion VARCHAR(32),
        followed_rules BOOLEAN,
        deviation_notes TEXT,
        lesson_learned TEXT,
        exit_snapshots JSONB,
        realized_pnl_rupees NUMERIC(18,2),
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS iron_condor_price_history (
        id BIGSERIAL PRIMARY KEY,
        position_id INTEGER NOT NULL REFERENCES iron_condor_position(id) ON DELETE CASCADE,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        spot NUMERIC(18,4),
        india_vix NUMERIC(18,6),
        sell_call_ltp NUMERIC(18,4),
        buy_call_ltp NUMERIC(18,4),
        sell_put_ltp NUMERIC(18,4),
        buy_put_ltp NUMERIC(18,4),
        mtm_estimate_rupees NUMERIC(18,2),
        extras JSONB
    );
    CREATE INDEX IF NOT EXISTS idx_ic_px_pos_ts ON iron_condor_price_history(position_id, ts DESC);
    """

    alter_alerts = """
    ALTER TABLE iron_condor_alert ADD COLUMN IF NOT EXISTS alert_type VARCHAR(64);
    ALTER TABLE iron_condor_alert ADD COLUMN IF NOT EXISTS severity VARCHAR(24);
    ALTER TABLE iron_condor_alert ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMPTZ;
    UPDATE iron_condor_alert SET severity = CASE
        WHEN severity IS NULL AND UPPER(rule_code) LIKE 'STOP%' THEN 'RED'
        WHEN severity IS NULL THEN 'INFO'
        ELSE severity END WHERE severity IS NULL;
    """

    alter_pos = """
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS entry_date DATE;
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS lot_size INTEGER DEFAULT 1;
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS num_lots INTEGER DEFAULT 1;
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS sell_call_entry_fill NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS buy_call_entry_fill NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS sell_put_entry_fill NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS buy_put_entry_fill NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS sell_call_current NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS buy_call_current NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS sell_put_current NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS buy_put_current NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS net_credit_pts NUMERIC(18,6);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS max_profit_rupees NUMERIC(18,2);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS max_loss_rupees NUMERIC(18,2);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS breakeven_upper NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS breakeven_lower NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS stop_sl_call_px NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS stop_sl_put_px NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS adjust_call_px NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS adjust_put_px NUMERIC(18,4);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS profit_target_rupees NUMERIC(18,2);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS adjustments_history JSONB;
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS realized_pnl_rupees NUMERIC(18,2);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS last_poll_at TIMESTAMPTZ;
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS position_health VARCHAR(32);
    ALTER TABLE iron_condor_position ADD COLUMN IF NOT EXISTS next_earnings_estimate DATE;
    ALTER TABLE iron_condor_user_settings ADD COLUMN IF NOT EXISTS ic_last_vix_alert_level NUMERIC(10,4);
    ALTER TABLE iron_condor_user_settings ADD COLUMN IF NOT EXISTS ic_poll_fail_streak INTEGER DEFAULT 0;
    ALTER TABLE iron_condor_user_settings ADD COLUMN IF NOT EXISTS ic_last_quote_success_at TIMESTAMPTZ;
    ALTER TABLE iron_condor_user_settings ADD COLUMN IF NOT EXISTS ic_position_verify_date DATE;
    """

    seeds = [
        ("2026-02-01", "BUDGET", "Union Budget proximity (approx) — verify dates."),
        ("2026-06-06", "RBI_POLICY", "RBI policy proximity — verify MPC calendar."),
        ("2026-07-04", "RBI_POLICY", "RBI policy proximity — verify MPC calendar."),
        ("2026-03-19", "FOMC", "US FOMC week proximity — verify calendar."),
        ("2026-05-06", "ELECTION_RESULTS", "Major election result proximity — illustrative seed."),
        ("2026-10-07", "FOMC", "US FOMC week proximity — verify calendar."),
    ]

    with engine.begin() as conn:
        for blk in ddl_macros.split(";"):
            s = blk.strip()
            if s:
                conn.execute(text(s))
        for blk in alter_pos.split(";"):
            s = blk.strip()
            if s:
                try:
                    conn.execute(text(s))
                except Exception as e:
                    logger.debug("ic migration alter:%s %s", s[:48], e)
        for blk in alter_alerts.split(";"):
            s = blk.strip()
            if s:
                try:
                    conn.execute(text(s))
                except Exception as e:
                    logger.debug("ic migration alert alter:%s", e)
        for ed, et, dsc in seeds:
            conn.execute(
                text(
                    """
                    INSERT INTO iron_condor_macro_calendar (event_date, event_type, description)
                    VALUES (CAST(:d AS DATE), :t, :x)
                    ON CONFLICT (event_date, event_type) DO NOTHING
                    """
                ),
                {"d": ed, "t": et, "x": dsc},
            )
        try:
            conn.execute(text("UPDATE iron_condor_position SET status='ACTIVE' WHERE UPPER(status)='OPEN'"))
        except Exception:
            pass


def resolve_lot_size(db: Session, underlying: str) -> int:
    try:
        sym = underlying.strip().upper()
        ls = db.execute(
            text(
                """
                SELECT COALESCE(lot_size, 1) FROM master_stock
                WHERE UPPER(TRIM(COALESCE(underlying_symbol, symbol_name, ''))) = :s
                LIMIT 1
                """
            ),
            {"s": sym},
        ).scalar()
        if ls:
            return max(1, int(float(ls)))
    except Exception:
        pass
    return 1


def _cached_spot(sym: str, equity_key: str) -> Optional[float]:
    now = time.monotonic()
    if now - float(LAST_POLL_CACHE["ts"]) < float(LAST_POLL_CACHE["ttl"]):
        v = LAST_POLL_CACHE["spot_by_sym"].get(sym)
        if v is not None:
            return float(v)
    spot = vwap_service.get_market_quote(sym) or {}
    lp = spot.get("last_price")
    if lp:
        LAST_POLL_CACHE["spot_by_sym"][sym] = float(lp)
        LAST_POLL_CACHE["ts"] = now
        return float(lp)
    vw = vwap_service.get_candle_vwap_by_instrument_key(equity_key, interval="days/1", days_back=3)
    if vw:
        LAST_POLL_CACHE["spot_by_sym"][sym] = float(vw)
        LAST_POLL_CACHE["ts"] = now
        return float(vw)
    return None


def enrich_leg(chain_strike_row: Dict[str, Any], ce: bool) -> Dict[str, Any]:
    key = "call_options" if ce else "put_options"
    node = chain_strike_row.get(key)
    option_data = None
    ik = None
    if isinstance(node, dict):
        ik = node.get("instrument_key")
        option_data = node.get("market_data", node)
    elif isinstance(node, list) and node and isinstance(node[0], dict):
        option_data = node[0]
        ik = node[0].get("instrument_key")
    bid_p, ask_p = (None, None)
    if isinstance(option_data, dict):
        bid_p, ask_p = vwap_service.extract_bid_ask_from_quote_data(option_data)
    ltp = 0.0
    if isinstance(option_data, dict):
        ltp = float(option_data.get("ltp") or option_data.get("last_price") or 0)
    oi = 0.0
    if isinstance(option_data, dict):
        oi = float(option_data.get("oi") or option_data.get("open_interest") or 0)
    iv = None
    if isinstance(option_data, dict):
        iv_raw = option_data.get("iv") or option_data.get("implied_volatility")
        try:
            if iv_raw is not None:
                iv = float(iv_raw)
        except Exception:
            iv = None
    return {
        "instrument_key": ik,
        "ltp": round(ltp, 4),
        "bid": None if bid_p is None else round(float(bid_p), 4),
        "ask": None if ask_p is None else round(float(ask_p), 4),
        "oi": oi,
        "iv": iv,
    }


def analyze_iron_condor_detailed(
    symbol: str, db: Session, user_id: int, strike_overrides: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    ic.ensure_iron_condor_tables()
    iron_condor_migrations_v2()
    core = ic.analyze_iron_condor(symbol, db, user_id)

    api_sym = core["option_chain_symbol"]
    und = core["underlying"]
    lot = resolve_lot_size(db, ic.option_chain_underlying(und))

    eq_key = vwap_service.get_instrument_key(api_sym)
    if not eq_key:
        raise RuntimeError("Equity instrument key missing for rich quote.")
    pct_day = None
    try:
        o = vwap_service.get_ohlc_data(eq_key) or {}
        lp = float(o.get("last_price") or 0)
        nested = (o.get("ohlc") or {}) if isinstance(o.get("ohlc"), dict) else {}
        pcl = float(nested.get("close") or 0)
        if lp and pcl:
            pct_day = round((lp - pcl) / pcl * 100.0, 2)
    except Exception:
        pct_day = None

    chain = vwap_service.get_option_chain(api_sym)
    payload = chain
    if isinstance(chain, dict) and chain.get("status") == "success":
        payload = chain.get("data") or chain
    strike_list = ic._extract_chain_strike_list(payload if isinstance(payload, dict) else {}) or []

    strikes = dict(core["strikes"])
    ov_warn = []
    if strike_overrides:
        for k in ("sell_call", "buy_call", "sell_put", "buy_put"):
            v = strike_overrides.get(k)
            if v is not None and str(v).strip() != "":
                strikes[k] = float(v)
        ov_warn.append("Strikes were overridden — quotes may be incomplete if not on chain; confirm in Upstox.")

    need_sp = {
        float(strikes["sell_call"]),
        float(strikes["buy_call"]),
        float(strikes["sell_put"]),
        float(strikes["buy_put"]),
    }
    agg_row = {}
    for sd in strike_list:
        if not isinstance(sd, dict):
            continue
        sp_raw = sd.get("strike_price")
        if sp_raw is None:
            continue
        if float(sp_raw) in need_sp:
            agg_row[float(sp_raw)] = sd

    def leg(sp: float, ce: bool) -> Dict[str, Any]:
        row = agg_row.get(float(sp))
        if not row:
            return {"strike": sp, "side": "CE" if ce else "PE", "ltp": None, "bid": None, "ask": None, "oi": 0}
        d = enrich_leg(row, ce)
        d["strike"] = sp
        d["side"] = "CE" if ce else "PE"
        return d

    sce = leg(float(strikes["sell_call"]), True)
    bce = leg(float(strikes["buy_call"]), True)
    spe = leg(float(strikes["sell_put"]), False)
    bpe = leg(float(strikes["buy_put"]), False)

    if strike_overrides:
        psc = float(sce.get("ltp") or 0.0) + float(spe.get("ltp") or 0.0)
        phc = float(bce.get("ltp") or 0.0) + float(bpe.get("ltp") or 0.0)
    else:
        psc = float(core["premium_collected"])
        phc = float(core["hedge_cost"])
    net_pts = float(psc) - float(phc)
    w_ce = float(strikes["buy_call"]) - float(strikes["sell_call"])
    w_pe = float(strikes["sell_put"]) - float(strikes["buy_put"])
    spread_width_pts = w_ce + w_pe
    max_loss_pts = max(0.0, spread_width_pts - net_pts)
    qty = float(lot) * 1.0
    max_loss_rupees = round(max_loss_pts * qty, 2)
    max_profit_rupees = round(max(0.0, net_pts) * qty, 2)
    be_u = float(strikes["sell_call"]) + net_pts if net_pts >= 0 else float(strikes["sell_call"])
    be_l = float(strikes["sell_put"]) - net_pts if net_pts >= 0 else float(strikes["sell_put"])
    rr = None
    if max_loss_pts > 0:
        rr = round(net_pts / max_loss_pts, 4)

    core_out = dict(core)
    core_out["strikes"] = strikes
    core_out["premiums"] = {
        "sell_call": float(sce.get("ltp") or 0.0),
        "buy_call": float(bce.get("ltp") or 0.0),
        "sell_put": float(spe.get("ltp") or 0.0),
        "buy_put": float(bpe.get("ltp") or 0.0),
    }
    core_out["premium_collected"] = round(psc, 4)
    core_out["hedge_cost"] = round(phc, 4)
    core_out["hedge_ratio"] = round((phc / psc) if psc > 0 else 0.0, 4)
    hg, hgm = ic.classify_hedge_ratio(float(core_out["hedge_ratio"]))
    core_out["hedge_gate"], core_out["hedge_gate_message"] = hg, hgm

    core_out["legs_quote"] = {
        "sell_call": sce,
        "buy_call": bce,
        "sell_put": spe,
        "buy_put": bpe,
    }
    gate_color = hg == "VALID" and "GREEN" or (hg == "WARN" and "YELLOW" or "RED")
    core_out["economics"] = {
        "lot_size": lot,
        "num_lots": 1,
        "premium_collected_pts": round(float(psc), 4),
        "hedge_cost_pts": round(float(phc), 4),
        "net_credit_pts": round(net_pts, 4),
        "spread_width_pts": round(spread_width_pts, 4),
        "max_profit_rupees_est": max_profit_rupees,
        "max_loss_rupees_est": max_loss_rupees,
        "breakeven_lower": round(be_l, 4),
        "breakeven_upper": round(be_u, 4),
        "risk_reward_net_to_max_loss": rr,
        "hedge_gate_color": gate_color,
    }
    core_out["live"] = {"spot_ltp": core.get("spot"), "underlying_change_pct_today": pct_day}
    core_out["strike_selection_warnings"] = list(dict.fromkeys((core.get("strike_selection_warnings") or []) + ov_warn))
    return core_out


def confirm_entry_iron_condor(db: Session, user_id: int, payload: Dict[str, Any]) -> int:
    """
    Persist ACTIVE position with user's fill prices & computed thresholds.
    payload: analysis core from analyze_iron_condor_detailed (+ optional overrides in strikes/premiums)
    fills: sell_call_fill, ...
    """
    ic.ensure_iron_condor_tables()
    iron_condor_migrations_v2()
    fills = payload.get("fills") or {}
    ana = payload.get("analysis") or payload
    st = ana["strikes"]
    econ = ana.get("economics") or {}
    lot = int(payload.get("lot_size") or econ.get("lot_size") or resolve_lot_size(db, ana["underlying"]))
    nlot = max(1, int(payload.get("num_lots") or econ.get("num_lots") or 1))

    scf = float(fills.get("sell_call_fill"))
    bpf = float(fills.get("buy_call_fill"))
    spf = float(fills.get("sell_put_fill"))
    bpgf = float(fills.get("buy_put_fill"))

    prem_c = scf + spf
    hedge_c = bpf + bpgf
    net_pts = prem_c - hedge_c
    w_ce = float(st["buy_call"]) - float(st["sell_call"])
    w_pe = float(st["sell_put"]) - float(st["buy_put"])
    spread_width = w_ce + w_pe
    max_loss_pts = max(0.0, spread_width - net_pts)
    qty_mult = float(lot * nlot)
    max_loss_rupees = round(max_loss_pts * qty_mult, 2)
    max_profit_rupees = round(max(0.0, net_pts) * qty_mult, 2)

    stop_c = round(2.0 * scf, 4)
    stop_p = round(2.0 * spf, 4)
    adj_c = round(1.5 * scf, 4)
    adj_p = round(1.5 * spf, 4)
    profit_tgt = round(0.5 * max_profit_rupees, 2)

    be_u = float(st["sell_call"]) + net_pts
    be_l = float(st["sell_put"]) - net_pts

    exp_d = datetime.strptime(str(ana["expiry_date"]), "%Y-%m-%d").date()
    hedge_ratio = (hedge_c / prem_c) if prem_c > 0 else 0.0
    gate, _ = ic.classify_hedge_ratio(hedge_ratio)
    td = datetime.now(IST).date()

    ic_row = ana.get("position_sizing") or {}
    ne_raw = payload.get("declared_next_earnings_iso") or ana.get("declared_next_earnings_iso")
    next_earn = _parse_next_earnings_date(ne_raw) if ne_raw else None
    db.execute(
        text(
            """
            INSERT INTO iron_condor_position (
                user_id, underlying, sector, status, expiry_date, entry_date,
                monthly_atr, strike_distance, spot_at_snapshot, strike_interval,
                sell_call_strike, buy_call_strike, sell_put_strike, buy_put_strike,
                premium_sell_call, premium_buy_call, premium_sell_put, premium_buy_put,
                premium_collected, hedge_cost, hedge_ratio, hedge_gate,
                allocation_pct, suggested_capital_rupees,
                lot_size, num_lots,
                sell_call_entry_fill, buy_call_entry_fill, sell_put_entry_fill, buy_put_entry_fill,
                sell_call_current, buy_call_current, sell_put_current, buy_put_current,
                net_credit_pts, max_profit_rupees, max_loss_rupees, breakeven_lower, breakeven_upper,
                stop_sl_call_px, stop_sl_put_px, adjust_call_px, adjust_put_px,
                profit_target_rupees, adjustments_history, next_earnings_estimate
            ) VALUES (
                :uid, :und, :sec, 'ACTIVE', :expd, :entd,
                :matr, :sdist, :spot, :sint,
                :sce, :bce, :spe, :bpe,
                :psce, :pbce, :pspe, :pbpe,
                :pcol, :hcost, :hrat, :hg,
                :apct, :scap,
                :lot, :nlot,
                :scf, :bcf, :spf, :bpf,
                :scf, :bcf, :spf, :bpf,
                :nc_pts, :maxp, :maxl, :bel, :beu,
                :stc, :stp, :adc, :adp,
                :ptgt, CAST(:adjh AS JSONB), CAST(:nee AS DATE))
            ON CONFLICT (user_id, underlying, expiry_date) DO UPDATE SET
                status = 'ACTIVE',
                entry_date = EXCLUDED.entry_date,
                sell_call_entry_fill = EXCLUDED.sell_call_entry_fill,
                buy_call_entry_fill = EXCLUDED.buy_call_entry_fill,
                sell_put_entry_fill = EXCLUDED.sell_put_entry_fill,
                buy_put_entry_fill = EXCLUDED.buy_put_entry_fill,
                sell_call_current = EXCLUDED.sell_call_current,
                buy_call_current = EXCLUDED.buy_call_current,
                sell_put_current = EXCLUDED.sell_put_current,
                buy_put_current = EXCLUDED.buy_put_current,
                net_credit_pts = EXCLUDED.net_credit_pts,
                max_profit_rupees = EXCLUDED.max_profit_rupees,
                max_loss_rupees = EXCLUDED.max_loss_rupees,
                breakeven_lower = EXCLUDED.breakeven_lower,
                breakeven_upper = EXCLUDED.breakeven_upper,
                stop_sl_call_px = EXCLUDED.stop_sl_call_px,
                stop_sl_put_px = EXCLUDED.stop_sl_put_px,
                adjust_call_px = EXCLUDED.adjust_call_px,
                adjust_put_px = EXCLUDED.adjust_put_px,
                profit_target_rupees = EXCLUDED.profit_target_rupees,
                hedge_ratio = EXCLUDED.hedge_ratio,
                hedge_gate = EXCLUDED.hedge_gate,
                next_earnings_estimate = COALESCE(EXCLUDED.next_earnings_estimate, iron_condor_position.next_earnings_estimate)
            """
        ),
        {
            "uid": user_id,
            "und": ana["underlying"],
            "sec": ana["sector"],
            "expd": exp_d,
            "entd": td,
            "matr": ana.get("monthly_atr_14"),
            "sdist": ana.get("strike_distance"),
            "spot": ana.get("spot"),
            "sint": ana.get("strike_interval"),
            "sce": st["sell_call"],
            "bce": st["buy_call"],
            "spe": st["sell_put"],
            "bpe": st["buy_put"],
            "psce": scf,
            "pbce": bpf,
            "pspe": spf,
            "pbpe": bpgf,
            "pcol": prem_c,
            "hcost": hedge_c,
            "hrat": hedge_ratio,
            "hg": gate,
            "apct": ic_row.get("suggested_allocation_pct"),
            "scap": ic_row.get("suggested_capital_rupees"),
            "lot": lot,
            "nlot": nlot,
            "scf": scf,
            "bcf": bpf,
            "spf": spf,
            "bpf": bpgf,
            "nc_pts": round(net_pts, 6),
            "maxp": max_profit_rupees,
            "maxl": max_loss_rupees,
            "bel": round(be_l, 4),
            "beu": round(be_u, 4),
            "stc": stop_c,
            "stp": stop_p,
            "adc": adj_c,
            "adp": adj_p,
            "ptgt": profit_tgt,
            "adjh": "[]",
            "nee": next_earn,
        },
    )
    db.commit()
    rid = db.execute(
        text(
            """
            SELECT id FROM iron_condor_position WHERE user_id=:uid AND underlying=:und AND expiry_date=:expd LIMIT 1
            """
        ),
        {"uid": user_id, "und": ana["underlying"], "expd": exp_d},
    ).scalar()
    return int(rid or 0)


def is_iron_condor_poll_window_ist(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    if mh.should_skip_scheduled_market_jobs_ist(now):
        return False
    t = now.time()
    from datetime import time as dt_time

    return dt_time(9, 15) <= t <= dt_time(15, 30)


def _insert_alert(db: Session, uid: int, pid: Optional[int], code: str, sev: str, msg: str, payload: Dict[str, Any]) -> None:
    db.execute(
        text(
            """
            INSERT INTO iron_condor_alert
            (user_id, position_id, rule_code, alert_type, severity, message, payload_json)
            VALUES (:uid, :pid, :rc, :at, :sev, :msg, CAST(:pl AS JSONB))
            """
        ),
        {"uid": uid, "pid": pid, "rc": code, "at": code, "sev": sev, "msg": msg, "pl": json.dumps(payload)},
    )


def _parse_next_earnings_date(ne: Any) -> Optional[date]:
    if ne is None:
        return None
    if isinstance(ne, datetime):
        return ne.date()
    if isinstance(ne, date):
        return ne
    try:
        return datetime.strptime(str(ne)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def alert_exists_recent(db: Session, uid: int, pid: Optional[int], rule: str, hours: float = 4.0) -> bool:
    if pid is None:
        q = db.execute(
            text(
                """
                SELECT 1 FROM iron_condor_alert WHERE user_id=:uid AND position_id IS NULL
                  AND rule_code=:rc AND created_at >= CURRENT_TIMESTAMP - (interval '1 hour' * :hrs)
                LIMIT 1
                """
            ),
            {"uid": uid, "rc": rule, "hrs": hours},
        ).scalar()
    else:
        q = db.execute(
            text(
                """
                SELECT 1 FROM iron_condor_alert WHERE user_id=:uid AND position_id=:pid
                  AND rule_code=:rc AND created_at >= CURRENT_TIMESTAMP - (interval '1 hour' * :hrs)
                LIMIT 1
                """
            ),
            {"uid": uid, "pid": pid, "rc": rule, "hrs": hours},
        ).scalar()
    return bool(q)


def evaluate_active_position(db: Session, user_id: int, rowm: Dict[str, Any]) -> Dict[str, Any]:
    """Full alert suite for one ACTIVE-ish position. Skip price-driven rules if quotes missing (stale feed)."""
    iron_condor_migrations_v2()
    pid = int(rowm["id"])
    uid = user_id
    new_alerts: List[Dict[str, Any]] = []

    def fire(code: str, sev: str, msg: str, pl: Dict[str, Any], hours_cooldown: float = 2.0) -> None:
        if alert_exists_recent(db, uid, pid, code, hours=hours_cooldown):
            return
        _insert_alert(db, uid, pid, code, sev, msg, pl)
        new_alerts.append({"rule_code": code, "severity": sev, "message": msg})

    today = datetime.now(IST).date()
    exp_raw = rowm["expiry_date"]
    if isinstance(exp_raw, datetime):
        exp = exp_raw.date()
    elif isinstance(exp_raw, date):
        exp = exp_raw
    else:
        exp = datetime.strptime(str(exp_raw)[:10], "%Y-%m-%d").date()
    dte = (exp - today).days

    ne_date = _parse_next_earnings_date(rowm.get("next_earnings_estimate"))
    if ne_date is not None:
        dd_e = (ne_date - today).days
        if 0 < dd_e <= 5:
            fire(
                "ALERT_EARNINGS_NEAR",
                "ORANGE",
                "{}: Declared/tracked result date in {} day(s) — review risk.".format(rowm["underlying"], dd_e),
                {"days": dd_e, "date": str(ne_date)},
                hours_cooldown=18.0,
            )

    strikes = {
        "sell_call": float(rowm["sell_call_strike"]),
        "buy_call": float(rowm["buy_call_strike"]),
        "sell_put": float(rowm["sell_put_strike"]),
        "buy_put": float(rowm["buy_put_strike"]),
    }
    qc = ic._fresh_chain_quotes(str(rowm["underlying"]), strikes)
    if not qc:
        db.commit()
        return {"alerts": new_alerts, "quotes_refreshed": False}

    sc = float(qc["sc_ltp"])
    bc = float(qc["bc_ltp"])
    sp = float(qc["sp_ltp"])
    bp = float(qc["bp_ltp"])

    esf = float(rowm.get("sell_call_entry_fill") or rowm.get("premium_sell_call") or 0)
    epf = float(rowm.get("sell_put_entry_fill") or rowm.get("premium_sell_put") or 0)
    ebcf = float(rowm.get("buy_call_entry_fill") or rowm.get("premium_buy_call") or 0)
    ebpf = float(rowm.get("buy_put_entry_fill") or rowm.get("premium_buy_put") or 0)

    lot = int(rowm.get("lot_size") or 1)
    nlot = int(rowm.get("num_lots") or 1)
    mult = float(lot * nlot)

    entry_net_pts = (esf + epf) - (ebcf + ebpf)
    close_cost_pts = (sc - bc) + (sp - bp)
    mtm_rupees = round((entry_net_pts - close_cost_pts) * mult, 2)

    max_profit = float(rowm.get("max_profit_rupees") or 0)
    max_loss = float(rowm.get("max_loss_rupees") or 0)

    health = "ON_TRACK"
    if esf > 0 and sc >= 2.0 * esf:
        health = "STOP_LOSS"
    elif esf > 0 and sc >= 1.5 * esf and dte > 10:
        health = "ADJUST_ZONE"
    elif epf > 0 and sp >= 2.0 * epf:
        health = "STOP_LOSS"
    elif epf > 0 and sp >= 1.5 * epf and dte > 10:
        health = "ADJUST_ZONE"
    elif esf > 0 and sc >= 1.3 * esf:
        health = "WATCH"
    elif epf > 0 and sp >= 1.3 * epf:
        health = "WATCH"
    if max_profit > 0 and mtm_rupees >= 0.5 * max_profit:
        health = "PROFIT_TARGET"

    db.execute(
        text(
            """
            UPDATE iron_condor_position SET
                sell_call_current=:sc, buy_call_current=:bc, sell_put_current=:sp, buy_put_current=:bp,
                last_poll_at=CURRENT_TIMESTAMP, position_health=:ph
            WHERE id=:pid AND user_id=:uid
            """
        ),
        {"sc": sc, "bc": bc, "sp": sp, "bp": bp, "ph": health, "pid": pid, "uid": uid},
    )

    vix, _ = fetch_india_vix()
    api_u = ic.option_chain_underlying(str(rowm["underlying"]))
    eqk = vwap_service.get_instrument_key(api_u) or ""
    spot_px = None
    if eqk:
        spot_px = _cached_spot(api_u, eqk)

    db.execute(
        text(
            """
            INSERT INTO iron_condor_price_history
            (position_id, user_id, spot, india_vix, sell_call_ltp, buy_call_ltp, sell_put_ltp, buy_put_ltp, mtm_estimate_rupees, extras)
            VALUES (:pid, :uid, :spot, :vix, :sc, :bc, :sp, :bp, :mtm, CAST(:ex AS JSONB))
            """
        ),
        {
            "pid": pid,
            "uid": uid,
            "spot": spot_px,
            "vix": vix,
            "sc": sc,
            "bc": bc,
            "sp": sp,
            "bp": bp,
            "mtm": mtm_rupees,
            "ex": json.dumps({"health": health}),
        },
    )

    if max_profit > 0 and mtm_rupees >= 0.5 * max_profit:
        pct = (mtm_rupees / max_profit * 100) if max_profit else 0
        fire(
            "ALERT_PROFIT_50",
            "GREEN",
            "{}: Book profit. ~{:.0f}% of max profit (₹{:,.2f}).".format(rowm["underlying"], pct, mtm_rupees),
            {"mtm": mtm_rupees, "max_profit": max_profit},
            hours_cooldown=6.0,
        )

    if esf > 0 and sc >= 1.5 * esf and dte > 10:
        fire(
            "ALERT_ADJUST_150",
            "ORANGE",
            "{}: Call short premium hit 150% of entry (adjustment window).".format(rowm["underlying"]),
            {"leg": "CALL", "ltp": sc, "entry": esf},
        )
    if epf > 0 and sp >= 1.5 * epf and dte > 10:
        fire(
            "ALERT_ADJUST_150",
            "ORANGE",
            "{}: Put short premium hit 150% of entry (adjustment window).".format(rowm["underlying"]),
            {"leg": "PUT", "ltp": sp, "entry": epf},
        )

    if esf > 0 and sc >= 2.0 * esf:
        fire(
            "ALERT_STOP_200",
            "RED",
            "{}: STOP reference — call short ≥200% of entry fill. Exit that side on Upstox.".format(rowm["underlying"]),
            {"leg": "CALL"},
            hours_cooldown=1.0,
        )
    if epf > 0 and sp >= 2.0 * epf:
        fire(
            "ALERT_STOP_200",
            "RED",
            "{}: STOP reference — put short ≥200% of entry fill. Exit that side on Upstox.".format(rowm["underlying"]),
            {"leg": "PUT"},
            hours_cooldown=1.0,
        )

    if max_profit > 0 and mtm_rupees <= -2.0 * max_profit:
        fire(
            "ALERT_TOTAL_LOSS_2X",
            "RED",
            "{}: Total MTM loss exceeded 2× max profit proxy — consider full exit.".format(rowm["underlying"]),
            {"mtm": mtm_rupees, "max_profit": max_profit},
        )

    st = ic.get_or_create_settings(db, uid)
    cap = float(st.get("trading_capital") or 0)
    if cap > 0 and mtm_rupees <= -0.015 * cap:
        fire(
            "ALERT_HARD_CAPITAL",
            "CRITICAL_RED",
            "HARD STOP: {} loss exceeds 1.5% of total trading capital (₹{:,.0f}).".format(rowm["underlying"], cap),
            {"mtm": mtm_rupees, "cap": cap},
            hours_cooldown=0.5,
        )

    if dte == 10 and not alert_exists_recent(db, uid, pid, "ALERT_EXPIRY_10D", hours=240.0):
        fire(
            "ALERT_EXPIRY_10D",
            "BLUE",
            "{}: 10 days to expiry — no new adjustments per playbook; plan exit.".format(rowm["underlying"]),
            {"dte": dte},
            hours_cooldown=240.0,
        )

    if today.weekday() == 0 and 3 <= dte <= 4:
        fire(
            "ALERT_EXPIRY_MONDAY",
            "ORANGE",
            "{}: Expiry-week Monday — time-based exit advisory.".format(rowm["underlying"]),
            {"dte": dte},
            hours_cooldown=12.0,
        )

    db.commit()
    return {"alerts": new_alerts, "quotes_refreshed": True}


def poll_user_iron_condors(db: Session, user_id: int) -> Dict[str, Any]:
    iron_condor_migrations_v2()
    if not is_iron_condor_poll_window_ist():
        return {
            "market_open": False,
            "new_alerts": [],
            "positions_updated": 0,
            "quote_feed": {"data_feed_lost": False, "poll_fail_streak": 0, "last_quote_success_at": None},
        }

    rows = db.execute(
        text(
            """
            SELECT * FROM iron_condor_position
            WHERE user_id=:uid AND UPPER(status) IN ('ACTIVE', 'OPEN', 'ADJUSTED')
            """
        ),
        {"uid": user_id},
    ).mappings().all()
    all_new: List[Dict[str, Any]] = []
    any_quotes_ok = False
    for r in rows:
        ev = evaluate_active_position(db, user_id, dict(r))
        all_new.extend(ev.get("alerts") or [])
        if ev.get("quotes_refreshed"):
            any_quotes_ok = True

    if not rows:
        qf = _update_quote_feed_streak(db, user_id, True)
    else:
        qf = _update_quote_feed_streak(db, user_id, any_quotes_ok)

    # Global VIX spike (per user, once per cross)
    vix, _ = fetch_india_vix()
    if vix is not None and vix > 22.0:
        st = ic.get_or_create_settings(db, user_id)
        last = st.get("ic_last_vix_alert_level")
        try:
            last_f = float(last) if last is not None else None
        except Exception:
            last_f = None
        if last_f is None or last_f <= 22.0:
            if not alert_exists_recent(db, user_id, None, "ALERT_VIX_SPIKE", hours=4.0):
                _insert_alert(
                    db,
                    user_id,
                    None,
                    "ALERT_VIX_SPIKE",
                    "ORANGE",
                    "India VIX at {:.2f} — elevated risk for open condors.".format(vix),
                    {"vix": vix},
                )
                all_new.append({"rule_code": "ALERT_VIX_SPIKE", "severity": "ORANGE"})
            db.execute(
                text("UPDATE iron_condor_user_settings SET ic_last_vix_alert_level=:v WHERE user_id=:u"),
                {"v": vix, "u": user_id},
            )
            db.commit()
    else:
        db.execute(
            text("UPDATE iron_condor_user_settings SET ic_last_vix_alert_level=NULL WHERE user_id=:u"),
            {"u": user_id},
        )
        db.commit()

    return {
        "market_open": True,
        "new_alerts": all_new,
        "positions_updated": len(rows),
        "quote_feed": qf,
    }


def acknowledge_alert(db: Session, user_id: int, alert_id: int) -> bool:
    iron_condor_migrations_v2()
    res = db.execute(
        text(
            """
            UPDATE iron_condor_alert SET acknowledged=TRUE, acknowledged_at=CURRENT_TIMESTAMP
            WHERE id=:aid AND user_id=:uid AND COALESCE(acknowledged,FALSE)=FALSE
            """
        ),
        {"aid": alert_id, "uid": user_id},
    )
    db.commit()
    return res.rowcount > 0


def close_with_journal(db: Session, user_id: int, body: Dict[str, Any]) -> bool:
    """Close position + journal row + realized PnL from exit fills."""
    iron_condor_migrations_v2()
    pid = int(body["position_id"])
    exits = body.get("exit_fills") or {}
    row = db.execute(
        text("SELECT * FROM iron_condor_position WHERE id=:pid AND user_id=:uid LIMIT 1"),
        {"pid": pid, "uid": user_id},
    ).mappings().first()
    if not row:
        return False
    r = dict(row)
    mult = float(int(r.get("lot_size") or 1) * int(r.get("num_lots") or 1))
    sce = exits.get("sell_call_exit"); bce = exits.get("buy_call_exit")
    spe = exits.get("sell_put_exit"); bpe = exits.get("buy_put_exit")

    realized = None
    if all(x is not None for x in (sce, bce, spe, bpe)):
        es = float(r.get("sell_call_entry_fill") or 0)
        eb = float(r.get("buy_call_entry_fill") or 0)
        eps = float(r.get("sell_put_entry_fill") or 0)
        ebp = float(r.get("buy_put_entry_fill") or 0)

        xs = float(sce)
        xb = float(bce)
        xps = float(spe)
        xbp = float(bpe)

        pnl_pts = ((es + eps + xb + xbp) - (eb + ebp + xs + xps))
        realized = round(pnl_pts * mult, 2)

    db.execute(
        text(
            """
            INSERT INTO iron_condor_trade_journal
            (user_id, position_id, exit_date, exit_reason, emotion, followed_rules,
             deviation_notes, lesson_learned, exit_snapshots, realized_pnl_rupees)
            VALUES (:uid,:pid,CURRENT_DATE,:er,:em,:fr,:dv,:ls,CAST(:ex AS JSONB),:rn)
            """
        ),
        {
            "uid": user_id,
            "pid": pid,
            "er": body.get("exit_reason"),
            "em": body.get("emotion"),
            "fr": body.get("followed_rules"),
            "dv": body.get("deviation_notes"),
            "ls": body.get("lesson_learned"),
            "ex": json.dumps({"exit_fills": exits}),
            "rn": realized,
        },
    )
    db.execute(
        text(
            """
            UPDATE iron_condor_position SET status='CLOSED', closed_at=CURRENT_TIMESTAMP,
                realized_pnl_rupees=COALESCE(:rn, realized_pnl_rupees)
            WHERE id=:pid AND user_id=:uid
            """
        ),
        {"pid": pid, "uid": user_id, "rn": realized},
    )
    db.commit()
    return True


def dashboard_summary(db: Session, user_id: int) -> Dict[str, Any]:
    iron_condor_migrations_v2()
    st = ic.get_or_create_settings(db, user_id)
    cap = float(st.get("trading_capital") or 0)
    dep = db.execute(
        text(
            """
            SELECT COALESCE(SUM(COALESCE(suggested_capital_rupees,0)),0) FROM iron_condor_position
            WHERE user_id=:uid AND UPPER(status) IN ('ACTIVE','OPEN','ADJUSTED')
            """
        ),
        {"uid": user_id},
    ).scalar()
    dep = float(dep or 0)
    mtm_sum = db.execute(
        text(
            """
            WITH last_px AS (
              SELECT DISTINCT ON (position_id)
                position_id,
                COALESCE(mtm_estimate_rupees, 0) AS mtm
              FROM iron_condor_price_history
              WHERE user_id=:uid AND position_id IN (
                SELECT id FROM iron_condor_position WHERE user_id=:uid2
                  AND UPPER(status) IN ('ACTIVE','OPEN','ADJUSTED'))
              ORDER BY position_id, ts DESC
            )
            SELECT COALESCE(SUM(mtm), 0) FROM last_px
            """
        ),
        {"uid": user_id, "uid2": user_id},
    ).scalar()
    mon = db.execute(
        text(
            """
            SELECT COALESCE(SUM(realized_pnl_rupees),0) FROM iron_condor_trade_journal
            WHERE user_id=:uid AND exit_date >= date_trunc('month', CURRENT_DATE)
            """
        ),
        {"uid": user_id},
    ).scalar()
    ytd = db.execute(
        text(
            """
            SELECT COALESCE(SUM(realized_pnl_rupees),0) FROM iron_condor_trade_journal
            WHERE user_id=:uid AND EXTRACT(YEAR FROM exit_date)=EXTRACT(YEAR FROM CURRENT_DATE)
            """
        ),
        {"uid": user_id},
    ).scalar()
    return {
        "trading_capital": cap,
        "deployed_capital_rupees": dep,
        "deployed_pct": round(dep / cap * 100, 2) if cap > 0 else None,
        "open_mtm_sum_rupees": float(mtm_sum or 0),
        "realized_month_rupees": float(mon or 0),
        "realized_year_rupees": float(ytd or 0),
        "capital_available_est": round(max(0.0, cap - dep), 2) if cap else None,
    }
