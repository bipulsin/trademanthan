"""RS Conviction Score board — 10-min stable ranking with hysteresis."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.relative_strength_scanner import RANKING_BEARISH
from backend.services.rs_conviction_candles import candles_cache_only, load_instrument_atr_maps
from backend.services.rs_conviction_config import get_config, persist_decay_factor
from backend.services.rs_conviction_signals import compute_symbol_signals

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

SIDE_BULL = "BULL"
SIDE_BEAR = "BEAR"

_BOARD_CLOSE_MINUTES = {25, 35, 45, 55, 5, 15}


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def is_board_cycle_for_scheduled_minute(hour: int, minute: int, cfg: Optional[Dict[str, Any]] = None) -> bool:
    """True when the RS 5m job's scheduled (hour, minute) is a board-close cycle.

    Uses the cron-fired minute captured at job start — not wall clock after a long RS
  scan — so APScheduler misfire/coalesce does not silently skip logging.
    """
    cfg = cfg or get_config()
    m = int(hour) * 60 + int(minute)
    if m > int(cfg.get("board_cutoff_min") or 915):
        return False
    if m < 9 * 60 + 25:
        return False
    return int(minute) in _BOARD_CLOSE_MINUTES


def is_board_cycle_minute(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return is_board_cycle_for_scheduled_minute(now.hour, now.minute)


def _load_opening_anchor_map(db, session_date: str, cfg: Dict[str, Any]) -> Dict[Tuple[str, str], float]:
    out: Dict[Tuple[str, str], float] = {}
    rows = db.execute(
        text(
            """
            SELECT symbol, direction, rank_position
            FROM rs_anchor_snapshot
            WHERE session_date = :d AND capture_label = '09:25'
            ORDER BY direction, rank_position
            """
        ),
        {"d": session_date},
    ).fetchall()
    r12 = float(cfg.get("anchor_rank12") or 100)
    r35 = float(cfg.get("anchor_rank35") or 80)
    for r in rows:
        side = SIDE_BULL if (r.direction or "").upper() in ("BULL", "BULLISH", "LONG") else SIDE_BEAR
        rank = int(r.rank_position or 99)
        out[(r.symbol, side)] = r12 if rank <= 2 else (r35 if rank <= 5 else 0.0)
    return out


def _normalize_rank_component(scores: List[float], value: float) -> float:
    if not scores:
        return 0.0
    lo, hi = min(scores), max(scores)
    if hi <= lo:
        return 50.0
    return max(0.0, min(100.0, (value - lo) / (hi - lo) * 100.0))


def _slope_time_multiplier(minutes: int, cfg: Dict[str, Any]) -> float:
    morning_end = int(cfg.get("slope_morning_end_min") or 660)
    midday_end = int(cfg.get("slope_midday_end_min") or 780)
    if minutes < morning_end:
        return float(cfg.get("slope_mult_morning") or 1.0)
    if minutes < midday_end:
        return float(cfg.get("slope_mult_midday") or 0.6)
    return float(cfg.get("slope_mult_late") or 0.4)


def _compute_whipsaw_penalty(cross_count: int, cfg: Dict[str, Any]) -> float:
    if cross_count <= 1:
        return 0.0
    if cross_count == 2:
        return float(cfg.get("whip_cross_2") or 30)
    if cross_count == 3:
        return float(cfg.get("whip_cross_3") or 60)
    return float(cfg.get("whip_cross_4") or 100)


def _load_raw_top5(db) -> Tuple[List[Any], List[Any]]:
    rows = db.execute(
        text(
            """
            SELECT symbol, relative_strength, trade_score, ranking_type, rank_position
            FROM relative_strength_snapshot
            WHERE scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
              AND rank_position <= 5
            ORDER BY ranking_type, rank_position
            """
        )
    ).fetchall()
    bull = [r for r in rows if (r.ranking_type or "").upper() != RANKING_BEARISH]
    bear = [r for r in rows if (r.ranking_type or "").upper() == RANKING_BEARISH]
    return bull, bear


def _load_state_map(db, session_date: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT symbol, side, persistence_credit, opening_anchor, rs_component,
                   slope_component, accum_component, whip_penalty, conviction_score,
                   whipsaw_cross_count, accum_active, ema10_10m
            FROM rs_conviction_state
            WHERE session_date = :d
            """
        ),
        {"d": session_date},
    ).fetchall()
    return {
        (r.symbol, r.side): {
            "persistence_credit": float(r.persistence_credit or 0),
            "opening_anchor": float(r.opening_anchor or 0),
            "rs_component": float(r.rs_component or 0),
            "slope_component": float(r.slope_component or 0),
            "accum_component": float(r.accum_component or 0),
            "whip_penalty": float(r.whip_penalty or 0),
            "conviction_score": float(r.conviction_score or 0),
            "whipsaw_cross_count": int(r.whipsaw_cross_count or 0),
            "accum_active": bool(r.accum_active),
            "ema10_10m": float(r.ema10_10m) if r.ema10_10m is not None else None,
        }
        for r in rows
    }


def _upsert_state(db, session_date: str, symbol: str, side: str, fields: Dict[str, Any]) -> None:
    db.execute(
        text(
            """
            INSERT INTO rs_conviction_state (
                session_date, symbol, side, persistence_credit, opening_anchor,
                rs_component, slope_component, accum_component, whip_penalty,
                conviction_score, whipsaw_cross_count, accum_active, ema10_10m, updated_at
            ) VALUES (
                :d, :sym, :side, :persist, :anchor, :rs, :slope, :accum, :whip,
                :conv, :crosses, :accum_active, :ema10, NOW()
            )
            ON CONFLICT (session_date, symbol, side) DO UPDATE SET
                persistence_credit = EXCLUDED.persistence_credit,
                opening_anchor = EXCLUDED.opening_anchor,
                rs_component = EXCLUDED.rs_component,
                slope_component = EXCLUDED.slope_component,
                accum_component = EXCLUDED.accum_component,
                whip_penalty = EXCLUDED.whip_penalty,
                conviction_score = EXCLUDED.conviction_score,
                whipsaw_cross_count = EXCLUDED.whipsaw_cross_count,
                accum_active = EXCLUDED.accum_active,
                ema10_10m = EXCLUDED.ema10_10m,
                updated_at = NOW()
            """
        ),
        {
            "d": session_date,
            "sym": symbol,
            "side": side,
            "persist": fields.get("persistence_credit", 0),
            "anchor": fields.get("opening_anchor", 0),
            "rs": fields.get("rs_component", 0),
            "slope": fields.get("slope_component", 0),
            "accum": fields.get("accum_component", 0),
            "whip": fields.get("whip_penalty", 0),
            "conv": fields.get("conviction_score", 0),
            "crosses": fields.get("whipsaw_cross_count", 0),
            "accum_active": fields.get("accum_active", False),
            "ema10": fields.get("ema10_10m"),
        },
    )


def _score_universe(db, session_date: str, cfg: Dict[str, Any], now: datetime) -> Dict[Tuple[str, str], Dict[str, Any]]:
    bull_raw, bear_raw = _load_raw_top5(db)
    anchor_map = _load_opening_anchor_map(db, session_date, cfg)
    state_map = _load_state_map(db, session_date)
    decay = persist_decay_factor(cfg)
    inc = float(cfg.get("persist_increment") or 15)
    cap = float(cfg.get("persist_cap") or 100)
    minutes = now.hour * 60 + now.minute
    slope_mult = _slope_time_multiplier(minutes, cfg)

    raw_top_bull = {r.symbol for r in bull_raw}
    raw_top_bear = {r.symbol for r in bear_raw}

    universe: Set[Tuple[str, str]] = set()
    for r in bull_raw:
        universe.add((r.symbol, SIDE_BULL))
    for r in bear_raw:
        universe.add((r.symbol, SIDE_BEAR))
    for sym, side in state_map:
        universe.add((sym, side))

    rs_vals_bull = [float(r.relative_strength or 0) for r in bull_raw]
    rs_vals_bear = [float(r.relative_strength or 0) for r in bear_raw]
    rs_by_sym = {r.symbol: float(r.relative_strength or 0) for r in bull_raw + bear_raw}

    sym_set = {sym for sym, _ in universe}
    ikey_map, atr_map = load_instrument_atr_maps(db, sym_set)

    scored: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for sym, side in universe:
        prev = state_map.get((sym, side), {})
        persist = float(prev.get("persistence_credit") or 0)
        in_raw = sym in (raw_top_bull if side == SIDE_BULL else raw_top_bear)
        if in_raw:
            persist = min(cap, persist + inc)
        else:
            persist *= decay

        anchor = anchor_map.get((sym, side), float(prev.get("opening_anchor") or 0))
        rs_val = rs_by_sym.get(sym, 0.0)
        rs_pool = rs_vals_bull if side == SIDE_BULL else rs_vals_bear
        rs_comp = _normalize_rank_component(rs_pool, rs_val) if sym in rs_by_sym else 0.0

        candles = candles_cache_only(ikey_map.get(sym, ""))
        atr_pct = atr_map.get(sym, 1.0)
        signals = compute_symbol_signals(candles, side=side, atr_daily_pct=atr_pct, cfg=cfg)
        if candles:
            slope_comp = float(signals["slope_component"]) * slope_mult
            accum_comp = float(signals["accum_component"])
            accum_active = bool(signals["accum_active"])
            whip_cross = int(signals["whipsaw_cross_count"])
            ema10 = signals.get("ema10_10m")
        else:
            slope_comp = float(prev.get("slope_component") or 0) * slope_mult
            accum_comp = float(prev.get("accum_component") or 0)
            accum_active = bool(prev.get("accum_active"))
            whip_cross = int(prev.get("whipsaw_cross_count") or 0)
            ema10 = prev.get("ema10_10m")
        whip_pen = _compute_whipsaw_penalty(whip_cross, cfg)

        ignition_sc = 0.0
        if cfg.get("ignition_conviction_enabled"):
            try:
                from backend.services.kavach_momentum_ignition import compute_ignition

                ign = compute_ignition(
                    sym, side, ikey_map.get(sym, ""),
                    candles=candles, atr_pct=atr_pct, cfg=cfg,
                )
                ignition_sc = float(ign.get("ignition_score") or 0)
            except Exception as exc:
                logger.debug("ignition conviction component failed %s: %s", sym, exc)

        w_rs = float(cfg.get("W_rs") or 0.3)
        w_anchor = float(cfg.get("W_anchor") or 0.2)
        w_persist = float(cfg.get("W_persist") or 0.2)
        w_slope = float(cfg.get("W_slope") or 0.15)
        w_accum = float(cfg.get("W_accum") or 0.15)
        w_whip = float(cfg.get("W_whip") or 0.1)
        w_ign = float(cfg.get("W_ignition_conviction") or 0) if cfg.get("ignition_conviction_enabled") else 0.0

        composite = (
            w_rs * rs_comp
            + w_anchor * anchor
            + w_persist * (persist / 100.0 * 100.0)
            + w_slope * slope_comp
            + w_accum * accum_comp
            + w_ign * ignition_sc
            - w_whip * whip_pen
        )
        composite = max(0.0, min(100.0, composite))

        scored[(sym, side)] = {
            "symbol": sym,
            "side": side,
            "persistence_credit": round(persist, 2),
            "opening_anchor": anchor,
            "rs_component": round(rs_comp, 2),
            "slope_component": round(slope_comp, 2),
            "accum_component": round(accum_comp, 2),
            "whip_penalty": round(whip_pen, 2),
            "conviction_score": round(composite, 2),
            "whipsaw_cross_count": whip_cross,
            "accum_active": accum_active,
            "ema10_10m": ema10,
            "in_raw_top5": in_raw,
            "has_anchor": anchor > 0,
        }
        _upsert_state(db, session_date, sym, side, scored[(sym, side)])

    return scored


def _load_core_board(db, session_date: str, side: str) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT rank, symbol, conviction_score, promoted_at
            FROM rs_conviction_board
            WHERE session_date = :d AND side = :side
            ORDER BY rank
            """
        ),
        {"d": session_date, "side": side},
    ).fetchall()
    return [
        {
            "rank": int(r.rank),
            "symbol": r.symbol,
            "conviction_score": float(r.conviction_score or 0),
            "promoted_at": r.promoted_at.isoformat() if r.promoted_at else None,
        }
        for r in rows
    ]


def _set_core_board(db, session_date: str, side: str, symbols: List[Tuple[str, float]]) -> None:
    db.execute(
        text("DELETE FROM rs_conviction_board WHERE session_date = :d AND side = :side"),
        {"d": session_date, "side": side},
    )
    now = datetime.now(IST)
    for rank, (sym, score) in enumerate(symbols[:5], start=1):
        db.execute(
            text(
                """
                INSERT INTO rs_conviction_board
                    (session_date, side, rank, symbol, conviction_score, promoted_at)
                VALUES (:d, :side, :rank, :sym, :score, :now)
                """
            ),
            {"d": session_date, "side": side, "rank": rank, "sym": sym, "score": score, "now": now},
        )


def _apply_hysteresis(
    db, session_date: str, side: str,
    scored: Dict[Tuple[str, str], Dict[str, Any]],
    cfg: Dict[str, Any], cycle_time: datetime,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    core = _load_core_board(db, session_date, side)

    side_scored = sorted(
        [v for (_, s), v in scored.items() if s == side and v["conviction_score"] > 0],
        key=lambda x: -x["conviction_score"],
    )

    if not core:
        top5 = [(s["symbol"], s["conviction_score"]) for s in side_scored[:5]]
        _set_core_board(db, session_date, side, top5)
        return events

    new_core = list(core)
    eject_floor = float(cfg.get("hard_eject_score_floor") or 40)
    whip_min = float(cfg.get("hard_eject_whip_min") or 100)
    for c in core:
        st = scored.get((c["symbol"], side), {})
        if st.get("whip_penalty", 0) >= whip_min and st.get("conviction_score", 0) < eject_floor:
            new_core = [x for x in new_core if x["symbol"] != c["symbol"]]
            events.append({"time": cycle_time.isoformat(), "side": side, "type": "eject", "symbol": c["symbol"]})

    core_syms = {c["symbol"] for c in new_core}
    challengers = [s for s in side_scored if s["symbol"] not in core_syms]
    if not challengers:
        _set_core_board(
            db, session_date, side,
            [(c["symbol"], scored.get((c["symbol"], side), {}).get("conviction_score", c["conviction_score"])) for c in sorted(new_core, key=lambda x: x["rank"])],
        )
        return events

    lowest = min(new_core, key=lambda c: scored.get((c["symbol"], side), {}).get("conviction_score", c["conviction_score"]))
    best = challengers[0]
    best_score = best["conviction_score"]
    lowest_score = scored.get((lowest["symbol"], side), {}).get("conviction_score", lowest["conviction_score"])

    if best_score <= lowest_score:
        _set_core_board(
            db, session_date, side,
            [(c["symbol"], scored.get((c["symbol"], side), {}).get("conviction_score", c["conviction_score"])) for c in sorted(new_core, key=lambda x: x["rank"])],
        )
        db.execute(text("DELETE FROM rs_conviction_challenger WHERE session_date = :d AND side = :side"), {"d": session_date, "side": side})
        return events

    ch = db.execute(
        text("SELECT challenger_symbol, displaced_symbol, cycles_won FROM rs_conviction_challenger WHERE session_date = :d AND side = :side"),
        {"d": session_date, "side": side},
    ).fetchone()
    req = int(cfg.get("promotion_cycles_required") or 2)

    if ch and ch.challenger_symbol == best["symbol"]:
        cycles = int(ch.cycles_won or 0) + 1
        if cycles >= req:
            new_core = [c for c in new_core if c["symbol"] != ch.displaced_symbol]
            new_core.append({"symbol": best["symbol"], "conviction_score": best_score, "rank": 99})
            new_core.sort(key=lambda c: -scored.get((c["symbol"], side), {}).get("conviction_score", 0))
            for i, c in enumerate(new_core[:5], start=1):
                c["rank"] = i
            events.append({"time": cycle_time.isoformat(), "side": side, "type": "promote", "symbol": best["symbol"], "replaced": ch.displaced_symbol})
            db.execute(text("DELETE FROM rs_conviction_challenger WHERE session_date = :d AND side = :side"), {"d": session_date, "side": side})
            _set_core_board(db, session_date, side, [(c["symbol"], scored.get((c["symbol"], side), {}).get("conviction_score", 0)) for c in new_core[:5]])
            db.execute(
                text(
                    """
                    INSERT INTO rs_conviction_promotion_log
                        (session_date, event_time, side, event_type, symbol, replaced_symbol, detail_json)
                    VALUES (:d, :t, :side, 'promote', :sym, :rep, :detail)
                    """
                ),
                {"d": session_date, "t": cycle_time, "side": side, "sym": best["symbol"], "rep": ch.displaced_symbol, "detail": json.dumps({"score": best_score})},
            )
        else:
            db.execute(
                text("UPDATE rs_conviction_challenger SET cycles_won = :c WHERE session_date = :d AND side = :side"),
                {"c": cycles, "d": session_date, "side": side},
            )
    else:
        db.execute(text("DELETE FROM rs_conviction_challenger WHERE session_date = :d AND side = :side"), {"d": session_date, "side": side})
        db.execute(
            text(
                """
                INSERT INTO rs_conviction_challenger (session_date, side, challenger_symbol, displaced_symbol, cycles_won)
                VALUES (:d, :side, :ch, :disp, 1)
                """
            ),
            {"d": session_date, "side": side, "ch": best["symbol"], "disp": lowest["symbol"]},
        )

    if not any(e.get("type") == "promote" for e in events):
        _set_core_board(
            db, session_date, side,
            [(c["symbol"], scored.get((c["symbol"], side), {}).get("conviction_score", c["conviction_score"])) for c in sorted(new_core, key=lambda x: x["rank"])],
        )
    return events


def run_conviction_board_cycle(
    force: bool = False,
    *,
    scheduled_hour: Optional[int] = None,
    scheduled_minute: Optional[int] = None,
) -> Dict[str, Any]:
    now = datetime.now(IST)
    if now.weekday() >= 5 and not force:
        return {"ok": False, "reason": "weekend"}
    cfg = get_config()
    if not force:
        if scheduled_hour is not None and scheduled_minute is not None:
            if not is_board_cycle_for_scheduled_minute(scheduled_hour, scheduled_minute, cfg):
                return {"ok": False, "reason": "not_board_minute"}
        elif not is_board_cycle_minute(now):
            return {"ok": False, "reason": "not_board_minute"}

    sd = today_ist()
    db = SessionLocal()
    scored: Dict[Tuple[str, str], Dict[str, Any]] = {}
    events: List[Dict[str, Any]] = []
    log_rows = 0
    try:
        scored = _score_universe(db, sd, cfg, now)
        for side in (SIDE_BULL, SIDE_BEAR):
            events.extend(_apply_hysteresis(db, sd, side, scored, cfg, now))
        for (_, side), st in scored.items():
            sym = st["symbol"]
            db.execute(
                text(
                    """
                    INSERT INTO rs_conviction_scoring_log (
                        session_date, cycle_time, symbol, side, rs_component, opening_anchor,
                        persistence_credit, slope_component, accum_component, whip_penalty,
                        conviction_score, in_raw_top5
                    ) VALUES (:d, :t, :sym, :side, :rs, :anchor, :persist, :slope, :accum, :whip, :conv, :raw)
                    """
                ),
                {
                    "d": sd, "t": now, "sym": sym, "side": side,
                    "rs": st["rs_component"], "anchor": st["opening_anchor"],
                    "persist": st["persistence_credit"], "slope": st["slope_component"],
                    "accum": st["accum_component"], "whip": st["whip_penalty"],
                    "conv": st["conviction_score"], "raw": st["in_raw_top5"],
                },
            )
            log_rows += 1
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("Conviction board cycle failed for %s: %s", sd, exc, exc_info=True)
        raise
    finally:
        db.close()

    logger.info(
        "Conviction board cycle %s: %d symbols scored, %d scoring_log rows, %d events",
        sd, len(scored), log_rows, len(events),
    )
    return {
        "ok": True,
        "session_date": sd,
        "scored": len(scored),
        "scoring_log_rows": log_rows,
        "events": events,
    }


def get_bench_symbols(db, session_date: str, side: str, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    floor = float(cfg.get("bench_persist_floor") or 30)
    core = {c["symbol"] for c in _load_core_board(db, session_date, side)}
    rows = db.execute(
        text(
            """
            SELECT symbol, persistence_credit, conviction_score, opening_anchor, accum_active
            FROM rs_conviction_state WHERE session_date = :d AND side = :side AND persistence_credit >= :floor
            ORDER BY conviction_score DESC
            """
        ),
        {"d": session_date, "side": side, "floor": floor},
    ).fetchall()
    return [
        {"symbol": r.symbol, "persistence_credit": float(r.persistence_credit or 0),
         "conviction_score": float(r.conviction_score or 0),
         "has_anchor": float(r.opening_anchor or 0) > 0, "accum_active": bool(r.accum_active)}
        for r in rows if r.symbol not in core
    ]


def get_challenger_pending(db, session_date: str, side: str) -> Optional[Dict[str, Any]]:
    r = db.execute(
        text("SELECT challenger_symbol, displaced_symbol, cycles_won FROM rs_conviction_challenger WHERE session_date = :d AND side = :side"),
        {"d": session_date, "side": side},
    ).fetchone()
    if not r:
        return None
    req = int(get_config().get("promotion_cycles_required") or 2)
    return {"challenger": r.challenger_symbol, "displaced": r.displaced_symbol,
            "cycles_won": int(r.cycles_won or 0), "cycles_required": req}


def get_conviction_board_payload() -> Dict[str, Any]:
    sd = today_ist()
    cfg = get_config()
    db = SessionLocal()
    try:
        state_map = _load_state_map(db, sd)
        bull_core = _load_core_board(db, sd, SIDE_BULL)
        bear_core = _load_core_board(db, sd, SIDE_BEAR)
        bull_bench = get_bench_symbols(db, sd, SIDE_BULL, cfg)
        bear_bench = get_bench_symbols(db, sd, SIDE_BEAR, cfg)
        bull_pending = get_challenger_pending(db, sd, SIDE_BULL)
        bear_pending = get_challenger_pending(db, sd, SIDE_BEAR)
        events = db.execute(
            text(
                """
                SELECT event_time, side, event_type, symbol, replaced_symbol
                FROM rs_conviction_promotion_log WHERE session_date = :d ORDER BY event_time DESC LIMIT 3
                """
            ),
            {"d": sd},
        ).fetchall()
    finally:
        db.close()

    from backend.services.relative_strength_scanner import get_latest_snapshot
    from backend.services.rs_setup_radar import get_radar_for_symbols, get_live_setups

    rs_snap = get_latest_snapshot()
    rs_by_sym = {r["symbol"]: r for r in rs_snap.get("bullish", []) + rs_snap.get("bearish", [])}
    monitor_syms = [c["symbol"] for c in bull_core + bear_core] + [b["symbol"] for b in bull_bench + bear_bench]
    radar_map = get_radar_for_symbols(monitor_syms)

    def enrich_core(core: List[Dict], side: str) -> List[Dict]:
        out = []
        for c in core:
            st = state_map.get((c["symbol"], side), {})
            rs = rs_by_sym.get(c["symbol"], {})
            radar = radar_map.get(c["symbol"], {})
            out.append({
                **rs, **c, **radar,
                "persistence_credit": st.get("persistence_credit", 0),
                "has_anchor": float(st.get("opening_anchor") or 0) > 0,
                "accum_active": st.get("accum_active", False),
                "whip_penalty": st.get("whip_penalty", 0),
                "chop_flag": st.get("whip_penalty", 0) >= 60,
                "ema10_10m": st.get("ema10_10m"),
            })
        return out

    fast_watch: List[Dict[str, Any]] = []
    checklist_cfg: Dict[str, Any] = {}
    if cfg.get("fast_watch_ui_enabled"):
        try:
            from backend.services.rs_fast_watch import get_fast_watch

            fast_watch = get_fast_watch(sd, off_lock_only=True)
            checklist_cfg["fast_watch_ui_enabled"] = True
        except Exception as exc:
            logger.debug("conviction board fast_watch: %s", exc)

    return {
        "session_date": sd,
        "last_board_cycle": rs_snap.get("last_updated"),
        "bullish_core": enrich_core(bull_core, SIDE_BULL),
        "bearish_core": enrich_core(bear_core, SIDE_BEAR),
        "bullish_bench": bull_bench,
        "bearish_bench": bear_bench,
        "bullish_pending": bull_pending,
        "bearish_pending": bear_pending,
        "promotion_events": [
            {"time": e.event_time.isoformat() if e.event_time else "", "side": e.side,
             "type": e.event_type, "symbol": e.symbol, "replaced": e.replaced_symbol}
            for e in events
        ],
        "live_setups": get_live_setups(),
        "fast_watch": fast_watch,
        "checklist_config": checklist_cfg,
        "bullish": enrich_core(bull_core, SIDE_BULL),
        "bearish": enrich_core(bear_core, SIDE_BEAR),
        "last_updated": rs_snap.get("last_updated"),
    }


def reset_conviction_day(session_date: Optional[str] = None) -> None:
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        for table in (
            "rs_conviction_state", "rs_conviction_board", "rs_conviction_challenger",
            "rs_conviction_promotion_log", "rs_conviction_scoring_log",
            "rs_setup_radar", "rs_setup_radar_log",
        ):
            db.execute(text(f"DELETE FROM {table} WHERE session_date = :d"), {"d": sd})
        db.commit()
    finally:
        db.close()
