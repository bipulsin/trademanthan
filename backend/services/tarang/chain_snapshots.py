"""Compressed full-chain snapshots (start immediately — cannot be recreated later)."""
from __future__ import annotations

import gzip
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import BYTEA

from backend.database import SessionLocal
from backend.services.tarang.calendar import ist_clock, mcx_session_open
from backend.services.tarang.chain_builder import ChainBuilder
from backend.services.tarang.config import get_profiles, get_risk
from backend.services.tarang.data_gaps import insert_data_gap
from backend.services.tarang.iv_snapshots import _atm_iv_from_chain
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

IV_WARMUP_DAYS_DEFAULT = 60
BACKTEST_HORIZON_DAYS_DEFAULT = 180
RETENTION_DAYS_DEFAULT = 400
ATM_WINDOW_DEFAULT = 10
NEAR_EXPIRIES_DEFAULT = 2
DELTA_ABS_MIN_DEFAULT = 0.03
DELTA_ABS_MAX_DEFAULT = 0.97


def _snapshot_cfg() -> Dict[str, Any]:
    return dict((get_risk().get("chain_snapshots") or {}))


def pack_chain_payload(payload: Dict[str, Any]) -> bytes:
    raw = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    return gzip.compress(raw, compresslevel=6)


def unpack_chain_payload(blob: bytes) -> Dict[str, Any]:
    return json.loads(gzip.decompress(blob).decode("utf-8"))


def _quote_row(q) -> Dict[str, Any]:
    d = q.to_dict() if hasattr(q, "to_dict") else dict(q)
    mark = d.get("mid")
    if mark is None:
        mark = d.get("last")
    return {
        "instrument_key": d.get("instrument_key"),
        "symbol": d.get("symbol"),
        "strike": d.get("strike"),
        "right": d.get("right"),
        "bid": d.get("bid"),
        "ask": d.get("ask"),
        "bid_qty": (d.get("meta") or {}).get("bid_qty") if isinstance(d.get("meta"), dict) else d.get("bid_qty"),
        "ask_qty": (d.get("meta") or {}).get("ask_qty") if isinstance(d.get("meta"), dict) else d.get("ask_qty"),
        "mark": mark,
        "last": d.get("last"),
        "iv": d.get("iv"),
        "delta": d.get("delta"),
        "oi": d.get("oi"),
        "volume": d.get("volume"),
        "greeks_source": d.get("greeks_source"),
        "two_sided": bool((d.get("meta") or {}).get("two_sided")) if isinstance(d.get("meta"), dict) else bool(d.get("bid") and d.get("ask")),
    }


def _chain_payload(chain, clock: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "captured_at": chain.built_at or datetime.now(timezone.utc).isoformat(),
        "underlying_price": chain.futures_or_spot,
        "expiry": chain.expiry,
        "lot_size": chain.lot_size,
        "strike_step": chain.strike_step,
        "ist_dow": clock["ist_dow"],
        "ist_dow_name": clock["ist_dow_name"],
        "ist_hour": clock["ist_hour"],
        "ist_weekend_window": clock["ist_weekend_window"],
        "quotes": [_quote_row(q) for q in (chain.quotes or [])],
    }


def _insert_iv_row(db, chain, atm: Dict[str, Any], clock: Dict[str, Any]) -> None:
    meta = {
        "n_quotes": len(chain.quotes or []),
        "error": (chain.meta or {}).get("error"),
        "ist_dow": clock["ist_dow"],
        "ist_dow_name": clock["ist_dow_name"],
        "ist_hour": clock["ist_hour"],
        "ist_weekend_window": clock["ist_weekend_window"],
        "full_chain": True,
    }
    from backend.services.tarang.rv import rv_for_profile

    rv = rv_for_profile(chain.profile_id)
    db.execute(
        text(
            """
            INSERT INTO tarang_iv_snapshots (
                profile_id, venue, underlying_symbol, expiry_date,
                futures_or_spot, atm_strike, atm_iv, atm_iv_raw, atm_iv_unit,
                realized_vol_20d, source, meta
            ) VALUES (
                :profile_id, :venue, :underlying_symbol,
                CAST(:expiry_date AS date),
                :futures_or_spot, :atm_strike, :atm_iv, :atm_iv_raw, :atm_iv_unit,
                :realized_vol_20d, :source, CAST(:meta AS jsonb)
            )
            """
        ),
        {
            "profile_id": chain.profile_id,
            "venue": chain.venue,
            "underlying_symbol": chain.underlying,
            "expiry_date": chain.expiry or None,
            "futures_or_spot": chain.futures_or_spot,
            "atm_strike": atm.get("atm_strike"),
            "atm_iv": atm.get("atm_iv"),
            "atm_iv_raw": atm.get("atm_iv_raw"),
            "atm_iv_unit": atm.get("atm_iv_unit"),
            "source": atm.get("source"),
            "realized_vol_20d": rv,
            "meta": json.dumps(meta),
        },
    )


def tag_narrow_window_snapshots(db) -> int:
    """Mark pre-widening ATM±10 rows so backtests skip them for short/wing validation."""
    res = db.execute(
        text(
            """
            UPDATE tarang_chain_snapshots
            SET meta = COALESCE(meta, '{}'::jsonb) || '{"window_kind":"narrow_window"}'::jsonb
            WHERE COALESCE(meta->>'window_kind', '') NOT IN ('delta_window', 'narrow_window')
            """
        )
    )
    return int(res.rowcount or 0)


def capture_chain_snapshots(
    profile_ids: Optional[List[str]] = None,
    *,
    venues: Optional[Sequence[str]] = None,
    respect_mcx_session: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Persist compressed delta-window chains for each in-scope expiry."""
    ensure_tarang_tables()
    cfg = _snapshot_cfg()
    min_atm = int(cfg.get("min_atm_window") or cfg.get("atm_window") or ATM_WINDOW_DEFAULT)
    profiles = get_profiles().get("profiles") or {}
    if profile_ids is None:
        profile_ids = [k for k, v in profiles.items() if v.get("enabled")]
    venue_filter = {str(v) for v in venues} if venues else None
    clock = ist_clock(now)
    builder = ChainBuilder()
    results: List[Dict[str, Any]] = []
    db = SessionLocal()
    try:
        tag_narrow_window_snapshots(db)
        for pid in profile_ids:
            prof = profiles.get(pid) or profiles.get(str(pid).upper()) or {}
            venue = str(prof.get("venue") or "")
            if venue_filter and venue not in venue_filter:
                continue
            if venue == "upstox_mcx" and respect_mcx_session and not mcx_session_open(now):
                results.append(
                    {
                        "profile_id": pid,
                        "ok": False,
                        "skipped": "mcx_session_closed",
                    }
                )
                continue
            if venue == "upstox_mcx":
                from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter

                th = UpstoxMcxAdapter().health()
                if th.get("token") in ("missing", "expired") or th.get("token_expired"):
                    insert_data_gap(
                        db,
                        venue=venue,
                        profile_id=pid,
                        reason="upstox_token_invalid_snapshot_skipped",
                        detail={"token": th.get("token")},
                    )
                    results.append({"profile_id": pid, "ok": False, "skipped": "upstox_token_invalid"})
                    continue
            try:
                expiries = builder.snapshot_expiries(pid) or [None]
            except Exception as e:
                logger.warning("tarang snapshot expiries failed %s: %s", pid, e)
                expiries = [None]
            for expiry in expiries:
                try:
                    chain = builder.build(pid, expiry=expiry, atm_window=min_atm)
                    n_quotes = len(chain.quotes or [])
                    two_sided_n = sum(
                        1
                        for q in (chain.quotes or [])
                        if q.bid is not None and q.ask is not None and float(q.bid) > 0 and float(q.ask) > 0
                    )
                    if n_quotes == 0 or two_sided_n == 0:
                        insert_data_gap(
                            db,
                            venue=venue,
                            profile_id=pid,
                            reason="no_valid_quotes",
                            detail={"expiry": chain.expiry or expiry, "n_quotes": n_quotes, "n_two_sided": two_sided_n},
                        )
                        results.append(
                            {
                                "profile_id": pid,
                                "underlying": chain.underlying,
                                "expiry": chain.expiry or expiry,
                                "ok": False,
                                "skipped": "no_valid_quotes",
                                "n_quotes": n_quotes,
                                "n_two_sided": two_sided_n,
                            }
                        )
                        continue
                    atm = _atm_iv_from_chain(chain)
                    payload = _chain_payload(chain, clock)
                    blob = pack_chain_payload(payload)
                    n_strikes = len({q.strike for q in (chain.quotes or [])})
                    win_meta = dict((chain.meta or {}).get("window") or {})
                    win_meta.update(
                        {
                            "window_kind": "delta_window",
                            "delta_abs_min": float(cfg.get("delta_abs_min") or DELTA_ABS_MIN_DEFAULT),
                            "delta_abs_max": float(cfg.get("delta_abs_max") or DELTA_ABS_MAX_DEFAULT),
                            "sigma_mult": float(cfg.get("sigma_mult") or 2.5),
                            "min_atm_window": min_atm,
                            "n_strikes": n_strikes,
                            "n_quotes": n_quotes,
                            "n_two_sided": two_sided_n,
                            "payload_bytes": len(blob),
                            "error": (chain.meta or {}).get("error"),
                            "ist_dow_name": clock["ist_dow_name"],
                        }
                    )
                    db.execute(
                        text(
                            """
                            INSERT INTO tarang_chain_snapshots (
                                profile_id, venue, underlying_symbol, expiry_date,
                                futures_or_spot, atm_strike, atm_iv,
                                ist_dow, ist_hour, ist_weekend_window,
                                n_quotes, payload_gzip, meta
                            ) VALUES (
                                :profile_id, :venue, :underlying_symbol,
                                CAST(:expiry_date AS date),
                                :futures_or_spot, :atm_strike, :atm_iv,
                                :ist_dow, :ist_hour, :ist_weekend_window,
                                :n_quotes, :payload_gzip, CAST(:meta AS jsonb)
                            )
                            """
                        ).bindparams(bindparam("payload_gzip", type_=BYTEA)),
                        {
                            "profile_id": pid,
                            "venue": chain.venue,
                            "underlying_symbol": chain.underlying,
                            "expiry_date": chain.expiry or None,
                            "futures_or_spot": chain.futures_or_spot,
                            "atm_strike": atm.get("atm_strike"),
                            "atm_iv": atm.get("atm_iv"),
                            "ist_dow": clock["ist_dow"],
                            "ist_hour": clock["ist_hour"],
                            "ist_weekend_window": clock["ist_weekend_window"],
                            "n_quotes": n_quotes,
                            "payload_gzip": blob,
                            "meta": json.dumps(win_meta),
                        },
                    )
                    _insert_iv_row(db, chain, atm, clock)
                    results.append(
                        {
                            "profile_id": pid,
                            "underlying": chain.underlying,
                            "expiry": chain.expiry,
                            "ok": True,
                            "n_quotes": n_quotes,
                            "n_strikes": n_strikes,
                            "n_two_sided": two_sided_n,
                            "atm_iv": atm.get("atm_iv"),
                            "payload_bytes": len(blob),
                            "window_kind": "delta_window",
                        }
                    )
                except Exception as e:
                    logger.exception("tarang chain snapshot failed %s %s: %s", pid, expiry, e)
                    try:
                        insert_data_gap(
                            db,
                            venue=venue,
                            profile_id=pid,
                            reason="capture_exception",
                            detail={"expiry": expiry, "error": str(e)[:200]},
                        )
                    except Exception:
                        logger.exception("data_gap insert failed after snapshot error")
                    results.append(
                        {
                            "profile_id": pid,
                            "expiry": expiry,
                            "ok": False,
                            "error": str(e)[:200],
                        }
                    )
        prune_expired_chain_snapshots(db)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ist": clock,
        "results": results,
    }


def prune_expired_chain_snapshots(db, *, now: Optional[datetime] = None) -> int:
    cfg = _snapshot_cfg()
    days = int(cfg.get("retention_days") or RETENTION_DAYS_DEFAULT)
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=days)
    res = db.execute(
        text("DELETE FROM tarang_chain_snapshots WHERE captured_at < :cutoff"),
        {"cutoff": cutoff},
    )
    return int(res.rowcount or 0)


def chain_coverage(*, warmup_days: Optional[int] = None, horizon_days: Optional[int] = None) -> Dict[str, Any]:
    """Days of full-chain history per underlying and earliest 6-month backtest date."""
    ensure_tarang_tables()
    cfg = _snapshot_cfg()
    warmup = int(warmup_days if warmup_days is not None else cfg.get("iv_warmup_days") or IV_WARMUP_DAYS_DEFAULT)
    horizon = int(
        horizon_days if horizon_days is not None else cfg.get("backtest_horizon_days") or BACKTEST_HORIZON_DAYS_DEFAULT
    )
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT
                    underlying_symbol,
                    MIN(captured_at) AS earliest,
                    MAX(captured_at) AS latest,
                    COUNT(*)::int AS n_snapshots,
                    COUNT(*) FILTER (WHERE COALESCE(meta->>'window_kind','') = 'delta_window')::int AS n_wide,
                    COUNT(*) FILTER (WHERE COALESCE(meta->>'window_kind','') = 'narrow_window')::int AS n_narrow,
                    COUNT(DISTINCT (captured_at AT TIME ZONE 'Asia/Kolkata')::date)::int AS days,
                    COUNT(DISTINCT (captured_at AT TIME ZONE 'Asia/Kolkata')::date)
                        FILTER (WHERE COALESCE(meta->>'window_kind','') = 'delta_window')::int AS days_wide
                FROM tarang_chain_snapshots
                GROUP BY underlying_symbol
                ORDER BY underlying_symbol
                """
            )
        ).mappings().all()
    finally:
        db.close()
    out_rows: List[Dict[str, Any]] = []
    for r in rows:
        earliest = r["earliest"]
        latest = r["latest"]
        days = int(r["days"] or 0)
        days_wide = int(r["days_wide"] or 0)
        earliest_date = earliest.date() if hasattr(earliest, "date") else None
        backtest_start = (earliest_date + timedelta(days=warmup)) if earliest_date else None
        six_month_ready_on = (earliest_date + timedelta(days=warmup + horizon)) if earliest_date else None
        out_rows.append(
            {
                "underlying": r["underlying_symbol"],
                "days": days,
                "days_wide": days_wide,
                "n_snapshots": int(r["n_snapshots"] or 0),
                "n_wide": int(r["n_wide"] or 0),
                "n_narrow": int(r["n_narrow"] or 0),
                "earliest": earliest.isoformat() if earliest is not None else None,
                "latest": latest.isoformat() if latest is not None else None,
                "iv_warmup_days": warmup,
                "earliest_backtest_start": backtest_start.isoformat() if backtest_start else None,
                "six_month_backtest_ready_on": six_month_ready_on.isoformat() if six_month_ready_on else None,
                "days_needed_for_6m": warmup + horizon,
                "ready_for_6m_backtest": days_wide >= (warmup + horizon),
            }
        )
    return {
        "iv_warmup_days": warmup,
        "backtest_horizon_days": horizon,
        "days_needed_for_6m": warmup + horizon,
        "note": (
            "A 6-month backtest needs 60 calendar days of IV snapshots before the window, "
            "then ~180 days of full-chain history. Earliest start = first snapshot date + 60d."
        ),
        "underlyings": out_rows,
    }
