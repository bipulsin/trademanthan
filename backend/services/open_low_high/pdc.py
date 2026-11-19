"""arbitrage_master ``equity_prev_close`` (PDC) fill — EOD after 4pm + 09:00 null cross-check."""
from __future__ import annotations

import logging
import threading
import time as time_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import inspect, text

from backend.database import SessionLocal, engine
from backend.services.open_high_low.config import NIFTY_INDEX_KEY
from backend.services.open_high_low.data import previous_trading_day
from backend.services.open_low_high.ohl_log import ohl_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Column name on arbitrage_master (equity previous-day close).
PDC_COLUMN = "equity_prev_close"
PDC_DATE_COLUMN = "equity_prev_close_date"

_pdc_lock = threading.Lock()


def _now() -> datetime:
    return datetime.now(IST)


def _upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def ensure_pdc_columns() -> None:
    """Add ``equity_prev_close`` (+ as-of date) to arbitrage_master if missing."""
    insp = inspect(engine)
    if "arbitrage_master" not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns("arbitrage_master")}
    with engine.begin() as conn:
        if PDC_COLUMN not in existing:
            conn.execute(
                text(f"ALTER TABLE arbitrage_master ADD COLUMN {PDC_COLUMN} DOUBLE PRECISION")
            )
            logger.info("arbitrage_master: added column %s", PDC_COLUMN)
        if PDC_DATE_COLUMN not in existing:
            conn.execute(
                text(f"ALTER TABLE arbitrage_master ADD COLUMN {PDC_DATE_COLUMN} DATE")
            )
            logger.info("arbitrage_master: added column %s", PDC_DATE_COLUMN)


def _resolve_asof(trade_date: date, *, eod: bool) -> date:
    """Which session's close to store.

    - Morning cross-check: previous trading day relative to ``trade_date``.
    - EOD after 16:00: today's close (becomes tomorrow's PDC).
    """
    if eod:
        return trade_date
    return previous_trading_day(trade_date)


def _load_equity_rows(*, only_null: bool) -> List[Tuple[str, str]]:
    ensure_pdc_columns()
    db = SessionLocal()
    try:
        where = f"AND ({PDC_COLUMN} IS NULL)" if only_null else ""
        rows = db.execute(
            text(
                f"""
                SELECT UPPER(TRIM(stock)) AS symbol,
                       NULLIF(TRIM(stock_instrument_key), '') AS ik
                FROM arbitrage_master
                WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                  AND stock_instrument_key IS NOT NULL
                  AND TRIM(stock_instrument_key) <> ''
                  AND UPPER(TRIM(stock)) NOT IN ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
                  {where}
                ORDER BY 1
                """
            )
        ).fetchall()
        out: List[Tuple[str, str]] = []
        for r in rows:
            sym = str(r[0] or "").strip().upper()
            ik = str(r[1] or "").strip().replace(":", "|")
            if not sym or not ik:
                continue
            if not ik.upper().startswith("NSE_EQ"):
                if ik.upper().startswith("INE") and "|" not in ik:
                    ik = f"NSE_EQ|{ik}"
                else:
                    continue
            out.append((sym, ik))
        return out
    finally:
        db.close()


def _write_pdc(symbol: str, close: float, asof: date) -> None:
    db = SessionLocal()
    try:
        db.execute(
            text(
                f"""
                UPDATE arbitrage_master
                SET {PDC_COLUMN} = :pc, {PDC_DATE_COLUMN} = :asof
                WHERE UPPER(TRIM(stock)) = :sym
                """
            ),
            {"pc": float(close), "asof": asof, "sym": symbol},
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_equity_prev_close(symbol: str) -> Optional[float]:
    ensure_pdc_columns()
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                f"""
                SELECT {PDC_COLUMN}, {PDC_DATE_COLUMN} FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) = :sym
                LIMIT 1
                """
            ),
            {"sym": str(symbol or "").strip().upper()},
        ).fetchone()
        if not row or row[0] is None:
            return None
        # Soft corporate-action / stale check: asof should be previous trading day.
        try:
            asof = row[1]
            if asof is not None:
                expected = previous_trading_day(_now().date())
                asof_d = asof if isinstance(asof, date) else date.fromisoformat(str(asof)[:10])
                if asof_d != expected and asof_d != _now().date():
                    ohl_log(
                        "failed",
                        action="pdc_asof_mismatch",
                        symbol=str(symbol or "").upper(),
                        detail=f"stored={asof_d} expected={expected}",
                    )
        except Exception:
            pass
        return float(row[0])
    except Exception:
        return None
    finally:
        db.close()


def _close_on_asof(upstox: Any, instrument_key: str, asof: date, trade_date: date) -> Optional[float]:
    from backend.services.open_low_high.screener import _bars_from_rows, _hist_range_end

    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="days/1",
            days_back=10,
            range_end_date=_hist_range_end(max(trade_date, asof)),
        )
    except Exception as exc:
        ohl_log(
            "failed",
            action="pdc_fetch",
            symbol=instrument_key,
            source="rest",
            detail=str(exc)[:160],
        )
        return None
    bars = _bars_from_rows(list(raw or []), instrument_key)
    for b in bars:
        if b.session_date == asof:
            return float(b.close)
    earlier = [b for b in bars if b.session_date <= asof]
    if earlier:
        return float(earlier[-1].close)
    return None


def fill_equity_prev_close(
    *,
    session: Optional[date] = None,
    only_null: bool = False,
    eod: bool = False,
    max_workers: int = 6,
    include_index: bool = True,
) -> Dict[str, Any]:
    """Fetch Upstox daily close into ``arbitrage_master.equity_prev_close``."""
    if not _pdc_lock.acquire(blocking=False):
        return {"ok": False, "status": "busy", "message": "PDC fill already running."}
    trade_date = session or _now().date()
    # Auto-detect EOD when called after 16:00 IST on the same calendar day.
    if not eod and _now().date() == trade_date and _now().time().hour >= 16:
        eod = True
    asof = _resolve_asof(trade_date, eod=eod)
    started = _now()
    try:
        ensure_pdc_columns()
        upstox = _upstox()
        rows = _load_equity_rows(only_null=only_null)
        ok = 0
        fail = 0

        def _one(item: Tuple[str, str]) -> Tuple[str, Optional[float]]:
            sym, ik = item
            return sym, _close_on_asof(upstox, ik, asof, trade_date)

        with ThreadPoolExecutor(max_workers=max(1, min(max_workers, 8))) as pool:
            futs = [pool.submit(_one, item) for item in rows]
            for fut in as_completed(futs):
                try:
                    sym, pc = fut.result()
                except Exception:
                    fail += 1
                    continue
                if pc is not None:
                    try:
                        _write_pdc(sym, pc, asof)
                        ok += 1
                        ohl_log(
                            "success",
                            action="pdc_fill",
                            symbol=sym,
                            source="rest",
                            detail=f"pc={pc} asof={asof}",
                        )
                    except Exception:
                        fail += 1
                        ohl_log("failed", action="pdc_write", symbol=sym, source="db", detail="write failed")
                else:
                    fail += 1
                    ohl_log("failed", action="pdc_fill", symbol=sym, source="rest", detail="null")
                time_mod.sleep(0.02)

        nifty_pc = None
        if include_index:
            nifty_pc = _close_on_asof(upstox, NIFTY_INDEX_KEY, asof, trade_date)
            ohl_log(
                "success" if nifty_pc is not None else "failed",
                action="pdc_fill_index",
                symbol="NIFTY",
                source="rest",
                detail=f"pc={nifty_pc} asof={asof}",
            )

        elapsed = (_now() - started).total_seconds()
        payload = {
            "ok": True,
            "status": "filled",
            "column": PDC_COLUMN,
            "trade_date": trade_date.isoformat(),
            "asof": asof.isoformat(),
            "eod": eod,
            "only_null": only_null,
            "ok_n": ok,
            "fail_n": fail,
            "universe": len(rows),
            "nifty_prev_close": nifty_pc,
            "elapsed_sec": round(elapsed, 1),
            "message": f"PDC fill {ok}/{len(rows)} (asof={asof}) in {elapsed:.1f}s",
        }
        logger.info(
            "OHL pdc fill column=%s ok=%s fail=%s asof=%s nifty=%s eod=%s",
            PDC_COLUMN,
            ok,
            fail,
            asof,
            nifty_pc,
            eod,
        )
        return payload
    except Exception as exc:
        logger.exception("OHL pdc fill failed")
        ohl_log("failed", action="pdc_fill", source="rest", detail=str(exc)[:160])
        return {"ok": False, "status": "error", "message": str(exc), "column": PDC_COLUMN}
    finally:
        _pdc_lock.release()


def crosscheck_pdc_at_0900(*, session: Optional[date] = None) -> Dict[str, Any]:
    """09:00 IST: any null PDC → fetch and fill (keys from latest arbitrage_master).

    Remaining nulls after repair are logged + alerted — never silently used in screen.
    """
    trade_date = session or _now().date()
    ohl_log("success", action="pdc_crosscheck_start", detail=f"session={trade_date}")
    ensure_pdc_columns()
    result = fill_equity_prev_close(
        session=trade_date, only_null=True, eod=False, include_index=True
    )
    result["action"] = "pdc_crosscheck_0900"
    # Re-check remaining nulls after repair.
    still_null = _load_equity_rows(only_null=True)
    result["still_null_n"] = len(still_null)
    result["still_null_symbols"] = [s for s, _ in still_null[:40]]
    if still_null or result.get("nifty_prev_close") is None:
        ohl_log(
            "failed",
            action="pdc_crosscheck_nulls",
            detail=(
                f"still_null={len(still_null)} "
                f"nifty={result.get('nifty_prev_close')} "
                f"exclude_from_screen=True"
            ),
        )
        try:
            from backend.services.open_low_high.alerts import alert_degraded

            alert_degraded(
                f"PDC null after 09:00 repair: equities={len(still_null)} "
                f"nifty={result.get('nifty_prev_close')}",
                trade_date=trade_date.isoformat(),
                still_null_n=len(still_null),
            )
        except Exception:
            pass
    return result


def fill_pdc_eod(*, session: Optional[date] = None) -> Dict[str, Any]:
    """After-4pm trading-day job: store today's equity/index close as PDC."""
    trade_date = session or _now().date()
    ohl_log("success", action="pdc_eod_start", detail=f"session={trade_date}")
    result = fill_equity_prev_close(
        session=trade_date, only_null=False, eod=True, include_index=True
    )
    result["action"] = "pdc_eod"
    return result


def pdc_completeness(
    *,
    session: Optional[date] = None,
    min_equities: int = 145,
) -> Dict[str, Any]:
    """Prev-close readiness for OLH health: ≥``min_equities`` + Nifty present."""
    ensure_pdc_columns()
    trade_date = session or _now().date()
    expected_asof = previous_trading_day(trade_date)
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                f"""
                SELECT
                  COUNT(*) FILTER (
                    WHERE {PDC_COLUMN} IS NOT NULL
                      AND stock_instrument_key IS NOT NULL
                      AND TRIM(stock_instrument_key) <> ''
                      AND UPPER(TRIM(stock)) NOT IN
                          ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
                  ) AS filled_n,
                  COUNT(*) FILTER (
                    WHERE stock_instrument_key IS NOT NULL
                      AND TRIM(stock_instrument_key) <> ''
                      AND UPPER(TRIM(stock)) NOT IN
                          ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
                  ) AS universe_n,
                  COUNT(*) FILTER (
                    WHERE {PDC_COLUMN} IS NULL
                      AND stock_instrument_key IS NOT NULL
                      AND TRIM(stock_instrument_key) <> ''
                      AND UPPER(TRIM(stock)) NOT IN
                          ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
                  ) AS null_n
                FROM arbitrage_master
                WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                """
            )
        ).fetchone()
        filled_n = int((row[0] if row else 0) or 0)
        universe_n = int((row[1] if row else 0) or 0)
        null_n = int((row[2] if row else 0) or 0)
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc)[:160],
            "trade_date": trade_date.isoformat(),
            "expected_asof": expected_asof.isoformat(),
            "min_equities": int(min_equities),
        }
    finally:
        db.close()

    nifty_pc = get_equity_prev_close("NIFTY")
    # Nifty may live only in fill result / index path — also accept master row.
    if nifty_pc is None:
        try:
            from backend.services.open_low_high.screener import _fetch_daily_prev, _upstox

            _ph, _pl, nifty_pc = _fetch_daily_prev(_upstox(), NIFTY_INDEX_KEY, trade_date)
        except Exception:
            nifty_pc = None

    equities_ok = filled_n >= int(min_equities)
    nifty_ok = nifty_pc is not None
    return {
        "ok": bool(equities_ok and nifty_ok),
        "trade_date": trade_date.isoformat(),
        "expected_asof": expected_asof.isoformat(),
        "min_equities": int(min_equities),
        "equities_filled": filled_n,
        "equities_universe": universe_n,
        "equities_null": null_n,
        "equities_ok": equities_ok,
        "nifty_prev_close": float(nifty_pc) if nifty_pc is not None else None,
        "nifty_ok": nifty_ok,
    }
