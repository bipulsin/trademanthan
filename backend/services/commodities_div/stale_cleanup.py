"""Stale Divergence cleanup for Commodities Div (CommDiv).

Trading calendar
----------------
MCX commodities use the shared ``holiday`` table + weekends via
``market_holiday`` (same helper Tarang uses for ``mcx_is_holiday_or_weekend``).
There is no separate MCX holiday CSV in this repo; session trading days are
therefore NSE-listed closed dates + Sat/Sun, evaluated in Asia/Kolkata.

Rule
----
A row stays Active while status is Divergence and no BULL-GO / BEAR-GO has
arrived. After **3 full MCX session trading days** counted strictly after the
IST calendar date of ``div_received_at`` (through today's IST date inclusive),
the row is marked Rejected with remark
``stale DIV — no GO in 3 trading days`` and drops off Active.
Activated / In-Trade / Exit Trade / History are never touched.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.commodities_div.webhook import STATUS_DIVERGENCE, STATUS_REJECTED
from backend.services.ist_datetime import naive_ist

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

STALE_DIV_TRADING_DAYS = 3
STALE_DIV_REMARK = "stale DIV — no GO in 3 trading days"


def _to_ist(dt: Optional[datetime] = None) -> datetime:
    if dt is None:
        return datetime.now(IST)
    if dt.tzinfo is None:
        # Naive timestamps in this desk are IST wall-clock (div_received_at).
        return IST.localize(dt)
    return dt.astimezone(IST)


def _holiday_dates() -> Set[date]:
    try:
        from backend.services.market_holiday import refresh_holiday_dates_from_db

        return set(refresh_holiday_dates_from_db() or ())
    except Exception:
        logger.debug("commodities_div stale: holiday load failed", exc_info=True)
        return set()


def is_mcx_session_trading_day(d: date, *, holidays: Optional[Set[date]] = None) -> bool:
    """True for weekday IST dates not listed in ``holiday`` (NSE calendar proxy for MCX)."""
    if d.weekday() >= 5:
        return False
    hol = holidays if holidays is not None else _holiday_dates()
    return d not in hol


def count_trading_days_after(
    start: date,
    end: date,
    *,
    holidays: Optional[Set[date]] = None,
) -> int:
    """Count session trading days strictly after *start* through *end* inclusive."""
    if end <= start:
        return 0
    hol = holidays if holidays is not None else _holiday_dates()
    n = 0
    cur = start + timedelta(days=1)
    while cur <= end:
        if is_mcx_session_trading_day(cur, holidays=hol):
            n += 1
        cur += timedelta(days=1)
    return n


def trading_days_since_div(
    div_received_at: datetime,
    now: Optional[datetime] = None,
    *,
    holidays: Optional[Set[date]] = None,
) -> int:
    """Trading days elapsed after DIV's IST date through *now*'s IST date."""
    div_d = _to_ist(div_received_at).date()
    now_d = _to_ist(now).date()
    return count_trading_days_after(div_d, now_d, holidays=holidays)


def is_stale_divergence(
    *,
    status: Any,
    go_received_at: Any,
    div_received_at: Any,
    now: Optional[datetime] = None,
    holidays: Optional[Set[date]] = None,
    min_trading_days: int = STALE_DIV_TRADING_DAYS,
) -> bool:
    if str(status or "") != STATUS_DIVERGENCE:
        return False
    if go_received_at is not None:
        return False
    if div_received_at is None:
        return False
    return (
        trading_days_since_div(div_received_at, now, holidays=holidays) >= min_trading_days
    )


def reject_stale_divergences(
    *,
    now: Optional[datetime] = None,
    holidays: Optional[Set[date]] = None,
    remark: str = STALE_DIV_REMARK,
) -> Dict[str, Any]:
    """
    Mark stale Divergence rows Rejected. Returns summary with rejected ids.

    Safe to call from a morning IST cron or ad hoc; skips non-Divergence / GO rows.
    """
    ensure_commodities_div_tables()
    now_ist = _to_ist(now)
    hol = holidays if holidays is not None else _holiday_dates()
    db = SessionLocal()
    rejected: List[Dict[str, Any]] = []
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol_raw, symbol_mapped, direction, status,
                       div_received_at, go_received_at, contract
                FROM commodities_div_signals
                WHERE status = 'Divergence'
                  AND go_received_at IS NULL
                ORDER BY id ASC
                """
            )
        ).mappings().all()
        for row in rows:
            if not is_stale_divergence(
                status=row["status"],
                go_received_at=row["go_received_at"],
                div_received_at=row["div_received_at"],
                now=now_ist,
                holidays=hol,
            ):
                continue
            rid = int(row["id"])
            db.execute(
                text(
                    """
                    UPDATE commodities_div_signals
                    SET status = :status,
                        status_remark = :remark,
                        updated_at = NOW()
                    WHERE id = :id
                      AND status = 'Divergence'
                      AND go_received_at IS NULL
                    """
                ),
                {
                    "id": rid,
                    "status": STATUS_REJECTED,
                    "remark": remark,
                },
            )
            db.execute(
                text(
                    """
                    UPDATE commodities_div_webhook_log
                    SET active_signal_id = NULL
                    WHERE active_signal_id = :id
                    """
                ),
                {"id": rid},
            )
            rejected.append(
                {
                    "id": rid,
                    "symbol_mapped": row["symbol_mapped"],
                    "symbol_raw": row["symbol_raw"],
                    "contract": row.get("contract"),
                    "div_received_at": naive_ist(row["div_received_at"]).isoformat()
                    if isinstance(row["div_received_at"], datetime)
                    else str(row["div_received_at"]),
                    "trading_days": trading_days_since_div(
                        row["div_received_at"], now_ist, holidays=hol
                    ),
                    "status_remark": remark,
                }
            )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("commodities_div stale DIV cleanup failed")
        raise
    finally:
        db.close()

    if rejected:
        logger.info(
            "commodities_div stale DIV rejected count=%s ids=%s",
            len(rejected),
            [r["id"] for r in rejected],
        )
    else:
        logger.debug("commodities_div stale DIV cleanup: none")
    return {
        "ok": True,
        "as_of_ist": now_ist.strftime("%Y-%m-%d %H:%M:%S"),
        "rejected_count": len(rejected),
        "rejected": rejected,
        "calendar": "MCX session days via NSE holiday table + weekends (IST)",
        "min_trading_days": STALE_DIV_TRADING_DAYS,
        "remark": remark,
    }
