"""
Final reconciliation (3:45 PM & 4:00 PM IST): align intraday_trade rows with Upstox fills
and tag late time-based exits as Exit-TM.
"""

import logging
from datetime import datetime, timedelta, time as dt_time

import pytz
from sqlalchemy import and_
from sqlalchemy.orm.attributes import flag_modified

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption
from backend.services import live_trading

logger = logging.getLogger(__name__)

# Time-based exits at/after 3:25 PM get exit_reason 'time_based'; after 3:15 PM we store 'Exit-TM'
_EXIT_TM_CUTOFF = dt_time(15, 15)


def run_final_reconciliation() -> None:
    """
    For today's trades (status != no_entry): refresh buy/sell/PnL from broker.
    Then set exit_reason to 'Exit-TM' when exit_reason was 'time_based' and sell_time >= 15:15 IST.
    Skips Sat/Sun.
    """
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if now.weekday() >= 5:
        logger.info("📋 Final reconciliation: skipped (weekend)")
        return

    db = SessionLocal()
    try:
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)

        rows = (
            db.query(IntradayStockOption)
            .filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    IntradayStockOption.trade_date < tomorrow,
                    IntradayStockOption.status != "no_entry",
                )
            )
            .all()
        )

        if not rows:
            logger.info("📋 Final reconciliation: no rows for today")
            return

        n_refresh = 0
        n_exit_tm = 0

        for trade in rows:
            try:
                if live_trading.final_reconciliation_refresh_trade_from_broker(db, trade):
                    n_refresh += 1
            except Exception as ex:
                logger.warning(
                    "Final recon broker sync failed for %s: %s",
                    getattr(trade, "stock_name", "?"),
                    ex,
                )

        cutoff_dt = ist.localize(datetime.combine(today.date(), _EXIT_TM_CUTOFF))

        for trade in rows:
            try:
                if getattr(trade, "status", None) != "sold":
                    continue
                if getattr(trade, "exit_reason", None) != "time_based":
                    continue
                st = getattr(trade, "sell_time", None)
                if not st:
                    continue
                if st.tzinfo is None:
                    st = ist.localize(st)
                else:
                    st = st.astimezone(ist)
                if st < cutoff_dt:
                    continue
                trade.exit_reason = "Exit-TM"
                flag_modified(trade, "exit_reason")
                n_exit_tm += 1
                logger.info(
                    "📋 Final recon: %s exit_reason time_based → Exit-TM (sell_time=%s)",
                    getattr(trade, "stock_name", "?"),
                    st.strftime("%H:%M"),
                )
            except Exception as ex:
                logger.warning(
                    "Final recon Exit-TM step failed for %s: %s",
                    getattr(trade, "stock_name", "?"),
                    ex,
                )

        db.commit()
        logger.info(
            "📋 Final reconciliation done: %s row(s) broker fields updated, %s → Exit-TM",
            n_refresh,
            n_exit_tm,
        )
    except Exception as e:
        logger.error("Final reconciliation failed: %s", e, exc_info=True)
        db.rollback()
    finally:
        db.close()
