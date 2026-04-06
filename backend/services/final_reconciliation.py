"""
Final reconciliation (3:45 PM & 4:00 PM IST): align intraday_trade rows with Upstox fills
and tag late time-based exits as Exit-TM.

Also: for status=bought, detect completed SELL on Upstox for the same instrument/qty (today's
order book) and close the row (bought→sold) with broker sell_price and sell_order_id.
"""

import logging
from datetime import date as date_type, datetime, timedelta, time as dt_time
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import and_
from sqlalchemy.orm.attributes import flag_modified

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption
from backend.services import live_trading

logger = logging.getLogger(__name__)

# Time-based exits at/after 3:25 PM get exit_reason 'time_based'; after 3:15 PM we store 'Exit-TM'
_EXIT_TM_CUTOFF = dt_time(15, 15)

# When closing bought→sold from broker with no prior exit_reason
_EXIT_REASON_FINAL_RECON = "final_reconciliation"


def run_final_reconciliation(
    force: bool = False,
    as_of_date: Optional[date_type] = None,
) -> Dict[str, Any]:
    """
    For today's trades (including no_entry when instrument_key/qty allow broker match):
    - no_entry: try Upstox BUY fill → bought (apply_broker_buy_fill_to_intraday_trade)
    - bought: try broker exit (SELL fill) → sold + sell_price + sell_order_id + pnl
    - bought/sold: refresh buy/sell/PnL from broker (sold with sell_price 0 still matches SELL leg)
    Then set exit_reason to 'Exit-TM' when exit_reason was 'time_based' and sell_time >= 15:15 IST.
    Skips Sat/Sun unless force=True (manual /scan/run-final-reconciliation?force=true).

    as_of_date: optional IST calendar day for trade_date filter (manual backfill); default is today IST.

    Returns a summary dict (for logging and /scan/run-final-reconciliation).
    """
    result: Dict[str, Any] = {
        "success": False,
        "weekend_skipped": False,
        "bought_to_sold": 0,
        "bought_to_sold_rows": [],
        "broker_refresh": 0,
        "exit_tm": 0,
    }

    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if not force and now.weekday() >= 5:
        logger.info("📋 Final reconciliation: skipped (weekend)")
        result["weekend_skipped"] = True
        result["success"] = True
        return result

    db = SessionLocal()
    try:
        if as_of_date is not None:
            today = ist.localize(
                datetime.combine(as_of_date, dt_time(0, 0, 0))
            ).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)

        rows = (
            db.query(IntradayStockOption)
            .filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    IntradayStockOption.trade_date < tomorrow,
                )
            )
            .all()
        )

        if not rows:
            logger.info("📋 Final reconciliation: no rows for today")
            result["success"] = True
            return result

        n_bought_to_sold = 0
        bought_to_sold_rows: List[Dict[str, Any]] = []
        n_refresh = 0
        n_exit_tm = 0

        for trade in rows:
            try:
                if getattr(trade, "status", None) == "no_entry":
                    live_trading.apply_broker_buy_fill_to_intraday_trade(db, trade)
            except Exception as ex:
                logger.warning(
                    "Final recon no_entry→broker BUY sync failed for %s: %s",
                    getattr(trade, "stock_name", "?"),
                    ex,
                )

        for trade in rows:
            try:
                if getattr(trade, "status", None) == "bought":
                    if live_trading.reconcile_intraday_exit_from_broker(
                        db,
                        trade,
                        default_exit_reason=_EXIT_REASON_FINAL_RECON,
                    ):
                        n_bought_to_sold += 1
                        st_sell = getattr(trade, "sell_time", None)
                        bought_to_sold_rows.append(
                            {
                                "id": trade.id,
                                "stock_name": getattr(trade, "stock_name", None),
                                "status": getattr(trade, "status", None),
                                "sell_order_id": getattr(trade, "sell_order_id", None),
                                "sell_price": getattr(trade, "sell_price", None),
                                "sell_time": st_sell.isoformat() if st_sell is not None else None,
                                "buy_price": getattr(trade, "buy_price", None),
                                "pnl": getattr(trade, "pnl", None),
                                "exit_reason": getattr(trade, "exit_reason", None),
                            }
                        )
            except Exception as ex:
                logger.warning(
                    "Final recon bought→sold failed for %s: %s",
                    getattr(trade, "stock_name", "?"),
                    ex,
                )

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
        result["success"] = True
        result["trade_date"] = today.date().isoformat()
        result["bought_to_sold"] = n_bought_to_sold
        result["bought_to_sold_rows"] = bought_to_sold_rows
        result["broker_refresh"] = n_refresh
        result["exit_tm"] = n_exit_tm
        logger.info(
            "📋 Final reconciliation done (trade_date %s IST): bought→sold=%s %s, broker_refresh=%s, Exit-TM=%s",
            today.date().isoformat(),
            n_bought_to_sold,
            bought_to_sold_rows,
            n_refresh,
            n_exit_tm,
        )
        return result
    except Exception as e:
        logger.error("Final reconciliation failed: %s", e, exc_info=True)
        db.rollback()
        result["success"] = False
        result["error"] = str(e)
        return result
    finally:
        db.close()
