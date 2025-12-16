"""
VWAP Updater Service
Updates stock VWAP hourly for all open positions during market hours
"""

import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, or_

# Add parent directory to path for imports
from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption, HistoricalMarketData

logger = logging.getLogger(__name__)


def historical_data_exists(db: Session, stock_name: str, scan_time: datetime, time_window_minutes: int = 2) -> bool:
    """
    Check if historical market data already exists for a stock at a specific time.
    Uses a time window to account for slight timing differences between different schedulers.
    
    Args:
        db: Database session
        stock_name: Name of the stock
        scan_time: The time to check for
        time_window_minutes: Time window in minutes (default 2 minutes)
    
    Returns:
        True if historical data exists within the time window, False otherwise
    """
    try:
        time_window_start = scan_time - timedelta(minutes=time_window_minutes)
        time_window_end = scan_time + timedelta(minutes=time_window_minutes)
        
        existing = db.query(HistoricalMarketData).filter(
            and_(
                HistoricalMarketData.stock_name == stock_name,
                HistoricalMarketData.scan_date >= time_window_start,
                HistoricalMarketData.scan_date <= time_window_end
            )
        ).first()
        
        return existing is not None
    except Exception as e:
        logger.warning(f"⚠️ Error checking existing historical data for {stock_name}: {str(e)}")
        return False  # If check fails, allow save (fail-safe)

class VWAPUpdater:
    """Scheduler for updating stock VWAP hourly during market hours"""
    
    def __init__(self):
        # AsyncIOScheduler creates its own event loop in a background thread
        # Don't pass event_loop parameter - let it handle it automatically
        self.scheduler = AsyncIOScheduler(timezone='Asia/Kolkata')
        self.is_running = False
        
    def start(self):
        """Start the VWAP updater scheduler"""
        if not self.is_running:
            # Update VWAP every hour during market hours (9:30 AM - 3:30 PM)
            for hour in range(9, 16):  # 9 AM to 3 PM
                # Run at 15 minutes past each hour (e.g., 9:15, 10:15, 11:15, etc.)
                self.scheduler.add_job(
                    update_vwap_for_all_open_positions,
                    trigger=CronTrigger(hour=hour, minute=15, timezone='Asia/Kolkata'),
                    id=f'vwap_update_{hour}',
                    name=f'Update VWAP {hour:02d}:15',
                    replace_existing=True
                )
            
            # Close all open trades at 3:25 PM (before market close)
            self.scheduler.add_job(
                close_all_open_trades,
                trigger=CronTrigger(hour=15, minute=25, timezone='Asia/Kolkata'),
                id='close_all_trades_eod',
                name='Close All Open Trades at 3:25 PM',
                replace_existing=True
            )
            
            # Cycle-based VWAP slope calculations
            # Cycle 1: 10:30 AM - Stocks from 10:15 AM webhook
            async def run_cycle_1():
                # #region agent log
                # Log scheduler trigger
                import json
                import os
                log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
                try:
                    os.makedirs(os.path.dirname(log_path), exist_ok=True)
                    with open(log_path, 'a') as f:
                        f.write(json.dumps({"id":f"log_scheduler_trigger_cycle1","timestamp":int(datetime.now(pytz.timezone('Asia/Kolkata')).timestamp()*1000),"location":"vwap_updater.py:89","message":"Scheduler triggered Cycle 1","data":{"scheduled":True},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"SCHEDULER"}) + "\n")
                except Exception as log_err:
                    logger.error(f"Failed to write scheduler log: {str(log_err)}")
                # #endregion
                await calculate_vwap_slope_for_cycle(1, datetime.now(pytz.timezone('Asia/Kolkata')))
            self.scheduler.add_job(
                run_cycle_1,
                trigger=CronTrigger(hour=10, minute=30, timezone='Asia/Kolkata'),
                id='cycle_1_vwap_slope_10_30',
                name='Cycle 1: VWAP Slope at 10:30 AM',
                replace_existing=True
            )
            
            # Cycle 2: 11:15 AM - Stocks from 11:15 AM webhook + No_Entry from 10:15 AM
            async def run_cycle_2():
                await calculate_vwap_slope_for_cycle(2, datetime.now(pytz.timezone('Asia/Kolkata')))
            self.scheduler.add_job(
                run_cycle_2,
                trigger=CronTrigger(hour=11, minute=15, timezone='Asia/Kolkata'),
                id='cycle_2_vwap_slope_11_15',
                name='Cycle 2: VWAP Slope at 11:15 AM',
                replace_existing=True
            )
            
            # Cycle 3: 12:15 PM - Stocks from 12:15 PM webhook + No_Entry up to 11:15 AM
            async def run_cycle_3():
                await calculate_vwap_slope_for_cycle(3, datetime.now(pytz.timezone('Asia/Kolkata')))
            self.scheduler.add_job(
                run_cycle_3,
                trigger=CronTrigger(hour=12, minute=15, timezone='Asia/Kolkata'),
                id='cycle_3_vwap_slope_12_15',
                name='Cycle 3: VWAP Slope at 12:15 PM',
                replace_existing=True
            )
            
            # Cycle 4: 13:15 PM - Stocks from 13:15 PM webhook + No_Entry up to 12:15 PM
            async def run_cycle_4():
                await calculate_vwap_slope_for_cycle(4, datetime.now(pytz.timezone('Asia/Kolkata')))
            self.scheduler.add_job(
                run_cycle_4,
                trigger=CronTrigger(hour=13, minute=15, timezone='Asia/Kolkata'),
                id='cycle_4_vwap_slope_13_15',
                name='Cycle 4: VWAP Slope at 13:15 PM',
                replace_existing=True
            )
            
            # Cycle 5: 14:15 PM - Stocks from 14:15 PM webhook + No_Entry up to 13:15 PM
            async def run_cycle_5():
                await calculate_vwap_slope_for_cycle(5, datetime.now(pytz.timezone('Asia/Kolkata')))
            self.scheduler.add_job(
                run_cycle_5,
                trigger=CronTrigger(hour=14, minute=15, timezone='Asia/Kolkata'),
                id='cycle_5_vwap_slope_14_15',
                name='Cycle 5: VWAP Slope at 14:15 PM',
                replace_existing=True
            )
            
            self.scheduler.start()
            self.is_running = True
            logger.info("✅ Market Data Updater started - Hourly updates + EOD exits at 3:25 PM + EOD VWAP at 3:30 PM")
    
    def stop(self):
        """Stop the VWAP updater"""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("VWAP Updater stopped")
    
    def run_now(self):
        """Manually trigger VWAP update (for testing)"""
        logger.info("Manually triggering VWAP update...")
        self.scheduler.add_job(
            update_vwap_for_all_open_positions,
            id='vwap_update_manual',
            replace_existing=True
        )


async def update_vwap_for_all_open_positions():
    """
    Update Stock VWAP, Stock LTP, and Option LTP for all open positions (not yet sold)
    This runs hourly during market hours
    
    Updates:
    - stock_vwap: Current VWAP of underlying stock
    - stock_ltp: Current Last Traded Price of stock
    - sell_price: Current Last Traded Price of option contract (for monitoring)
    
    These values are used for exit decisions (VWAP cross, stop loss, target, etc.)
    """
    # CRITICAL: Log function entry IMMEDIATELY - use print as ultimate fallback
    import sys
    try:
        import pytz as pytz_module
        from datetime import datetime as dt_module
        ist_temp = pytz_module.timezone('Asia/Kolkata')
        now_temp = dt_module.now(ist_temp)
        entry_msg = f"🚀 FUNCTION ENTRY: update_vwap_for_all_open_positions() called at {now_temp.strftime('%Y-%m-%d %H:%M:%S IST')}"
        print(entry_msg, file=sys.stderr)  # Print to stderr as ultimate fallback
        try:
            logger.info(entry_msg)
        except Exception:
            pass  # If logger fails, print already captured it
    except Exception as entry_err:
        print(f"CRITICAL: Failed to log function entry: {entry_err}", file=sys.stderr)
    
    # #region agent log
    import json as json_module
    import os as os_module
    log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
    try:
        import pytz as pytz_module_inner
        from datetime import datetime as dt_module_inner
        ist_temp_inner = pytz_module_inner.timezone('Asia/Kolkata')
        now_temp_inner = dt_module_inner.now(ist_temp_inner)
        
        os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
        with open(log_path, 'a') as f:
            entry_log = json_module.dumps({
                "id": f"log_hourly_update_entry_{int(now_temp_inner.timestamp())}",
                "timestamp": int(now_temp_inner.timestamp() * 1000),
                "location": "vwap_updater.py:176",
                "message": "Hourly update function entry",
                "data": {"function": "update_vwap_for_all_open_positions", "time": str(now_temp_inner)},
                "sessionId": "debug-session",
                "runId": "sell-price-fix",
                "hypothesisId": "ENTRY"
            }) + "\n"
            f.write(entry_log)
            f.flush()
        try:
            logger.info(f"✅ Debug log entry written to {log_path}")
        except Exception:
            pass
    except Exception as log_err:
        print(f"CRITICAL: Failed to write debug log entry: {log_err}", file=sys.stderr)
        try:
            logger.error(f"❌ Failed to write debug log entry: {str(log_err)}")
            import traceback
            logger.error(traceback.format_exc())
        except Exception:
            pass
    # #endregion
    
    # CRITICAL: Verify database session can be created
    try:
        db = SessionLocal()
        logger.info(f"✅ Database session created successfully")
    except Exception as db_err:
        import sys
        error_msg = f"❌ CRITICAL: Failed to create database session: {str(db_err)}"
        print(error_msg, file=sys.stderr)
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        return
    
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Make log_path available in function scope
        import json as json_module
        import os as os_module
        log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
        
        logger.info(f"📊 Starting hourly market data update at {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
        logger.info(f"🔍 DEBUG: Function update_vwap_for_all_open_positions() called at {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
        logger.info(f"🔍 DEBUG: Database session active: {db.is_active if hasattr(db, 'is_active') else 'unknown'}")
        
        # Import VWAP service
        try:
            from services.upstox_service import upstox_service
            vwap_service = upstox_service
            logger.info(f"✅ VWAP service imported successfully")
        except ImportError as import_err:
            error_msg = f"❌ CRITICAL: Could not import upstox_service: {str(import_err)}"
            logger.error(error_msg)
            import sys
            print(error_msg, file=sys.stderr)
            import traceback
            logger.error(traceback.format_exc())
            return
        
        # ═══════════════════════════════════════════════════════════════
        # RE-EVALUATE "no_entry" TRADES: Check if conditions are now met
        # ═══════════════════════════════════════════════════════════════
        # If a trade had "no_entry" status due to VWAP slope or candle size
        # conditions not being met at alert time, but conditions become
        # favorable later, enter the trade with current time and prices
        # ═══════════════════════════════════════════════════════════════
        no_entry_trades = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.status == 'no_entry',
                IntradayStockOption.exit_reason == None
            )
        ).all()
        
        if no_entry_trades:
            logger.info(f"🔍 Found {len(no_entry_trades)} 'no_entry' trades to re-evaluate")
            
            # Check index trends
            index_trends = vwap_service.check_index_trends()
            nifty_trend = index_trends.get("nifty_trend", "unknown")
            banknifty_trend = index_trends.get("banknifty_trend", "unknown")
            
            # Check if time is before 3:00 PM
            is_before_3pm = now.hour < 15
            
            # Re-evaluate each no_entry trade
            for no_entry_trade in no_entry_trades:
                try:
                    stock_name = no_entry_trade.stock_name
                    option_type = no_entry_trade.option_type or 'PE'
                    
                    # Fetch current stock LTP and VWAP
                    stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                    if not stock_data:
                        logger.warning(f"⚠️ Could not fetch stock data for {stock_name} - skipping re-evaluation")
                        continue
                    
                    current_stock_ltp = stock_data.get('ltp', 0)
                    current_stock_vwap = stock_data.get('vwap', 0)
                    
                    if current_stock_ltp <= 0 or current_stock_vwap <= 0:
                        logger.warning(f"⚠️ Invalid stock data for {stock_name} (LTP: {current_stock_ltp}, VWAP: {current_stock_vwap}) - skipping")
                        continue
                    
                    # ====================================================================
                    # NEW ENTRY FILTERS: VWAP Slope + Option Candle Size
                    # ====================================================================
                    # Get previous hour VWAP (use stored if available, else fetch)
                    stock_vwap_prev = no_entry_trade.stock_vwap_previous_hour
                    stock_vwap_prev_time = no_entry_trade.stock_vwap_previous_hour_time
                    
                    # If not stored, fetch it
                    if not stock_vwap_prev or not stock_vwap_prev_time:
                        prev_vwap_data = vwap_service.get_stock_vwap_for_previous_hour(stock_name)
                        if prev_vwap_data:
                            stock_vwap_prev = prev_vwap_data.get('vwap')
                            stock_vwap_prev_time = prev_vwap_data.get('time')
                            # Update database
                            no_entry_trade.stock_vwap_previous_hour = stock_vwap_prev
                            no_entry_trade.stock_vwap_previous_hour_time = stock_vwap_prev_time
                    
                    # Check VWAP slope
                    vwap_slope_passed = False
                    if stock_vwap_prev and stock_vwap_prev > 0 and stock_vwap_prev_time and current_stock_vwap > 0:
                        try:
                            slope_result = vwap_service.vwap_slope(
                                vwap1=stock_vwap_prev,
                                time1=stock_vwap_prev_time,
                                vwap2=current_stock_vwap,
                                time2=now
                            )
                            vwap_slope_passed = (slope_result == "Yes")
                        except Exception as slope_error:
                            logger.warning(f"Error calculating VWAP slope for {stock_name}: {str(slope_error)}")
                    
                    # Fetch option daily candles and check size (current day vs previous day)
                    candle_size_passed = False
                    if no_entry_trade.instrument_key:
                        try:
                            option_candles = vwap_service.get_option_daily_candles_current_and_previous(no_entry_trade.instrument_key)
                            if option_candles:
                                current_day_candle = option_candles.get('current_day_candle', {})
                                previous_day_candle = option_candles.get('previous_day_candle', {})
                                
                                if current_day_candle and previous_day_candle:
                                    current_size = abs(current_day_candle.get('high', 0) - current_day_candle.get('low', 0))
                                    previous_size = abs(previous_day_candle.get('high', 0) - previous_day_candle.get('low', 0))
                                    
                                    if previous_size > 0:
                                        size_ratio = current_size / previous_size
                                        candle_size_passed = (size_ratio < 7.5)
                                        
                                        # Update database with daily candle data
                                        no_entry_trade.option_current_candle_open = current_day_candle.get('open')
                                        no_entry_trade.option_current_candle_high = current_day_candle.get('high')
                                        no_entry_trade.option_current_candle_low = current_day_candle.get('low')
                                        no_entry_trade.option_current_candle_close = current_day_candle.get('close')
                                        no_entry_trade.option_current_candle_time = current_day_candle.get('time')
                                        no_entry_trade.option_previous_candle_open = previous_day_candle.get('open')
                                        no_entry_trade.option_previous_candle_high = previous_day_candle.get('high')
                                        no_entry_trade.option_previous_candle_low = previous_day_candle.get('low')
                                        no_entry_trade.option_previous_candle_close = previous_day_candle.get('close')
                                        no_entry_trade.option_previous_candle_time = previous_day_candle.get('time')
                        except Exception as candle_error:
                            logger.warning(f"Error fetching option daily candles for {stock_name}: {str(candle_error)}")
                    
                    # Check index trends alignment
                    # Rules:
                    # 1. If both indices are Bullish → trade will be considered for both bullish & bearish alerts
                    # 2. If both indices are Bearish → only Bearish scan alerts trade will be processed
                    # 3. If indices are in opposite directions → no trade will be processed
                    can_enter_by_index = False
                    both_bullish = (nifty_trend == "bullish" and banknifty_trend == "bullish")
                    both_bearish = (nifty_trend == "bearish" and banknifty_trend == "bearish")
                    opposite_directions = not both_bullish and not both_bearish
                    
                    if option_type == 'PE':
                        # Bearish alert
                        if both_bullish or both_bearish:
                            # Both indices bullish OR both bearish → bearish alerts can enter
                            can_enter_by_index = True
                        elif opposite_directions:
                            # Indices in opposite directions → no trade
                            can_enter_by_index = False
                    elif option_type == 'CE':
                        # Bullish alert
                        if both_bullish:
                            # Both indices bullish → bullish alerts can enter
                            can_enter_by_index = True
                        elif both_bearish or opposite_directions:
                            # Both indices bearish OR opposite directions → bullish alerts cannot enter
                            can_enter_by_index = False
                    
                    # Check if all entry conditions are met
                    if (is_before_3pm and 
                        can_enter_by_index and 
                        vwap_slope_passed and 
                        candle_size_passed and 
                        no_entry_trade.option_contract and 
                        no_entry_trade.instrument_key):
                        
                        # Fetch current option LTP
                        option_quote = vwap_service.get_market_quote_by_key(no_entry_trade.instrument_key)
                        if option_quote and option_quote.get('last_price', 0) > 0:
                            current_option_ltp = float(option_quote.get('last_price', 0))
                            
                            # Enter the trade with CURRENT time and prices
                            import math
                            SL_LOSS_TARGET = 3100.0
                            
                            no_entry_trade.buy_price = current_option_ltp
                            no_entry_trade.buy_time = now  # Use CURRENT time, not alert time
                            no_entry_trade.stock_ltp = current_stock_ltp
                            no_entry_trade.stock_vwap = current_stock_vwap
                            no_entry_trade.option_ltp = current_option_ltp
                            no_entry_trade.status = 'bought'
                            no_entry_trade.pnl = 0.0
                            
                            # Calculate stop loss
                            qty = no_entry_trade.qty or 0
                            if qty > 0:
                                calculated_sl = current_option_ltp - (SL_LOSS_TARGET / qty)
                                no_entry_trade.stop_loss = max(0.05, math.floor(calculated_sl / 0.10) * 0.10)
                            
                            re_entry_time_str = now.strftime('%Y-%m-%d %H:%M:%S IST')
                            alert_time_str = no_entry_trade.alert_time.strftime('%H:%M:%S') if no_entry_trade.alert_time else 'N/A'
                            logger.info(f"✅ RE-ENTERED TRADE: {stock_name} ({no_entry_trade.option_contract})")
                            logger.info(f"   Entry Time: {re_entry_time_str} (was 'no_entry' at alert time: {alert_time_str})")
                            logger.info(f"   Buy Price: ₹{current_option_ltp:.2f} (current LTP)")
                            logger.info(f"   Stock LTP: ₹{current_stock_ltp:.2f}, VWAP: ₹{current_stock_vwap:.2f}")
                            logger.info(f"   VWAP Slope: ✅ >= 45° (Previous: ₹{stock_vwap_prev:.2f if stock_vwap_prev else 0:.2f}, Current: ₹{current_stock_vwap:.2f})")
                            logger.info(f"   Candle Size: ✅ Passed")
                            logger.info(f"   Index Trends: NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend}")
                            print(f"✅ RE-ENTRY DECISION: {stock_name} ({no_entry_trade.option_contract})")
                            print(f"   ⏰ Entry Time: {re_entry_time_str} (was 'no_entry' at alert time: {alert_time_str})")
                            print(f"   📊 Entry Conditions:")
                            print(f"      - Time Check: ✅ Before 3:00 PM ({now.strftime('%H:%M:%S')})")
                            print(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                            print(f"      - VWAP Slope: ✅ >= 45° (Previous: ₹{stock_vwap_prev:.2f if stock_vwap_prev else 0:.2f} at {stock_vwap_prev_time.strftime('%H:%M') if stock_vwap_prev_time else 'N/A'}, Current: ₹{current_stock_vwap:.2f})")
                            print(f"      - Candle Size: ✅ Passed (now sufficient)")
                            print(f"      - Option Data: ✅ Valid")
                            print(f"   💰 Trade Details:")
                            print(f"      - Buy Price: ₹{current_option_ltp:.2f} (current LTP at {now.strftime('%H:%M:%S')})")
                            print(f"      - Quantity: {qty}")
                            print(f"      - Stop Loss: ₹{no_entry_trade.stop_loss:.2f}")
                            print(f"      - Stock LTP: ₹{current_stock_ltp:.2f}")
                            print(f"      - Stock VWAP: ₹{current_stock_vwap:.2f}")
                            logger.info(f"✅ RE-ENTRY DECISION: {stock_name} | Time: {re_entry_time_str} | Price: ₹{current_option_ltp:.2f} | VWAP Slope: ✅ | Candle Size: ✅ | Indices: NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend}")
                        else:
                            logger.warning(f"⚠️ Could not fetch option LTP for {stock_name} - cannot enter")
                    else:
                        # Log why entry conditions are not met
                        reasons = []
                        if not is_before_3pm:
                            reasons.append("time >= 3:00 PM")
                        if not can_enter_by_index:
                            reasons.append(f"index trends not aligned (NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend})")
                        if not vwap_slope_passed:
                            reasons.append("VWAP slope < 45°")
                        if not candle_size_passed:
                            reasons.append("candle size >= 7.5× previous")
                        if not no_entry_trade.option_contract or not no_entry_trade.instrument_key:
                            reasons.append("missing option data")
                        
                        logger.debug(f"⚪ {stock_name} still 'no_entry': {', '.join(reasons)}")
                        
                except Exception as e:
                    logger.error(f"Error re-evaluating no_entry trade for {no_entry_trade.stock_name}: {str(e)}")
                    import traceback
                    traceback.print_exc()
        
        # Commit any re-entries before updating existing positions
        db.commit()
        
        # Get all open positions from today (not sold/exited) - includes newly entered trades
        open_positions = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.status != 'sold',
                IntradayStockOption.exit_reason == None  # No exit reason means still open
            )
        ).all()
        
        # Also get all trades (including no_entry) for historical data saving
        all_trades_for_history = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.trade_date < today + timedelta(days=1),
                IntradayStockOption.exit_reason == None  # Only include trades that haven't exited yet
            )
        ).all()
        
        if not open_positions:
            logger.info("No open positions found to update")
            # Still save historical data even if no open positions
            if all_trades_for_history:
                logger.info(f"Saving historical data for {len(all_trades_for_history)} trades (including no_entry)")
        else:
            logger.info(f"Found {len(open_positions)} open positions to update")
        
        # ═══════════════════════════════════════════════════════════════
        # RE-EVALUATE "no_entry" TRADES: Check if conditions are now met
        # ═══════════════════════════════════════════════════════════════
        # If a trade had "no_entry" status due to VWAP slope or candle size
        # conditions not being met at alert time, but conditions become
        # favorable later, enter the trade with current time and prices
        # ═══════════════════════════════════════════════════════════════
        no_entry_trades = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.status == 'no_entry',
                IntradayStockOption.exit_reason == None
            )
        ).all()
        
        if no_entry_trades:
            logger.info(f"🔍 Found {len(no_entry_trades)} 'no_entry' trades to re-evaluate")
            
            # Check index trends
            index_trends = vwap_service.check_index_trends()
            nifty_trend = index_trends.get("nifty_trend", "unknown")
            banknifty_trend = index_trends.get("banknifty_trend", "unknown")
            
            # Check if time is before 3:00 PM
            is_before_3pm = now.hour < 15
            
            # Re-evaluate each no_entry trade
            for no_entry_trade in no_entry_trades:
                try:
                    stock_name = no_entry_trade.stock_name
                    option_type = no_entry_trade.option_type or 'PE'
                    
                    # Fetch current stock LTP and VWAP
                    stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                    if not stock_data:
                        logger.warning(f"⚠️ Could not fetch stock data for {stock_name} - skipping re-evaluation")
                        continue
                    
                    current_stock_ltp = stock_data.get('ltp', 0)
                    current_stock_vwap = stock_data.get('vwap', 0)
                    
                    if current_stock_ltp <= 0 or current_stock_vwap <= 0:
                        logger.warning(f"⚠️ Invalid stock data for {stock_name} (LTP: {current_stock_ltp}, VWAP: {current_stock_vwap}) - skipping")
                        continue
                    
                    # ====================================================================
                    # NEW ENTRY FILTERS: VWAP Slope + Option Candle Size
                    # ====================================================================
                    # Get previous hour VWAP (use stored if available, else fetch)
                    stock_vwap_prev = no_entry_trade.stock_vwap_previous_hour
                    stock_vwap_prev_time = no_entry_trade.stock_vwap_previous_hour_time
                    
                    # If not stored, fetch it
                    if not stock_vwap_prev or not stock_vwap_prev_time:
                        prev_vwap_data = vwap_service.get_stock_vwap_for_previous_hour(stock_name)
                        if prev_vwap_data:
                            stock_vwap_prev = prev_vwap_data.get('vwap')
                            stock_vwap_prev_time = prev_vwap_data.get('time')
                            # Update database
                            no_entry_trade.stock_vwap_previous_hour = stock_vwap_prev
                            no_entry_trade.stock_vwap_previous_hour_time = stock_vwap_prev_time
                    
                    # Check VWAP slope
                    vwap_slope_passed = False
                    if stock_vwap_prev and stock_vwap_prev > 0 and stock_vwap_prev_time and current_stock_vwap > 0:
                        try:
                            slope_result = vwap_service.vwap_slope(
                                vwap1=stock_vwap_prev,
                                time1=stock_vwap_prev_time,
                                vwap2=current_stock_vwap,
                                time2=now
                            )
                            vwap_slope_passed = (slope_result == "Yes")
                        except Exception as slope_error:
                            logger.warning(f"Error calculating VWAP slope for {stock_name}: {str(slope_error)}")
                    
                    # Fetch option daily candles and check size (current day vs previous day)
                    candle_size_passed = False
                    if no_entry_trade.instrument_key:
                        try:
                            option_candles = vwap_service.get_option_daily_candles_current_and_previous(no_entry_trade.instrument_key)
                            if option_candles:
                                current_day_candle = option_candles.get('current_day_candle', {})
                                previous_day_candle = option_candles.get('previous_day_candle', {})
                                
                                if current_day_candle and previous_day_candle:
                                    current_size = abs(current_day_candle.get('high', 0) - current_day_candle.get('low', 0))
                                    previous_size = abs(previous_day_candle.get('high', 0) - previous_day_candle.get('low', 0))
                                    
                                    if previous_size > 0:
                                        size_ratio = current_size / previous_size
                                        candle_size_passed = (size_ratio < 7.5)
                                        
                                        # Update database with daily candle data
                                        no_entry_trade.option_current_candle_open = current_day_candle.get('open')
                                        no_entry_trade.option_current_candle_high = current_day_candle.get('high')
                                        no_entry_trade.option_current_candle_low = current_day_candle.get('low')
                                        no_entry_trade.option_current_candle_close = current_day_candle.get('close')
                                        no_entry_trade.option_current_candle_time = current_day_candle.get('time')
                                        no_entry_trade.option_previous_candle_open = previous_day_candle.get('open')
                                        no_entry_trade.option_previous_candle_high = previous_day_candle.get('high')
                                        no_entry_trade.option_previous_candle_low = previous_day_candle.get('low')
                                        no_entry_trade.option_previous_candle_close = previous_day_candle.get('close')
                                        no_entry_trade.option_previous_candle_time = previous_day_candle.get('time')
                        except Exception as candle_error:
                            logger.warning(f"Error fetching option daily candles for {stock_name}: {str(candle_error)}")
                    
                    # Check index trends alignment
                    # Rules:
                    # 1. If both indices are Bullish → trade will be considered for both bullish & bearish alerts
                    # 2. If both indices are Bearish → only Bearish scan alerts trade will be processed
                    # 3. If indices are in opposite directions → no trade will be processed
                    can_enter_by_index = False
                    both_bullish = (nifty_trend == "bullish" and banknifty_trend == "bullish")
                    both_bearish = (nifty_trend == "bearish" and banknifty_trend == "bearish")
                    opposite_directions = not both_bullish and not both_bearish
                    
                    if option_type == 'PE':
                        # Bearish alert
                        if both_bullish or both_bearish:
                            # Both indices bullish OR both bearish → bearish alerts can enter
                            can_enter_by_index = True
                        elif opposite_directions:
                            # Indices in opposite directions → no trade
                            can_enter_by_index = False
                    elif option_type == 'CE':
                        # Bullish alert
                        if both_bullish:
                            # Both indices bullish → bullish alerts can enter
                            can_enter_by_index = True
                        elif both_bearish or opposite_directions:
                            # Both indices bearish OR opposite directions → bullish alerts cannot enter
                            can_enter_by_index = False
                    
                    # Check if all entry conditions are met
                    if (is_before_3pm and 
                        can_enter_by_index and 
                        vwap_slope_passed and 
                        candle_size_passed and 
                        no_entry_trade.option_contract and 
                        no_entry_trade.instrument_key):
                        
                        # Fetch current option LTP
                        option_quote = vwap_service.get_market_quote_by_key(no_entry_trade.instrument_key)
                        if option_quote and option_quote.get('last_price', 0) > 0:
                            current_option_ltp = float(option_quote.get('last_price', 0))
                            
                            # Enter the trade with CURRENT time and prices
                            import math
                            SL_LOSS_TARGET = 3100.0
                            
                            no_entry_trade.buy_price = current_option_ltp
                            no_entry_trade.buy_time = now  # Use CURRENT time, not alert time
                            no_entry_trade.stock_ltp = current_stock_ltp
                            no_entry_trade.stock_vwap = current_stock_vwap
                            no_entry_trade.option_ltp = current_option_ltp
                            no_entry_trade.status = 'bought'
                            no_entry_trade.pnl = 0.0
                            
                            # Calculate stop loss
                            qty = no_entry_trade.qty or 0
                            if qty > 0:
                                calculated_sl = current_option_ltp - (SL_LOSS_TARGET / qty)
                                no_entry_trade.stop_loss = max(0.05, math.floor(calculated_sl / 0.10) * 0.10)
                            
                            re_entry_time_str = now.strftime('%Y-%m-%d %H:%M:%S IST')
                            alert_time_str = no_entry_trade.alert_time.strftime('%H:%M:%S') if no_entry_trade.alert_time else 'N/A'
                            logger.info(f"✅ RE-ENTERED TRADE: {stock_name} ({no_entry_trade.option_contract})")
                            logger.info(f"   Entry Time: {re_entry_time_str} (was 'no_entry' at alert time: {alert_time_str})")
                            logger.info(f"   Buy Price: ₹{current_option_ltp:.2f} (current LTP)")
                            logger.info(f"   Stock LTP: ₹{current_stock_ltp:.2f}, VWAP: ₹{current_stock_vwap:.2f}")
                            logger.info(f"   VWAP Slope: ✅ >= 45° (Previous: ₹{stock_vwap_prev:.2f if stock_vwap_prev else 0:.2f}, Current: ₹{current_stock_vwap:.2f})")
                            logger.info(f"   Candle Size: ✅ Passed")
                            logger.info(f"   Index Trends: NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend}")
                            print(f"✅ RE-ENTRY DECISION: {stock_name} ({no_entry_trade.option_contract})")
                            print(f"   ⏰ Entry Time: {re_entry_time_str} (was 'no_entry' at alert time: {alert_time_str})")
                            print(f"   📊 Entry Conditions:")
                            print(f"      - Time Check: ✅ Before 3:00 PM ({now.strftime('%H:%M:%S')})")
                            print(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                            print(f"      - VWAP Slope: ✅ >= 45° (Previous: ₹{stock_vwap_prev:.2f if stock_vwap_prev else 0:.2f} at {stock_vwap_prev_time.strftime('%H:%M') if stock_vwap_prev_time else 'N/A'}, Current: ₹{current_stock_vwap:.2f})")
                            print(f"      - Candle Size: ✅ Passed (now sufficient)")
                            print(f"      - Option Data: ✅ Valid")
                            print(f"   💰 Trade Details:")
                            print(f"      - Buy Price: ₹{current_option_ltp:.2f} (current LTP at {now.strftime('%H:%M:%S')})")
                            print(f"      - Quantity: {qty}")
                            print(f"      - Stop Loss: ₹{no_entry_trade.stop_loss:.2f}")
                            print(f"      - Stock LTP: ₹{current_stock_ltp:.2f}")
                            print(f"      - Stock VWAP: ₹{current_stock_vwap:.2f}")
                            logger.info(f"✅ RE-ENTRY DECISION: {stock_name} | Time: {re_entry_time_str} | Price: ₹{current_option_ltp:.2f} | VWAP Slope: ✅ | Candle Size: ✅ | Indices: NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend}")
                        else:
                            logger.warning(f"⚠️ Could not fetch option LTP for {stock_name} - cannot enter")
                    else:
                        # Log why entry conditions are not met
                        reasons = []
                        if not is_before_3pm:
                            reasons.append("time >= 3:00 PM")
                        if not can_enter_by_index:
                            reasons.append(f"index trends not aligned (NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend})")
                        if not vwap_slope_passed:
                            reasons.append("VWAP slope < 45°")
                        if not candle_size_passed:
                            reasons.append("candle size >= 7.5× previous")
                        if not no_entry_trade.option_contract or not no_entry_trade.instrument_key:
                            reasons.append("missing option data")
                        
                        logger.debug(f"⚪ {stock_name} still 'no_entry': {', '.join(reasons)}")
                        
                except Exception as e:
                    logger.error(f"Error re-evaluating no_entry trade for {no_entry_trade.stock_name}: {str(e)}")
                    import traceback
                    traceback.print_exc()
        
        # Commit any re-entries before updating existing positions
        db.commit()
        
        # Refresh open_positions query to include newly entered trades
        open_positions = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.status != 'sold',
                IntradayStockOption.status != 'no_entry',  # exclude never-entered trades from hourly update
                IntradayStockOption.exit_reason == None
            )
        ).all()
        
        # #region agent log
        try:
            status_breakdown = {}
            positions_detail = []
            for pos in open_positions:
                status = pos.status or 'unknown'
                status_breakdown[status] = status_breakdown.get(status, 0) + 1
                positions_detail.append({
                    "stock_name": pos.stock_name,
                    "status": pos.status,
                    "sell_price": float(pos.sell_price) if pos.sell_price else None,
                    "buy_price": float(pos.buy_price) if pos.buy_price else None,
                    "pnl": float(pos.pnl) if pos.pnl else None,
                    "has_instrument_key": bool(pos.instrument_key),
                    "instrument_key": pos.instrument_key,
                    "option_contract": pos.option_contract,
                    "updated_at": str(pos.updated_at) if pos.updated_at else None
                })
            
            os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
            with open(log_path, 'a') as f:
                query_log = json_module.dumps({
                    "id": f"log_open_positions_query_{int(now.timestamp())}",
                    "timestamp": int(now.timestamp() * 1000),
                    "location": "vwap_updater.py:730",
                    "message": "Open positions query result",
                    "data": {
                        "total_count": len(open_positions),
                        "status_breakdown": status_breakdown,
                        "positions": positions_detail[:20]  # First 20 for detailed analysis
                    },
                    "sessionId": "debug-session",
                    "runId": "sell-price-fix",
                    "hypothesisId": "QUERY"
                }) + "\n"
                f.write(query_log)
                f.flush()
            
            logger.info(f"🔍 DEBUG QUERY: Found {len(open_positions)} open positions. Status breakdown: {status_breakdown}")
            logger.info(f"🔍 DEBUG QUERY: Positions without sell_price: {sum(1 for p in open_positions if not p.sell_price or p.sell_price == 0)}")
            logger.info(f"🔍 DEBUG QUERY: Positions without instrument_key: {sum(1 for p in open_positions if not p.instrument_key)}")
            logger.info(f"🔍 DEBUG QUERY: Sample positions: {positions_detail[:3]}")
            
            # CRITICAL: If no positions found, log this clearly
            if len(open_positions) == 0:
                logger.warning(f"⚠️ WARNING: No open positions found! Query returned 0 results.")
                logger.warning(f"   Query filters: trade_date >= {today}, status != 'sold', exit_reason == None")
        except Exception as log_err:
            logger.error(f"❌ Failed to write query log: {str(log_err)}")
            import traceback
            logger.error(traceback.format_exc())
        # #endregion
        
        # Update each position
        updated_count = 0
        failed_count = 0
        stocks_with_history_saved = set()
        
        logger.info(f"🔍 DEBUG: Starting to process {len(open_positions)} positions...")
        
        if len(open_positions) == 0:
            logger.warning(f"⚠️ WARNING: No positions to process! This might indicate:")
            logger.warning(f"   1. No trades were entered today")
            logger.warning(f"   2. All trades have been sold/exited")
            logger.warning(f"   3. Query filters are too restrictive")
            logger.warning(f"   4. Database connection issue")
        
        for idx, position in enumerate(open_positions, 1):
            try:
                stock_name = position.stock_name
                option_contract = position.option_contract
                logger.info(f"🔍 DEBUG: Processing position {idx}/{len(open_positions)}: {stock_name} (status={position.status}, sell_price={position.sell_price}, has_instrument_key={bool(position.instrument_key)})")
                
                # ═══════════════════════════════════════════════════════════════
                # SAFETY CHECK: Ensure trade is in HOLD status (still open)
                # ═══════════════════════════════════════════════════════════════
                # A trade has "HOLD" status (frontend) when:
                #   - exit_reason = None (database)
                #   - status = 'bought' (database)
                #   - No exit conditions met (profit target, SL, VWAP cross)
                #
                # We ONLY update sell_price/sell_time for trades that are OPEN.
                # Once exit_reason is set, the trade is CLOSED and excluded from future updates.
                # ═══════════════════════════════════════════════════════════════
                
                if position.exit_reason is not None:
                    logger.warning(f"⚠️ Skipping {stock_name} - exit_reason already set to '{position.exit_reason}' (trade closed)")
                    continue
                
                if position.status == 'sold':
                    logger.warning(f"⚠️ Skipping {stock_name} - status already 'sold' (trade closed)")
                    continue
                
                # Check if this position was updated recently (within last 30 minutes)
                # This helps detect if multiple update systems are running simultaneously
                if position.updated_at:
                    # Handle timezone-aware vs timezone-naive datetime comparison
                    try:
                        updated_at = position.updated_at
                        # Convert to IST timezone-aware datetime for comparison
                        if updated_at.tzinfo is None:
                            # If updated_at is timezone-naive, assume it's in IST and localize it
                            # Note: localize() can only be called on naive datetimes
                            updated_at = ist.localize(updated_at)
                        else:
                            # If it's already timezone-aware (could be UTC from PostgreSQL), convert to IST
                            # Note: astimezone() requires timezone-aware datetime
                            updated_at = updated_at.astimezone(ist)
                        
                        # Now both now and updated_at are timezone-aware in IST, safe to subtract
                        time_since_last_update = (now - updated_at).total_seconds() / 60
                        if time_since_last_update < 30:
                            logger.warning(f"⚠️ {stock_name} was updated {time_since_last_update:.1f} minutes ago - possible duplicate update!")
                            logger.warning(f"   Current sell_price: ₹{position.sell_price:.2f}, buy_price: ₹{position.buy_price:.2f}")
                            logger.warning(f"   This may indicate multiple update systems running simultaneously")
                    except (ValueError, TypeError) as tz_error:
                        # If timezone conversion fails (e.g., can't subtract naive/aware), log but don't block update
                        logger.warning(f"⚠️ Timezone conversion error for {stock_name}: {str(tz_error)} - continuing with update")
                        # Continue processing - don't let timezone check block the update
                
                # 1. Fetch fresh Stock VWAP from API
                new_vwap = vwap_service.get_stock_vwap(stock_name)
                
                # 2. Fetch fresh Stock LTP (Last Traded Price)
                new_stock_ltp = vwap_service.get_stock_ltp_from_market_quote(stock_name)
                
                # 3. Fetch fresh Option LTP (if option contract exists) - SIMPLIFIED
                new_option_ltp = 0.0
                if option_contract and position.instrument_key:
                    try:
                        instrument_key = position.instrument_key
                        option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                        
                        if option_quote and isinstance(option_quote, dict) and 'last_price' in option_quote:
                            option_ltp_data = option_quote['last_price']
                            if option_ltp_data and option_ltp_data > 0:
                                new_option_ltp = float(option_ltp_data)
                                logger.info(f"✅ [{now.strftime('%H:%M:%S')}] {stock_name}: Fetched option LTP ₹{new_option_ltp:.2f}")
                            else:
                                logger.warning(f"⚠️ {stock_name}: Invalid LTP data: {option_ltp_data}")
                        else:
                            logger.warning(f"⚠️ {stock_name}: No valid option quote returned")
                    except Exception as ltp_error:
                        logger.error(f"❌ {stock_name}: Error fetching option LTP: {str(ltp_error)}")
                
                # FALLBACK: Lookup instrument_key for old records that don't have it stored
                if not option_contract or not position.instrument_key:
                    logger.warning(f"⚠️ No stored instrument_key for {option_contract} - falling back to lookup")
                    from pathlib import Path
                    import json as json_lib
                    
                    instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
                    
                    if instruments_file.exists():
                        try:
                            with open(instruments_file, 'r') as f:
                                instruments_data = json_lib.load(f)
                            
                            # Find option contract in instruments data
                            import re
                            match = re.match(r'^([A-Z-]+)-(\w{3})(\d{4})-(\d+\.?\d*?)-(CE|PE)$', option_contract)
                            
                            if match:
                                symbol, month, year, strike, opt_type = match.groups()
                                strike_value = float(strike)
                                
                                # Parse expiry month and year
                                month_map = {
                                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
                                    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
                                    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                                }
                                target_month = month_map.get(month[:3].capitalize(), 11)
                                target_year = int(year)
                                
                                # Search for matching instrument - CRITICAL: Also check expiry month/year
                                for instrument in instruments_data:
                                    if (instrument.get('underlying_symbol', '').upper() == symbol.upper() and
                                        instrument.get('segment') == 'NSE_FO' and
                                        instrument.get('instrument_type') == opt_type):
                                        
                                        # Check strike price match
                                        inst_strike = float(instrument.get('strike_price', 0))
                                        if abs(inst_strike - strike_value) < 0.01:
                                            # CRITICAL: Check expiry month/year matches
                                            expiry_timestamp = instrument.get('expiry')
                                            if expiry_timestamp:
                                                try:
                                                    # Convert timestamp (milliseconds) to datetime
                                                    if expiry_timestamp > 1e12:
                                                        expiry_timestamp = expiry_timestamp / 1000
                                                    inst_expiry = datetime.fromtimestamp(expiry_timestamp, tz=pytz.UTC)
                                                    
                                                    # Check if expiry month/year matches
                                                    if inst_expiry.year == target_year and inst_expiry.month == target_month:
                                                        # Found the correct option - fetch its LTP
                                                        instrument_key = instrument.get('instrument_key')
                                                        if instrument_key:
                                                            logger.info(f"🔍 [{now.strftime('%H:%M:%S')}] Found instrument_key via lookup: {instrument_key}")
                                                            logger.info(f"   Strike: {inst_strike}, Type: {opt_type}, Expiry: {inst_expiry.strftime('%d-%b-%Y')}")
                                                            
                                                            option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                                                            
                                                            if option_quote and 'last_price' in option_quote:
                                                                option_ltp_data = option_quote['last_price']
                                                                if option_ltp_data and option_ltp_data > 0:
                                                                    new_option_ltp = option_ltp_data
                                                                    logger.info(f"📥 [{now.strftime('%H:%M:%S')}] API returned option LTP: ₹{new_option_ltp:.2f} for {option_contract}")
                                                                    # Update stored instrument_key for future updates
                                                                    position.instrument_key = instrument_key
                                                                    logger.info(f"✅ Stored instrument_key {instrument_key} for future updates")
                                                                    break  # Found correct match, exit loop
                                                except (ValueError, TypeError) as exp_error:
                                                    logger.warning(f"⚠️ Error parsing expiry for {option_contract}: {exp_error}")
                                                    continue
                        except Exception as e:
                            logger.warning(f"Could not fetch option LTP for {option_contract}: {str(e)}")
                
                # CRITICAL: If option LTP fetch fails, retry once before giving up
                # This ensures we always have option LTP for sell_price updates and exit decisions
                if new_option_ltp == 0 and option_contract and position.instrument_key:
                    logger.warning(f"⚠️ Option LTP fetch FAILED for {stock_name} {option_contract} - RETRYING...")
                    try:
                        # Retry option LTP fetch
                        option_quote_retry = vwap_service.get_market_quote_by_key(position.instrument_key)
                        if option_quote_retry and 'last_price' in option_quote_retry:
                            option_ltp_retry = option_quote_retry['last_price']
                            if option_ltp_retry and option_ltp_retry > 0:
                                new_option_ltp = option_ltp_retry
                                logger.info(f"✅ Retry successful: Got option LTP ₹{new_option_ltp:.2f} for {stock_name}")
                    except Exception as retry_error:
                        logger.warning(f"⚠️ Retry also failed for {stock_name}: {str(retry_error)}")
                
                # CRITICAL: Even if option LTP fetch fails after retry, check VWAP cross using stock data
                # If VWAP cross detected, we MUST fetch option LTP one more time before exiting
                if new_option_ltp == 0:
                    logger.warning(f"⚠️ Option LTP fetch FAILED (after retry) for {stock_name} {option_contract}")
                    logger.warning(f"   new_vwap={new_vwap}, new_stock_ltp={new_stock_ltp}, option_type={position.option_type}")
                    
                    # Check VWAP cross condition
                    vwap_cross_detected = False
                    if new_vwap > 0 and new_stock_ltp > 0 and position.option_type:
                        if now.hour >= 11 and now.minute >= 15:
                            option_type = position.option_type
                            if (option_type == 'CE' and new_stock_ltp < new_vwap) or \
                               (option_type == 'PE' and new_stock_ltp > new_vwap):
                                vwap_cross_detected = True
                                logger.critical(f"🚨 VWAP CROSS DETECTED for {stock_name} but option LTP fetch FAILED!")
                                logger.critical(f"   Stock LTP: ₹{new_stock_ltp:.2f}, VWAP: ₹{new_vwap:.2f}, Type: {option_type}")
                                
                                # CRITICAL: Try one final time to fetch option LTP before exiting
                                if position.instrument_key:
                                    try:
                                        logger.critical(f"   🔄 Final attempt to fetch option LTP for exit...")
                                        final_quote = vwap_service.get_market_quote_by_key(position.instrument_key)
                                        if final_quote and 'last_price' in final_quote:
                                            final_ltp = final_quote['last_price']
                                            if final_ltp and final_ltp > 0:
                                                new_option_ltp = final_ltp
                                                logger.critical(f"   ✅ Final fetch successful: ₹{new_option_ltp:.2f}")
                                    except Exception as final_error:
                                        logger.error(f"   ❌ Final fetch also failed: {str(final_error)}")
                                
                                # If still no option LTP, use last known sell_price or buy_price as fallback
                                if new_option_ltp == 0:
                                    if position.sell_price and position.sell_price > 0:
                                        new_option_ltp = position.sell_price
                                        logger.critical(f"   Using last known sell_price: ₹{new_option_ltp:.2f} for exit")
                                    elif position.buy_price and position.buy_price > 0:
                                        new_option_ltp = position.buy_price
                                        logger.critical(f"   ⚠️ Using buy_price as fallback: ₹{new_option_ltp:.2f} (P&L will be 0)")
                                    else:
                                        logger.error(f"   🚨 CRITICAL: No option price available for exit! Using 0.0")
                                        new_option_ltp = 0.0
                                
                                # Exit with VWAP cross
                                position.exit_reason = 'stock_vwap_cross'
                                position.sell_time = now
                                position.status = 'sold'
                                position.sell_price = new_option_ltp  # CRITICAL: Always set sell_price
                                if position.buy_price and position.qty:
                                    position.pnl = (new_option_ltp - position.buy_price) * position.qty
                                else:
                                    position.pnl = 0.0
                                logger.critical(f"✅ FORCED EXIT: {stock_name} on VWAP cross with price ₹{new_option_ltp:.2f}, PnL=₹{position.pnl:.2f}")
                                updates_made.append(f"🚨 EXITED: stock_vwap_cross at ₹{new_option_ltp:.2f}")
                                updated_count += 1
                                # Continue to update stock LTP/VWAP below for record keeping
                
                # Update position with new values
                updates_made = []
                
                if new_vwap and new_vwap > 0:
                    old_vwap = position.stock_vwap or 0.0
                    position.stock_vwap = new_vwap
                    updates_made.append(f"VWAP: {old_vwap:.2f}→{new_vwap:.2f}")
                
                if new_stock_ltp and new_stock_ltp > 0:
                    old_stock_ltp = position.stock_ltp or 0.0
                    position.stock_ltp = new_stock_ltp
                    updates_made.append(f"Stock LTP: {old_stock_ltp:.2f}→{new_stock_ltp:.2f}")
                
                # ═══════════════════════════════════════════════════════════════
                # SAVE HISTORICAL MARKET DATA
                # ═══════════════════════════════════════════════════════════════
                # Store historical snapshot of market data for analysis
                # Check if data already exists to prevent duplicates (e.g., when cycle scheduler also runs)
                try:
                    if not historical_data_exists(db, stock_name, now):
                        # Get VWAP slope from position record if available
                        vwap_slope_angle = position.vwap_slope_angle if hasattr(position, 'vwap_slope_angle') else None
                        vwap_slope_status = position.vwap_slope_status if hasattr(position, 'vwap_slope_status') else None
                        vwap_slope_direction = position.vwap_slope_direction if hasattr(position, 'vwap_slope_direction') else None
                        vwap_slope_time = position.vwap_slope_time if hasattr(position, 'vwap_slope_time') else None
                        
                        historical_record = HistoricalMarketData(
                            stock_name=stock_name,
                            stock_vwap=new_vwap if new_vwap and new_vwap > 0 else None,
                            stock_ltp=new_stock_ltp if new_stock_ltp and new_stock_ltp > 0 else None,
                            vwap_slope_angle=vwap_slope_angle,
                            vwap_slope_status=vwap_slope_status,
                            vwap_slope_direction=vwap_slope_direction,
                            vwap_slope_time=vwap_slope_time,
                            option_contract=option_contract,
                            option_instrument_key=position.instrument_key,
                            option_ltp=new_option_ltp if new_option_ltp > 0 else None,
                            scan_date=now,
                            scan_time=now.strftime('%I:%M %p').lower()
                        )
                        db.add(historical_record)
                        stocks_with_history_saved.add(stock_name)
                        logger.debug(f"📊 Saved historical data for {stock_name} at {now.strftime('%H:%M:%S')}")
                    else:
                        logger.debug(f"⏭️ Skipping duplicate historical data for {stock_name} at {now.strftime('%H:%M:%S')} (already exists)")
                        stocks_with_history_saved.add(stock_name)  # Still mark as saved to avoid no_entry processing
                except Exception as hist_error:
                    logger.warning(f"⚠️ Failed to save historical data for {stock_name}: {str(hist_error)}")
                    # Don't fail the entire update if historical save fails
                
                # CRITICAL FIX: Always update sell_price and PnL for open positions
                # Even if option LTP fetch failed, we should still update if we have any value
                old_option_ltp = position.sell_price or 0.0
                
                if new_option_ltp > 0:
                    # CRITICAL SANITY CHECKS
                    sanity_passed = True
                    
                    # Check 1: Flag suspicious price movements (>100% change)
                    if old_option_ltp > 0:
                        price_change_pct = abs((new_option_ltp - old_option_ltp) / old_option_ltp) * 100
                        if price_change_pct > 100:
                            logger.error(f"🚨 SUSPICIOUS PRICE CHANGE for {stock_name}: ₹{old_option_ltp:.2f} → ₹{new_option_ltp:.2f} ({price_change_pct:.1f}% change)")
                            logger.error(f"   This suggests possible data corruption or API error")
                    
                    # Check 2: Detect if new_option_ltp seems like a sum instead of replacement
                    if position.buy_price and position.buy_price > 0:
                        ratio = new_option_ltp / position.buy_price
                        if ratio > 3.0:  # If option LTP is more than 3x buy price
                            logger.error(f"🚨 UNREALISTIC OPTION PRICE for {stock_name}:")
                            logger.error(f"   Buy Price: ₹{position.buy_price:.2f}")
                            logger.error(f"   New Option LTP: ₹{new_option_ltp:.2f} ({ratio:.1f}x buy price)")
                            logger.error(f"   Old sell_price: ₹{old_option_ltp:.2f}")
                            logger.error(f"   This may indicate cumulative addition bug or API error")
                            
                            # If this looks like cumulative addition (new = old + actual_ltp)
                            # Try to detect and fix
                            if old_option_ltp > 0 and new_option_ltp > old_option_ltp:
                                difference = new_option_ltp - old_option_ltp
                                if difference < position.buy_price * 2:  # Difference seems reasonable
                                    logger.error(f"   Possible fix: Use difference ₹{difference:.2f} as actual LTP instead of ₹{new_option_ltp:.2f}")
                                    new_option_ltp = difference
                                    logger.error(f"   CORRECTED: Using ₹{new_option_ltp:.2f} as option LTP")
                    
                    # Update sell_price with current option price
                    # CRITICAL: Explicitly mark as modified to ensure SQLAlchemy tracks the change
                    from sqlalchemy.orm.attributes import flag_modified
                    from sqlalchemy import inspect
                    
                    old_sell_price = position.sell_price
                    position.sell_price = new_option_ltp
                    
                    # CRITICAL: Ensure object is in session and tracked
                    if position not in db:
                        db.add(position)
                        logger.warning(f"⚠️ Position {stock_name} was not in session - added it")
                    
                    # Explicitly mark as modified
                    flag_modified(position, 'sell_price')
                    
                    # Verify the change is tracked
                    insp = inspect(position)
                    if insp.modified:
                        logger.debug(f"🔍 Position {stock_name} is marked as modified in session")
                        logger.debug(f"🔍 Modified attributes: {list(insp.modified.keys())}")
                    else:
                        logger.warning(f"⚠️ Position {stock_name} is NOT marked as modified after setting sell_price!")
                    
                    updates_made.append(f"Option LTP: {old_option_ltp:.2f}→{new_option_ltp:.2f}")
                    logger.info(f"📌 {stock_name} Option LTP updated at {now.strftime('%H:%M:%S')}: ₹{old_option_ltp:.2f} → ₹{new_option_ltp:.2f}")
                    logger.info(f"🔍 Flagged sell_price as modified for {stock_name}: {old_sell_price} → {new_option_ltp}")
                    
                    # #region agent log
                    try:
                        os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
                        with open(log_path, 'a') as f:
                            sell_price_update_log = json_module.dumps({
                                "id": f"log_sell_price_update_{stock_name}_{int(now.timestamp())}",
                                "timestamp": int(now.timestamp() * 1000),
                                "location": "vwap_updater.py:984",
                                "message": "sell_price updated",
                                "data": {
                                    "stock_name": stock_name,
                                    "old_sell_price": old_option_ltp,
                                    "new_sell_price": new_option_ltp,
                                    "option_contract": option_contract,
                                    "position_id": position.id if hasattr(position, 'id') else None,
                                    "in_session": position in db.session if hasattr(db, 'session') else "unknown"
                                },
                                "sessionId": "debug-session",
                                "runId": "sell-price-fix",
                                "hypothesisId": "SELL_PRICE_UPDATE"
                            }) + "\n"
                            f.write(sell_price_update_log)
                            f.flush()
                        logger.info(f"🔍 DEBUG: Updated sell_price for {stock_name}: {old_option_ltp:.2f} → {new_option_ltp:.2f}")
                    except Exception as log_err:
                        logger.error(f"❌ Failed to write sell_price update log: {str(log_err)}")
                    # #endregion
                    
                    # Calculate and update unrealized P&L for open trades
                    if position.buy_price and position.qty:
                        old_pnl = position.pnl or 0.0
                        new_pnl = (new_option_ltp - position.buy_price) * position.qty
                        position.pnl = new_pnl
                        updates_made.append(f"P&L: ₹{old_pnl:.2f}→₹{new_pnl:.2f}")
                else:
                    # Option LTP fetch failed - use buy_price as fallback if sell_price is NULL/0
                    if (not position.sell_price or position.sell_price == 0) and position.buy_price:
                        from sqlalchemy.orm.attributes import flag_modified
                        position.sell_price = position.buy_price
                        flag_modified(position, 'sell_price')
                        logger.warning(f"⚠️ {stock_name}: Using buy_price ₹{position.buy_price:.2f} as sell_price fallback")
                    
                    # Update PnL if we have sell_price
                    if position.buy_price and position.qty and position.sell_price:
                        position.pnl = (position.sell_price - position.buy_price) * position.qty
                        flag_modified(position, 'pnl')
                    
                    position.updated_at = now
                    flag_modified(position, 'updated_at')
                
                # CHECK ALL EXIT CONDITIONS INDEPENDENTLY (ALWAYS CHECK, regardless of option LTP fetch success)
                # Then apply the highest priority exit
                # Priority: Stop Loss > VWAP Cross > Profit Target
                # Only check if trade is still open (not already exited)
                if position.exit_reason is None and position.status != 'sold':
                    exit_conditions = {
                        'stop_loss': False,
                        'vwap_cross': False,
                        'profit_target': False
                    }
                    
                    # 1. CHECK STOP LOSS (requires option LTP)
                    if new_option_ltp > 0 and position.stop_loss and new_option_ltp <= position.stop_loss:
                        exit_conditions['stop_loss'] = True
                        logger.info(f"🛑 STOP LOSS CONDITION MET for {stock_name}: LTP ₹{new_option_ltp:.2f} <= SL ₹{position.stop_loss:.2f}")
                    
                    # 2. CHECK VWAP CROSS (only after 11:15 AM) - ALWAYS CHECK using stock data
                    # This works even if option LTP fetch failed
                    if now.hour >= 11 and now.minute >= 15:
                        if new_vwap and new_vwap > 0 and new_stock_ltp and new_stock_ltp > 0:
                            option_type = position.option_type or 'CE'
                            logger.info(f"📊 VWAP CHECK for {stock_name} ({option_type}): Stock LTP=₹{new_stock_ltp:.2f}, VWAP=₹{new_vwap:.2f}")
                            
                            # CE: Exit if stock LTP falls below VWAP
                            # PE: Exit if stock LTP rises above VWAP
                            if (option_type == 'CE' and new_stock_ltp < new_vwap):
                                exit_conditions['vwap_cross'] = True
                                logger.info(f"📉 VWAP CROSS CONDITION MET for {stock_name} (CE): Stock LTP ₹{new_stock_ltp:.2f} < VWAP ₹{new_vwap:.2f}")
                            elif (option_type == 'PE' and new_stock_ltp > new_vwap):
                                exit_conditions['vwap_cross'] = True
                                logger.info(f"📈 VWAP CROSS CONDITION MET for {stock_name} (PE): Stock LTP ₹{new_stock_ltp:.2f} > VWAP ₹{new_vwap:.2f}")
                            else:
                                logger.info(f"✅ VWAP OK for {stock_name} - Stock {'>' if option_type == 'CE' else '<'} VWAP")
                    
                    # 3. CHECK PROFIT TARGET (1.5x buy price) - requires option LTP
                    if new_option_ltp > 0 and position.buy_price:
                        profit_target = position.buy_price * 1.5
                        if new_option_ltp >= profit_target:
                            exit_conditions['profit_target'] = True
                            logger.info(f"🎯 PROFIT TARGET CONDITION MET for {stock_name}: LTP ₹{new_option_ltp:.2f} >= Target ₹{profit_target:.2f}")
                    
                    # APPLY THE HIGHEST PRIORITY EXIT CONDITION
                    exit_triggered = False
                    exit_reason_to_set = None
                    
                    if exit_conditions['stop_loss']:
                        exit_triggered = True
                        exit_reason_to_set = 'stop_loss'
                        exit_time_str = now.strftime('%Y-%m-%d %H:%M:%S IST')
                        logger.warning(f"✅ APPLIED: STOP LOSS EXIT for {stock_name}")
                        logger.info(f"🛑 EXIT DECISION: {stock_name} | Time: {exit_time_str} | Reason: Stop Loss | Option LTP: ₹{new_option_ltp:.2f} <= SL: ₹{position.stop_loss:.2f} | PnL: ₹{position.pnl:.2f}")
                        print(f"🛑 EXIT DECISION: {stock_name} ({option_contract})")
                        print(f"   ⏰ Exit Time: {exit_time_str}")
                        print(f"   📊 Exit Conditions:")
                        print(f"      - Stop Loss: ✅ Triggered (LTP: ₹{new_option_ltp:.2f} <= SL: ₹{position.stop_loss:.2f})")
                        print(f"      - VWAP Cross: {'✅' if exit_conditions['vwap_cross'] else '❌'} {'Triggered' if exit_conditions['vwap_cross'] else 'Not Triggered'}")
                        print(f"      - Profit Target: {'✅' if exit_conditions['profit_target'] else '❌'} {'Triggered' if exit_conditions['profit_target'] else 'Not Triggered'}")
                        print(f"   💰 Exit Details:")
                        print(f"      - Buy Price: ₹{position.buy_price:.2f}")
                        print(f"      - Sell Price: ₹{new_option_ltp:.2f}")
                        print(f"      - Quantity: {position.qty}")
                        print(f"      - PnL: ₹{position.pnl:.2f}")
                    
                    elif exit_conditions['vwap_cross']:
                        # CRITICAL: If option LTP fetch failed, try one more time before exiting
                        exit_option_ltp = new_option_ltp
                        if exit_option_ltp == 0 and position.instrument_key:
                            try:
                                logger.warning(f"⚠️ VWAP cross detected but option LTP is 0 - retrying fetch...")
                                final_quote = vwap_service.get_market_quote_by_key(position.instrument_key)
                                if final_quote and 'last_price' in final_quote:
                                    final_ltp = final_quote['last_price']
                                    if final_ltp and final_ltp > 0:
                                        exit_option_ltp = final_ltp
                                        logger.info(f"✅ Retry successful: Got option LTP ₹{exit_option_ltp:.2f} for VWAP cross exit")
                            except Exception as final_error:
                                logger.error(f"❌ Final fetch failed: {str(final_error)}")
                        
                        # Use exit_option_ltp (which may be retried value) or fallback to last known sell_price
                        if exit_option_ltp == 0:
                            if old_option_ltp > 0:
                                exit_option_ltp = old_option_ltp
                                logger.warning(f"⚠️ Using last known sell_price ₹{exit_option_ltp:.2f} for VWAP cross exit")
                            elif position.buy_price > 0:
                                exit_option_ltp = position.buy_price
                                logger.error(f"🚨 CRITICAL: No option LTP available, using buy_price ₹{exit_option_ltp:.2f} (P&L will be 0)")
                        
                        # Update new_option_ltp for use in exit logic below
                        new_option_ltp = exit_option_ltp
                        
                        exit_triggered = True
                        exit_reason_to_set = 'stock_vwap_cross'
                        exit_time_str = now.strftime('%Y-%m-%d %H:%M:%S IST')
                        logger.warning(f"✅ APPLIED: VWAP CROSS EXIT for {stock_name}")
                        logger.info(f"📉 EXIT DECISION: {stock_name} | Time: {exit_time_str} | Reason: VWAP Cross | Stock LTP: ₹{new_stock_ltp:.2f}, VWAP: ₹{new_vwap:.2f} | PnL: ₹{position.pnl:.2f}")
                        print(f"📉 EXIT DECISION: {stock_name} ({option_contract})")
                        print(f"   ⏰ Exit Time: {exit_time_str}")
                        print(f"   📊 Exit Conditions:")
                        print(f"      - Stop Loss: {'✅' if exit_conditions['stop_loss'] else '❌'} {'Triggered' if exit_conditions['stop_loss'] else 'Not Triggered'}")
                        print(f"      - VWAP Cross: ✅ Triggered (Stock LTP: ₹{new_stock_ltp:.2f} {'<' if option_type == 'CE' else '>'} VWAP: ₹{new_vwap:.2f})")
                        print(f"      - Profit Target: {'✅' if exit_conditions['profit_target'] else '❌'} {'Triggered' if exit_conditions['profit_target'] else 'Not Triggered'}")
                        print(f"   💰 Exit Details:")
                        print(f"      - Buy Price: ₹{position.buy_price:.2f}")
                        print(f"      - Sell Price: ₹{exit_option_ltp:.2f}")
                        print(f"      - Quantity: {position.qty}")
                        print(f"      - PnL: ₹{position.pnl:.2f}")
                    
                    elif exit_conditions['profit_target']:
                        exit_triggered = True
                        exit_reason_to_set = 'profit_target'
                        exit_time_str = now.strftime('%Y-%m-%d %H:%M:%S IST')
                        profit_target = position.buy_price * 1.5
                        logger.warning(f"✅ APPLIED: PROFIT TARGET EXIT for {stock_name}")
                        logger.info(f"🎯 EXIT DECISION: {stock_name} | Time: {exit_time_str} | Reason: Profit Target | Option LTP: ₹{new_option_ltp:.2f} >= Target: ₹{profit_target:.2f} | PnL: ₹{position.pnl:.2f}")
                        print(f"🎯 EXIT DECISION: {stock_name} ({option_contract})")
                        print(f"   ⏰ Exit Time: {exit_time_str}")
                        print(f"   📊 Exit Conditions:")
                        print(f"      - Stop Loss: {'✅' if exit_conditions['stop_loss'] else '❌'} {'Triggered' if exit_conditions['stop_loss'] else 'Not Triggered'}")
                        print(f"      - VWAP Cross: {'✅' if exit_conditions['vwap_cross'] else '❌'} {'Triggered' if exit_conditions['vwap_cross'] else 'Not Triggered'}")
                        print(f"      - Profit Target: ✅ Triggered (LTP: ₹{new_option_ltp:.2f} >= Target: ₹{profit_target:.2f})")
                        print(f"   💰 Exit Details:")
                        print(f"      - Buy Price: ₹{position.buy_price:.2f}")
                        print(f"      - Sell Price: ₹{new_option_ltp:.2f}")
                        print(f"      - Quantity: {position.qty}")
                        print(f"      - PnL: ₹{position.pnl:.2f}")
                        
                    # Set exit fields if any exit condition was triggered
                    # ═══════════════════════════════════════════════════════════════
                    # IMPORTANT: sell_time is ONLY set here, at the moment of exit
                    # After this update:
                    #   - exit_reason will be set → Trade excluded from future updates
                    #   - sell_price is FROZEN at the current value (new_option_ltp)
                    #   - sell_time is FROZEN at the current timestamp
                    #   - No more updates will be applied to this trade
                    # ═══════════════════════════════════════════════════════════════
                    if exit_triggered and exit_reason_to_set:
                        # CRITICAL: Ensure sell_price is ALWAYS set when exiting
                        from sqlalchemy.orm.attributes import flag_modified
                        if position.sell_price is None or position.sell_price == 0:
                            if new_option_ltp > 0:
                                position.sell_price = new_option_ltp
                            elif old_option_ltp > 0:
                                position.sell_price = old_option_ltp
                                logger.warning(f"⚠️ Using last known sell_price ₹{old_option_ltp:.2f} for exit")
                            elif position.buy_price > 0:
                                position.sell_price = position.buy_price
                                logger.error(f"🚨 CRITICAL: No option LTP available, using buy_price ₹{position.buy_price:.2f} (P&L will be 0)")
                            else:
                                position.sell_price = 0.0
                                logger.error(f"🚨 CRITICAL: No sell_price, no buy_price - setting to 0.0")
                            flag_modified(position, 'sell_price')  # Explicitly mark as modified
                        
                        position.exit_reason = exit_reason_to_set
                        flag_modified(position, 'exit_reason')  # Explicitly mark as modified
                        position.sell_time = now  # Set ONLY once at exit
                        position.status = 'sold'
                        
                        # CRITICAL: Ensure PnL is ALWAYS calculated when exiting
                        from sqlalchemy.orm.attributes import flag_modified
                        if position.buy_price and position.qty and position.sell_price:
                            position.pnl = (position.sell_price - position.buy_price) * position.qty
                            flag_modified(position, 'pnl')  # Explicitly mark as modified
                        elif position.buy_price and position.qty:
                            # If sell_price is still 0, PnL will be negative (loss)
                            position.pnl = (0 - position.buy_price) * position.qty
                            flag_modified(position, 'pnl')  # Explicitly mark as modified
                            logger.error(f"🚨 CRITICAL: PnL calculated with sell_price=0, result: ₹{position.pnl:.2f}")
                        else:
                            position.pnl = 0.0
                            flag_modified(position, 'pnl')  # Explicitly mark as modified
                            logger.error(f"🚨 CRITICAL: Cannot calculate PnL - missing buy_price or qty")
                        
                        flag_modified(position, 'status')  # Explicitly mark status change
                        flag_modified(position, 'sell_time')  # Explicitly mark sell_time change
                        
                        updates_made.append(f"🚨 EXITED: {exit_reason_to_set} at ₹{position.sell_price:.2f}")
                        logger.critical(f"🔴 EXIT RECORDED for {stock_name}:")
                        logger.critical(f"   Exit Reason: {exit_reason_to_set}")
                        logger.critical(f"   Sell Price: ₹{position.sell_price:.2f}")
                        logger.critical(f"   Option LTP (fetched): ₹{new_option_ltp:.2f}")
                        logger.critical(f"   Sell Time: {now.strftime('%H:%M:%S')}")
                        logger.critical(f"   Stock LTP: ₹{new_stock_ltp:.2f if new_stock_ltp else 0:.2f}, VWAP: ₹{new_vwap:.2f if new_vwap else 0:.2f}")
                        logger.critical(f"   PnL: ₹{position.pnl:.2f}")
                
                # SIMPLIFIED: Always update updated_at and count as updated
                from sqlalchemy.orm.attributes import flag_modified
                position.updated_at = now
                flag_modified(position, 'updated_at')
                updated_count += 1
                
                if updates_made:
                    logger.info(f"✅ {stock_name}: {', '.join(updates_made)}")
                else:
                    logger.info(f"✅ {stock_name}: Stock data updated (VWAP: ₹{new_vwap:.2f}, LTP: ₹{new_stock_ltp:.2f})")
                    
            except Exception as e:
                logger.error(f"Error updating position for {position.stock_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                failed_count += 1
        
        # ═══════════════════════════════════════════════════════════════
        # SAVE HISTORICAL MARKET DATA FOR NO_ENTRY TRADES
        # ═══════════════════════════════════════════════════════════════
        # Save historical data for no_entry trades that weren't included in open_positions
        # This ensures we have historical data at every hourly update (9:15, 10:15, 11:15, etc.)
        try:
            # Get all trades (including no_entry) for historical data saving
            all_trades_for_history = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    IntradayStockOption.trade_date < today + timedelta(days=1),
                    IntradayStockOption.exit_reason == None  # Only include trades that haven't exited yet
                )
            ).all()
            
            # Track which stocks we've already saved historical data for (from open_positions)
            stocks_with_history_saved = set()
            for position in open_positions:
                if hasattr(position, 'stock_name'):
                    stocks_with_history_saved.add(position.stock_name)
            
            if all_trades_for_history:
                no_entry_trades_for_history = [t for t in all_trades_for_history if t.stock_name not in stocks_with_history_saved and t.status == 'no_entry']
                if no_entry_trades_for_history:
                    logger.info(f"📊 Saving historical data for {len(no_entry_trades_for_history)} no_entry trades")
                    for trade in no_entry_trades_for_history:
                        try:
                            stock_name = trade.stock_name
                            stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                            if stock_data:
                                current_stock_ltp = stock_data.get('ltp', 0)
                                current_stock_vwap = stock_data.get('vwap', 0)
                                
                                # Get option LTP if available
                                current_option_ltp = None
                                if trade.instrument_key:
                                    try:
                                        option_quote = vwap_service.get_market_quote_by_key(trade.instrument_key)
                                        if option_quote and option_quote.get('last_price', 0) > 0:
                                            current_option_ltp = float(option_quote.get('last_price', 0))
                                    except:
                                        current_option_ltp = trade.option_ltp
                                else:
                                    current_option_ltp = trade.option_ltp
                                
                                # Check if historical data already exists to prevent duplicates
                                if not historical_data_exists(db, stock_name, now):
                                    # Get VWAP slope from trade record if available
                                    vwap_slope_angle = trade.vwap_slope_angle if hasattr(trade, 'vwap_slope_angle') else None
                                    vwap_slope_status = trade.vwap_slope_status if hasattr(trade, 'vwap_slope_status') else None
                                    vwap_slope_direction = trade.vwap_slope_direction if hasattr(trade, 'vwap_slope_direction') else None
                                    vwap_slope_time = trade.vwap_slope_time if hasattr(trade, 'vwap_slope_time') else None
                                    
                                    historical_record = HistoricalMarketData(
                                        stock_name=stock_name,
                                        stock_vwap=current_stock_vwap if current_stock_vwap > 0 else None,
                                        stock_ltp=current_stock_ltp if current_stock_ltp > 0 else None,
                                        vwap_slope_angle=vwap_slope_angle,
                                        vwap_slope_status=vwap_slope_status,
                                        vwap_slope_direction=vwap_slope_direction,
                                        vwap_slope_time=vwap_slope_time,
                                        option_contract=trade.option_contract,
                                        option_instrument_key=trade.instrument_key,
                                        option_ltp=current_option_ltp if current_option_ltp and current_option_ltp > 0 else None,
                                        scan_date=now,
                                        scan_time=now.strftime('%I:%M %p').lower()
                                    )
                                    db.add(historical_record)
                                    logger.debug(f"📊 Saved historical data for no_entry trade {stock_name} at {now.strftime('%H:%M:%S')}")
                            else:
                                logger.debug(f"⏭️ Skipping duplicate historical data for no_entry trade {stock_name} at {now.strftime('%H:%M:%S')} (already exists)")
                        except Exception as hist_error:
                            logger.warning(f"⚠️ Failed to save historical data for no_entry trade {trade.stock_name}: {str(hist_error)}")
        except Exception as e:
            logger.warning(f"⚠️ Error saving historical data for no_entry trades: {str(e)}")
        
        # Commit all updates
        # #region agent log
        try:
            # Check which positions have pending changes before commit
            pending_changes = []
            for pos in open_positions:
                if pos in db.dirty:
                    pending_changes.append({
                        "stock_name": pos.stock_name,
                        "sell_price": pos.sell_price,
                        "status": pos.status
                    })
            
            os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
            with open(log_path, 'a') as f:
                pre_commit_log = json_module.dumps({
                    "id": f"log_pre_commit_{int(now.timestamp())}",
                    "timestamp": int(now.timestamp() * 1000),
                    "location": "vwap_updater.py:1360",
                    "message": "Before database commit",
                    "data": {
                        "updated_count": updated_count,
                        "failed_count": failed_count,
                        "total_positions": len(open_positions),
                        "dirty_objects_count": len(db.dirty),
                        "pending_changes": pending_changes[:10]  # First 10 for brevity
                    },
                    "sessionId": "debug-session",
                    "runId": "sell-price-fix",
                    "hypothesisId": "PRE_COMMIT"
                }) + "\n"
                f.write(pre_commit_log)
                f.flush()
            logger.info(f"🔍 DEBUG PRE-COMMIT: {len(db.dirty)} objects marked as dirty, {updated_count} positions updated")
        except Exception as log_err:
            logger.error(f"❌ Failed to write pre-commit log: {str(log_err)}")
        # #endregion
        
        # SIMPLIFIED: Flush and commit all changes
        try:
            db.flush()
            db.commit()
            logger.info(f"✅ Committed {updated_count} position updates to database")
            
            # #region agent log
            try:
                os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
                with open(log_path, 'a') as f:
                    commit_success_log = json_module.dumps({
                        "id": f"log_commit_success_{int(now.timestamp())}",
                        "timestamp": int(now.timestamp() * 1000),
                        "location": "vwap_updater.py:1455",
                        "message": "Database commit successful",
                        "data": {
                            "updated_count": updated_count,
                            "failed_count": failed_count
                        },
                        "sessionId": "debug-session",
                        "runId": "sell-price-fix",
                        "hypothesisId": "COMMIT_SUCCESS"
                    }) + "\n"
                    f.write(commit_success_log)
                    f.flush()
            except Exception:
                pass
            # #endregion
        except Exception as commit_err:
            logger.error(f"❌ Database commit failed: {str(commit_err)}")
            import traceback
            traceback.print_exc()
            
            # #region agent log
            try:
                os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
                with open(log_path, 'a') as f:
                    commit_fail_log = json_module.dumps({
                        "id": f"log_commit_failed_{int(now.timestamp())}",
                        "timestamp": int(now.timestamp() * 1000),
                        "location": "vwap_updater.py:1470",
                        "message": "Database commit failed",
                        "data": {
                            "error": str(commit_err),
                            "updated_count": updated_count,
                            "failed_count": failed_count
                        },
                        "sessionId": "debug-session",
                        "runId": "sell-price-fix",
                        "hypothesisId": "COMMIT_FAILED"
                    }) + "\n"
                    f.write(commit_fail_log)
                    f.flush()
            except Exception:
                pass
            # #endregion
            
            db.rollback()
            raise
        
        # #region agent log
        try:
            # CRITICAL: Refresh all positions from database to see what's actually persisted
            # After commit, objects in the session might be stale, so refresh them
            for pos in open_positions:
                try:
                    db.refresh(pos)
                except Exception:
                    pass  # If refresh fails, continue
            
            # Verify sell_price was actually saved
            verification_positions = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    IntradayStockOption.status != 'sold',
                    IntradayStockOption.exit_reason == None
                )
            ).all()
            
            sell_price_status = {}
            for pos in verification_positions:
                has_sell_price = pos.sell_price is not None and pos.sell_price > 0
                sell_price_status[pos.stock_name] = {
                    "has_sell_price": has_sell_price,
                    "sell_price": float(pos.sell_price) if pos.sell_price else None,
                    "status": pos.status,
                    "buy_price": float(pos.buy_price) if pos.buy_price else None,
                    "pnl": float(pos.pnl) if pos.pnl else None,
                    "has_instrument_key": bool(pos.instrument_key),
                    "updated_at": str(pos.updated_at) if pos.updated_at else None
                }
            
            with_sell_price_count = sum(1 for v in sell_price_status.values() if v["has_sell_price"])
            without_sell_price_count = sum(1 for v in sell_price_status.values() if not v["has_sell_price"])
            
            os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
            with open(log_path, 'a') as f:
                post_commit_log = json_module.dumps({
                    "id": f"log_post_commit_{int(now.timestamp())}",
                    "timestamp": int(now.timestamp() * 1000),
                    "location": "vwap_updater.py:1520",
                    "message": "After database commit - verification",
                    "data": {
                        "sell_price_status": sell_price_status,
                        "positions_with_sell_price": with_sell_price_count,
                        "positions_without_sell_price": without_sell_price_count,
                        "total_positions": len(sell_price_status)
                    },
                    "sessionId": "debug-session",
                    "runId": "sell-price-fix",
                    "hypothesisId": "POST_COMMIT"
                }) + "\n"
                f.write(post_commit_log)
                f.flush()
            
            logger.info(f"🔍 DEBUG POST-COMMIT: {with_sell_price_count}/{len(sell_price_status)} positions have sell_price")
            if without_sell_price_count > 0:
                logger.warning(f"⚠️ POST-COMMIT WARNING: {without_sell_price_count} positions still missing sell_price!")
                missing_details = [k for k, v in sell_price_status.items() if not v["has_sell_price"]]
                logger.warning(f"   Missing sell_price: {missing_details[:5]}")
        except Exception as log_err:
            logger.error(f"❌ Failed to write post-commit log: {str(log_err)}")
            import traceback
            logger.error(traceback.format_exc())
        # #endregion
        
        logger.info(f"📊 Hourly Update Complete: {updated_count} positions updated, {failed_count} failed")
        
        # #region agent log - Final summary
        try:
            # Final verification query after all updates
            final_positions = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    IntradayStockOption.status != 'sold',
                    IntradayStockOption.exit_reason == None
                )
            ).all()
            
            final_summary = {
                "total_open_positions": len(final_positions),
                "with_sell_price": sum(1 for p in final_positions if p.sell_price and p.sell_price > 0),
                "without_sell_price": sum(1 for p in final_positions if not p.sell_price or p.sell_price == 0),
                "without_instrument_key": sum(1 for p in final_positions if not p.instrument_key),
                "positions_detail": [
                    {
                        "stock_name": p.stock_name,
                        "status": p.status,
                        "sell_price": float(p.sell_price) if p.sell_price else None,
                        "has_instrument_key": bool(p.instrument_key)
                    }
                    for p in final_positions[:10]
                ]
            }
            
            os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
            with open(log_path, 'a') as f:
                final_summary_log = json_module.dumps({
                    "id": f"log_final_summary_{int(now.timestamp())}",
                    "timestamp": int(now.timestamp() * 1000),
                    "location": "vwap_updater.py:1430",
                    "message": "Final summary after update",
                    "data": final_summary,
                    "sessionId": "debug-session",
                    "runId": "sell-price-fix",
                    "hypothesisId": "FINAL_SUMMARY"
                }) + "\n"
                f.write(final_summary_log)
                f.flush()
            
            logger.info(f"🔍 DEBUG FINAL SUMMARY: {final_summary['with_sell_price']}/{final_summary['total_open_positions']} positions have sell_price")
            logger.info(f"🔍 DEBUG FINAL SUMMARY: {final_summary['without_instrument_key']} positions missing instrument_key")
            if final_summary['without_sell_price'] > 0:
                logger.warning(f"⚠️ WARNING: {final_summary['without_sell_price']} positions still missing sell_price!")
                for pos_detail in final_summary['positions_detail']:
                    if not pos_detail['sell_price']:
                        logger.warning(f"   - {pos_detail['stock_name']} (status={pos_detail['status']}, has_instrument_key={pos_detail['has_instrument_key']})")
            
            # CRITICAL: Direct database query to verify what's actually persisted
            # Query fresh from database (new session) to see what's actually saved
            verification_db = SessionLocal()
            try:
                verification_query = verification_db.query(IntradayStockOption).filter(
                    and_(
                        IntradayStockOption.trade_date >= today,
                        IntradayStockOption.status != 'sold',
                        IntradayStockOption.exit_reason == None
                    )
                ).all()
                
                verification_results = []
                for vpos in verification_query:
                    verification_results.append({
                        "stock_name": vpos.stock_name,
                        "status": vpos.status,
                        "buy_price": float(vpos.buy_price) if vpos.buy_price else None,
                        "sell_price": float(vpos.sell_price) if vpos.sell_price else None,
                        "pnl": float(vpos.pnl) if vpos.pnl else None,
                        "has_instrument_key": bool(vpos.instrument_key),
                        "updated_at": str(vpos.updated_at) if vpos.updated_at else None
                    })
                
                with_sell_price_verification = sum(1 for v in verification_results if v['sell_price'])
                without_sell_price_verification = sum(1 for v in verification_results if not v['sell_price'])
                
                logger.info(f"🔍 DEBUG VERIFICATION QUERY: Fresh database query returned {len(verification_query)} positions")
                logger.info(f"🔍 DEBUG VERIFICATION: Positions with sell_price: {with_sell_price_verification}")
                logger.info(f"🔍 DEBUG VERIFICATION: Positions without sell_price: {without_sell_price_verification}")
                if verification_results:
                    logger.info(f"🔍 DEBUG VERIFICATION: Sample positions: {verification_results[:3]}")
                
                if without_sell_price_verification > 0:
                    logger.error(f"🚨 CRITICAL: {without_sell_price_verification} positions still missing sell_price after commit!")
                    missing_positions = [v for v in verification_results if not v['sell_price']]
                    for missing in missing_positions[:5]:
                        logger.error(f"   MISSING sell_price: {missing['stock_name']} - status={missing['status']}, has_instrument_key={missing['has_instrument_key']}, buy_price={missing['buy_price']}")
                
                # #region agent log
                try:
                    os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
                    with open(log_path, 'a') as f:
                        verification_log = json_module.dumps({
                            "id": f"log_verification_query_{int(now.timestamp())}",
                            "timestamp": int(now.timestamp() * 1000),
                            "location": "vwap_updater.py:1713",
                            "message": "Fresh database verification query",
                            "data": {
                                "total_positions": len(verification_query),
                                "with_sell_price": with_sell_price_verification,
                                "without_sell_price": without_sell_price_verification,
                                "positions": verification_results[:10]
                            },
                            "sessionId": "debug-session",
                            "runId": "sell-price-fix",
                            "hypothesisId": "VERIFICATION_QUERY"
                        }) + "\n"
                        f.write(verification_log)
                        f.flush()
                except Exception as verif_log_err:
                    logger.error(f"❌ Failed to write verification query log: {str(verif_log_err)}")
                # #endregion
            finally:
                verification_db.close()
        except Exception as summary_err:
            logger.error(f"❌ Failed to write final summary log: {str(summary_err)}")
        # #endregion
        
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR in hourly market data update job: {str(e)}")
        import traceback
        error_trace = traceback.format_exc()
        logger.error(error_trace)
        
        # #region agent log - Error logging
        try:
            import json as json_module
            import os as os_module
            log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
            os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
            with open(log_path, 'a') as f:
                error_log = json_module.dumps({
                    "id": f"log_function_error_{int(datetime.now(pytz.timezone('Asia/Kolkata')).timestamp())}",
                    "timestamp": int(datetime.now(pytz.timezone('Asia/Kolkata')).timestamp() * 1000),
                    "location": "vwap_updater.py:1666",
                    "message": "Function error",
                    "data": {
                        "error": str(e),
                        "traceback": error_trace[:500]  # First 500 chars of traceback
                    },
                    "sessionId": "debug-session",
                    "runId": "sell-price-fix",
                    "hypothesisId": "FUNCTION_ERROR"
                }) + "\n"
                f.write(error_log)
                f.flush()
        except Exception:
            pass
        # #endregion
        
        if db:
            try:
                db.rollback()
            except Exception:
                pass
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
        
        # CRITICAL: Log function exit with summary
        try:
            import pytz as pytz_module_exit
            from datetime import datetime as dt_module_exit
            ist_exit = pytz_module_exit.timezone('Asia/Kolkata')
            now_exit = dt_module_exit.now(ist_exit)
            exit_msg = f"🏁 FUNCTION EXIT: update_vwap_for_all_open_positions() completed at {now_exit.strftime('%Y-%m-%d %H:%M:%S IST')}"
            print(exit_msg, file=sys.stderr)
            try:
                logger.info(exit_msg)
            except Exception:
                pass
        except Exception:
            print("🏁 FUNCTION EXIT: update_vwap_for_all_open_positions() completed (timestamp unavailable)", file=sys.stderr)


async def calculate_vwap_slope_for_cycle(cycle_number: int, cycle_time: datetime):
    """
    Calculate VWAP slope for stocks based on cycle-based logic
    
    Cycle Rules:
    1. Cycle 1 (10:30 AM): Stocks from 10:15 AM webhook
       - Previous VWAP: 10:15 AM (1-hour candle, represents 9:15-10:15 AM)
       - Current VWAP: 10:30 AM (15-minute candle or real-time VWAP as fallback)
    
    2. Cycle 2 (11:15 AM): Stocks from 11:15 AM webhook + No_Entry from 10:15 AM
       - Previous VWAP: 10:15 AM (1-hour candle)
       - Current VWAP: 11:15 AM (1-hour candle)
    
    3. Cycle 3 (12:15 PM): Stocks from 12:15 PM webhook + No_Entry up to 11:15 AM
       - Previous VWAP: 11:15 AM (1-hour candle)
       - Current VWAP: 12:15 PM (1-hour candle)
    
    4. Cycle 4 (13:15 PM): Stocks from 13:15 PM webhook + No_Entry up to 12:15 PM
       - Previous VWAP: 12:15 PM (1-hour candle)
       - Current VWAP: 13:15 PM (1-hour candle)
    
    5. Cycle 5 (14:15 PM): Stocks from 14:15 PM webhook + No_Entry up to 13:15 PM
       - Previous VWAP: 13:15 PM (1-hour candle)
       - Current VWAP: 14:15 PM (1-hour candle)
    
    Args:
        cycle_number: Cycle number (1-5)
        cycle_time: Current cycle time (datetime, timezone-aware)
    """
    db = SessionLocal()
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = cycle_time if cycle_time.tzinfo else ist.localize(cycle_time)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # #region agent log
        # Log function entry immediately - CRITICAL: This must work
        import json
        import os
        log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            # Write entry log
            with open(log_path, 'a') as f:
                entry_log = json.dumps({"id":f"log_cycle1_entry_{int(now.timestamp())}","timestamp":int(now.timestamp()*1000),"location":"vwap_updater.py:1190","message":"Cycle 1 - Function entry","data":{"cycle_number":cycle_number,"cycle_time":str(cycle_time),"now":str(now),"today":str(today)},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"ENTRY"}) + "\n"
                f.write(entry_log)
                f.flush()  # Force write to disk
            logger.info(f"📝 Debug log written to {log_path}")
        except Exception as log_err:
            logger.error(f"❌ CRITICAL: Failed to write debug log entry: {str(log_err)}")
            import traceback
            logger.error(traceback.format_exc())
        # #endregion
        
        logger.info(f"🔄 Starting Cycle {cycle_number} VWAP slope calculation at {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        # Import VWAP service
        try:
            from backend.services.upstox_service import upstox_service
            vwap_service = upstox_service
        except ImportError:
            try:
                # Fallback for different import paths
                from services.upstox_service import upstox_service
                vwap_service = upstox_service
            except ImportError:
                logger.error("Could not import upstox_service")
                return
        
        # Determine previous VWAP time and current VWAP time based on cycle
        if cycle_number == 1:
            # Cycle 1: 10:30 AM
            # Previous VWAP: Use 1-hour candle at 10:15 AM (1-hour candle closes at 10:15 AM, represents 9:15-10:15 AM)
            # Current VWAP: Use real-time VWAP or 15-minute candle at 10:30 AM
            # Market opens at 9:15 AM, so hourly candles form at :15 times
            prev_vwap_time = today.replace(hour=10, minute=15, second=0, microsecond=0)
            current_vwap_time = today.replace(hour=10, minute=30, second=0, microsecond=0)
            prev_interval = "hours/1"  # Use 1-hour candle for 10:15 AM (more reliable than 15-minute)
            current_interval = "minutes/15"  # Use 15-minute candle for 10:30 AM, fallback to real-time if unavailable
            # Stocks from 10:15 AM webhook
            target_alert_times = [today.replace(hour=10, minute=15, second=0, microsecond=0)]
        elif cycle_number == 2:
            # Cycle 2: 11:15 AM
            # Previous VWAP: Use 1-hour candle at 10:15 AM (1-hour candle closes at 10:15 AM, represents 9:15-10:15 AM)
            # Market opens at 9:15 AM, so hourly candles form at :15 times
            prev_vwap_time = today.replace(hour=10, minute=15, second=0, microsecond=0)
            current_vwap_time = today.replace(hour=11, minute=15, second=0, microsecond=0)
            prev_interval = "hours/1"  # Use 1-hour candle (closes at 10:15 AM)
            current_interval = "hours/1"  # Use 1-hour candle (closes at 11:15 AM)
            # Stocks from 11:15 AM webhook + No_Entry from 10:15 AM
            target_alert_times = [
                today.replace(hour=10, minute=15, second=0, microsecond=0),
                today.replace(hour=11, minute=15, second=0, microsecond=0)
            ]
        elif cycle_number == 3:
            # Cycle 3: 12:15 PM
            # Previous VWAP: Use 1-hour candle at 11:15 AM (1-hour candle closes at 11:15 AM, represents 10:15-11:15 AM)
            # Market opens at 9:15 AM, so hourly candles form at :15 times
            prev_vwap_time = today.replace(hour=11, minute=15, second=0, microsecond=0)
            current_vwap_time = today.replace(hour=12, minute=15, second=0, microsecond=0)
            prev_interval = "hours/1"  # Use 1-hour candle (closes at 11:15 AM)
            current_interval = "hours/1"  # Use 1-hour candle (closes at 12:15 PM)
            # Stocks from 12:15 PM webhook + No_Entry up to 11:15 AM
            target_alert_times = [
                today.replace(hour=10, minute=15, second=0, microsecond=0),
                today.replace(hour=11, minute=15, second=0, microsecond=0),
                today.replace(hour=12, minute=15, second=0, microsecond=0)
            ]
        elif cycle_number == 4:
            # Cycle 4: 13:15 PM
            # Previous VWAP: Use 1-hour candle at 12:15 PM (1-hour candle closes at 12:15 PM, represents 11:15 AM-12:15 PM)
            # Market opens at 9:15 AM, so hourly candles form at :15 times
            prev_vwap_time = today.replace(hour=12, minute=15, second=0, microsecond=0)
            current_vwap_time = today.replace(hour=13, minute=15, second=0, microsecond=0)
            prev_interval = "hours/1"  # Use 1-hour candle (closes at 12:15 PM)
            current_interval = "hours/1"  # Use 1-hour candle (closes at 13:15 PM)
            # Stocks from 13:15 PM webhook + No_Entry up to 12:15 PM
            target_alert_times = [
                today.replace(hour=10, minute=15, second=0, microsecond=0),
                today.replace(hour=11, minute=15, second=0, microsecond=0),
                today.replace(hour=12, minute=15, second=0, microsecond=0),
                today.replace(hour=13, minute=15, second=0, microsecond=0)
            ]
        elif cycle_number == 5:
            # Cycle 5: 14:15 PM
            # Previous VWAP: Use 1-hour candle at 13:15 PM (1-hour candle closes at 13:15 PM, represents 12:15 PM-13:15 PM)
            # Market opens at 9:15 AM, so hourly candles form at :15 times
            prev_vwap_time = today.replace(hour=13, minute=15, second=0, microsecond=0)
            current_vwap_time = today.replace(hour=14, minute=15, second=0, microsecond=0)
            prev_interval = "hours/1"  # Use 1-hour candle (closes at 13:15 PM)
            current_interval = "hours/1"  # Use 1-hour candle (closes at 14:15 PM)
            # Stocks from 14:15 PM webhook + No_Entry up to 13:15 PM
            target_alert_times = [
                today.replace(hour=10, minute=15, second=0, microsecond=0),
                today.replace(hour=11, minute=15, second=0, microsecond=0),
                today.replace(hour=12, minute=15, second=0, microsecond=0),
                today.replace(hour=13, minute=15, second=0, microsecond=0),
                today.replace(hour=14, minute=15, second=0, microsecond=0)
            ]
        else:
            logger.error(f"Invalid cycle number: {cycle_number}")
            return
        
        # Query stocks that need VWAP slope calculation
        # Rules:
        # 1. Cycle 1 (10:30 AM): ALL stocks from 10:15 AM webhook (regardless of status)
        #    - This ensures VWAP slope is calculated for all 10:15 AM records
        # 2. Other cycles: Stocks from webhook alerts at CURRENT cycle's alert time (if status is still 'alert_received' or 'no_entry')
        # 3. No_Entry stocks from PREVIOUS cycles (up to previous cycle's alert time)
        # 4. For cycles 2-5: VWAP slope is NOT calculated if status is not No_Entry (already entered)
        # 5. Candle size is only calculated when stock is received from webhook alert scan
        #    If status is No_Entry, candle size will not be recalculated in subsequent cycles
        from datetime import timedelta
        
        # Determine current cycle's alert time
        current_cycle_alert_time = max(target_alert_times)  # Latest alert time = current cycle
        
        # Build query based on cycle number
        if cycle_number == 1:
            # #region agent log
            # First, query ALL 10:15 AM records to see status breakdown
            all_10_15_records = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    IntradayStockOption.alert_time >= target_alert_times[0],
                    IntradayStockOption.alert_time < target_alert_times[0] + timedelta(minutes=1)
                )
            ).all()
            status_breakdown = {}
            for r in all_10_15_records:
                status_breakdown[r.status] = status_breakdown.get(r.status, 0) + 1
            
            # Log to application logger FIRST (always works)
            logger.info(f"🔍 DEBUG Cycle 1: Found {len(all_10_15_records)} total 10:15 AM records. Status breakdown: {status_breakdown}")
            
            import json
            import os
            log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
            try:
                # Ensure directory exists
                os.makedirs(os.path.dirname(log_path), exist_ok=True)
                with open(log_path, 'a') as f:
                    f.write(json.dumps({"id":f"log_cycle1_{int(now.timestamp())}","timestamp":int(now.timestamp()*1000),"location":"vwap_updater.py:1296","message":"Cycle 1 - All 10:15 AM records status breakdown","data":{"total_records":len(all_10_15_records),"status_breakdown":status_breakdown,"cycle_number":1,"target_alert_time":str(target_alert_times[0])},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"A"}) + "\n")
                    f.flush()
            except Exception as log_err:
                logger.error(f"Failed to write debug log (hypothesis A): {str(log_err)}")
            # #endregion
            
            # Cycle 1: ALL stocks from 10:15 AM webhook (regardless of status)
            # This ensures VWAP slope is calculated for all 10:15 AM records at 10:30 AM
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    IntradayStockOption.alert_time >= target_alert_times[0],
                    IntradayStockOption.alert_time < target_alert_times[0] + timedelta(minutes=1)
                )
            ).all()
            
            # Log query result to application logger
            logger.info(f"🔍 DEBUG Cycle 1: Query returned {len(stocks_to_process)} stocks to process (should match {len(all_10_15_records)} total records)")
        elif cycle_number == 2:
            # Cycle 2: Stocks from 11:15 AM webhook (ALL statuses for VWAP slope) + No_Entry from 10:15 AM
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    or_(
                        # Current cycle: ALL stocks from 11:15 AM webhook (for VWAP slope calculation)
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[1],
                            IntradayStockOption.alert_time < target_alert_times[1] + timedelta(minutes=1)
                        ),
                        # Previous cycle: No_Entry OR alert_received stocks from 10:15 AM (not yet entered)
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[0],
                            IntradayStockOption.alert_time < target_alert_times[0] + timedelta(minutes=1),
                            or_(
                                IntradayStockOption.status == 'no_entry',
                                IntradayStockOption.status == 'alert_received'
                            )
                        )
                    )
                )
            ).all()
        elif cycle_number == 3:
            # Cycle 3: Stocks from 12:15 PM webhook (ALL statuses for VWAP slope) + No_Entry up to 11:15 AM
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    or_(
                        # Current cycle: ALL stocks from 12:15 PM webhook (for VWAP slope calculation)
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[2],
                            IntradayStockOption.alert_time < target_alert_times[2] + timedelta(minutes=1)
                        ),
                        # Previous cycles: No_Entry OR alert_received stocks up to 11:15 AM (not yet entered)
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[0],
                            IntradayStockOption.alert_time < target_alert_times[2],
                            or_(
                                IntradayStockOption.status == 'no_entry',
                                IntradayStockOption.status == 'alert_received'
                            )
                        )
                    )
                )
            ).all()
        elif cycle_number == 4:
            # Cycle 4: Stocks from 13:15 PM webhook (ALL statuses for VWAP slope) + No_Entry up to 12:15 PM
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    or_(
                        # Current cycle: ALL stocks from 13:15 PM webhook (for VWAP slope calculation)
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[3],
                            IntradayStockOption.alert_time < target_alert_times[3] + timedelta(minutes=1)
                        ),
                        # Previous cycles: No_Entry OR alert_received stocks up to 12:15 PM (not yet entered)
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[0],
                            IntradayStockOption.alert_time < target_alert_times[3],
                            or_(
                                IntradayStockOption.status == 'no_entry',
                                IntradayStockOption.status == 'alert_received'
                            )
                        )
                    )
                )
            ).all()
        elif cycle_number == 5:
            # Cycle 5: Stocks from 14:15 PM webhook (ALL statuses for VWAP slope) + No_Entry up to 13:15 PM
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    or_(
                        # Current cycle: ALL stocks from 14:15 PM webhook (for VWAP slope calculation)
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[4],
                            IntradayStockOption.alert_time < target_alert_times[4] + timedelta(minutes=1)
                        ),
                        # Previous cycles: No_Entry OR alert_received stocks up to 13:15 PM (not yet entered)
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[0],
                            IntradayStockOption.alert_time < target_alert_times[4],
                            or_(
                                IntradayStockOption.status == 'no_entry',
                                IntradayStockOption.status == 'alert_received'
                            )
                        )
                    )
                )
            ).all()
        else:
            stocks_to_process = []
        
        if not stocks_to_process:
            # #region agent log
            # Log when no stocks found
            import json
            with open('/Users/bipulsahay/TradeManthan/.cursor/debug.log', 'a') as f:
                f.write(json.dumps({"id":f"log_cycle1_no_stocks_{int(now.timestamp())}","timestamp":int(now.timestamp()*1000),"location":"vwap_updater.py:1436","message":"Cycle 1 - No stocks found","data":{"cycle_number":cycle_number,"target_alert_times":[str(t) for t in target_alert_times],"today":str(today)},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"F"}) + "\n")
            # #endregion
            logger.info(f"ℹ️ No stocks found for Cycle {cycle_number} VWAP slope calculation")
            return
        
        logger.info(f"📋 Found {len(stocks_to_process)} stocks for Cycle {cycle_number} VWAP slope calculation")
        
        # #region agent log
        # Log status breakdown of stocks to process
        process_status_breakdown = {}
        for t in stocks_to_process:
            process_status_breakdown[t.status] = process_status_breakdown.get(t.status, 0) + 1
        import json
        import os
        log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, 'a') as f:
                f.write(json.dumps({"id":f"log_cycle1_process_{int(now.timestamp())}","timestamp":int(now.timestamp()*1000),"location":"vwap_updater.py:1424","message":"Cycle 1 - Stocks to process status breakdown","data":{"total_to_process":len(stocks_to_process),"status_breakdown":process_status_breakdown,"cycle_number":1},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"B"}) + "\n")
        except Exception as log_err:
            logger.error(f"Failed to write debug log (hypothesis B): {str(log_err)}")
        # #endregion
        
        processed_count = 0
        success_count = 0
        
        for trade in stocks_to_process:
            try:
                stock_name = trade.stock_name
                
                # #region agent log
                # Log each record being processed and its status
                # Log to application logger FIRST
                logger.info(f"🔍 DEBUG Cycle {cycle_number}: Processing {stock_name} (status: {trade.status}, alert_time: {trade.alert_time})")
                
                import json
                import os
                log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
                try:
                    os.makedirs(os.path.dirname(log_path), exist_ok=True)
                    with open(log_path, 'a') as f:
                        f.write(json.dumps({"id":f"log_cycle1_trade_{trade.id}","timestamp":int(now.timestamp()*1000),"location":"vwap_updater.py:1430","message":"Cycle 1 - Processing trade","data":{"stock_name":stock_name,"status":trade.status,"alert_time":str(trade.alert_time) if trade.alert_time else None,"cycle_number":1},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"C"}) + "\n")
                        f.flush()
                except Exception as log_err:
                    logger.error(f"Failed to write debug log (hypothesis C): {str(log_err)}")
                # #endregion
                
                # Calculate VWAP slope for ALL records regardless of status (informational)
                # Entry decisions are only made for 'no_entry' or 'alert_received' status
                # But VWAP slope should be calculated for all trades to display in frontend
                if cycle_number == 1:
                    # Cycle 1: Process ALL records regardless of status
                    pass  # Continue processing
                else:
                    # Other cycles: Calculate VWAP slope for ALL trades (including 'bought' and 'sold')
                    # Entry decisions will be checked separately below for 'no_entry' and 'alert_received' only
                    pass  # Continue processing for VWAP slope calculation
                
                # Get current stock data first (needed for historical record)
                stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                
                # Try to get previous VWAP from candle API
                # Market opens at 9:15 AM, so 1-hour candles form at :15 times (10:15, 11:15, etc.)
                prev_vwap_data = vwap_service.get_stock_vwap_from_candle_at_time(
                    stock_name,
                    prev_vwap_time,
                    interval=prev_interval
                )
                
                # Validate that the returned candle is from the correct date
                # If it's from a different date (e.g., yesterday), treat it as a failure
                if prev_vwap_data and prev_vwap_data.get('time'):
                    candle_time = prev_vwap_data.get('time')
                    # Ensure both are timezone-aware for comparison
                    if candle_time.tzinfo is None:
                        candle_time = ist.localize(candle_time)
                    elif candle_time.tzinfo != ist:
                        candle_time = candle_time.astimezone(ist)
                    
                    if prev_vwap_time.tzinfo is None:
                        prev_vwap_time_tz = ist.localize(prev_vwap_time)
                    elif prev_vwap_time.tzinfo != ist:
                        prev_vwap_time_tz = prev_vwap_time.astimezone(ist)
                    else:
                        prev_vwap_time_tz = prev_vwap_time
                    
                    # Check if candle date matches target date
                    if candle_time.date() != prev_vwap_time_tz.date():
                        logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Candle date mismatch (got {candle_time.date()}, expected {prev_vwap_time_tz.date()}), treating as failure")
                        prev_vwap_data = None
                
                # If candle API fails or returned wrong date, try using stored previous VWAP from database
                # BUT only if the stored time is correct (not 20:15:00 which indicates wrong calculation)
                if not prev_vwap_data:
                    # Check if stored time is reasonable (should be around :15 minutes, not :15 hours)
                    stored_time_valid = False
                    if trade.stock_vwap_previous_hour and trade.stock_vwap_previous_hour > 0 and trade.stock_vwap_previous_hour_time:
                        stored_time = trade.stock_vwap_previous_hour_time
                        if stored_time.tzinfo is None:
                            stored_time = ist.localize(stored_time)
                        elif stored_time.tzinfo != ist:
                            stored_time = stored_time.astimezone(ist)
                        
                        # Check if stored time hour is reasonable (should be < 16, i.e., before 4 PM)
                        # If hour is 20 (8 PM), it's definitely wrong
                        if stored_time.hour < 16 and stored_time.date() == prev_vwap_time_tz.date():
                            stored_time_valid = True
                    
                    if stored_time_valid:
                        logger.info(f"🔄 Cycle {cycle_number} - {stock_name}: Using stored previous VWAP from database")
                        prev_vwap_data = {
                            'vwap': trade.stock_vwap_previous_hour,
                            'time': trade.stock_vwap_previous_hour_time
                        }
                
                # If still no previous VWAP, try alternative method
                if not prev_vwap_data:
                    try:
                        # Try getting previous hour VWAP using alternative method
                        # Pass cycle_time as reference_time so it calculates previous hour correctly
                        alt_prev_vwap = vwap_service.get_stock_vwap_for_previous_hour(stock_name, reference_time=now)
                        if alt_prev_vwap and alt_prev_vwap.get('vwap', 0) > 0:
                            prev_vwap_data = alt_prev_vwap
                            logger.info(f"🔄 Cycle {cycle_number} - {stock_name}: Using alternative method for previous VWAP")
                    except Exception as alt_error:
                        logger.debug(f"Alternative previous VWAP method also failed for {stock_name}: {str(alt_error)}")
                
                # Final fallback: Use previous trading day's daily VWAP (close price)
                # BUT: For Cycle 1 (10:30 AM), don't use this fallback because:
                # - Previous day's close (3:30 PM) vs today's 10:30 AM is not a fair comparison
                # - The time difference is too large (~19 hours) and not representative of intraday slope
                # - Instead, we should use market open VWAP or skip calculation
                if not prev_vwap_data:
                    if cycle_number == 1:
                        # For Cycle 1: Try to get VWAP at market open (9:15 AM) as fallback
                        try:
                            logger.info(f"🔄 Cycle {cycle_number} - {stock_name}: Attempting to get market open VWAP (9:15 AM) as fallback for Cycle 1")
                            market_open_time = today.replace(hour=9, minute=15, second=0, microsecond=0)
                            market_open_vwap = vwap_service.get_stock_vwap_from_candle_at_time(
                                stock_name,
                                market_open_time,
                                interval="hours/1"
                            )
                            if market_open_vwap and market_open_vwap.get('vwap', 0) > 0:
                                # Validate date
                                vwap_time = market_open_vwap.get('time')
                                if vwap_time:
                                    if vwap_time.tzinfo is None:
                                        vwap_time = ist.localize(vwap_time)
                                    elif vwap_time.tzinfo != ist:
                                        vwap_time = vwap_time.astimezone(ist)
                                    
                                    if vwap_time.date() == today:
                                        prev_vwap_data = market_open_vwap
                                        logger.info(f"✅ Cycle {cycle_number} - {stock_name}: Using market open VWAP (9:15 AM, ₹{market_open_vwap.get('vwap', 0):.2f}) as fallback")
                                    else:
                                        logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Market open VWAP date mismatch")
                                else:
                                    prev_vwap_data = market_open_vwap
                                    logger.info(f"✅ Cycle {cycle_number} - {stock_name}: Using market open VWAP (₹{market_open_vwap.get('vwap', 0):.2f}) as fallback")
                        except Exception as market_open_error:
                            logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Error getting market open VWAP: {str(market_open_error)}")
                        
                        # If still no data, try previous trading day as last resort (but log warning)
                        if not prev_vwap_data:
                            try:
                                logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Using previous trading day VWAP as last resort (not ideal for Cycle 1)")
                                prev_day_vwap = vwap_service.get_previous_trading_day_vwap(stock_name, reference_time=now)
                                if prev_day_vwap and prev_day_vwap.get('vwap', 0) > 0:
                                    prev_vwap_data = prev_day_vwap
                                    logger.info(f"✅ Cycle {cycle_number} - {stock_name}: Using previous trading day's daily VWAP (₹{prev_day_vwap.get('vwap', 0):.2f}) as last resort")
                            except Exception as fallback_error:
                                logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Error in previous trading day VWAP fallback: {str(fallback_error)}")
                    else:
                        # For other cycles: Use previous trading day VWAP as fallback (acceptable)
                        try:
                            logger.info(f"🔄 Cycle {cycle_number} - {stock_name}: Attempting fallback to previous trading day's daily VWAP")
                            prev_day_vwap = vwap_service.get_previous_trading_day_vwap(stock_name, reference_time=now)
                            if prev_day_vwap and prev_day_vwap.get('vwap', 0) > 0:
                                prev_vwap_data = prev_day_vwap
                                logger.info(f"✅ Cycle {cycle_number} - {stock_name}: Using previous trading day's daily VWAP (₹{prev_day_vwap.get('vwap', 0):.2f}) as fallback")
                            else:
                                logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Previous trading day VWAP fallback also failed")
                        except Exception as fallback_error:
                            logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Error in previous trading day VWAP fallback: {str(fallback_error)}")
                
                if not prev_vwap_data:
                    logger.warning(f"⚠️ Could not get previous VWAP for {stock_name} at {prev_vwap_time.strftime('%H:%M')}")
                    # Still save historical data even if VWAP slope cannot be calculated
                    current_stock_ltp = stock_data.get('ltp', 0) if stock_data else (trade.stock_ltp or 0)
                    current_stock_vwap = stock_data.get('vwap', 0) if stock_data else (trade.stock_vwap or 0)
                    current_option_ltp = None
                    if trade.instrument_key:
                        try:
                            option_quote = vwap_service.get_market_quote_by_key(trade.instrument_key)
                            if option_quote and option_quote.get('last_price', 0) > 0:
                                current_option_ltp = float(option_quote.get('last_price', 0))
                        except:
                            current_option_ltp = trade.option_ltp
                    else:
                        current_option_ltp = trade.option_ltp
                    
                    # Save historical data even when VWAP slope calculation fails
                    try:
                        if not historical_data_exists(db, stock_name, now):
                            # VWAP slope not calculated yet (previous VWAP unavailable)
                            historical_record = HistoricalMarketData(
                                stock_name=stock_name,
                                stock_vwap=current_stock_vwap if current_stock_vwap > 0 else None,
                                stock_ltp=current_stock_ltp if current_stock_ltp > 0 else None,
                                vwap_slope_angle=None,
                                vwap_slope_status=None,
                                vwap_slope_direction=None,
                                vwap_slope_time=None,
                                option_contract=trade.option_contract,
                                option_instrument_key=trade.instrument_key,
                                option_ltp=current_option_ltp if current_option_ltp and current_option_ltp > 0 else None,
                                scan_date=now,
                                scan_time=now.strftime('%I:%M %p').lower()
                            )
                            db.add(historical_record)
                            logger.debug(f"📊 Cycle {cycle_number} - Saved historical data for {stock_name} (VWAP calc failed) at {now.strftime('%H:%M:%S')}")
                    except Exception as hist_error:
                        logger.warning(f"⚠️ Cycle {cycle_number} - Failed to save historical data for {stock_name}: {str(hist_error)}")
                    
                    success_count += 1
                    processed_count += 1
                    continue
                
                prev_vwap = prev_vwap_data.get('vwap', 0)
                prev_vwap_time_actual = prev_vwap_data.get('time')
                
                # Get current VWAP
                # Market opens at 9:15 AM, so 1-hour candles form at :15 times (10:15, 11:15, etc.)
                current_vwap_data = vwap_service.get_stock_vwap_from_candle_at_time(
                    stock_name,
                    current_vwap_time,
                    interval=current_interval
                )
                
                # If candle API fails, try using current stock VWAP from real-time data
                if not current_vwap_data and stock_data and stock_data.get('vwap', 0) > 0:
                    logger.info(f"🔄 Cycle {cycle_number} - {stock_name}: Using real-time VWAP from stock_data")
                    current_vwap_data = {
                        'vwap': stock_data.get('vwap', 0),
                        'time': now
                    }
                
                if not current_vwap_data:
                    logger.warning(f"⚠️ Could not get current VWAP for {stock_name} at {current_vwap_time.strftime('%H:%M')}")
                    # Still save historical data even if VWAP slope cannot be calculated
                    current_stock_ltp = stock_data.get('ltp', 0) if stock_data else (trade.stock_ltp or 0)
                    current_stock_vwap = stock_data.get('vwap', 0) if stock_data else (trade.stock_vwap or 0)
                    current_option_ltp = None
                    if trade.instrument_key:
                        try:
                            option_quote = vwap_service.get_market_quote_by_key(trade.instrument_key)
                            if option_quote and option_quote.get('last_price', 0) > 0:
                                current_option_ltp = float(option_quote.get('last_price', 0))
                        except:
                            current_option_ltp = trade.option_ltp
                    else:
                        current_option_ltp = trade.option_ltp
                    
                    # Save historical data even when VWAP slope calculation fails
                    try:
                        if not historical_data_exists(db, stock_name, now):
                            # VWAP slope not calculated yet (previous VWAP unavailable)
                            historical_record = HistoricalMarketData(
                                stock_name=stock_name,
                                stock_vwap=current_stock_vwap if current_stock_vwap > 0 else None,
                                stock_ltp=current_stock_ltp if current_stock_ltp > 0 else None,
                                vwap_slope_angle=None,
                                vwap_slope_status=None,
                                vwap_slope_direction=None,
                                vwap_slope_time=None,
                                option_contract=trade.option_contract,
                                option_instrument_key=trade.instrument_key,
                                option_ltp=current_option_ltp if current_option_ltp and current_option_ltp > 0 else None,
                                scan_date=now,
                                scan_time=now.strftime('%I:%M %p').lower()
                            )
                            db.add(historical_record)
                            logger.debug(f"📊 Cycle {cycle_number} - Saved historical data for {stock_name} (VWAP calc failed) at {now.strftime('%H:%M:%S')}")
                    except Exception as hist_error:
                        logger.warning(f"⚠️ Cycle {cycle_number} - Failed to save historical data for {stock_name}: {str(hist_error)}")
                    
                    success_count += 1
                    processed_count += 1
                    continue
                
                current_vwap = current_vwap_data.get('vwap', 0)
                current_vwap_time_actual = current_vwap_data.get('time')
                
                if prev_vwap <= 0 or current_vwap <= 0:
                    logger.warning(f"⚠️ Invalid VWAP values for {stock_name} (prev: {prev_vwap}, current: {current_vwap})")
                    # Still save historical data even if VWAP values are invalid
                    current_stock_ltp = stock_data.get('ltp', 0) if stock_data else (trade.stock_ltp or 0)
                    current_stock_vwap = stock_data.get('vwap', 0) if stock_data else (trade.stock_vwap or 0)
                    current_option_ltp = None
                    if trade.instrument_key:
                        try:
                            option_quote = vwap_service.get_market_quote_by_key(trade.instrument_key)
                            if option_quote and option_quote.get('last_price', 0) > 0:
                                current_option_ltp = float(option_quote.get('last_price', 0))
                        except:
                            current_option_ltp = trade.option_ltp
                    else:
                        current_option_ltp = trade.option_ltp
                    
                    # Save historical data even when VWAP values are invalid
                    try:
                        if not historical_data_exists(db, stock_name, now):
                            # VWAP slope not calculated (invalid VWAP values)
                            historical_record = HistoricalMarketData(
                                stock_name=stock_name,
                                stock_vwap=current_stock_vwap if current_stock_vwap > 0 else None,
                                stock_ltp=current_stock_ltp if current_stock_ltp > 0 else None,
                                vwap_slope_angle=None,
                                vwap_slope_status=None,
                                vwap_slope_direction=None,
                                vwap_slope_time=None,
                                option_contract=trade.option_contract,
                                option_instrument_key=trade.instrument_key,
                                option_ltp=current_option_ltp if current_option_ltp and current_option_ltp > 0 else None,
                                scan_date=now,
                                scan_time=now.strftime('%I:%M %p').lower()
                            )
                            db.add(historical_record)
                            logger.debug(f"📊 Cycle {cycle_number} - Saved historical data for {stock_name} (invalid VWAP) at {now.strftime('%H:%M:%S')}")
                    except Exception as hist_error:
                        logger.warning(f"⚠️ Cycle {cycle_number} - Failed to save historical data for {stock_name}: {str(hist_error)}")
                    
                    success_count += 1
                    processed_count += 1
                    continue
                
                # Calculate VWAP slope
                # Log VWAP values before calculation for debugging
                logger.info(f"🔍 Cycle {cycle_number} - {stock_name}: Calculating VWAP slope - Prev: ₹{prev_vwap:.2f} @ {prev_vwap_time_actual.strftime('%H:%M:%S') if prev_vwap_time_actual else 'N/A'}, Current: ₹{current_vwap:.2f} @ {current_vwap_time_actual.strftime('%H:%M:%S') if current_vwap_time_actual else 'N/A'}")
                
                slope_result = vwap_service.vwap_slope(
                    vwap1=prev_vwap,
                    time1=prev_vwap_time_actual,
                    vwap2=current_vwap,
                    time2=current_vwap_time_actual
                )
                
                if isinstance(slope_result, dict):
                    slope_status = slope_result.get("status", "No")
                    slope_angle = slope_result.get("angle", 0.0)
                    slope_direction = slope_result.get("direction", "flat")
                    vwap_slope_passed = (slope_status == "Yes")
                    
                    # Log if angle is 0 to help diagnose
                    if slope_angle == 0.0 and prev_vwap != current_vwap:
                        logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: VWAP slope angle is 0.0 but VWAP values differ (prev: ₹{prev_vwap:.2f}, current: ₹{current_vwap:.2f})")
                else:
                    slope_status = slope_result if isinstance(slope_result, str) else "No"
                    slope_angle = 0.0
                    slope_direction = "flat"
                    vwap_slope_passed = (slope_status == "Yes")
                    logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: vwap_slope returned non-dict result: {type(slope_result)}")
                
                # Update database with VWAP slope data
                trade.stock_vwap_previous_hour = prev_vwap
                trade.stock_vwap_previous_hour_time = prev_vwap_time_actual
                trade.stock_vwap = current_vwap
                
                # Store VWAP slope results
                trade.vwap_slope_status = slope_status
                trade.vwap_slope_angle = slope_angle
                trade.vwap_slope_direction = slope_direction
                trade.vwap_slope_time = current_vwap_time_actual
                
                # #region agent log
                # Log VWAP slope calculation result before commit
                # Log to application logger FIRST
                logger.info(f"🔍 DEBUG Cycle {cycle_number}: VWAP slope calculated for {stock_name} (status: {trade.status}) - Angle: {slope_angle:.2f}°, Status: {slope_status}, Direction: {slope_direction}")
                
                import json
                import os
                log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
                try:
                    os.makedirs(os.path.dirname(log_path), exist_ok=True)
                    with open(log_path, 'a') as f:
                        f.write(json.dumps({"id":f"log_vwap_slope_calc_{trade.id}","timestamp":int(now.timestamp()*1000),"location":"vwap_updater.py:1722","message":"VWAP slope calculated and stored","data":{"stock_name":stock_name,"status":trade.status,"vwap_slope_angle":slope_angle,"vwap_slope_status":slope_status,"vwap_slope_direction":slope_direction,"cycle_number":cycle_number},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"D"}) + "\n")
                        f.flush()
                except Exception as log_err:
                    logger.error(f"Failed to write debug log (hypothesis D): {str(log_err)}")
                # #endregion
                
                logger.info(f"✅ Cycle {cycle_number} - {stock_name}: VWAP slope {slope_angle:.2f}° ({slope_direction}) - {'PASS' if vwap_slope_passed else 'FAIL'}")
                
                # ====================================================================
                # RETRY OPTION CONTRACT DETERMINATION IF MISSING
                # ====================================================================
                # If option contract was not determined initially, retry now
                # This must be done BEFORE recalculating candle size, so we have instrument_key
                if not trade.option_contract or not trade.instrument_key:
                    try:
                        from backend.routers.scan import find_option_contract_from_master_stock
                        
                        # Get current stock LTP
                        stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                        if stock_data and stock_data.get('ltp', 0) > 0:
                            stock_ltp = stock_data.get('ltp', 0)
                            # Determine option_type from alert_type if not set
                            if trade.option_type:
                                option_type = trade.option_type
                            elif trade.alert_type == 'Bearish':
                                option_type = 'PE'
                            elif trade.alert_type == 'Bullish':
                                option_type = 'CE'
                            else:
                                option_type = 'PE'  # Default to PE
                            logger.info(f"🔄 Cycle {cycle_number} - {stock_name}: Using option_type {option_type} (from alert_type: {trade.alert_type})")
                            
                            # Try to find option contract
                            option_contract = find_option_contract_from_master_stock(
                                db, stock_name, option_type, stock_ltp, vwap_service
                            )
                            
                            if option_contract:
                                trade.option_contract = option_contract
                                
                                # Extract strike from contract name
                                import re
                                match = re.search(r'-(\d+\.?\d*)-(?:CE|PE)$', option_contract)
                                if match:
                                    trade.option_strike = float(match.group(1))
                                
                                # Fetch lot_size and instrument_key from master_stock table
                                from backend.models.trading import MasterStock
                                master_record = db.query(MasterStock).filter(
                                    MasterStock.symbol_name == option_contract
                                ).first()
                                
                                if master_record:
                                    if master_record.lot_size:
                                        trade.qty = int(master_record.lot_size)
                                    
                                    # Get instrument_key from instruments JSON
                                    try:
                                        from pathlib import Path
                                        import json as json_lib
                                        
                                        instruments_path = Path(__file__).parent.parent.parent / "instruments.json"
                                        if instruments_path.exists():
                                            with open(instruments_path, 'r') as f:
                                                instruments_data = json_lib.load(f)
                                                
                                            for instrument in instruments_data:
                                                if instrument.get('symbol') == option_contract:
                                                    trade.instrument_key = instrument.get('instrument_key')
                                                    logger.info(f"✅ Cycle {cycle_number} - {stock_name}: Found instrument_key: {trade.instrument_key}")
                                                    break
                                    except Exception as inst_error:
                                        logger.warning(f"Error fetching instrument_key for {option_contract}: {str(inst_error)}")
                                
                                logger.info(f"✅ Cycle {cycle_number} - {stock_name}: Option contract determined: {option_contract}")
                            else:
                                logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Could not determine option contract")
                        else:
                            logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Could not fetch stock LTP for option contract determination")
                    except Exception as contract_error:
                        logger.warning(f"Error retrying option contract determination for {stock_name}: {str(contract_error)}")
                
                # ====================================================================
                # RECALCULATE CANDLE SIZE FOR NO_ENTRY / ALERT_RECEIVED STOCKS
                # ====================================================================
                # Recalculate candle size for stocks that are still in "no_entry" or
                # "alert_received" status. This ensures candle size is updated with
                # latest data at each cycle. Do this AFTER option contract determination
                # so we have instrument_key.
                # Initialize from database values (may be None if not calculated yet)
                candle_size_passed = False
                candle_size_ratio = trade.candle_size_ratio
                is_10_15_alert = trade.alert_time and trade.alert_time.hour == 10 and trade.alert_time.minute == 15
                
                # If candle size was already calculated and stored, use that value
                if trade.candle_size_status == "Pass":
                    candle_size_passed = True
                elif trade.candle_size_status == "Fail":
                    candle_size_passed = False
                # Otherwise (None, "Retry", "Pending", "Skipped"), will be recalculated below
                
                # Try to recalculate candle size if:
                # 1. instrument_key exists (always recalculate)
                # 2. OR candle size was never calculated (ratio is None) or needs retry (status is None, "Retry", or "Pending")
                # 3. OR option_contract exists but instrument_key is missing (try to get instrument_key and recalculate)
                # 4. OR it's a 10:15 AM alert at Cycle 1 (10:30 AM) - try to get instrument_key
                should_recalculate = False
                if trade.instrument_key:
                    # Always recalculate if instrument_key exists, especially if:
                    # - Candle size was never calculated (ratio is None)
                    # - Status indicates retry needed (None, "Retry", "Pending")
                    # - Or if we want to refresh the calculation
                    if trade.candle_size_ratio is None or trade.candle_size_status in [None, "Retry", "Pending", "Skipped"]:
                        should_recalculate = True
                    else:
                        # Even if already calculated, recalculate to ensure we have latest data
                        should_recalculate = True
                elif trade.option_contract and not trade.instrument_key:
                    # Option contract exists but instrument_key is missing - try to get it
                    try:
                        from pathlib import Path
                        import json as json_lib
                        
                        instruments_path = Path(__file__).parent.parent.parent / "instruments.json"
                        if instruments_path.exists():
                            with open(instruments_path, 'r') as f:
                                instruments_data = json_lib.load(f)
                                
                            for instrument in instruments_data:
                                if instrument.get('symbol') == trade.option_contract:
                                    trade.instrument_key = instrument.get('instrument_key')
                                    logger.info(f"✅ Cycle {cycle_number} - {stock_name}: Found instrument_key for candle size: {trade.instrument_key}")
                                    should_recalculate = True
                                    break
                    except Exception as inst_error:
                        logger.warning(f"Error fetching instrument_key for candle size calculation: {str(inst_error)}")
                elif is_10_15_alert and cycle_number == 1:
                    # At 10:30 AM, try to get instrument_key and recalculate for 10:15 AM alerts
                    if trade.option_contract:
                        # Try to get instrument_key from instruments.json
                        try:
                            from pathlib import Path
                            import json as json_lib
                            
                            instruments_path = Path(__file__).parent.parent.parent / "instruments.json"
                            if instruments_path.exists():
                                with open(instruments_path, 'r') as f:
                                    instruments_data = json_lib.load(f)
                                    
                                for instrument in instruments_data:
                                    if instrument.get('symbol') == trade.option_contract:
                                        trade.instrument_key = instrument.get('instrument_key')
                                        logger.info(f"✅ Cycle {cycle_number} - {stock_name}: Found instrument_key for 10:15 alert: {trade.instrument_key}")
                                        should_recalculate = True
                                        break
                        except Exception as inst_error:
                            logger.warning(f"Error fetching instrument_key for 10:15 alert: {str(inst_error)}")
                
                if should_recalculate and trade.instrument_key:
                    try:
                        option_candles = vwap_service.get_option_daily_candles_current_and_previous(trade.instrument_key)
                        if option_candles:
                            current_day_candle = option_candles.get('current_day_candle', {})
                            previous_day_candle = option_candles.get('previous_day_candle', {})
                            
                            if current_day_candle and previous_day_candle:
                                current_size = abs(current_day_candle.get('high', 0) - current_day_candle.get('low', 0))
                                previous_size = abs(previous_day_candle.get('high', 0) - previous_day_candle.get('low', 0))
                                
                                if previous_size > 0:
                                    candle_size_ratio = current_size / previous_size
                                    candle_size_passed = (candle_size_ratio < 7.5)
                                    
                                    # Update database with daily candle data
                                    trade.option_current_candle_open = current_day_candle.get('open')
                                    trade.option_current_candle_high = current_day_candle.get('high')
                                    trade.option_current_candle_low = current_day_candle.get('low')
                                    trade.option_current_candle_close = current_day_candle.get('close')
                                    trade.option_current_candle_time = current_day_candle.get('time')
                                    trade.option_previous_candle_open = previous_day_candle.get('open')
                                    trade.option_previous_candle_high = previous_day_candle.get('high')
                                    trade.option_previous_candle_low = previous_day_candle.get('low')
                                    trade.option_previous_candle_close = previous_day_candle.get('close')
                                    trade.option_previous_candle_time = previous_day_candle.get('time')
                                    
                                    # Update candle size ratio and status
                                    trade.candle_size_ratio = candle_size_ratio
                                    trade.candle_size_status = "Pass" if candle_size_passed else "Fail"
                                    
                                    # Update local variables for entry check
                                    candle_size_passed = candle_size_passed
                                    candle_size_ratio = candle_size_ratio
                                    
                                    logger.info(f"✅ Cycle {cycle_number} - {stock_name}: Candle size recalculated - Ratio: {candle_size_ratio:.2f}x - {'PASS' if candle_size_passed else 'FAIL'}")
                                else:
                                    # Previous size is zero - cannot calculate ratio
                                    trade.candle_size_status = "Skipped"
                                    logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Previous day candle size is zero, cannot calculate ratio")
                            else:
                                # Missing candle data - set status to indicate retry needed
                                if not trade.candle_size_status or trade.candle_size_status == "Pending":
                                    trade.candle_size_status = "Retry"
                                logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Missing candle data (current: {bool(current_day_candle)}, previous: {bool(previous_day_candle)}) - Will retry in next cycle")
                        else:
                            # API returned None - set status to indicate retry needed
                            if not trade.candle_size_status or trade.candle_size_status == "Pending":
                                trade.candle_size_status = "Retry"
                            logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: get_option_daily_candles_current_and_previous returned None - Will retry in next cycle")
                    except Exception as candle_error:
                        # Exception occurred - set status to indicate retry needed
                        if not trade.candle_size_status or trade.candle_size_status == "Pending":
                            trade.candle_size_status = "Retry"
                        logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Error recalculating candle size: {str(candle_error)} - Will retry in next cycle")
                        import traceback
                        logger.debug(f"Traceback: {traceback.format_exc()}")
                elif not trade.option_contract:
                    # If still no instrument_key after retry, log warning
                    logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: No instrument_key available for candle size calculation")
                
                # ====================================================================
                # CHECK ENTRY CONDITIONS AND ENTER TRADE IF ALL MET
                # ====================================================================
                # After recalculating candle size and determining option contract,
                # check if all entry conditions are met
                if trade.status == 'no_entry' or trade.status == 'alert_received':
                    # Check index trends
                    index_trends = vwap_service.check_index_trends()
                    nifty_trend = index_trends.get("nifty_trend", "unknown")
                    banknifty_trend = index_trends.get("banknifty_trend", "unknown")
                    
                    # Check if time is before 3:00 PM
                    is_before_3pm = now.hour < 15
                    
                    # Check index trends alignment
                    option_type = trade.option_type or 'PE'
                    can_enter_by_index = False
                    both_bullish = (nifty_trend == "bullish" and banknifty_trend == "bullish")
                    both_bearish = (nifty_trend == "bearish" and banknifty_trend == "bearish")
                    opposite_directions = not both_bullish and not both_bearish
                    
                    if option_type == 'PE':
                        # Bearish alert
                        if both_bullish or both_bearish:
                            can_enter_by_index = True
                        elif opposite_directions:
                            can_enter_by_index = False
                    elif option_type == 'CE':
                        # Bullish alert
                        if both_bullish:
                            can_enter_by_index = True
                        elif both_bearish or opposite_directions:
                            can_enter_by_index = False
                    
                    # For 10:15 AM alerts: Candle size is calculated but NOT used to block entry
                    # Candle size is recalculated in cycles for informational purposes
                    # Entry decisions for 10:15 AM alerts are based on other conditions (time, index trends, VWAP slope)
                    is_10_15_alert = trade.alert_time and trade.alert_time.hour == 10 and trade.alert_time.minute == 15
                    
                    if is_10_15_alert:
                        # For 10:15 AM alerts: Don't block entry based on candle size
                        # Candle size is calculated and stored, but not used as a blocking condition
                        candle_size_check_passed = True
                    else:
                        # For all other alerts: Apply candle size check normally
                        candle_size_check_passed = candle_size_passed
                    
                    # Check if all entry conditions are met
                    # Log each condition for debugging
                    conditions_status = {
                        'time_before_3pm': is_before_3pm,
                        'index_trends_aligned': can_enter_by_index,
                        'vwap_slope_passed': vwap_slope_passed,
                        'candle_size_passed': candle_size_check_passed,
                        'has_option_contract': bool(trade.option_contract),
                        'has_instrument_key': bool(trade.instrument_key)
                    }
                    
                    all_conditions_met = (is_before_3pm and 
                                        can_enter_by_index and 
                                        vwap_slope_passed and 
                                        candle_size_check_passed and 
                                        trade.option_contract and 
                                        trade.instrument_key)
                    
                    # Log which conditions passed/failed
                    if not all_conditions_met:
                        failed_conditions = [k for k, v in conditions_status.items() if not v]
                        logger.warning(f"⚠️ Cycle {cycle_number} - {stock_name}: Entry conditions NOT met. Failed: {', '.join(failed_conditions)}")
                        logger.info(f"   📊 Condition Details:")
                        logger.info(f"      - Time Before 3PM: {'✅' if is_before_3pm else '❌'} ({now.strftime('%H:%M:%S')})")
                        logger.info(f"      - Index Trends: {'✅' if can_enter_by_index else '❌'} (NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend}, Option={option_type})")
                        logger.info(f"      - VWAP Slope: {'✅' if vwap_slope_passed else '❌'} ({slope_angle:.2f}°)" if slope_angle else f"      - VWAP Slope: ❌ (not calculated)")
                        if is_10_15_alert:
                            logger.info(f"      - Candle Size: ✅ Calculated but not blocking for 10:15 alert (Ratio: {candle_size_ratio:.2f}x)" if candle_size_ratio is not None else "      - Candle Size: ⚠️ Not calculated yet for 10:15 alert")
                        else:
                            logger.info(f"      - Candle Size: {'✅' if candle_size_check_passed else '❌'} (Ratio: {candle_size_ratio:.2f}x)" if candle_size_ratio is not None else "      - Candle Size: ❌ (not calculated)")
                        logger.info(f"      - Option Contract: {'✅' if trade.option_contract else '❌'} ({trade.option_contract or 'Missing'})")
                        logger.info(f"      - Instrument Key: {'✅' if trade.instrument_key else '❌'} ({trade.instrument_key or 'Missing'})")
                    
                    if all_conditions_met:
                        # Fetch current option LTP
                        option_quote = vwap_service.get_market_quote_by_key(trade.instrument_key)
                        if option_quote and option_quote.get('last_price', 0) > 0:
                            current_option_ltp = float(option_quote.get('last_price', 0))
                            
                            # Enter the trade with CURRENT time and prices
                            import math
                            SL_LOSS_TARGET = 3100.0
                            
                            trade.buy_price = current_option_ltp
                            trade.buy_time = now  # Use CURRENT time, not alert time
                            trade.stock_ltp = stock_data.get('ltp', 0) if stock_data else trade.stock_ltp
                            trade.stock_vwap = current_vwap
                            trade.option_ltp = current_option_ltp
                            trade.status = 'bought'
                            trade.pnl = 0.0
                            
                            # Calculate stop loss
                            qty = trade.qty or 0
                            if qty > 0:
                                calculated_sl = current_option_ltp - (SL_LOSS_TARGET / qty)
                                trade.stop_loss = max(0.05, math.floor(calculated_sl / 0.10) * 0.10)
                            
                            entry_time_str = now.strftime('%Y-%m-%d %H:%M:%S IST')
                            alert_time_str = trade.alert_time.strftime('%H:%M:%S') if trade.alert_time else 'N/A'
                            logger.info(f"✅ Cycle {cycle_number} - TRADE ENTERED: {stock_name} ({trade.option_contract})")
                            logger.info(f"   Entry Time: {entry_time_str} (was 'no_entry' at alert time: {alert_time_str})")
                            logger.info(f"   Buy Price: ₹{current_option_ltp:.2f} (current LTP)")
                            logger.info(f"   VWAP Slope: ✅ >= 45° ({slope_angle:.2f}°)")
                            if is_10_15_alert:
                                logger.info(f"   Candle Size: ✅ Calculated (Ratio: {candle_size_ratio:.2f}x) - Not blocking for 10:15 alert" if candle_size_ratio is not None else "   Candle Size: ⚠️ Not calculated yet for 10:15 alert")
                            else:
                                logger.info(f"   Candle Size: ✅ Passed (Ratio: {candle_size_ratio:.2f}x)" if candle_size_ratio is not None else "   Candle Size: ❌ (not calculated)")
                            logger.info(f"   Index Trends: NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend}")
                            print(f"✅ Cycle {cycle_number} - TRADE ENTERED: {stock_name} ({trade.option_contract})")
                            print(f"   ⏰ Entry Time: {entry_time_str} (was 'no_entry' at alert time: {alert_time_str})")
                            print(f"   📊 Entry Conditions:")
                            print(f"      - Time Check: ✅ Before 3:00 PM")
                            print(f"      - Index Trends: ✅ Aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})")
                            print(f"      - VWAP Slope: ✅ >= 45° ({slope_angle:.2f}°)")
                            if is_10_15_alert:
                                print(f"      - Candle Size: ✅ Calculated (Ratio: {candle_size_ratio:.2f}x) - Not blocking for 10:15 alert" if candle_size_ratio is not None else "      - Candle Size: ⚠️ Not calculated yet for 10:15 alert")
                            else:
                                print(f"      - Candle Size: ✅ {'Passed' if candle_size_check_passed else 'Skipped'}")
                            print(f"      - Option Data: ✅ Valid")
                            print(f"   💰 Trade Details:")
                            print(f"      - Buy Price: ₹{current_option_ltp:.2f} (current LTP)")
                            print(f"      - Quantity: {qty}")
                            print(f"      - Stop Loss: ₹{trade.stop_loss:.2f}")
                        else:
                            # Option LTP fetch failed - log detailed error
                            logger.error(f"❌ Cycle {cycle_number} - {stock_name}: All conditions met BUT option LTP fetch FAILED - cannot enter")
                            logger.error(f"   Instrument Key: {trade.instrument_key}")
                            logger.error(f"   Option Contract: {trade.option_contract}")
                            if option_quote:
                                logger.error(f"   Option Quote Response: {option_quote}")
                                logger.error(f"   Last Price in Response: {option_quote.get('last_price', 'NOT FOUND')}")
                            else:
                                logger.error(f"   Option Quote Response: None (API call returned None)")
                            logger.error(f"   📊 All Other Conditions Were Met:")
                            logger.error(f"      - Time Before 3PM: ✅ ({now.strftime('%H:%M:%S')})")
                            logger.error(f"      - Index Trends: ✅ (NIFTY={nifty_trend}, BANKNIFTY={banknifty_trend})")
                            logger.error(f"      - VWAP Slope: ✅ ({slope_angle:.2f}°)" if slope_angle else f"      - VWAP Slope: ✅")
                            logger.error(f"      - Candle Size: ✅ (Ratio: {candle_size_ratio:.2f}x)" if candle_size_ratio else f"      - Candle Size: ✅")
                            logger.error(f"      - Option Contract: ✅ ({trade.option_contract})")
                            logger.error(f"      - Instrument Key: ✅ ({trade.instrument_key})")
                            print(f"❌ Cycle {cycle_number} - {stock_name}: Entry BLOCKED - Option LTP fetch failed")
                            print(f"   All conditions passed but cannot fetch option price to enter trade")
                
                # ═══════════════════════════════════════════════════════════════
                # SAVE HISTORICAL MARKET DATA FOR ALL TRADES PROCESSED IN CYCLE
                # ═══════════════════════════════════════════════════════════════
                # Store historical snapshot of market data for analysis
                # This ensures we have data at every cycle run (10:30, 11:15, 12:15, 13:15, 14:15)
                # NOTE: Historical data is also saved earlier if VWAP calculation fails (before continue statements)
                try:
                    # Get current stock LTP and VWAP if not already fetched
                    # stock_data should already be fetched earlier, but fetch again if needed
                    if 'stock_data' not in locals() or not stock_data:
                        stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                    
                    current_stock_ltp = stock_data.get('ltp', 0) if stock_data else (trade.stock_ltp or 0)
                    current_stock_vwap = current_vwap if current_vwap > 0 else (trade.stock_vwap or 0)
                    
                    # Get option LTP if available
                    current_option_ltp = None
                    if trade.instrument_key:
                        try:
                            option_quote = vwap_service.get_market_quote_by_key(trade.instrument_key)
                            if option_quote and option_quote.get('last_price', 0) > 0:
                                current_option_ltp = float(option_quote.get('last_price', 0))
                        except:
                            current_option_ltp = trade.option_ltp
                    else:
                        current_option_ltp = trade.option_ltp
                    
                    # Check if historical data already exists to prevent duplicates (e.g., when hourly update also runs)
                    if not historical_data_exists(db, stock_name, now):
                        # Get VWAP slope data from trade record (if calculated)
                        vwap_slope_angle = trade.vwap_slope_angle if hasattr(trade, 'vwap_slope_angle') else None
                        vwap_slope_status = trade.vwap_slope_status if hasattr(trade, 'vwap_slope_status') else None
                        vwap_slope_direction = trade.vwap_slope_direction if hasattr(trade, 'vwap_slope_direction') else None
                        vwap_slope_time = trade.vwap_slope_time if hasattr(trade, 'vwap_slope_time') else None
                        
                        historical_record = HistoricalMarketData(
                            stock_name=stock_name,
                            stock_vwap=current_stock_vwap if current_stock_vwap > 0 else None,
                            stock_ltp=current_stock_ltp if current_stock_ltp > 0 else None,
                            vwap_slope_angle=vwap_slope_angle,
                            vwap_slope_status=vwap_slope_status,
                            vwap_slope_direction=vwap_slope_direction,
                            vwap_slope_time=vwap_slope_time,
                            option_contract=trade.option_contract,
                            option_instrument_key=trade.instrument_key,
                            option_ltp=current_option_ltp if current_option_ltp and current_option_ltp > 0 else None,
                            scan_date=now,
                            scan_time=now.strftime('%I:%M %p').lower()
                        )
                        db.add(historical_record)
                        logger.debug(f"📊 Cycle {cycle_number} - Saved historical data for {stock_name} at {now.strftime('%H:%M:%S')} (VWAP slope: {vwap_slope_angle:.2f}°)" if vwap_slope_angle else f"📊 Cycle {cycle_number} - Saved historical data for {stock_name} at {now.strftime('%H:%M:%S')}")
                    else:
                        logger.debug(f"⏭️ Cycle {cycle_number} - Skipping duplicate historical data for {stock_name} at {now.strftime('%H:%M:%S')} (already exists)")
                except Exception as hist_error:
                    logger.warning(f"⚠️ Cycle {cycle_number} - Failed to save historical data for {stock_name}: {str(hist_error)}")
                    # Don't fail the entire cycle if historical save fails
                
                success_count += 1
                
            except Exception as e:
                # #region agent log
                # Log per-trade exception
                import json
                import traceback
                error_trace = traceback.format_exc()
                stock_name_for_log = trade.stock_name if trade else 'unknown'
                trade_id_for_log = trade.id if trade else None
                with open('/Users/bipulsahay/TradeManthan/.cursor/debug.log', 'a') as f:
                    f.write(json.dumps({"id":f"log_cycle1_trade_exception_{trade_id_for_log}","timestamp":int(now.timestamp()*1000),"location":"vwap_updater.py:2119","message":"Cycle 1 - Exception processing trade","data":{"stock_name":stock_name_for_log,"trade_id":trade_id_for_log,"status":trade.status if trade else None,"cycle_number":cycle_number,"error":str(e),"traceback":error_trace},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"H"}) + "\n")
                # #endregion
                logger.error(f"❌ Error processing {stock_name_for_log} in Cycle {cycle_number}: {str(e)}")
                import traceback
                traceback.print_exc()
            
            processed_count += 1
        
        db.commit()
        
        # #region agent log
        # Log after commit and verify records were actually saved to database
        import os
        log_path = '/Users/bipulsahay/TradeManthan/.cursor/debug.log'
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            # Verify by querying database after commit
            if cycle_number == 1:
                # Query all 10:15 AM records to see if VWAP slope was saved
                from datetime import timedelta
                verified_records = db.query(IntradayStockOption).filter(
                    and_(
                        IntradayStockOption.trade_date >= today,
                        IntradayStockOption.alert_time >= target_alert_times[0],
                        IntradayStockOption.alert_time < target_alert_times[0] + timedelta(minutes=1)
                    )
                ).all()
                
                vwap_slope_status_breakdown = {}
                for r in verified_records:
                    status_key = f"{r.status}_vwap_slope_{'calculated' if r.vwap_slope_angle is not None else 'not_calculated'}"
                    vwap_slope_status_breakdown[status_key] = vwap_slope_status_breakdown.get(status_key, 0) + 1
                
                with open(log_path, 'a') as f:
                    f.write(json.dumps({"id":f"log_cycle1_after_commit_{int(now.timestamp())}","timestamp":int(now.timestamp()*1000),"location":"vwap_updater.py:2174","message":"Cycle 1 - After commit verification","data":{"cycle_number":cycle_number,"commit_successful":True,"processed_count":processed_count,"success_count":success_count,"verified_total_records":len(verified_records),"vwap_slope_status_breakdown":vwap_slope_status_breakdown},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"E"}) + "\n")
                    f.flush()
            else:
                with open(log_path, 'a') as f:
                    f.write(json.dumps({"id":f"log_cycle1_after_commit_{int(now.timestamp())}","timestamp":int(now.timestamp()*1000),"location":"vwap_updater.py:2174","message":"Cycle 1 - After commit","data":{"cycle_number":cycle_number,"commit_successful":True},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"E"}) + "\n")
                    f.flush()
        except Exception as log_err:
            logger.error(f"Failed to write verification log: {str(log_err)}")
        # #endregion
        
        # Final summary log
        if cycle_number == 1:
            logger.info(f"🔍 DEBUG Cycle 1 FINAL SUMMARY: Processed {processed_count} stocks, Success: {success_count}")
            logger.info(f"🔍 DEBUG Cycle 1: Expected to process ALL 10:15 AM records regardless of status")
        
        logger.info(f"✅ Cycle {cycle_number} completed: {success_count}/{processed_count} stocks processed successfully")
        
    except Exception as e:
        # #region agent log
        # Log exception details
        import json
        import traceback
        error_trace = traceback.format_exc()
        with open('/Users/bipulsahay/TradeManthan/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({"id":f"log_cycle1_exception_{int(datetime.now(pytz.timezone('Asia/Kolkata')).timestamp())}","timestamp":int(datetime.now(pytz.timezone('Asia/Kolkata')).timestamp()*1000),"location":"vwap_updater.py:2116","message":"Cycle 1 - Exception occurred","data":{"cycle_number":cycle_number,"error":str(e),"traceback":error_trace},"sessionId":"debug-session","runId":"post-fix","hypothesisId":"G"}) + "\n")
        # #endregion
        logger.error(f"❌ Error in Cycle {cycle_number} VWAP slope calculation: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


async def update_10_15_alert_stocks_at_10_30():
    """
    Special scan at 10:30 AM for stocks that were alerted at 10:15 AM
    Fetches Stock LTP, Stock VWAP, and Option LTP and stores in historical_market_data table
    """
    db = SessionLocal()
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Target alert time: 10:15 AM
        target_alert_time = today.replace(hour=10, minute=15, second=0, microsecond=0)
        
        logger.info(f"📊 Starting 10:30 AM scan for 10:15 AM alert stocks at {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        # Import VWAP service
        try:
            from services.upstox_service import upstox_service
            vwap_service = upstox_service
        except ImportError:
            logger.error("Could not import upstox_service")
            return
        
        # Query all stocks that were alerted at 10:15 AM today
        # Match alert_time exactly at 10:15 AM (within a small time window to account for timezone differences)
        from datetime import timedelta
        alert_time_start = target_alert_time
        alert_time_end = target_alert_time + timedelta(minutes=1)  # 1 minute window
        
        stocks_from_10_15 = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.alert_time >= alert_time_start,
                IntradayStockOption.alert_time < alert_time_end
            )
        ).all()
        
        if not stocks_from_10_15:
            logger.info(f"ℹ️ No stocks found with alert time at 10:15 AM")
            return
        
        logger.info(f"📋 Found {len(stocks_from_10_15)} stocks from 10:15 AM alert")
        
        saved_count = 0
        failed_count = 0
        
        for stock_record in stocks_from_10_15:
            try:
                stock_name = stock_record.stock_name
                option_contract = stock_record.option_contract
                instrument_key = stock_record.instrument_key
                
                logger.info(f"📊 Processing {stock_name} (from 10:15 AM alert)")
                
                # Fetch Stock LTP and VWAP
                stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                stock_ltp = None
                stock_vwap = None
                
                if stock_data:
                    stock_ltp = stock_data.get('ltp', None)
                    stock_vwap = stock_data.get('vwap', None)
                    logger.info(f"   Stock LTP: ₹{stock_ltp:.2f}" if stock_ltp else "   Stock LTP: Not available")
                    logger.info(f"   Stock VWAP: ₹{stock_vwap:.2f}" if stock_vwap else "   Stock VWAP: Not available")
                else:
                    logger.warning(f"   ⚠️ Could not fetch stock data for {stock_name}")
                
                # Fetch Option LTP if instrument_key is available
                option_ltp = None
                if instrument_key:
                    try:
                        option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                        if option_quote and option_quote.get('last_price', 0) > 0:
                            option_ltp = float(option_quote.get('last_price', 0))
                            logger.info(f"   Option LTP: ₹{option_ltp:.2f}")
                        else:
                            logger.warning(f"   ⚠️ Could not fetch option LTP for {stock_name}")
                    except Exception as opt_error:
                        logger.warning(f"   ⚠️ Error fetching option LTP: {str(opt_error)}")
                elif option_contract:
                    logger.warning(f"   ⚠️ No instrument_key available for {stock_name} ({option_contract})")
                
                # ====================================================================
                # RECALCULATE CANDLE SIZE FOR 10:15 AM STOCKS AT 10:30 AM
                # ====================================================================
                # At 10:30 AM, we have more market data, so recalculate candle size
                # This was initially skipped at 10:15 AM due to insufficient data
                if instrument_key:
                    try:
                        option_candles = vwap_service.get_option_daily_candles_current_and_previous(instrument_key)
                        if option_candles:
                            current_day_candle = option_candles.get('current_day_candle', {})
                            previous_day_candle = option_candles.get('previous_day_candle', {})
                            
                            if current_day_candle and previous_day_candle:
                                current_size = abs(current_day_candle.get('high', 0) - current_day_candle.get('low', 0))
                                previous_size = abs(previous_day_candle.get('high', 0) - previous_day_candle.get('low', 0))
                                
                                if previous_size > 0:
                                    candle_size_ratio = current_size / previous_size
                                    candle_size_passed = (candle_size_ratio < 7.5)
                                    
                                    # Update database with daily candle data
                                    stock_record.option_current_candle_open = current_day_candle.get('open')
                                    stock_record.option_current_candle_high = current_day_candle.get('high')
                                    stock_record.option_current_candle_low = current_day_candle.get('low')
                                    stock_record.option_current_candle_close = current_day_candle.get('close')
                                    stock_record.option_current_candle_time = current_day_candle.get('time')
                                    stock_record.option_previous_candle_open = previous_day_candle.get('open')
                                    stock_record.option_previous_candle_high = previous_day_candle.get('high')
                                    stock_record.option_previous_candle_low = previous_day_candle.get('low')
                                    stock_record.option_previous_candle_close = previous_day_candle.get('close')
                                    stock_record.option_previous_candle_time = previous_day_candle.get('time')
                                    
                                    # Update candle size ratio and status
                                    stock_record.candle_size_ratio = candle_size_ratio
                                    stock_record.candle_size_status = "Pass" if candle_size_passed else "Fail"
                                    
                                    logger.info(f"   ✅ Candle size recalculated at 10:30 AM - Ratio: {candle_size_ratio:.2f}x - {'PASS' if candle_size_passed else 'FAIL'}")
                                else:
                                    stock_record.candle_size_status = "Skipped"
                            else:
                                logger.warning(f"   ⚠️ Missing candle data for {stock_name}")
                    except Exception as candle_error:
                        logger.warning(f"   ⚠️ Error recalculating candle size for {stock_name}: {str(candle_error)}")
                else:
                    logger.warning(f"   ⚠️ No instrument_key available for candle size recalculation for {stock_name}")
                
                # Save to historical_market_data table
                # Check if historical data already exists to prevent duplicates
                if not historical_data_exists(db, stock_name, now):
                    # Get VWAP slope from stock_record if available
                    vwap_slope_angle = stock_record.vwap_slope_angle if hasattr(stock_record, 'vwap_slope_angle') else None
                    vwap_slope_status = stock_record.vwap_slope_status if hasattr(stock_record, 'vwap_slope_status') else None
                    vwap_slope_direction = stock_record.vwap_slope_direction if hasattr(stock_record, 'vwap_slope_direction') else None
                    vwap_slope_time = stock_record.vwap_slope_time if hasattr(stock_record, 'vwap_slope_time') else None
                    
                    historical_record = HistoricalMarketData(
                        stock_name=stock_name,
                        stock_vwap=stock_vwap if stock_vwap and stock_vwap > 0 else None,
                        stock_ltp=stock_ltp if stock_ltp and stock_ltp > 0 else None,
                        vwap_slope_angle=vwap_slope_angle,
                        vwap_slope_status=vwap_slope_status,
                        vwap_slope_direction=vwap_slope_direction,
                        vwap_slope_time=vwap_slope_time,
                        option_contract=option_contract,
                        option_instrument_key=instrument_key,
                        option_ltp=option_ltp if option_ltp and option_ltp > 0 else None,
                        scan_date=now,
                        scan_time=now.strftime('%I:%M %p').lower()
                    )
                    db.add(historical_record)
                    saved_count += 1
                    logger.info(f"   ✅ Saved historical data for {stock_name} at 10:30 AM")
                else:
                    logger.debug(f"   ⏭️ Skipping duplicate historical data for {stock_name} at 10:30 AM (already exists)")
                
            except Exception as e:
                logger.error(f"   ❌ Error processing {stock_record.stock_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                failed_count += 1
        
        # Commit all historical records
        db.commit()
        
        logger.info(f"📊 10:30 AM Scan Complete: {saved_count} stocks saved, {failed_count} failed")
        logger.info(f"   Stocks from 10:15 AM alert: {len(stocks_from_10_15)}")
        
    except Exception as e:
        logger.error(f"❌ Error in 10:30 AM scan for 10:15 AM alert stocks: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


async def close_all_open_trades():
    """
    Close all open trades at 3:25 PM (before market close)
    Sets exit_reason = 'time_based', status = 'sold', sell_time = now
    """
    from database import SessionLocal
    from models.trading import IntradayStockOption
    from services.upstox_service import upstox_service as vwap_service
    import pytz
    
    logger.info("🔔 3:25 PM - Closing all open trades (End of Day)")
    
    db = SessionLocal()
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.date()
        
        # Find all open positions for today
        # CRITICAL: Only include trades that have NO exit_reason (not already exited)
        # This ensures trades exited with VWAP cross, stop loss, or profit target are NOT overwritten
        open_positions = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date == today,
            IntradayStockOption.exit_reason.is_(None),  # Must be NULL - excludes all already-exited trades
            IntradayStockOption.status != 'sold',  # Additional safety check
            IntradayStockOption.status != 'no_entry'  # Exclude trades that were never entered
        ).all()
        
        if not open_positions:
            logger.info("✅ No open positions to close - all already exited")
            return
        
        logger.info(f"📊 Found {len(open_positions)} open positions to close")
        
        closed_count = 0
        for position in open_positions:
            try:
                stock_name = position.stock_name
                option_contract = position.option_contract
                
                # Skip if already no_entry (was never bought)
                if position.status == 'no_entry':
                    logger.info(f"⚪ Skipping {stock_name} - never entered (no_entry)")
                    continue
                
                # Get current option LTP for final sell price
                option_ltp = None
                
                if option_contract:
                    try:
                        # PREFERRED: Use stored instrument_key from trade entry (more reliable)
                        instrument_key = position.instrument_key
                        
                        if instrument_key:
                            # Use stored instrument_key directly - no lookup needed
                            logger.info(f"🔍 Fetching final LTP for {option_contract} using stored instrument_key: {instrument_key}")
                            
                            # Retry logic: Try up to 3 times to fetch LTP
                            max_retries = 3
                            option_quote = None
                            for retry in range(max_retries):
                                try:
                                    option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                                    if option_quote and 'last_price' in option_quote:
                                        break  # Success, exit retry loop
                                    elif retry < max_retries - 1:
                                        logger.warning(f"⚠️ Retry {retry + 1}/{max_retries}: No last_price in quote for {option_contract}, retrying...")
                                        time.sleep(1)  # Wait 1 second before retry
                                except Exception as retry_error:
                                    if retry < max_retries - 1:
                                        logger.warning(f"⚠️ Retry {retry + 1}/{max_retries}: Error fetching LTP for {option_contract}: {retry_error}, retrying...")
                                        time.sleep(1)  # Wait 1 second before retry
                                    else:
                                        raise  # Re-raise on final retry
                            
                            if option_quote and 'last_price' in option_quote:
                                raw_ltp = option_quote['last_price']
                                
                                # CRITICAL: Sanity check for unrealistic option LTP
                                # If option LTP is >3x buy_price, it's likely wrong
                                if position.buy_price and position.buy_price > 0:
                                    ratio = raw_ltp / position.buy_price
                                    if ratio > 3.0:
                                        logger.error(f"🚨 UNREALISTIC LTP DETECTED for {option_contract}!")
                                        logger.error(f"   Buy Price: ₹{position.buy_price:.2f}")
                                        logger.error(f"   Raw LTP from API: ₹{raw_ltp:.2f} ({ratio:.2f}x buy price)")
                                        logger.error(f"   This is likely an API error or data corruption")
                                        
                                        # Try to use last known sell_price if it's reasonable
                                        if position.sell_price and position.sell_price > 0:
                                            last_ratio = position.sell_price / position.buy_price
                                            if last_ratio <= 3.0:
                                                logger.warning(f"   Using last known sell_price: ₹{position.sell_price:.2f} instead")
                                                raw_ltp = position.sell_price
                                            else:
                                                logger.error(f"   Last sell_price also unrealistic: ₹{position.sell_price:.2f}")
                                                logger.error(f"   Using buy_price as fallback: ₹{position.buy_price:.2f}")
                                                raw_ltp = position.buy_price
                                        else:
                                            logger.error(f"   Using buy_price as fallback: ₹{position.buy_price:.2f}")
                                            raw_ltp = position.buy_price
                                
                                option_ltp = raw_ltp
                                logger.info(f"📍 {option_contract}: Final LTP = ₹{option_ltp:.2f}")
                        else:
                            # FALLBACK: Lookup instrument_key for old records that don't have it stored
                            logger.warning(f"⚠️ No stored instrument_key for {option_contract} - falling back to lookup")
                            from pathlib import Path
                            import json as json_lib
                            import re
                            
                            instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
                            if not instruments_file.exists():
                                instruments_file = Path(__file__).parent.parent.parent / 'data' / 'instruments' / 'nse_instruments.json'
                            
                            if instruments_file.exists():
                                with open(instruments_file, 'r') as f:
                                    instruments_data = json_lib.load(f)
                                
                                # Parse option contract: SYMBOL-MonthYYYY-STRIKE-TYPE
                                match = re.match(r'^([A-Z&]+)-(\w{3})(\d{4})-(\d+\.?\d*?)-(CE|PE)$', option_contract)
                                
                                if match:
                                    symbol, month, year, strike, opt_type = match.groups()
                                    strike_value = float(strike)
                                    
                                    # Parse expiry month and year
                                    month_map = {
                                        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
                                        'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
                                        'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                                    }
                                    target_month = month_map.get(month[:3].capitalize(), 11)
                                    target_year = int(year)
                                    
                                    # Find matching instrument - CRITICAL: Also check expiry month/year
                                    for instrument in instruments_data:
                                        if (instrument.get('underlying_symbol', '').upper() == symbol.upper() and
                                            instrument.get('segment') == 'NSE_FO' and
                                            instrument.get('instrument_type') == opt_type):
                                            
                                            inst_strike = float(instrument.get('strike_price', 0))
                                            if abs(inst_strike - strike_value) < 0.01:
                                                # CRITICAL: Check expiry month/year matches
                                                expiry_timestamp = instrument.get('expiry')
                                                if expiry_timestamp:
                                                    try:
                                                        # Convert timestamp (milliseconds) to datetime
                                                        if expiry_timestamp > 1e12:
                                                            expiry_timestamp = expiry_timestamp / 1000
                                                        inst_expiry = datetime.fromtimestamp(expiry_timestamp, tz=pytz.UTC)
                                                        
                                                        # Check if expiry month/year matches
                                                        if inst_expiry.year == target_year and inst_expiry.month == target_month:
                                                            instrument_key = instrument.get('instrument_key')
                                                            if instrument_key:
                                                                logger.info(f"🔍 Found instrument_key via lookup: {instrument_key}")
                                                                
                                                                # Retry logic: Try up to 3 times to fetch LTP
                                                                max_retries = 3
                                                                option_quote = None
                                                                for retry in range(max_retries):
                                                                    try:
                                                                        option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                                                                        if option_quote and 'last_price' in option_quote:
                                                                            break  # Success, exit retry loop
                                                                        elif retry < max_retries - 1:
                                                                            logger.warning(f"⚠️ Retry {retry + 1}/{max_retries}: No last_price in quote for {option_contract}, retrying...")
                                                                            time.sleep(1)  # Wait 1 second before retry
                                                                    except Exception as retry_error:
                                                                        if retry < max_retries - 1:
                                                                            logger.warning(f"⚠️ Retry {retry + 1}/{max_retries}: Error fetching LTP for {option_contract}: {retry_error}, retrying...")
                                                                            time.sleep(1)  # Wait 1 second before retry
                                                                        else:
                                                                            raise  # Re-raise on final retry
                                                                
                                                                if option_quote and 'last_price' in option_quote:
                                                                    raw_ltp = option_quote['last_price']
                                                                    
                                                                    # CRITICAL: Sanity check for unrealistic option LTP
                                                                    # If option LTP is >3x buy_price, it's likely wrong
                                                                    if position.buy_price and position.buy_price > 0:
                                                                        ratio = raw_ltp / position.buy_price
                                                                        if ratio > 3.0:
                                                                            logger.error(f"🚨 UNREALISTIC LTP DETECTED for {option_contract}!")
                                                                            logger.error(f"   Buy Price: ₹{position.buy_price:.2f}")
                                                                            logger.error(f"   Raw LTP from API: ₹{raw_ltp:.2f} ({ratio:.2f}x buy price)")
                                                                            logger.error(f"   This is likely an API error or data corruption")
                                                                            
                                                                            # Try to use last known sell_price if it's reasonable
                                                                            if position.sell_price and position.sell_price > 0:
                                                                                last_ratio = position.sell_price / position.buy_price
                                                                                if last_ratio <= 3.0:
                                                                                    logger.warning(f"   Using last known sell_price: ₹{position.sell_price:.2f} instead")
                                                                                    raw_ltp = position.sell_price
                                                                                else:
                                                                                    logger.error(f"   Last sell_price also unrealistic: ₹{position.sell_price:.2f}")
                                                                                    logger.error(f"   Using buy_price as fallback: ₹{position.buy_price:.2f}")
                                                                                    raw_ltp = position.buy_price
                                                                            else:
                                                                                logger.error(f"   Using buy_price as fallback: ₹{position.buy_price:.2f}")
                                                                                raw_ltp = position.buy_price
                                                                    
                                                                    option_ltp = raw_ltp
                                                                    logger.info(f"📍 {option_contract}: Final LTP = ₹{option_ltp:.2f}")
                                                                    # Update stored instrument_key for future reference
                                                                    position.instrument_key = instrument_key
                                                                    logger.info(f"✅ Stored instrument_key {instrument_key} for future reference")
                                                                    break  # Found correct match, exit loop
                                                    except (ValueError, TypeError) as exp_error:
                                                        logger.warning(f"⚠️ Error parsing expiry for {option_contract}: {exp_error}")
                                                        continue
                    except Exception as e:
                        logger.warning(f"⚠️ Could not fetch final LTP for {option_contract}: {e}")
                
                # Update position for EOD exit
                old_sell_price = position.sell_price or 0.0
                if option_ltp and option_ltp > 0:
                    # Successfully fetched current LTP - use it
                    position.sell_price = option_ltp
                    logger.info(f"✅ Using fetched LTP: ₹{option_ltp:.2f} for {option_contract}")
                else:
                    # API call failed or returned invalid data - try fallback options
                    if old_sell_price and old_sell_price > 0:
                        # Use last known sell_price from hourly updates (better than buy_price)
                        position.sell_price = old_sell_price
                        logger.warning(f"⚠️ Could not fetch current LTP for {option_contract}, using last known sell_price: ₹{old_sell_price:.2f}")
                    elif position.buy_price and position.buy_price > 0:
                        # Last resort: use buy_price (results in 0 P&L, but at least has a value)
                        position.sell_price = position.buy_price
                        logger.error(f"🚨 CRITICAL: No LTP available and no previous sell_price for {option_contract}, using buy_price as fallback: ₹{position.buy_price:.2f} (P&L will be 0)")
                    else:
                        # Absolute worst case: no buy_price either
                        logger.error(f"🚨 CRITICAL: No sell_price, no buy_price for {option_contract} - cannot set sell_price!")
                        position.sell_price = 0.0
                
                exit_time_str = now.strftime('%Y-%m-%d %H:%M:%S IST')
                position.sell_time = now
                position.exit_reason = 'time_based'
                position.status = 'sold'
                
                # ═══════════════════════════════════════════════════════════════
                # SAVE HISTORICAL MARKET DATA AT 15:25 PM (END OF DAY)
                # ═══════════════════════════════════════════════════════════════
                try:
                    # Get current stock LTP and VWAP
                    stock_data = vwap_service.get_stock_ltp_and_vwap(stock_name)
                    current_stock_ltp = stock_data.get('ltp', 0) if stock_data else (position.stock_ltp or 0)
                    current_stock_vwap = stock_data.get('vwap', 0) if stock_data else (position.stock_vwap or 0)
                    
                    # Check if historical data already exists to prevent duplicates
                    if not historical_data_exists(db, stock_name, now):
                        # Get VWAP slope from position record if available
                        vwap_slope_angle = position.vwap_slope_angle if hasattr(position, 'vwap_slope_angle') else None
                        vwap_slope_status = position.vwap_slope_status if hasattr(position, 'vwap_slope_status') else None
                        vwap_slope_direction = position.vwap_slope_direction if hasattr(position, 'vwap_slope_direction') else None
                        vwap_slope_time = position.vwap_slope_time if hasattr(position, 'vwap_slope_time') else None
                        
                        historical_record = HistoricalMarketData(
                            stock_name=stock_name,
                            stock_vwap=current_stock_vwap if current_stock_vwap > 0 else None,
                            stock_ltp=current_stock_ltp if current_stock_ltp > 0 else None,
                            vwap_slope_angle=vwap_slope_angle,
                            vwap_slope_status=vwap_slope_status,
                            vwap_slope_direction=vwap_slope_direction,
                            vwap_slope_time=vwap_slope_time,
                            option_contract=option_contract,
                            option_instrument_key=position.instrument_key,
                            option_ltp=option_ltp if option_ltp and option_ltp > 0 else (position.sell_price if position.sell_price else None),
                            scan_date=now,
                            scan_time=now.strftime('%I:%M %p').lower()
                        )
                        db.add(historical_record)
                        logger.debug(f"📊 Saved historical data for {stock_name} at 15:25 PM (EOD)")
                    else:
                        logger.debug(f"⏭️ Skipping duplicate historical data for {stock_name} at 15:25 PM (already exists)")
                except Exception as hist_error:
                    logger.warning(f"⚠️ Failed to save historical data for {stock_name} at 15:25 PM: {str(hist_error)}")
                
                # Calculate final P&L if not already set
                if position.buy_price and position.qty and position.sell_price:
                    position.pnl = (position.sell_price - position.buy_price) * position.qty
                    
                    logger.info(f"🔴 EOD EXIT: {stock_name} {option_contract}")
                    logger.info(f"   Buy: ₹{position.buy_price:.2f}, Sell: ₹{position.sell_price:.2f}, P&L: ₹{position.pnl:.2f}")
                    logger.info(f"⏰ EXIT DECISION: {stock_name} | Time: {exit_time_str} | Reason: Time Based (3:25 PM) | PnL: ₹{position.pnl:.2f}")
                    print(f"⏰ EXIT DECISION: {stock_name} ({option_contract})")
                    print(f"   ⏰ Exit Time: {exit_time_str}")
                    print(f"   📊 Exit Conditions:")
                    print(f"      - Time Based: ✅ Triggered (Current time >= 3:25 PM)")
                    print(f"      - Stop Loss: ❌ Not Checked (Time-based exit takes priority)")
                    print(f"      - VWAP Cross: ❌ Not Checked (Time-based exit takes priority)")
                    print(f"      - Profit Target: ❌ Not Checked (Time-based exit takes priority)")
                    print(f"   💰 Exit Details:")
                    print(f"      - Buy Price: ₹{position.buy_price:.2f}")
                    print(f"      - Buy Time: {position.buy_time.strftime('%H:%M:%S') if position.buy_time else 'N/A'}")
                    print(f"      - Sell Price: ₹{position.sell_price:.2f}")
                    print(f"      - Quantity: {position.qty}")
                    print(f"      - Hold Duration: {((now - position.buy_time).total_seconds() / 60):.0f} minutes" if position.buy_time else "N/A")
                    print(f"      - PnL: ₹{position.pnl:.2f}")
                else:
                    logger.warning(f"⚠️ Could not calculate P&L for {stock_name}")
                
                closed_count += 1
                
            except Exception as e:
                logger.error(f"Error closing position for {stock_name}: {str(e)}")
                continue
        
        # Commit all changes
        db.commit()
        
        logger.info(f"✅ EOD: Closed {closed_count} open positions at 3:25 PM")
        
    except Exception as e:
        logger.error(f"Error in close_all_open_trades: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


async def update_end_of_day_vwap():
    """
    Update Stock VWAP with end-of-day (complete trading day) VWAP at market close
    
    This function runs at 3:30 PM and 3:35 PM to capture the final VWAP for the entire trading day.
    Updates ALL positions from today (both open and exited) so traders can see the final day VWAP.
    
    This is the complete, final VWAP that includes all trading activity from 9:15 AM to 3:30 PM.
    """
    db = SessionLocal()
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"📊 Starting END-OF-DAY VWAP update at {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
        logger.info("💫 Updating ALL positions (open + exited) with final day VWAP")
        
        # Get ALL positions from today (both open and exited)
        all_positions = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today
        ).all()
        
        if not all_positions:
            logger.info("No positions found for today")
            return
        
        logger.info(f"Found {len(all_positions)} total positions for end-of-day VWAP update")
        
        # Import VWAP service
        try:
            from services.upstox_service import upstox_service
            vwap_service = upstox_service
        except ImportError:
            logger.error("Could not import upstox_service")
            return
        
        # Get unique stock names (to avoid redundant API calls)
        unique_stocks = {}
        for position in all_positions:
            if position.stock_name not in unique_stocks:
                unique_stocks[position.stock_name] = []
            unique_stocks[position.stock_name].append(position)
        
        logger.info(f"Fetching EOD VWAP for {len(unique_stocks)} unique stocks")
        
        # Fetch and update VWAP for each unique stock
        updated_count = 0
        failed_count = 0
        
        for stock_name, positions in unique_stocks.items():
            try:
                # Fetch final day VWAP from API (includes entire trading day)
                final_vwap = vwap_service.get_stock_vwap(stock_name)
                
                if final_vwap and final_vwap > 0:
                    # Update all positions for this stock
                    for position in positions:
                        old_vwap = position.stock_vwap or 0.0
                        position.stock_vwap = final_vwap
                        position.updated_at = now
                        
                        logger.info(f"✅ {stock_name} (ID:{position.id}): Final Day VWAP = ₹{final_vwap:.2f} (was: ₹{old_vwap:.2f})")
                    
                    updated_count += len(positions)
                else:
                    logger.warning(f"⚠️ Could not fetch EOD VWAP for {stock_name}")
                    failed_count += len(positions)
                    
            except Exception as e:
                logger.error(f"Error fetching EOD VWAP for {stock_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                failed_count += len(positions)
        
        # Commit all updates
        db.commit()
        
        logger.info(f"📊 END-OF-DAY VWAP Update Complete: {updated_count} positions updated, {failed_count} failed")
        logger.info(f"💫 All positions now have final trading day VWAP")
        
    except Exception as e:
        logger.error(f"Error in end-of-day VWAP update job: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


# Global VWAP updater instance
vwap_updater = VWAPUpdater()


def start_vwap_updater():
    """Start the VWAP updater scheduler"""
    vwap_updater.start()


def stop_vwap_updater():
    """Stop the VWAP updater scheduler"""
    vwap_updater.stop()


def trigger_manual_update():
    """Manually trigger a VWAP update (for testing/admin)"""
    vwap_updater.run_now()


