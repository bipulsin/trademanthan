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

class VWAPUpdater:
    """Scheduler for updating stock VWAP hourly during market hours"""
    
    def __init__(self):
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
    db = SessionLocal()
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"📊 Starting hourly market data update at {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        # Import VWAP service
        try:
            from services.upstox_service import upstox_service
            vwap_service = upstox_service
        except ImportError:
            logger.error("Could not import upstox_service")
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
        
        if not open_positions:
            logger.info("No open positions found to update")
            return
        
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
                IntradayStockOption.exit_reason == None
            )
        ).all()
        
        # Update each position
        updated_count = 0
        failed_count = 0
        
        for position in open_positions:
            try:
                stock_name = position.stock_name
                option_contract = position.option_contract
                
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
                
                # 3. Fetch fresh Option LTP (if option contract exists)
                new_option_ltp = 0.0
                if option_contract:
                    try:
                        # PREFERRED: Use stored instrument_key from trade entry (more reliable)
                        instrument_key = position.instrument_key
                        
                        if instrument_key:
                            # Use stored instrument_key directly - no lookup needed
                            logger.info(f"🔍 [{now.strftime('%H:%M:%S')}] Fetching option LTP for {option_contract}")
                            logger.info(f"   Using stored Instrument Key: {instrument_key}")
                            
                            option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                            
                            logger.info(f"   API Response: {option_quote}")
                            
                            if option_quote and 'last_price' in option_quote:
                                option_ltp_data = option_quote['last_price']
                                if option_ltp_data and option_ltp_data > 0:
                                    new_option_ltp = option_ltp_data
                                    logger.info(f"📥 [{now.strftime('%H:%M:%S')}] API returned option LTP: ₹{new_option_ltp:.2f} for {option_contract}")
                                else:
                                    logger.warning(f"⚠️ Invalid LTP data: {option_ltp_data}")
                            else:
                                logger.warning(f"⚠️ No last_price in quote data for {instrument_key}: {option_quote}")
                        else:
                            # FALLBACK: Lookup instrument_key for old records that don't have it stored
                            logger.warning(f"⚠️ No stored instrument_key for {option_contract} - falling back to lookup")
                            from pathlib import Path
                            import json as json_lib
                            
                            instruments_file = Path("/home/ubuntu/trademanthan/data/instruments/nse_instruments.json")
                            
                            if instruments_file.exists():
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
                
                # CRITICAL: Even if option LTP fetch fails, check VWAP cross using stock data
                # If VWAP cross detected, exit with last known option price (sell_price)
                if new_option_ltp == 0:
                    logger.warning(f"⚠️ Option LTP fetch FAILED for {stock_name} {option_contract}")
                    logger.warning(f"   new_vwap={new_vwap}, new_stock_ltp={new_stock_ltp}, option_type={position.option_type}")
                    
                    if new_vwap > 0 and new_stock_ltp > 0 and position.option_type:
                        if now.hour >= 11 and now.minute >= 15:
                            option_type = position.option_type
                            if (option_type == 'CE' and new_stock_ltp < new_vwap) or \
                               (option_type == 'PE' and new_stock_ltp > new_vwap):
                                logger.critical(f"🚨 VWAP CROSS DETECTED for {stock_name} but option LTP fetch FAILED!")
                                logger.critical(f"   Stock LTP: ₹{new_stock_ltp:.2f}, VWAP: ₹{new_vwap:.2f}, Type: {option_type}")
                                logger.critical(f"   Using last known sell_price: ₹{position.sell_price:.2f} for exit")
                                
                                # Use last known sell_price for exit
                                if position.sell_price and position.sell_price > 0:
                                    position.exit_reason = 'stock_vwap_cross'
                                    position.sell_time = now
                                    position.status = 'sold'
                                    if position.buy_price and position.qty:
                                        position.pnl = (position.sell_price - position.buy_price) * position.qty
                                    logger.critical(f"✅ FORCED EXIT: {stock_name} on VWAP cross with last known price ₹{position.sell_price:.2f}, PnL=₹{position.pnl:.2f}")
                                    updated_count += 1
                                    # Don't continue - still update stock LTP/VWAP below for record keeping
                
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
                try:
                    historical_record = HistoricalMarketData(
                        stock_name=stock_name,
                        stock_vwap=new_vwap if new_vwap and new_vwap > 0 else None,
                        stock_ltp=new_stock_ltp if new_stock_ltp and new_stock_ltp > 0 else None,
                        option_contract=option_contract,
                        option_instrument_key=position.instrument_key,
                        option_ltp=new_option_ltp if new_option_ltp > 0 else None,
                        scan_date=now,
                        scan_time=now.strftime('%I:%M %p').lower()
                    )
                    db.add(historical_record)
                    logger.debug(f"📊 Saved historical data for {stock_name} at {now.strftime('%H:%M:%S')}")
                except Exception as hist_error:
                    logger.warning(f"⚠️ Failed to save historical data for {stock_name}: {str(hist_error)}")
                    # Don't fail the entire update if historical save fails
                
                if new_option_ltp > 0:
                    old_option_ltp = position.sell_price or 0.0
                    
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
                    
                    position.sell_price = new_option_ltp  # Update sell_price with current option price
                    updates_made.append(f"Option LTP: {old_option_ltp:.2f}→{new_option_ltp:.2f}")
                    logger.info(f"📌 {stock_name} Option LTP updated at {now.strftime('%H:%M:%S')}: ₹{old_option_ltp:.2f} → ₹{new_option_ltp:.2f}")
                    
                    # Calculate and update unrealized P&L for open trades
                    if position.buy_price and position.qty:
                        old_pnl = position.pnl or 0.0
                        new_pnl = (new_option_ltp - position.buy_price) * position.qty
                        position.pnl = new_pnl
                        updates_made.append(f"P&L: ₹{old_pnl:.2f}→₹{new_pnl:.2f}")
                        
                        # CHECK ALL EXIT CONDITIONS INDEPENDENTLY
                        # Then apply the highest priority exit
                        # Priority: Stop Loss > VWAP Cross > Profit Target
                        exit_conditions = {
                            'stop_loss': False,
                            'vwap_cross': False,
                            'profit_target': False
                        }
                        
                        # 1. CHECK STOP LOSS
                        if position.stop_loss and new_option_ltp <= position.stop_loss:
                            exit_conditions['stop_loss'] = True
                            logger.info(f"🛑 STOP LOSS CONDITION MET for {stock_name}: LTP ₹{new_option_ltp:.2f} <= SL ₹{position.stop_loss:.2f}")
                        
                        # 2. CHECK VWAP CROSS (only after 11:15 AM)
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
                        
                        # 3. CHECK PROFIT TARGET (1.5x buy price)
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
                            print(f"      - Time Based: {'✅' if exit_conditions['time_based'] else '❌'} {'Triggered' if exit_conditions['time_based'] else 'Not Triggered'}")
                            print(f"   💰 Exit Details:")
                            print(f"      - Buy Price: ₹{position.buy_price:.2f}")
                            print(f"      - Sell Price: ₹{new_option_ltp:.2f}")
                            print(f"      - Quantity: {position.qty}")
                            print(f"      - PnL: ₹{position.pnl:.2f}")
                        
                        elif exit_conditions['vwap_cross']:
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
                            print(f"      - Time Based: {'✅' if exit_conditions['time_based'] else '❌'} {'Triggered' if exit_conditions['time_based'] else 'Not Triggered'}")
                            print(f"   💰 Exit Details:")
                            print(f"      - Buy Price: ₹{position.buy_price:.2f}")
                            print(f"      - Sell Price: ₹{new_option_ltp:.2f}")
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
                            print(f"      - Time Based: {'✅' if exit_conditions['time_based'] else '❌'} {'Triggered' if exit_conditions['time_based'] else 'Not Triggered'}")
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
                            position.exit_reason = exit_reason_to_set
                            position.sell_time = now  # Set ONLY once at exit
                            position.status = 'sold'
                            # sell_price is already set to new_option_ltp above (line 252)
                            updates_made.append(f"🚨 EXITED: {exit_reason_to_set} at ₹{new_option_ltp:.2f}")
                            logger.critical(f"🔴 EXIT RECORDED for {stock_name}:")
                            logger.critical(f"   Exit Reason: {exit_reason_to_set}")
                            logger.critical(f"   Sell Price: ₹{position.sell_price:.2f}")
                            logger.critical(f"   Option LTP (fetched): ₹{new_option_ltp:.2f}")
                            logger.critical(f"   Sell Time: {now.strftime('%H:%M:%S')}")
                            logger.critical(f"   Stock LTP: ₹{new_stock_ltp:.2f if new_stock_ltp else 0:.2f}, VWAP: ₹{new_vwap:.2f if new_vwap else 0:.2f}")
                            logger.critical(f"   PnL: ₹{position.pnl:.2f}")
                
                if updates_made:
                    position.updated_at = now
                    logger.info(f"✅ {stock_name}: {', '.join(updates_made)}")
                    updated_count += 1
                else:
                    logger.warning(f"⚠️ Could not fetch updated data for {stock_name}")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error updating position for {position.stock_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                failed_count += 1
        
        # Commit all updates
        db.commit()
        
        logger.info(f"📊 Hourly Update Complete: {updated_count} positions updated, {failed_count} failed")
        
    except Exception as e:
        logger.error(f"Error in hourly market data update job: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


async def calculate_vwap_slope_for_cycle(cycle_number: int, cycle_time: datetime):
    """
    Calculate VWAP slope for stocks based on cycle-based logic
    
    Cycle Rules:
    1. Cycle 1 (10:30 AM): Stocks from 10:15 AM webhook
       - Previous VWAP: 10:15 AM (1-hour candle)
       - Current VWAP: 10:30 AM (15-minute candle)
    
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
            # Previous VWAP: Use 15-minute candle at 10:15 AM
            prev_vwap_time = today.replace(hour=10, minute=15, second=0, microsecond=0)
            current_vwap_time = today.replace(hour=10, minute=30, second=0, microsecond=0)
            prev_interval = "minutes/15"  # Use 15-minute candle for 10:15 AM
            current_interval = "minutes/15"  # Use 15-minute candle for 10:30 AM
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
        # 1. Stocks from webhook alerts at CURRENT cycle's alert time (if status is still 'alert_received' or 'no_entry')
        # 2. No_Entry stocks from PREVIOUS cycles (up to previous cycle's alert time)
        # 3. VWAP slope is NOT calculated if status is not No_Entry (already entered)
        # 4. Candle size is only calculated when stock is received from webhook alert scan
        #    If status is No_Entry, candle size will not be recalculated in subsequent cycles
        from datetime import timedelta
        
        # Determine current cycle's alert time
        current_cycle_alert_time = max(target_alert_times)  # Latest alert time = current cycle
        
        # Build query based on cycle number
        if cycle_number == 1:
            # Cycle 1: Only stocks from 10:15 AM webhook
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    IntradayStockOption.alert_time >= target_alert_times[0],
                    IntradayStockOption.alert_time < target_alert_times[0] + timedelta(minutes=1),
                    or_(
                        IntradayStockOption.status == 'no_entry',
                        IntradayStockOption.status == 'alert_received'
                    )
                )
            ).all()
        elif cycle_number == 2:
            # Cycle 2: Stocks from 11:15 AM webhook + No_Entry from 10:15 AM
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    or_(
                        # Current cycle: stocks from 11:15 AM webhook
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[1],
                            IntradayStockOption.alert_time < target_alert_times[1] + timedelta(minutes=1),
                            or_(
                                IntradayStockOption.status == 'no_entry',
                                IntradayStockOption.status == 'alert_received'
                            )
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
            # Cycle 3: Stocks from 12:15 PM webhook + No_Entry up to 11:15 AM
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    or_(
                        # Current cycle: stocks from 12:15 PM webhook
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[2],
                            IntradayStockOption.alert_time < target_alert_times[2] + timedelta(minutes=1),
                            or_(
                                IntradayStockOption.status == 'no_entry',
                                IntradayStockOption.status == 'alert_received'
                            )
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
            # Cycle 4: Stocks from 13:15 PM webhook + No_Entry up to 12:15 PM
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    or_(
                        # Current cycle: stocks from 13:15 PM webhook
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[3],
                            IntradayStockOption.alert_time < target_alert_times[3] + timedelta(minutes=1),
                            or_(
                                IntradayStockOption.status == 'no_entry',
                                IntradayStockOption.status == 'alert_received'
                            )
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
            # Cycle 5: Stocks from 14:15 PM webhook + No_Entry up to 13:15 PM
            stocks_to_process = db.query(IntradayStockOption).filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    or_(
                        # Current cycle: stocks from 14:15 PM webhook
                        and_(
                            IntradayStockOption.alert_time >= target_alert_times[4],
                            IntradayStockOption.alert_time < target_alert_times[4] + timedelta(minutes=1),
                            or_(
                                IntradayStockOption.status == 'no_entry',
                                IntradayStockOption.status == 'alert_received'
                            )
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
            logger.info(f"ℹ️ No stocks found for Cycle {cycle_number} VWAP slope calculation")
            return
        
        logger.info(f"📋 Found {len(stocks_to_process)} stocks for Cycle {cycle_number} VWAP slope calculation")
        
        processed_count = 0
        success_count = 0
        
        for trade in stocks_to_process:
            try:
                stock_name = trade.stock_name
                
                # VWAP slope should NOT be calculated if status is not No_Entry
                # If status changed from No_Entry (already entered), skip VWAP slope calculation
                if trade.status != 'no_entry' and trade.status != 'alert_received':
                    logger.debug(f"⚪ Skipping {stock_name} - already entered (status: {trade.status}), VWAP slope not calculated for subsequent cycles")
                    continue
                
                # Get previous VWAP
                # Market opens at 9:15 AM, so 1-hour candles form at :15 times (10:15, 11:15, etc.)
                prev_vwap_data = vwap_service.get_stock_vwap_from_candle_at_time(
                    stock_name,
                    prev_vwap_time,
                    interval=prev_interval
                )
                
                if not prev_vwap_data:
                    logger.warning(f"⚠️ Could not get previous VWAP for {stock_name} at {prev_vwap_time.strftime('%H:%M')}")
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
                
                if not current_vwap_data:
                    logger.warning(f"⚠️ Could not get current VWAP for {stock_name} at {current_vwap_time.strftime('%H:%M')}")
                    continue
                
                current_vwap = current_vwap_data.get('vwap', 0)
                current_vwap_time_actual = current_vwap_data.get('time')
                
                if prev_vwap <= 0 or current_vwap <= 0:
                    logger.warning(f"⚠️ Invalid VWAP values for {stock_name} (prev: {prev_vwap}, current: {current_vwap})")
                    continue
                
                # Calculate VWAP slope
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
                else:
                    slope_status = slope_result if isinstance(slope_result, str) else "No"
                    slope_angle = 0.0
                    slope_direction = "flat"
                    vwap_slope_passed = (slope_status == "Yes")
                
                # Update database with VWAP slope data
                trade.stock_vwap_previous_hour = prev_vwap
                trade.stock_vwap_previous_hour_time = prev_vwap_time_actual
                trade.stock_vwap = current_vwap
                
                # Store VWAP slope results
                trade.vwap_slope_status = slope_status
                trade.vwap_slope_angle = slope_angle
                trade.vwap_slope_direction = slope_direction
                trade.vwap_slope_time = current_vwap_time_actual
                
                logger.info(f"✅ Cycle {cycle_number} - {stock_name}: VWAP slope {slope_angle:.2f}° ({slope_direction}) - {'PASS' if vwap_slope_passed else 'FAIL'}")
                success_count += 1
                
            except Exception as e:
                logger.error(f"❌ Error processing {trade.stock_name if trade else 'unknown'} in Cycle {cycle_number}: {str(e)}")
                import traceback
                traceback.print_exc()
            
            processed_count += 1
        
        db.commit()
        logger.info(f"✅ Cycle {cycle_number} completed: {success_count}/{processed_count} stocks processed successfully")
        
    except Exception as e:
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
                
                # Save to historical_market_data table
                historical_record = HistoricalMarketData(
                    stock_name=stock_name,
                    stock_vwap=stock_vwap if stock_vwap and stock_vwap > 0 else None,
                    stock_ltp=stock_ltp if stock_ltp and stock_ltp > 0 else None,
                    option_contract=option_contract,
                    option_instrument_key=instrument_key,
                    option_ltp=option_ltp if option_ltp and option_ltp > 0 else None,
                    scan_date=now,
                    scan_time=now.strftime('%I:%M %p').lower()
                )
                db.add(historical_record)
                saved_count += 1
                
                logger.info(f"   ✅ Saved historical data for {stock_name} at 10:30 AM")
                
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


