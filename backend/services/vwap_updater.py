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
from sqlalchemy import and_

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models.trading import IntradayStockOption

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
            
            # Also run at market open (9:15 AM) and mid-day (12:15 PM)
            self.scheduler.add_job(
                update_vwap_for_all_open_positions,
                trigger=CronTrigger(hour=9, minute=30, timezone='Asia/Kolkata'),
                id='vwap_update_market_open',
                name='Update VWAP at Market Open',
                replace_existing=True
            )
            
            # End-of-day VWAP update at 3:30 PM (market close)
            # This updates ALL positions (including exited ones) with final day VWAP
            self.scheduler.add_job(
                update_end_of_day_vwap,
                trigger=CronTrigger(hour=15, minute=30, timezone='Asia/Kolkata'),
                id='vwap_update_eod',
                name='End of Day VWAP Update',
                replace_existing=True
            )
            
            # Also update at 3:35 PM to ensure we have complete market data
            self.scheduler.add_job(
                update_end_of_day_vwap,
                trigger=CronTrigger(hour=15, minute=35, timezone='Asia/Kolkata'),
                id='vwap_update_eod_final',
                name='Final End of Day VWAP Update',
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
        
        # Get all open positions from today (not sold/exited)
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
        
        # Import VWAP service
        try:
            from services.upstox_service import upstox_service
            vwap_service = upstox_service
        except ImportError:
            logger.error("Could not import upstox_service")
            return
        
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
                
                # 1. Fetch fresh Stock VWAP from API
                new_vwap = vwap_service.get_stock_vwap(stock_name)
                
                # 2. Fetch fresh Stock LTP (Last Traded Price)
                new_stock_ltp = vwap_service.get_stock_ltp_from_market_quote(stock_name)
                
                # 3. Fetch fresh Option LTP (if option contract exists)
                new_option_ltp = 0.0
                if option_contract:
                    try:
                        # Fetch option LTP using the same method as during alert processing
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
                                
                                # Search for matching instrument (instruments_data is a list)
                                for instrument in instruments_data:
                                    if (instrument.get('underlying_symbol', '').upper() == symbol.upper() and
                                        instrument.get('segment') == 'NSE_FO' and
                                        instrument.get('instrument_type') == opt_type):
                                        
                                        # Check strike price match
                                        inst_strike = float(instrument.get('strike_price', 0))
                                        if abs(inst_strike - strike_value) < 0.01:
                                            # Found the option - fetch its LTP
                                            instrument_key = instrument.get('instrument_key')
                                            if instrument_key:
                                                # Use get_market_quote_by_key which takes only instrument_key
                                                logger.info(f"🔍 [{now.strftime('%H:%M:%S')}] Fetching option LTP for {option_contract}")
                                                logger.info(f"   Instrument Key: {instrument_key}")
                                                logger.info(f"   Strike: {inst_strike}, Type: {opt_type}")
                                                
                                                option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                                                
                                                logger.info(f"   API Response: {option_quote}")
                                                
                                                if option_quote and 'last_price' in option_quote:
                                                    option_ltp_data = option_quote['last_price']
                                                    if option_ltp_data and option_ltp_data > 0:
                                                        new_option_ltp = option_ltp_data
                                                        logger.info(f"📥 [{now.strftime('%H:%M:%S')}] API returned option LTP: ₹{new_option_ltp:.2f} for {option_contract}")
                                                        break
                                                    else:
                                                        logger.warning(f"⚠️ Invalid LTP data: {option_ltp_data}")
                                                else:
                                                    logger.warning(f"⚠️ No last_price in quote data for {instrument_key}: {option_quote}")
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
                
                if new_option_ltp > 0:
                    old_option_ltp = position.sell_price or 0.0
                    
                    # Sanity check: Flag suspicious price movements
                    if old_option_ltp > 0:
                        price_change_pct = abs((new_option_ltp - old_option_ltp) / old_option_ltp) * 100
                        if price_change_pct > 100:
                            logger.warning(f"⚠️ SUSPICIOUS PRICE CHANGE for {stock_name}: ₹{old_option_ltp:.2f} → ₹{new_option_ltp:.2f} ({price_change_pct:.1f}% change)")
                    
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
                            logger.warning(f"✅ APPLIED: STOP LOSS EXIT for {stock_name}")
                        
                        elif exit_conditions['vwap_cross']:
                            exit_triggered = True
                            exit_reason_to_set = 'stock_vwap_cross'
                            logger.warning(f"✅ APPLIED: VWAP CROSS EXIT for {stock_name}")
                        
                        elif exit_conditions['profit_target']:
                            exit_triggered = True
                            exit_reason_to_set = 'profit_target'
                            logger.warning(f"✅ APPLIED: PROFIT TARGET EXIT for {stock_name}")
                        
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
        
        # Find all open positions for today (exit_reason is NULL or status != 'sold')
        open_positions = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date == today,
            IntradayStockOption.exit_reason.is_(None),
            IntradayStockOption.status != 'sold'
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
                        # Get instrument key by parsing option contract
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
                                
                                # Find matching instrument
                                for instrument in instruments_data:
                                    if (instrument.get('underlying_symbol', '').upper() == symbol.upper() and
                                        instrument.get('segment') == 'NSE_FO' and
                                        instrument.get('instrument_type') == opt_type):
                                        
                                        inst_strike = float(instrument.get('strike_price', 0))
                                        if abs(inst_strike - strike_value) < 0.01:
                                            instrument_key = instrument.get('instrument_key')
                                            if instrument_key:
                                                option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                                                if option_quote and 'last_price' in option_quote:
                                                    option_ltp = option_quote['last_price']
                                                    logger.info(f"📍 {option_contract}: Final LTP = ₹{option_ltp:.2f}")
                                                break
                    except Exception as e:
                        logger.warning(f"⚠️ Could not fetch final LTP for {option_contract}: {e}")
                
                # Update position for EOD exit
                old_sell_price = position.sell_price or 0.0
                if option_ltp and option_ltp > 0:
                    position.sell_price = option_ltp
                else:
                    # If can't fetch LTP, use last known sell_price
                    if not position.sell_price or position.sell_price == 0:
                        logger.warning(f"⚠️ No LTP available for {option_contract}, using buy_price as fallback")
                        position.sell_price = position.buy_price
                
                position.sell_time = now
                position.exit_reason = 'time_based'
                position.status = 'sold'
                
                # Calculate final P&L if not already set
                if position.buy_price and position.qty and position.sell_price:
                    position.pnl = (position.sell_price - position.buy_price) * position.qty
                    
                    logger.info(f"🔴 EOD EXIT: {stock_name} {option_contract}")
                    logger.info(f"   Buy: ₹{position.buy_price:.2f}, Sell: ₹{position.sell_price:.2f}, P&L: ₹{position.pnl:.2f}")
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


