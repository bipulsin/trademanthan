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
            
            self.scheduler.start()
            self.is_running = True
            logger.info("✅ Market Data Updater started - Hourly updates + EOD VWAP at 3:30 PM")
    
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
                                                option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                                                if option_quote and 'last_price' in option_quote:
                                                    option_ltp_data = option_quote['last_price']
                                                    if option_ltp_data and option_ltp_data > 0:
                                                        new_option_ltp = option_ltp_data
                                                        break
                    except Exception as e:
                        logger.warning(f"Could not fetch option LTP for {option_contract}: {str(e)}")
                
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
                    position.sell_price = new_option_ltp  # Update sell_price with current option price
                    updates_made.append(f"Option LTP: {old_option_ltp:.2f}→{new_option_ltp:.2f}")
                    
                    # Calculate and update unrealized P&L for open trades
                    if position.buy_price and position.qty:
                        old_pnl = position.pnl or 0.0
                        new_pnl = (new_option_ltp - position.buy_price) * position.qty
                        position.pnl = new_pnl
                        updates_made.append(f"P&L: ₹{old_pnl:.2f}→₹{new_pnl:.2f}")
                        
                        # AUTO-EXIT LOGIC: Check if exit conditions are met
                        # This ensures database exit_reason is set when frontend shows exit signals
                        exit_triggered = False
                        exit_reason_to_set = None
                        
                        # Check profit target (1.5x buy price)
                        profit_target = position.buy_price * 1.5
                        if new_option_ltp >= profit_target:
                            exit_triggered = True
                            exit_reason_to_set = 'profit_target'
                            logger.info(f"🎯 AUTO-EXIT: {stock_name} hit profit target! LTP: ₹{new_option_ltp:.2f} >= Target: ₹{profit_target:.2f}")
                        
                        # Check stop loss
                        elif position.stop_loss and new_option_ltp <= position.stop_loss:
                            exit_triggered = True
                            exit_reason_to_set = 'stop_loss'
                            logger.info(f"🛑 AUTO-EXIT: {stock_name} hit stop loss! LTP: ₹{new_option_ltp:.2f} <= SL: ₹{position.stop_loss:.2f}")
                        
                        # Check VWAP cross (only after 11:15 AM)
                        elif now.hour >= 11 and now.minute >= 15:
                            if new_vwap and new_vwap > 0 and new_stock_ltp and new_stock_ltp > 0:
                                option_type = position.option_type or 'CE'
                                # CE: Exit if stock LTP falls below VWAP
                                # PE: Exit if stock LTP rises above VWAP
                                if (option_type == 'CE' and new_stock_ltp < new_vwap) or \
                                   (option_type == 'PE' and new_stock_ltp > new_vwap):
                                    exit_triggered = True
                                    exit_reason_to_set = 'stock_vwap_cross'
                                    logger.info(f"📉 AUTO-EXIT: {stock_name} VWAP cross! Stock LTP: ₹{new_stock_ltp:.2f} vs VWAP: ₹{new_vwap:.2f} ({option_type})")
                        
                        # Set exit fields if any exit condition was triggered
                        # ═══════════════════════════════════════════════════════════════
                        # IMPORTANT: sell_time is ONLY set here, at the moment of exit
                        # After this update:
                        #   - exit_reason will be set → Trade excluded from future updates
                        #   - sell_price is FROZEN at the current value
                        #   - sell_time is FROZEN at the current timestamp
                        #   - No more updates will be applied to this trade
                        # ═══════════════════════════════════════════════════════════════
                        if exit_triggered and exit_reason_to_set:
                            position.exit_reason = exit_reason_to_set
                            position.sell_time = now  # Set ONLY once at exit
                            position.status = 'sold'
                            updates_made.append(f"🚨 EXITED: {exit_reason_to_set}")
                            logger.warning(f"⚠️ {stock_name} automatically exited: {exit_reason_to_set}")
                
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


