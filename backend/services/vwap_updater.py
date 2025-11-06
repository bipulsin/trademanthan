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
            
            self.scheduler.start()
            self.is_running = True
            logger.info("✅ VWAP Updater started - Updating hourly during market hours")
    
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
    Update stock VWAP for all open positions (not yet sold)
    This runs hourly during market hours
    """
    db = SessionLocal()
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"📊 Starting VWAP update at {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
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
                
                # Fetch fresh VWAP from API
                new_vwap = vwap_service.get_stock_vwap(stock_name)
                
                if new_vwap and new_vwap > 0:
                    old_vwap = position.stock_vwap or 0.0
                    position.stock_vwap = new_vwap
                    position.updated_at = now
                    
                    logger.info(f"✅ Updated {stock_name}: VWAP {old_vwap:.2f} → {new_vwap:.2f}")
                    updated_count += 1
                else:
                    logger.warning(f"⚠️ Could not fetch VWAP for {stock_name}")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error updating VWAP for {position.stock_name}: {str(e)}")
                failed_count += 1
        
        # Commit all updates
        db.commit()
        
        logger.info(f"📊 VWAP Update Complete: {updated_count} updated, {failed_count} failed")
        
    except Exception as e:
        logger.error(f"Error in VWAP update job: {str(e)}")
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


