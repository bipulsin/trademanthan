"""
Master Stock Data Scheduler Service
Downloads Dhan API scrip master data daily at 9 AM IST
Filters and stores NSE options data in PostgreSQL
"""

import requests
import csv
import io
from datetime import datetime, timedelta
from typing import List, Dict
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
from sqlalchemy import delete
import logging
import sys
import os

# Add parent directory to path for imports
from backend.database import SessionLocal
from backend.models.trading import MasterStock

logger = logging.getLogger(__name__)

# Dhan API scrip master URL
DHAN_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

class MasterStockScheduler:
    """Scheduler for downloading and updating master stock data"""
    
    def __init__(self):
        # Use default event loop policy for AsyncIOScheduler
        import asyncio
        try:
            # Try to get existing event loop
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # No event loop exists, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        self.scheduler = AsyncIOScheduler(timezone='Asia/Kolkata', event_loop=loop)
        self.is_running = False
        
    def start(self):
        """Start the scheduler"""
        if not self.is_running:
            # Schedule daily download at 9:00 AM IST
            self.scheduler.add_job(
                download_and_update_master_stock,
                trigger=CronTrigger(hour=9, minute=0, timezone='Asia/Kolkata'),
                id='master_stock_daily_download',
                name='Download Master Stock Data',
                replace_existing=True
            )
            
            self.scheduler.start()
            self.is_running = True
            logger.info("Master Stock Scheduler started - Daily download at 9:00 AM IST")
    
    def stop(self):
        """Stop the scheduler"""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("Master Stock Scheduler stopped")
    
    def run_now(self):
        """Manually trigger the download (for testing)"""
        logger.info("Manually triggering master stock download...")
        self.scheduler.add_job(
            download_and_update_master_stock,
            id='master_stock_manual_download',
            replace_existing=True
        )


def get_target_expiry_month() -> datetime:
    """
    Determine which month's expiry to filter
    - If today's date < 18: Use current month expiry
    - If today's date >= 18: Use next month expiry
    """
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    if now.day < 18:
        # Current month
        target_month = now.month
        target_year = now.year
    else:
        # Next month
        if now.month == 12:
            target_month = 1
            target_year = now.year + 1
        else:
            target_month = now.month + 1
            target_year = now.year
    
    logger.info(f"Target expiry month: {target_year}-{target_month:02d}")
    return target_year, target_month


async def download_and_update_master_stock():
    """
    Download scrip master CSV from Dhan API
    Filter and update database with NSE OPTSTK monthly options
    """
    db = SessionLocal()
    try:
        logger.info(f"Starting master stock download from {DHAN_SCRIP_MASTER_URL}")
        
        # Download CSV
        response = requests.get(DHAN_SCRIP_MASTER_URL, timeout=60)
        response.raise_for_status()
        
        logger.info(f"Downloaded CSV successfully ({len(response.content)} bytes)")
        
        # Parse CSV
        csv_content = response.content.decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        
        # Get target expiry month
        target_year, target_month = get_target_expiry_month()
        
        # Filter and prepare records
        filtered_records = []
        total_rows = 0
        
        for row in csv_reader:
            total_rows += 1
            
            # Apply filters
            if (row.get('EXCH_ID') != 'NSE' or 
                row.get('INSTRUMENT') != 'OPTSTK' or 
                row.get('EXPIRY_FLAG') != 'M'):
                continue
            
            # Check expiry date
            expiry_date_str = row.get('SM_EXPIRY_DATE')
            if not expiry_date_str:
                continue
            
            try:
                # Parse date (format: YYYY-MM-DD)
                expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%d')
                
                # Check if it matches target month
                if expiry_date.year != target_year or expiry_date.month != target_month:
                    continue
                
            except ValueError:
                logger.warning(f"Invalid expiry date format: {expiry_date_str}")
                continue
            
            # Parse numeric fields
            try:
                strike_price = float(row.get('STRIKE_PRICE', 0))
                lot_size = float(row.get('LOT_SIZE', 0))
                tick_size = float(row.get('TICK_SIZE', 0))
            except (ValueError, TypeError):
                strike_price = 0.0
                lot_size = 0.0
                tick_size = 0.0
            
            # Create record
            record = {
                'security_id': row.get('SECURITY_ID', ''),
                'isin': row.get('ISIN', '') if row.get('ISIN') != 'NA' else None,
                'exch_id': row.get('EXCH_ID', ''),
                'segment': row.get('SEGMENT', ''),
                'instrument': row.get('INSTRUMENT', ''),
                'underlying_security_id': row.get('UNDERLYING_SECURITY_ID', ''),
                'underlying_symbol': row.get('UNDERLYING_SYMBOL', ''),
                'symbol_name': row.get('SYMBOL_NAME', ''),
                'display_name': row.get('DISPLAY_NAME', ''),
                'instrument_type': row.get('INSTRUMENT_TYPE', ''),
                'series': row.get('SERIES', ''),
                'lot_size': lot_size,
                'sm_expiry_date': expiry_date,
                'strike_price': strike_price,
                'option_type': row.get('OPTION_TYPE', ''),
                'tick_size': tick_size,
                'expiry_flag': row.get('EXPIRY_FLAG', '')
            }
            
            filtered_records.append(record)
        
        logger.info(f"Processed {total_rows} total rows, filtered to {len(filtered_records)} records")
        
        if not filtered_records:
            logger.warning("No records matched the filter criteria")
            return
        
        # Clear existing records for this month's expiry
        delete_stmt = delete(MasterStock).where(
            MasterStock.sm_expiry_date >= datetime(target_year, target_month, 1),
            MasterStock.sm_expiry_date < datetime(target_year, target_month + 1 if target_month < 12 else 1, 1)
        )
        deleted_count = db.execute(delete_stmt).rowcount
        logger.info(f"Deleted {deleted_count} existing records for {target_year}-{target_month:02d}")
        
        # Bulk insert new records
        master_stock_objects = [MasterStock(**record) for record in filtered_records]
        db.bulk_save_objects(master_stock_objects)
        db.commit()
        
        logger.info(f"Successfully inserted {len(filtered_records)} master stock records")
        
    except requests.RequestException as e:
        logger.error(f"Error downloading CSV: {str(e)}")
        db.rollback()
    except Exception as e:
        logger.error(f"Error updating master stock data: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


# Global scheduler instance
master_stock_scheduler = MasterStockScheduler()


def start_scheduler():
    """Start the master stock scheduler"""
    master_stock_scheduler.start()


def stop_scheduler():
    """Stop the master stock scheduler"""
    master_stock_scheduler.stop()


def trigger_manual_download():
    """Manually trigger a download (for testing/admin)"""
    master_stock_scheduler.run_now()

