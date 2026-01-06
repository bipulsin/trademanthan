"""
Index Price Scheduler
Fetches and stores NIFTY50 and BANKNIFTY prices every 5 minutes during market hours
Stores prices at 9:15 AM (market open) and 3:30 PM (market close)
"""

import logging
import pytz
from datetime import datetime, timedelta
from typing import Dict, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

logger = logging.getLogger(__name__)

class IndexPriceScheduler:
    """Scheduler for fetching and storing index prices during market hours"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone='Asia/Kolkata')
        self.is_running = False
    
    def start(self):
        """Start the index price scheduler"""
        if not self.is_running:
            ist = pytz.timezone('Asia/Kolkata')
            
            # Schedule job to run every 5 minutes during market hours (9:15 AM - 3:30 PM)
            # This will run at: 9:15, 9:20, 9:25, ... 3:25, 3:30
            # Cron expression: minute='*/5' means every 5 minutes
            # But we need to restrict to market hours only
            
            # Schedule for every 5 minutes from 9:15 to 15:30
            # We'll use a custom trigger that checks market hours
            def run_index_price_check():
                try:
                    self.fetch_and_store_index_prices()
                except Exception as e:
                    logger.error(f"❌ Error in index price check: {str(e)}", exc_info=True)
            
            # Schedule every 5 minutes, but the function will check if market is open
            self.scheduler.add_job(
                run_index_price_check,
                trigger='interval',
                minutes=5,
                id='index_price_check',
                name='Index Price Check (Every 5 minutes)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            
            # Special jobs for 9:15 AM and 3:30 PM
            self.scheduler.add_job(
                self.fetch_and_store_index_prices,
                trigger=CronTrigger(hour=9, minute=15, timezone='Asia/Kolkata'),
                id='index_price_9_15',
                name='Index Price at 9:15 AM (Market Open)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            
            self.scheduler.add_job(
                self.fetch_and_store_index_prices,
                trigger=CronTrigger(hour=15, minute=30, timezone='Asia/Kolkata'),
                id='index_price_15_30',
                name='Index Price at 3:30 PM (Market Close)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            
            try:
                self.scheduler.start()
                self.is_running = True
                logger.info("✅ Index Price Scheduler started - Checking every 5 minutes during market hours (9:15 AM - 3:30 PM)")
                print(f"✅ Index Price Scheduler started - Jobs: {len(self.scheduler.get_jobs())}", flush=True)
            except Exception as e:
                logger.error(f"❌ Failed to start Index Price Scheduler: {e}", exc_info=True)
                print(f"❌ Failed to start Index Price Scheduler: {e}", flush=True)
                raise
    
    def stop(self):
        """Stop the index price scheduler"""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("Index Price Scheduler stopped")
    
    def is_market_open(self, check_time: datetime = None) -> bool:
        """
        Check if market is open at the given time
        
        Args:
            check_time: Time to check (default: current time)
        
        Returns:
            True if market is open (9:15 AM - 3:30 PM IST), False otherwise
        """
        ist = pytz.timezone('Asia/Kolkata')
        if check_time is None:
            check_time = datetime.now(ist)
        elif check_time.tzinfo is None:
            check_time = ist.localize(check_time)
        elif check_time.tzinfo != ist:
            check_time = check_time.astimezone(ist)
        
        current_hour = check_time.hour
        current_minute = check_time.minute
        
        # Market hours: 9:15 AM to 3:30 PM IST
        # Check if it's a trading day (not weekend)
        if check_time.weekday() >= 5:  # Saturday or Sunday
            return False
        
        # Check if within market hours
        if current_hour < 9:
            return False
        elif current_hour == 9:
            return current_minute >= 15
        elif 10 <= current_hour <= 14:
            return True
        elif current_hour == 15:
            return current_minute <= 30
        else:
            return False
    
    def fetch_and_store_index_prices(self):
        """
        Fetch NIFTY50 and BANKNIFTY prices from Upstox API and store in database
        Only runs during market hours (9:15 AM - 3:30 PM)
        """
        try:
            from database import SessionLocal
            from models.trading import IndexPrice
            from services.upstox_service import upstox_service
            
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            # Check if market is open
            if not self.is_market_open(now):
                logger.debug(f"⏰ Market closed (current time: {now.strftime('%H:%M:%S IST')}), skipping index price fetch")
                return
            
            # Check if it's a trading day (not holiday)
            if not upstox_service.is_trading_day(now):
                logger.info(f"📅 Not a trading day ({now.strftime('%Y-%m-%d')}), skipping index price fetch")
                return
            
            # Check if it's a special time (9:15 AM or 3:30 PM)
            is_special_time = (now.hour == 9 and now.minute == 15) or (now.hour == 15 and now.minute == 30)
            
            logger.info(f"📊 Fetching index prices at {now.strftime('%H:%M:%S IST')} (special_time={is_special_time})")
            
            # Fetch index prices from Upstox API
            nifty_quote = upstox_service.get_market_quote_by_key(upstox_service.NIFTY50_KEY)
            banknifty_quote = upstox_service.get_market_quote_by_key(upstox_service.BANKNIFTY_KEY)
            
            db = SessionLocal()
            try:
                # Process and store NIFTY50 price
                if nifty_quote and nifty_quote.get('last_price', 0) > 0:
                    ltp = float(nifty_quote.get('last_price', 0))
                    day_open = float(nifty_quote.get('ohlc', {}).get('open', 0))
                    close_price = float(nifty_quote.get('close_price', ltp)) if is_special_time and now.hour == 15 else None
                    
                    # Determine trend
                    if day_open > 0:
                        if ltp > day_open:
                            trend = 'bullish'
                        elif ltp < day_open:
                            trend = 'bearish'
                        else:
                            trend = 'neutral'
                        change = ltp - day_open
                        change_percent = (change / day_open * 100) if day_open > 0 else 0
                    else:
                        trend = 'neutral'
                        change = 0
                        change_percent = 0
                    
                    # Store NIFTY50 price
                    index_price = IndexPrice(
                        index_name='NIFTY50',
                        instrument_key=upstox_service.NIFTY50_KEY,
                        ltp=ltp,
                        day_open=day_open if day_open > 0 else None,
                        close_price=close_price,
                        trend=trend,
                        change=change,
                        change_percent=change_percent,
                        price_time=now,
                        is_market_open=True,
                        is_special_time=is_special_time
                    )
                    db.add(index_price)
                    logger.info(f"✅ Stored NIFTY50 price: ₹{ltp:.2f} (trend: {trend}, special_time: {is_special_time})")
                
                # Process and store BANKNIFTY price
                if banknifty_quote and banknifty_quote.get('last_price', 0) > 0:
                    ltp = float(banknifty_quote.get('last_price', 0))
                    day_open = float(banknifty_quote.get('ohlc', {}).get('open', 0))
                    close_price = float(banknifty_quote.get('close_price', ltp)) if is_special_time and now.hour == 15 else None
                    
                    # Determine trend
                    if day_open > 0:
                        if ltp > day_open:
                            trend = 'bullish'
                        elif ltp < day_open:
                            trend = 'bearish'
                        else:
                            trend = 'neutral'
                        change = ltp - day_open
                        change_percent = (change / day_open * 100) if day_open > 0 else 0
                    else:
                        trend = 'neutral'
                        change = 0
                        change_percent = 0
                    
                    # Store BANKNIFTY price
                    index_price = IndexPrice(
                        index_name='BANKNIFTY',
                        instrument_key=upstox_service.BANKNIFTY_KEY,
                        ltp=ltp,
                        day_open=day_open if day_open > 0 else None,
                        close_price=close_price,
                        trend=trend,
                        change=change,
                        change_percent=change_percent,
                        price_time=now,
                        is_market_open=True,
                        is_special_time=is_special_time
                    )
                    db.add(index_price)
                    logger.info(f"✅ Stored BANKNIFTY price: ₹{ltp:.2f} (trend: {trend}, special_time: {is_special_time})")
                
                db.commit()
                logger.info(f"✅ Index prices stored successfully at {now.strftime('%H:%M:%S IST')}")
                
            except Exception as e:
                db.rollback()
                logger.error(f"❌ Error storing index prices: {str(e)}", exc_info=True)
                raise
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"❌ Error fetching index prices: {str(e)}", exc_info=True)
    
    def get_latest_stored_price(self, index_name: str, db: Session) -> Optional[Dict]:
        """
        Get the latest stored price for an index from database
        
        Args:
            index_name: 'NIFTY50' or 'BANKNIFTY'
            db: Database session
        
        Returns:
            Dict with price data or None
        """
        try:
            from models.trading import IndexPrice
            
            # Get the latest price record for this index
            latest_price = db.query(IndexPrice).filter(
                IndexPrice.index_name == index_name
            ).order_by(desc(IndexPrice.price_time)).first()
            
            if latest_price:
                return {
                    'ltp': latest_price.ltp,
                    'day_open': latest_price.day_open,
                    'close_price': latest_price.close_price or latest_price.ltp,
                    'trend': latest_price.trend,
                    'change': latest_price.change,
                    'change_percent': latest_price.change_percent,
                    'price_time': latest_price.price_time,
                    'is_special_time': latest_price.is_special_time
                }
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting latest stored price for {index_name}: {str(e)}")
            return None

# Global instance
index_price_scheduler = IndexPriceScheduler()

def start_index_price_scheduler():
    """Start the index price scheduler"""
    index_price_scheduler.start()

def stop_index_price_scheduler():
    """Stop the index price scheduler"""
    index_price_scheduler.stop()

