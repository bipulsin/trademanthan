"""
Daily scheduler to download Upstox Instruments JSON
Downloads every day at 9:00 AM IST from:
https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz
"""

import requests
import gzip
import json
import os
from datetime import datetime
import logging
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

class InstrumentsDownloader:
    """Downloads and stores Upstox instruments data"""
    
    def __init__(self, storage_path: str = None):
        """
        Initialize the downloader
        
        Args:
            storage_path: Path to store the instruments file (default: project root)
        """
        if storage_path is None:
            # Default to project root
            self.storage_path = Path(__file__).parent.parent.parent / "data" / "instruments"
        else:
            self.storage_path = Path(storage_path)
        
        # Create directory if it doesn't exist
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # File path for the instruments data
        self.instruments_file = self.storage_path / "nse_instruments.json"
        self.instruments_index = {}  # For quick lookup
    
    def download_instruments(self) -> bool:
        """
        Download the instruments JSON file from Upstox
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting download of Upstox instruments...")
            
            url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
            
            # Download the gzipped file
            response = requests.get(url, timeout=300)  # 5 minute timeout for large file
            
            if response.status_code == 200:
                # Decompress the gzipped content
                logger.info("Decompressing instruments data...")
                decompressed_data = gzip.decompress(response.content)
                
                # Parse JSON
                logger.info("Parsing JSON data...")
                instruments_data = json.loads(decompressed_data)
                
                # Save to file
                logger.info(f"Saving instruments to {self.instruments_file}")
                with open(self.instruments_file, 'w') as f:
                    json.dump(instruments_data, f)
                
                # Build index for quick lookup
                logger.info("Building index for quick lookup...")
                self._build_index(instruments_data)
                
                # Reload ISIN cache in symbol_isin_mapping
                try:
                    from services.symbol_isin_mapping import reload_isin_cache
                    reload_isin_cache()
                    logger.info("✅ ISIN cache reloaded with new instruments data")
                except Exception as e:
                    logger.warning(f"Could not reload ISIN cache: {e}")
                
                logger.info(f"✅ Successfully downloaded {len(instruments_data)} instruments")
                return True
            else:
                logger.error(f"Failed to download instruments: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error downloading instruments: {str(e)}")
            return False
    
    def _build_index(self, instruments_data: list):
        """
        Build an index for quick lookup of instruments
        
        Args:
            instruments_data: List of instrument data
        """
        self.instruments_index = {}
        
        for instrument in instruments_data:
            try:
                instrument_key = instrument.get('instrument_key', '')
                tradingsymbol = instrument.get('tradingsymbol', '')
                
                # Index by both instrument_key and tradingsymbol
                if instrument_key:
                    self.instruments_index[instrument_key] = instrument
                
                if tradingsymbol:
                    self.instruments_index[tradingsymbol] = instrument
                    
            except Exception as e:
                continue
        
        logger.info(f"Built index with {len(self.instruments_index)} entries")
    
    def load_instruments(self) -> bool:
        """
        Load instruments from file
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.instruments_file.exists():
                logger.warning(f"Instruments file not found: {self.instruments_file}")
                return False
            
            logger.info(f"Loading instruments from {self.instruments_file}")
            
            with open(self.instruments_file, 'r') as f:
                instruments_data = json.load(f)
            
            # Build index
            self._build_index(instruments_data)
            
            logger.info(f"✅ Loaded {len(instruments_data)} instruments")
            return True
            
        except Exception as e:
            logger.error(f"Error loading instruments: {str(e)}")
            return False
    
    def get_instrument_key_by_symbol(self, tradingsymbol: str) -> str:
        """
        Get instrument_key by tradingsymbol
        
        Args:
            tradingsymbol: Trading symbol (e.g., 'IDFCFIRSTB-Nov2025-85-CE')
            
        Returns:
            Instrument key if found, None otherwise
        """
        if not self.instruments_index:
            self.load_instruments()
        
        instrument = self.instruments_index.get(tradingsymbol)
        
        if instrument:
            return instrument.get('instrument_key')
        
        return None
    
    def get_instrument_data(self, symbol: str) -> dict:
        """
        Get instrument data by symbol or instrument_key
        
        Args:
            symbol: Trading symbol or instrument_key
            
        Returns:
            Instrument data if found, None otherwise
        """
        if not self.instruments_index:
            self.load_instruments()
        
        return self.instruments_index.get(symbol)


# Create singleton instance
instruments_downloader = InstrumentsDownloader()


async def download_daily_instruments():
    """Function to be called by scheduler for daily download"""
    try:
        logger.info("Starting daily instruments download at {}".format(datetime.now()))
        
        success = instruments_downloader.download_instruments()
        
        if success:
            logger.info("✅ Daily instruments download completed successfully")
        else:
            logger.error("❌ Daily instruments download failed")
        
        return success
        
    except Exception as e:
        logger.error(f"Error in daily instruments download: {str(e)}")
        return False


class InstrumentsScheduler:
    """Scheduler for downloading Upstox instruments data"""
    
    def __init__(self):
        # AsyncIOScheduler creates its own event loop in a background thread
        # Don't pass event_loop parameter - let it handle it automatically
        self.scheduler = AsyncIOScheduler(timezone='Asia/Kolkata')
        self.is_running = False
        
    def start(self):
        """Start the scheduler"""
        if not self.is_running:
            # Schedule daily download at 9:05 AM IST (5 min after master_stock)
            self.scheduler.add_job(
                download_daily_instruments,
                trigger=CronTrigger(hour=9, minute=5, timezone='Asia/Kolkata'),
                id='instruments_daily_download',
                name='Download Upstox Instruments',
                replace_existing=True
            )
            
            try:
                self.scheduler.start()
                self.is_running = True
                logger.info("Instruments Scheduler started - Daily download at 9:05 AM IST")
                print(f"✅ Instruments Scheduler started - Jobs: {len(self.scheduler.get_jobs())}", flush=True)
            except Exception as e:
                logger.error(f"❌ Failed to start Instruments Scheduler: {e}", exc_info=True)
                print(f"❌ Failed to start Instruments Scheduler: {e}", flush=True)
                raise
    
    def stop(self):
        """Stop the scheduler"""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("Instruments Scheduler stopped")
    
    def run_now(self):
        """Manually trigger the download (for testing)"""
        logger.info("Manually triggering instruments download...")
        self.scheduler.add_job(
            download_daily_instruments,
            id='instruments_manual_download',
            replace_existing=True
        )


# Global scheduler instance
instruments_scheduler = InstrumentsScheduler()


def start_instruments_scheduler():
    """Start the instruments scheduler"""
    instruments_scheduler.start()


def stop_instruments_scheduler():
    """Stop the instruments scheduler"""
    instruments_scheduler.stop()


if __name__ == "__main__":
    # Test the download
    logging.basicConfig(level=logging.INFO)
    import asyncio
    asyncio.run(download_daily_instruments())
