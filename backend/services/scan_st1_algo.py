"""
Scan ST1 Algo Scheduler Controller
Consolidates all scan algorithm schedulers into a single controller:
- Instruments Downloader (daily at 9:05 AM)
- Health Monitor (every 30 min from 8:39 AM to 4:09 PM)
- VWAP Updater (hourly updates + cycles + EOD close)
- Index Price Scheduler (every 5 min during market hours)

Note: Master Stock download from Dhan has been removed.

All logs go to logs/scan_st1_algo.log
"""

import logging
import os
from pathlib import Path
from datetime import datetime
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# Configure dedicated logger for scan_st1_algo
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'scan_st1_algo.log'

# Create file handler for scan_st1_algo.log with immediate flushing
class FlushingFileHandler(logging.FileHandler):
    """FileHandler that flushes after each log entry to ensure immediate writes"""
    def emit(self, record):
        super().emit(record)
        self.flush()
        # Also force OS-level flush to ensure data is written to disk
        if hasattr(self.stream, 'fileno'):
            try:
                import os
                os.fsync(self.stream.fileno())
            except (OSError, AttributeError):
                pass  # Ignore if fsync fails

file_handler = FlushingFileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))

# Create logger for scan_st1_algo (does NOT propagate to root logger)
logger = logging.getLogger('scan_st1_algo')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.propagate = False  # Only log to scan_st1_algo.log, not to root logger

# Import all the job functions from existing schedulers
# Master Stock Scheduler removed - no longer downloading from Dhan
from backend.services.instruments_downloader import download_daily_instruments
from backend.services.vwap_updater import (
    update_vwap_for_all_open_positions,
    close_all_open_trades,
    calculate_vwap_slope_for_cycle
)

# Import health monitor instance and methods
from backend.services.health_monitor import health_monitor

# Import index price scheduler instance and method
from backend.services.index_price_scheduler import index_price_scheduler

# Configure job function loggers to ALSO write to scan_st1_algo.log
# This ensures all scheduler-related logs go to scan_st1_algo.log
job_loggers = [
    logging.getLogger('backend.services.instruments_downloader'),
    logging.getLogger('backend.services.vwap_updater'),
    logging.getLogger('backend.services.health_monitor'),
    logging.getLogger('backend.services.index_price_scheduler')
]

for job_logger in job_loggers:
    # Add scan_st1_algo.log handler to job loggers (in addition to their existing handlers)
    # Check if handler already exists to avoid duplicates by checking baseFilename attribute
    handler_exists = False
    for h in job_logger.handlers:
        if isinstance(h, logging.FileHandler):
            # Check if this handler points to our log file
            handler_path = getattr(h, 'baseFilename', None) or getattr(h, 'stream', {}).name if hasattr(getattr(h, 'stream', None), 'name') else None
            if handler_path and 'scan_st1_algo.log' in str(handler_path):
                handler_exists = True
                break
    
    if not handler_exists:
        # Create a new handler instance for this logger
        job_file_handler = FlushingFileHandler(log_file, mode='a', encoding='utf-8')
        job_file_handler.setLevel(logging.INFO)
        job_file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        job_logger.addHandler(job_file_handler)
        # Keep propagation enabled so logs also go to root logger (for trademanthan.log)
        # This way logs appear in both places


class ScanST1AlgoScheduler:
    """Unified scheduler controller for all scan algorithm jobs"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone='Asia/Kolkata')
        self.is_running = False
        logger.info("=" * 60)
        logger.info("🔧 Scan ST1 Algo Scheduler Controller initialized")
        logger.info("=" * 60)
    
    def start(self):
        """Start all scheduled jobs - runs as independent scheduler"""
        if self.is_running:
            logger.warning("⚠️ Scan ST1 Algo Scheduler is already running")
            return
        
        logger.info("🚀 Starting Scan ST1 Algo Scheduler Controller (Independent Schedule Job)...")
        logger.info("📝 All scheduler logs will be written to: logs/scan_st1_algo.log")
        
        try:
            # 1. Instruments Downloader - Daily at 9:05 AM
            def run_instruments_download():
                logger.info("🔧 Triggering Instruments Download job...")
                try:
                    download_daily_instruments()
                    logger.info("✅ Instruments Download job completed")
                except Exception as e:
                    logger.error(f"❌ Instruments Download job failed: {e}", exc_info=True)
            
            self.scheduler.add_job(
                run_instruments_download,
                trigger=CronTrigger(hour=9, minute=5, timezone='Asia/Kolkata'),
                id='scan_st1_instruments',
                name='Instruments Download (9:05 AM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300
            )
            logger.info("✅ Scheduled: Instruments Download (9:05 AM)")
            
            # 3. Health Monitor - Every 30 minutes from 8:39 AM to 4:09 PM
            health_check_times = [
                (8, 39), (9, 9), (9, 39), (10, 9), (10, 39), (11, 9), (11, 39), (12, 9),
                (12, 39), (13, 9), (13, 39), (14, 9), (14, 39), (15, 9), (15, 39), (16, 9)
            ]
            
            for hour, minute in health_check_times:
                def create_health_check_wrapper(h, m):
                    def run_health_check():
                        logger.info(f"🔧 Triggering Health Check job at {h:02d}:{m:02d}...")
                        try:
                            health_monitor.perform_health_check()
                            logger.info(f"✅ Health Check job at {h:02d}:{m:02d} completed")
                        except Exception as e:
                            logger.error(f"❌ Health Check job at {h:02d}:{m:02d} failed: {e}", exc_info=True)
                    return run_health_check
                
                self.scheduler.add_job(
                    create_health_check_wrapper(hour, minute),
                    trigger=CronTrigger(hour=hour, minute=minute, timezone='Asia/Kolkata'),
                    id=f'scan_st1_health_check_{hour}_{minute}',
                    name=f'Health Check {hour:02d}:{minute:02d}',
                    replace_existing=True,
                    max_instances=1,
                    misfire_grace_time=300,
                    coalesce=True
                )
            logger.info(f"✅ Scheduled: Health Checks ({len(health_check_times)} times)")
            
            # Daily health report at 4:00 PM
            def run_daily_health_report():
                logger.info("🔧 Triggering Daily Health Report job...")
                try:
                    health_monitor.send_daily_health_report()
                    logger.info("✅ Daily Health Report job completed")
                except Exception as e:
                    logger.error(f"❌ Daily Health Report job failed: {e}", exc_info=True)
            
            self.scheduler.add_job(
                run_daily_health_report,
                trigger=CronTrigger(hour=16, minute=0, timezone='Asia/Kolkata'),
                id='scan_st1_daily_health_report',
                name='Daily Health Report (4:00 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            logger.info("✅ Scheduled: Daily Health Report (4:00 PM)")
            
            # 4. VWAP Updater - Hourly updates (9:15 AM - 3:15 PM)
            for hour in range(9, 16):  # 9 AM to 3 PM
                def create_vwap_update_wrapper(h):
                    def run_vwap_update():
                        logger.info(f"🔧 Triggering VWAP Update job at {h:02d}:15...")
                        try:
                            update_vwap_for_all_open_positions()
                            logger.info(f"✅ VWAP Update job at {h:02d}:15 completed")
                        except Exception as e:
                            logger.error(f"❌ VWAP Update job at {h:02d}:15 failed: {e}", exc_info=True)
                    return run_vwap_update
                
                self.scheduler.add_job(
                    create_vwap_update_wrapper(hour),
                    trigger=CronTrigger(hour=hour, minute=15, timezone='Asia/Kolkata'),
                    id=f'scan_st1_vwap_update_{hour}',
                    name=f'Update VWAP {hour:02d}:15',
                    replace_existing=True,
                    max_instances=1,
                    misfire_grace_time=300,
                    coalesce=True
                )
            logger.info("✅ Scheduled: VWAP Updates (hourly 9:15 AM - 3:15 PM)")
            
            # EOD Close at 3:25 PM
            def run_eod_close():
                logger.info("🔧 Triggering EOD Close All Trades job...")
                try:
                    close_all_open_trades()
                    logger.info("✅ EOD Close All Trades job completed")
                except Exception as e:
                    logger.error(f"❌ EOD Close All Trades job failed: {e}", exc_info=True)
            
            self.scheduler.add_job(
                run_eod_close,
                trigger=CronTrigger(hour=15, minute=25, timezone='Asia/Kolkata'),
                id='scan_st1_close_all_trades_eod',
                name='Close All Open Trades (3:25 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            logger.info("✅ Scheduled: EOD Close All Trades (3:25 PM)")
            
            # VWAP Slope Cycles
            def run_cycle_1():
                logger.info("🔧 Triggering Cycle 1 VWAP Slope calculation...")
                try:
                    import asyncio
                    asyncio.run(calculate_vwap_slope_for_cycle(1, datetime.now(pytz.timezone('Asia/Kolkata'))))
                    logger.info("✅ Cycle 1 VWAP Slope calculation completed")
                except Exception as e:
                    logger.error(f"❌ Cycle 1 VWAP Slope calculation failed: {e}", exc_info=True)
            
            def run_cycle_2():
                logger.info("🔧 Triggering Cycle 2 VWAP Slope calculation...")
                try:
                    import asyncio
                    asyncio.run(calculate_vwap_slope_for_cycle(2, datetime.now(pytz.timezone('Asia/Kolkata'))))
                    logger.info("✅ Cycle 2 VWAP Slope calculation completed")
                except Exception as e:
                    logger.error(f"❌ Cycle 2 VWAP Slope calculation failed: {e}", exc_info=True)
            
            def run_cycle_3():
                logger.info("🔧 Triggering Cycle 3 VWAP Slope calculation...")
                try:
                    import asyncio
                    asyncio.run(calculate_vwap_slope_for_cycle(3, datetime.now(pytz.timezone('Asia/Kolkata'))))
                    logger.info("✅ Cycle 3 VWAP Slope calculation completed")
                except Exception as e:
                    logger.error(f"❌ Cycle 3 VWAP Slope calculation failed: {e}", exc_info=True)
            
            def run_cycle_4():
                logger.info("🔧 Triggering Cycle 4 VWAP Slope calculation...")
                try:
                    import asyncio
                    asyncio.run(calculate_vwap_slope_for_cycle(4, datetime.now(pytz.timezone('Asia/Kolkata'))))
                    logger.info("✅ Cycle 4 VWAP Slope calculation completed")
                except Exception as e:
                    logger.error(f"❌ Cycle 4 VWAP Slope calculation failed: {e}", exc_info=True)
            
            def run_cycle_5():
                logger.info("🔧 Triggering Cycle 5 VWAP Slope calculation...")
                try:
                    import asyncio
                    asyncio.run(calculate_vwap_slope_for_cycle(5, datetime.now(pytz.timezone('Asia/Kolkata'))))
                    logger.info("✅ Cycle 5 VWAP Slope calculation completed")
                except Exception as e:
                    logger.error(f"❌ Cycle 5 VWAP Slope calculation failed: {e}", exc_info=True)
            
            # Cycle 1: 10:30 AM
            self.scheduler.add_job(
                run_cycle_1,
                trigger=CronTrigger(hour=10, minute=30, timezone='Asia/Kolkata'),
                id='scan_st1_cycle_1_vwap_slope',
                name='Cycle 1: VWAP Slope (10:30 AM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            
            # Cycle 2: 11:15 AM
            self.scheduler.add_job(
                run_cycle_2,
                trigger=CronTrigger(hour=11, minute=15, timezone='Asia/Kolkata'),
                id='scan_st1_cycle_2_vwap_slope',
                name='Cycle 2: VWAP Slope (11:15 AM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            
            # Cycle 3: 12:15 PM
            self.scheduler.add_job(
                run_cycle_3,
                trigger=CronTrigger(hour=12, minute=15, timezone='Asia/Kolkata'),
                id='scan_st1_cycle_3_vwap_slope',
                name='Cycle 3: VWAP Slope (12:15 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            
            # Cycle 4: 13:15 PM
            self.scheduler.add_job(
                run_cycle_4,
                trigger=CronTrigger(hour=13, minute=15, timezone='Asia/Kolkata'),
                id='scan_st1_cycle_4_vwap_slope',
                name='Cycle 4: VWAP Slope (13:15 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            
            # Cycle 5: 14:15 PM
            self.scheduler.add_job(
                run_cycle_5,
                trigger=CronTrigger(hour=14, minute=15, timezone='Asia/Kolkata'),
                id='scan_st1_cycle_5_vwap_slope',
                name='Cycle 5: VWAP Slope (14:15 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            logger.info("✅ Scheduled: VWAP Slope Cycles (5 cycles)")
            
            # 5. Index Price Scheduler - Every 5 minutes during market hours
            def run_index_price_check():
                logger.info("🔧 Triggering Index Price Check job (every 5 minutes)...")
                try:
                    index_price_scheduler.fetch_and_store_index_prices()
                    logger.info("✅ Index Price Check job completed")
                except Exception as e:
                    logger.error(f"❌ Index Price Check job failed: {e}", exc_info=True)
            
            self.scheduler.add_job(
                run_index_price_check,
                trigger=IntervalTrigger(minutes=5),
                id='scan_st1_index_price_check',
                name='Index Price Check (Every 5 minutes)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            logger.info("✅ Scheduled: Index Price Check (Every 5 minutes)")
            
            # Special jobs for 9:15 AM and 3:30 PM
            def run_index_price_9_15():
                logger.info("🔧 Triggering Index Price at 9:15 AM (Market Open) job...")
                try:
                    index_price_scheduler.fetch_and_store_index_prices()
                    logger.info("✅ Index Price at 9:15 AM job completed")
                except Exception as e:
                    logger.error(f"❌ Index Price at 9:15 AM job failed: {e}", exc_info=True)
            
            def run_index_price_15_30():
                logger.info("🔧 Triggering Index Price at 3:30 PM (Market Close) job...")
                try:
                    index_price_scheduler.fetch_and_store_index_prices()
                    logger.info("✅ Index Price at 3:30 PM job completed")
                except Exception as e:
                    logger.error(f"❌ Index Price at 3:30 PM job failed: {e}", exc_info=True)
            
            self.scheduler.add_job(
                run_index_price_9_15,
                trigger=CronTrigger(hour=9, minute=15, timezone='Asia/Kolkata'),
                id='scan_st1_index_price_9_15',
                name='Index Price at 9:15 AM (Market Open)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            
            self.scheduler.add_job(
                run_index_price_15_30,
                trigger=CronTrigger(hour=15, minute=30, timezone='Asia/Kolkata'),
                id='scan_st1_index_price_15_30',
                name='Index Price at 3:30 PM (Market Close)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            logger.info("✅ Scheduled: Index Price at Market Open/Close (9:15 AM, 3:30 PM)")
            
            # Start the scheduler
            self.scheduler.start()
            self.is_running = True
            
            total_jobs = len(self.scheduler.get_jobs())
            logger.info("=" * 60)
            logger.info(f"✅ Scan ST1 Algo Scheduler Controller STARTED")
            logger.info(f"   Status: Running as independent scheduled job")
            logger.info(f"   Total Jobs: {total_jobs}")
            logger.info(f"   Log File: {log_file}")
            logger.info(f"   Scheduler State: {self.scheduler.state if hasattr(self.scheduler, 'state') else 'RUNNING'}")
            
            # List all scheduled jobs
            jobs = self.scheduler.get_jobs()
            logger.info(f"   Scheduled Jobs ({len(jobs)} total):")
            for job in jobs[:10]:  # Show first 10 jobs
                next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S IST') if job.next_run_time else 'Not scheduled'
                logger.info(f"      - {job.name} (Next: {next_run})")
            if len(jobs) > 10:
                logger.info(f"      ... and {len(jobs) - 10} more jobs")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ Failed to start Scan ST1 Algo Scheduler: {e}", exc_info=True)
            raise
    
    def stop(self):
        """Stop all scheduled jobs"""
        if not self.is_running:
            logger.warning("⚠️ Scan ST1 Algo Scheduler is not running")
            return
        
        try:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("✅ Scan ST1 Algo Scheduler Controller STOPPED")
        except Exception as e:
            logger.error(f"❌ Error stopping Scan ST1 Algo Scheduler: {e}", exc_info=True)
    
    def get_status(self):
        """Get scheduler status"""
        return {
            "is_running": self.is_running,
            "jobs_count": len(self.scheduler.get_jobs()) if self.is_running else 0,
            "jobs": [{"id": job.id, "name": job.name, "next_run": str(job.next_run_time)} 
                     for job in self.scheduler.get_jobs()] if self.is_running else []
        }


# Global instance
scan_st1_algo_scheduler = ScanST1AlgoScheduler()


def start_scan_st1_algo():
    """Start the scan ST1 algo scheduler"""
    scan_st1_algo_scheduler.start()


def stop_scan_st1_algo():
    """Stop the scan ST1 algo scheduler"""
    scan_st1_algo_scheduler.stop()
