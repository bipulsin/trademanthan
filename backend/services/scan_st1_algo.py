"""
Scan ST1 Algo Scheduler Controller
Consolidates all scan algorithm schedulers into a single controller:
- Morning Telegram ping (8:10 AM IST): TradeWithCTO channel — healthy vs not started
- Instruments Downloader (daily at 9:05 AM)
- Health Monitor (every 30 min from 8:30 AM to 4:00 PM IST)
- VWAP Updater (every 5 min during 9:15–15:35 IST session + cycles + EOD close)
- Index Price Scheduler (every 5 min during market hours)
- Entry slip monitor (every 15 min during market hours): cancel unfilled entry orders after 2 checks
- Final reconciliation (3:45 PM & 4:00 PM): broker buy/sell/PnL sync; time_based → Exit-TM after 3:15 PM exits
- Fin sentiment (weekdays 9:17–13:17 IST, 15 min): NSE corporate announcements + FinBERT for arbitrage_master, store in stock_fin_sentiment (NSE date window: last-run→now; 09:17 only uses today IST)

Interval-driven jobs only run real work between 08:30 and 21:00 IST (see scheduler_window).
Exception: 8:10 AM Telegram ping (before 8:30).

Note: Master Stock download from Dhan has been removed.

All logs go to logs/scan_st1_algo.log
"""

import logging
import os
import threading
import requests
from pathlib import Path
from datetime import datetime, time as dt_time
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
from backend.services.instruments_downloader import download_daily_instruments, ensure_instruments_available
from backend.services.vwap_updater import (
    update_vwap_for_all_open_positions,
    close_all_open_trades,
    calculate_vwap_slope_for_cycle
)
from backend.services.final_reconciliation import run_final_reconciliation

# Import health monitor instance and methods
from backend.services.health_monitor import health_monitor

# Import index price scheduler instance and method
from backend.services.index_price_scheduler import index_price_scheduler

# CAR NIFTY200 updater (Yahoo + Upstox fallback)
from backend.services.car_nifty200_updater import run_car_nifty200_update_job
from backend.services.entry_slip_monitor import run_entry_slip_monitor
from backend.services.fin_sentiment_job import run_fin_sentiment_job
from backend.services.scheduler_window import is_allowed_scheduler_window_ist
from backend.services.telegram_trade_channel import send_trade_with_cto_channel_message


def run_morning_trade_channel_health_ping() -> None:
    """8:10 AM IST: Telegram @TradeWithCTO — healthy API vs system not up."""
    url = os.getenv("BACKEND_HEALTH_URL", "http://127.0.0.1:8000/scan/health")
    ok = False
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            ok = data.get("status") == "healthy"
    except Exception as e:
        logger.warning("Morning trade channel health ping failed: %s", e)
        ok = False
    text = "TradeWithCTO Running good" if ok else "System not started!"
    send_trade_with_cto_channel_message(text)

# Configure job function loggers to write ONLY to scan_st1_algo.log
# This ensures all scan algorithm logs (webhooks, option contracts, trades, exits) go to scan_st1_algo.log
job_loggers = [
    logging.getLogger('backend.services.instruments_downloader'),
    logging.getLogger('backend.services.vwap_updater'),
    logging.getLogger('backend.services.health_monitor'),
    logging.getLogger('backend.services.index_price_scheduler'),
    logging.getLogger('backend.routers.scan'),  # Add scan router logger for webhook processing, option contracts, trades
    logging.getLogger('backend.services.entry_slip_monitor'),
    logging.getLogger('backend.services.final_reconciliation'),
    logging.getLogger('backend.services.fin_sentiment_job'),
    logging.getLogger('backend.services.fin_sentiment_reason_openai'),
    logging.getLogger('backend.services.nse_corporate_client'),
]

for job_logger in job_loggers:
    # Add scan_st1_algo.log handler to job loggers
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
        # Disable propagation to root logger so logs ONLY go to scan_st1_algo.log
        # This prevents duplicate logs in trademanthan.log
        job_logger.propagate = False


def _attach_scan_st1_mirror_loggers() -> None:
    """
    Add scan_st1_algo.log handler to selected loggers without disabling propagation,
    so the same messages also reach trademanthan.log via the root logger.
    Used for live entry/exit lines (market, limit fallback, GTT backup).
    """
    mirror_names = [
        "backend.services.live_trading",
    ]
    for name in mirror_names:
        ml = logging.getLogger(name)
        handler_exists = False
        for h in ml.handlers:
            if isinstance(h, logging.FileHandler):
                handler_path = getattr(h, "baseFilename", None) or (
                    getattr(h.stream, "name", None) if getattr(h, "stream", None) else None
                )
                if handler_path and "scan_st1_algo.log" in str(handler_path):
                    handler_exists = True
                    break
        if handler_exists:
            continue
        mirror_handler = FlushingFileHandler(log_file, mode="a", encoding="utf-8")
        mirror_handler.setLevel(logging.INFO)
        mirror_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        ml.addHandler(mirror_handler)
        # propagate stays True: logs go to scan_st1_algo.log AND root/trademanthan.log


_attach_scan_st1_mirror_loggers()


def run_scan_st1_vwap_update_gated() -> None:
    """
    APScheduler fires every 5 minutes in the 09:00–15:55 IST hour range; we only run real work
    on weekdays between 09:15 and 15:35 so open-position VWAP / SL / VWAP-exit checks stay aligned
    with scan.html signals (previously hourly-only, so UI showed EXIT VWAP long before broker exit).
    """
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if now.weekday() >= 5:
        return
    t = now.time()
    if t < dt_time(9, 15) or t > dt_time(15, 35):
        return
    logger.info("🔧 Triggering VWAP Update job (5-min cadence, IST session gate)...")
    try:
        update_vwap_for_all_open_positions()
        logger.info("✅ VWAP Update job (5-min) completed")
    except Exception as e:
        logger.error("❌ VWAP Update job (5-min) failed: %s", e, exc_info=True)


class ScanST1AlgoScheduler:
    """Unified scheduler controller for all scan algorithm jobs"""
    
    def __init__(self):
        try:
            self.scheduler = BackgroundScheduler(timezone='Asia/Kolkata')
            self.is_running = False
            logger.info("=" * 60)
            logger.info("🔧 Scan ST1 Algo Scheduler Controller initialized")
            logger.info("=" * 60)
        except Exception as e:
            # Log error but don't raise - allow scheduler to be created but not started
            print(f"ERROR: Failed to initialize ScanST1AlgoScheduler: {e}", flush=True)
            import traceback
            traceback.print_exc()
            # Create a dummy scheduler object to prevent AttributeError
            self.scheduler = None
            self.is_running = False
    
    def start(self):
        """Start all scheduled jobs - runs as independent scheduler"""
        if self.is_running:
            logger.warning("⚠️ Scan ST1 Algo Scheduler is already running")
            return
        
        logger.info("🚀 Starting Scan ST1 Algo Scheduler Controller (Independent Schedule Job)...")
        logger.info("📝 All scheduler logs will be written to: logs/scan_st1_algo.log")
        
        try:
            # 0. Ensure instruments file exists on startup (download if missing or stale).
            # Run in a daemon thread so FastAPI lifespan / Uvicorn can bind to port 8000 immediately.
            logger.info("🔧 Checking instruments file on startup (background)...")

            def _ensure_instruments_bg():
                try:
                    if ensure_instruments_available():
                        logger.info("✅ Instruments file ready for scan/option lookups")
                    else:
                        logger.warning(
                            "⚠️ Instruments download failed - scan may show 'Missing option data' for some stocks"
                        )
                except Exception as e:
                    logger.error(f"❌ Startup instruments check failed: {e}", exc_info=True)

            threading.Thread(target=_ensure_instruments_bg, name="instruments_startup", daemon=True).start()
            
            # 0b. Morning Telegram to @TradeWithCTO — 8:10 AM IST (exception to 08:30–21:00 work window)
            self.scheduler.add_job(
                run_morning_trade_channel_health_ping,
                trigger=CronTrigger(hour=8, minute=10, timezone="Asia/Kolkata"),
                id="scan_st1_morning_trade_channel_ping",
                name="Morning TradeWithCTO Telegram Health (8:10 AM)",
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            logger.info("✅ Scheduled: Morning TradeWithCTO Telegram Health (8:10 AM)")
            
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
            
            # 3. Health Monitor — every 30 min from 8:30 AM to 4:00 PM IST (within 08:30–21:00 window)
            health_check_times = [
                (8, 30), (9, 0), (9, 30), (10, 0), (10, 30), (11, 0), (11, 30), (12, 0),
                (12, 30), (13, 0), (13, 30), (14, 0), (14, 30), (15, 0), (15, 30), (16, 0),
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
            
            # 4. VWAP Updater — every 5 minutes in 9–15 IST (gated to 9:15–15:35 weekdays) so exits track UI
            self.scheduler.add_job(
                run_scan_st1_vwap_update_gated,
                trigger=CronTrigger(
                    minute="0,5,10,15,20,25,30,35,40,45,50,55",
                    hour="9-15",
                    timezone="Asia/Kolkata",
                ),
                id="scan_st1_vwap_update_every5",
                name="Update VWAP + exits (every 5 min, session gated)",
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            logger.info("✅ Scheduled: VWAP Updates (every 5 min 9:00–15:55 IST clock, work 9:15–15:35 weekdays)")
            
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

            # Final reconciliation: broker fill alignment + Exit-TM tagging
            def run_final_reconciliation_job():
                logger.info("🔧 Triggering Final Reconciliation...")
                try:
                    run_final_reconciliation()
                    logger.info("✅ Final Reconciliation completed")
                except Exception as e:
                    logger.error(f"❌ Final Reconciliation failed: {e}", exc_info=True)

            self.scheduler.add_job(
                run_final_reconciliation_job,
                trigger=CronTrigger(hour=15, minute=45, timezone='Asia/Kolkata'),
                id='scan_st1_final_reconciliation_1545',
                name='Final Reconciliation (3:45 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            self.scheduler.add_job(
                run_final_reconciliation_job,
                trigger=CronTrigger(hour=16, minute=0, timezone='Asia/Kolkata'),
                id='scan_st1_final_reconciliation_1600',
                name='Final Reconciliation (4:00 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            logger.info("✅ Scheduled: Final Reconciliation (3:45 PM & 4:00 PM)")
            
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
                ist = pytz.timezone('Asia/Kolkata')
                now = datetime.now(ist)
                if not is_allowed_scheduler_window_ist(now):
                    logger.debug(
                        "Outside 08:30–21:00 IST — skip Index Price Check tick (%s)",
                        now.strftime("%H:%M"),
                    )
                    return
                # Check if market is open before logging and running
                
                # Market hours: 9:15 AM to 3:30 PM IST
                if now.weekday() >= 5:  # Weekend
                    logger.debug(f"⏰ Weekend ({now.strftime('%A')}) - skipping Index Price Check")
                    return
                
                current_hour = now.hour
                current_minute = now.minute
                is_market_hours = False
                
                if current_hour < 9:
                    is_market_hours = False
                elif current_hour == 9:
                    is_market_hours = current_minute >= 15
                elif 10 <= current_hour <= 14:
                    is_market_hours = True
                elif current_hour == 15:
                    is_market_hours = current_minute <= 30
                else:
                    is_market_hours = False
                
                if not is_market_hours:
                    logger.debug(f"⏰ Market closed ({now.strftime('%H:%M:%S IST')}) - skipping Index Price Check")
                    return
                
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

            def run_entry_slip_monitor_job():
                if not is_allowed_scheduler_window_ist():
                    logger.debug("Outside 08:30–21:00 IST — skip Entry Slip Monitor tick")
                    return
                logger.info("🔧 Triggering Entry Slip Monitor job (every 15 minutes)...")
                try:
                    result = run_entry_slip_monitor()
                    logger.info("✅ Entry Slip Monitor job completed: %s", result)
                except Exception as e:
                    logger.error(f"❌ Entry Slip Monitor job failed: {e}", exc_info=True)

            self.scheduler.add_job(
                run_entry_slip_monitor_job,
                trigger=IntervalTrigger(minutes=15),
                id='scan_st1_entry_slip_monitor',
                name='Entry Slip Monitor (Every 15 minutes)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            logger.info("✅ Scheduled: Entry Slip Monitor (Every 15 minutes)")

            # Fin sentiment: NSE corporate announcements + FinBERT — weekdays 9:17–13:17 IST, every 15 minutes
            fin_sentiment_times = []
            for m in (17, 32, 47):
                fin_sentiment_times.append((9, m))
            for h in range(10, 13):
                for m in (2, 17, 32, 47):
                    fin_sentiment_times.append((h, m))
            for m in (2, 17):
                fin_sentiment_times.append((13, m))

            def create_fin_sentiment_wrapper(h, m):
                def run_fin_sentiment_scheduled():
                    if not is_allowed_scheduler_window_ist():
                        logger.debug("Outside 08:30–21:00 IST — skip Fin Sentiment tick")
                        return
                    ist = pytz.timezone("Asia/Kolkata")
                    now = datetime.now(ist)
                    if now.weekday() >= 5:
                        return
                    logger.info("🔧 Fin Sentiment job (%02d:%02d IST)...", h, m)
                    try:
                        run_fin_sentiment_job(use_single_ist_day_nse=(h == 9 and m == 17))
                        logger.info("✅ Fin Sentiment job (%02d:%02d) completed", h, m)
                    except Exception as e:
                        logger.error("❌ Fin Sentiment job (%02d:%02d) failed: %s", h, m, e, exc_info=True)

                return run_fin_sentiment_scheduled

            for hour, minute in fin_sentiment_times:
                self.scheduler.add_job(
                    create_fin_sentiment_wrapper(hour, minute),
                    trigger=CronTrigger(
                        day_of_week="mon-fri",
                        hour=hour,
                        minute=minute,
                        timezone="Asia/Kolkata",
                    ),
                    id=f"scan_st1_fin_sentiment_{hour}_{minute:02d}",
                    name=f"Fin Sentiment ({hour:02d}:{minute:02d} IST)",
                    replace_existing=True,
                    max_instances=1,
                    misfire_grace_time=300,
                    coalesce=True,
                )
            logger.info(
                "✅ Scheduled: Fin Sentiment (%s weekday slots 9:17–13:17 IST, 15 min)",
                len(fin_sentiment_times),
            )

            # 6. CAR NIFTY200 Updater - Every 3 hours (Yahoo first, Upstox fallback)
            def run_car_nifty200_update():
                if not is_allowed_scheduler_window_ist():
                    logger.debug("Outside 08:30–21:00 IST — skip CAR NIFTY200 Update tick")
                    return
                logger.info("🔧 Triggering CAR NIFTY200 Update job...")
                try:
                    run_car_nifty200_update_job()
                    logger.info("✅ CAR NIFTY200 Update job completed")
                except Exception as e:
                    logger.error(f"❌ CAR NIFTY200 Update job failed: {e}", exc_info=True)

            self.scheduler.add_job(
                run_car_nifty200_update,
                trigger=IntervalTrigger(hours=3),
                id='scan_st1_car_nifty200_update',
                name='CAR NIFTY200 Update (Every 3 hours)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=600,
                coalesce=True
            )
            logger.info("✅ Scheduled: CAR NIFTY200 Update (Every 3 hours)")
            
            # Start the scheduler
            self.scheduler.start()
            self.is_running = True

            # Run CAR NIFTY200 update once after startup (e.g. after deploy) — background only so HTTP comes up first.
            def _car_nifty_startup_bg():
                try:
                    logger.info("🔧 Running CAR NIFTY200 Update once on startup...")
                    run_car_nifty200_update_job()
                    logger.info("✅ CAR NIFTY200 startup run completed")
                except Exception as e:
                    logger.error(f"❌ CAR NIFTY200 startup run failed: {e}", exc_info=True)

            threading.Thread(target=_car_nifty_startup_bg, name="car_nifty200_startup", daemon=True).start()
            
            total_jobs = len(self.scheduler.get_jobs())
            logger.info("=" * 60)
            logger.info(f"✅ Scan ST1 Algo Scheduler Controller STARTED")
            logger.info(f"   Status: Running as independent scheduled job")
            logger.info(f"   Total Jobs: {total_jobs}")
            logger.info(f"   Log File: {log_file}")
            logger.info(f"   Scheduler State: {self.scheduler.state if hasattr(self.scheduler, 'state') else 'RUNNING'}")
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
        try:
            if not self.scheduler:
                return {
                    "is_running": False,
                    "jobs_count": 0,
                    "jobs": []
                }
            jobs_count = len(self.scheduler.get_jobs()) if self.is_running and self.scheduler.running else 0
            jobs = [{"id": job.id, "name": job.name, "next_run": str(job.next_run_time) if job.next_run_time else "Not scheduled"} 
                     for job in self.scheduler.get_jobs()] if self.is_running and self.scheduler.running else []
            return {
                "is_running": self.is_running and (self.scheduler.running if self.scheduler else False),
                "jobs_count": jobs_count,
                "jobs": jobs
            }
        except Exception as e:
            logger.error(f"Error getting scheduler status: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "is_running": False,
                "jobs_count": 0,
                "jobs": []
            }


# Global instance
scan_st1_algo_scheduler = ScanST1AlgoScheduler()


def start_scan_st1_algo():
    """Start the scan ST1 algo scheduler"""
    scan_st1_algo_scheduler.start()


def stop_scan_st1_algo():
    """Stop the scan ST1 algo scheduler"""
    scan_st1_algo_scheduler.stop()
