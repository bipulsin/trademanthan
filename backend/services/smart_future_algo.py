"""
Smart Future Algo Scheduler Controller
Consolidates all scan algorithm schedulers into a single controller:
- Morning Telegram ping (8:10 AM IST): TradeWithCTO channel — healthy vs not started
- Instruments Downloader (daily at 9:05 AM)
- Health Monitor (every 30 min from 8:30 AM to 4:00 PM IST)
- VWAP Updater (every 5 min during 9:15–15:35 IST session + cycles + EOD close)
- Index Price Scheduler (every 5 min during market hours)
- Entry slip monitor (every 15 min during market hours): cancel unfilled entry orders after 2 checks
- Final reconciliation (3:45 PM & 4:00 PM): broker buy/sell/PnL sync; time_based → Exit-TM after 3:15 PM exits
- Fin sentiment (weekdays 9:17–13:17 IST, 15 min): NSE corporate announcements + FinBERT for arbitrage_master, store in stock_fin_sentiment (NSE date window: last-run→now; 09:17 only uses today IST)
- Smart Futures CMS picker (weekdays 9:15, 9:30, then 10:00–15:00 every 30 min IST): arbitrage_master current-month futures → smart_futures_daily
- Pre-market F&O watchlist (weekdays 9:14 IST default, PREMKET_RUN_TIME): ~203 equities → Top N in premarket_watchlist (same scoring as test_premkt_scanner / premarket_scoring)
- Live OI heatmap (weekdays, every 15 min 9:15–15:15 IST): Upstox batch quotes → oi_heatmap cache + DB; API falls back to DB snapshot when live is empty

Interval-driven jobs only run real work between 08:30 and 21:00 IST (see scheduler_window).
Exception: 8:10 AM Telegram ping (before 8:30).
Scheduled market-data jobs also skip IST dates listed in the ``holiday`` table (weekends + NSE holidays).

Note: Master Stock download from Dhan has been removed.

All logs go to logs/smart_future_algo.log
"""

import logging
import os
import threading
import requests
from pathlib import Path
from datetime import datetime, time as dt_time
from typing import Tuple
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# Configure dedicated logger for smart_future_algo
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'smart_future_algo.log'

# Create file handler for smart_future_algo.log with immediate flushing
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

# Create logger for smart_future_algo (does NOT propagate to root logger)
logger = logging.getLogger('smart_future_algo')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.propagate = False  # Only log to smart_future_algo.log, not to root logger

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
from backend.services.smart_futures_picker.job import run_smart_futures_picker_job
from backend.services.premarket_watchlist_job import run_premarket_watchlist_job
from backend.services.scheduler_window import is_allowed_scheduler_window_ist
from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist
from backend.services.telegram_trade_channel import send_trade_with_cto_channel_message
from backend.config import settings


def _parse_hh_mm(s: str) -> Tuple[int, int]:
    """IST clock time from ``HH:MM`` (e.g. PREMKET_RUN_TIME)."""
    try:
        parts = (s or "09:00").strip().split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return max(0, min(23, h)), max(0, min(59, m))
    except Exception:
        return 9, 0


def _skip_ist_non_trading_job(reason: str, now=None) -> bool:
    """True when IST date is Sat/Sun or listed in ``holiday`` — skip market data schedulers."""
    if should_skip_scheduled_market_jobs_ist(now):
        logger.debug("IST non-trading day (weekend or holiday) — skip %s", reason)
        return True
    return False


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

# Configure job function loggers to write ONLY to smart_future_algo.log
# This ensures all scan algorithm logs (webhooks, option contracts, trades, exits) go to smart_future_algo.log
job_loggers = [
    logging.getLogger('backend.services.instruments_downloader'),
    logging.getLogger('backend.services.vwap_updater'),
    logging.getLogger('backend.services.health_monitor'),
    logging.getLogger('backend.services.index_price_scheduler'),
    logging.getLogger('backend.routers.scan'),  # Add scan router logger for webhook processing, option contracts, trades
    logging.getLogger('backend.services.entry_slip_monitor'),
    logging.getLogger('backend.services.final_reconciliation'),
    logging.getLogger('backend.services.fin_sentiment_job'),
    logging.getLogger('backend.services.smart_futures_picker.job'),
    logging.getLogger('backend.services.premarket_watchlist_job'),
    logging.getLogger('backend.services.oi_heatmap'),
    logging.getLogger('backend.services.fin_sentiment_reason_openai'),
    logging.getLogger('backend.services.nse_corporate_client'),
]

for job_logger in job_loggers:
    # Add smart_future_algo.log handler to job loggers
    # Check if handler already exists to avoid duplicates by checking baseFilename attribute
    handler_exists = False
    for h in job_logger.handlers:
        if isinstance(h, logging.FileHandler):
            # Check if this handler points to our log file
            handler_path = getattr(h, 'baseFilename', None) or getattr(h, 'stream', {}).name if hasattr(getattr(h, 'stream', None), 'name') else None
            if handler_path and 'smart_future_algo.log' in str(handler_path):
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
        # Disable propagation to root logger so logs ONLY go to smart_future_algo.log
        # This prevents duplicate logs in trademanthan.log
        job_logger.propagate = False


def _attach_smart_future_mirror_loggers() -> None:
    """
    Add smart_future_algo.log handler to selected loggers without disabling propagation,
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
                if handler_path and "smart_future_algo.log" in str(handler_path):
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
        # propagate stays True: logs go to smart_future_algo.log AND root/trademanthan.log


_attach_smart_future_mirror_loggers()


def run_smart_future_vwap_update_gated() -> None:
    """
    APScheduler fires every 5 minutes in the 09:00–15:55 IST hour range; we only run real work
    on weekdays between 09:15 and 15:35 so open-position VWAP / SL / VWAP-exit checks stay aligned
    with scan.html signals (previously hourly-only, so UI showed EXIT VWAP long before broker exit).
    """
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if _skip_ist_non_trading_job("VWAP update (5-min)", now):
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


class SmartFutureAlgoScheduler:
    """Unified scheduler controller for all scan algorithm jobs"""
    
    def __init__(self):
        try:
            self.scheduler = BackgroundScheduler(timezone='Asia/Kolkata')
            self.is_running = False
            logger.info("=" * 60)
            logger.info("🔧 Smart Future Algo Scheduler Controller initialized")
            logger.info("=" * 60)
        except Exception as e:
            # Log error but don't raise - allow scheduler to be created but not started
            print(f"ERROR: Failed to initialize SmartFutureAlgoScheduler: {e}", flush=True)
            import traceback
            traceback.print_exc()
            # Create a dummy scheduler object to prevent AttributeError
            self.scheduler = None
            self.is_running = False
    
    def start(self):
        """Start all scheduled jobs - runs as independent scheduler"""
        if self.is_running:
            logger.warning("⚠️ Smart Future Algo Scheduler is already running")
            return
        
        logger.info("🚀 Starting Smart Future Algo Scheduler Controller (Independent Schedule Job)...")
        logger.info("📝 All scheduler logs will be written to: logs/smart_future_algo.log")
        
        try:
            # 0. Ensure instruments file exists on startup (download if missing or stale).
            # Run in a daemon thread so FastAPI lifespan / Uvicorn can bind to port 8000 immediately.
            logger.info("🔧 Checking instruments file on startup (background)...")

            def _ensure_instruments_bg():
                try:
                    if _skip_ist_non_trading_job("startup instruments check"):
                        return
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
                id="smart_future_morning_trade_channel_ping",
                name="Morning TradeWithCTO Telegram Health (8:10 AM)",
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            logger.info("✅ Scheduled: Morning TradeWithCTO Telegram Health (8:10 AM)")
            
            # 1. Instruments Downloader - Daily at 9:05 AM
            def run_instruments_download():
                if _skip_ist_non_trading_job("instruments download"):
                    return
                logger.info("🔧 Triggering Instruments Download job...")
                try:
                    download_daily_instruments()
                    logger.info("✅ Instruments Download job completed")
                except Exception as e:
                    logger.error(f"❌ Instruments Download job failed: {e}", exc_info=True)
            
            self.scheduler.add_job(
                run_instruments_download,
                trigger=CronTrigger(hour=9, minute=5, timezone='Asia/Kolkata'),
                id='smart_future_instruments',
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
                        if _skip_ist_non_trading_job(f"health check {h:02d}:{m:02d}"):
                            return
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
                    id=f'smart_future_health_check_{hour}_{minute}',
                    name=f'Health Check {hour:02d}:{minute:02d}',
                    replace_existing=True,
                    max_instances=1,
                    misfire_grace_time=300,
                    coalesce=True
                )
            logger.info(f"✅ Scheduled: Health Checks ({len(health_check_times)} times)")
            
            # Daily health report at 4:00 PM
            def run_daily_health_report():
                if _skip_ist_non_trading_job("daily health report"):
                    return
                logger.info("🔧 Triggering Daily Health Report job...")
                try:
                    health_monitor.send_daily_health_report()
                    logger.info("✅ Daily Health Report job completed")
                except Exception as e:
                    logger.error(f"❌ Daily Health Report job failed: {e}", exc_info=True)
            
            self.scheduler.add_job(
                run_daily_health_report,
                trigger=CronTrigger(hour=16, minute=0, timezone='Asia/Kolkata'),
                id='smart_future_daily_health_report',
                name='Daily Health Report (4:00 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            logger.info("✅ Scheduled: Daily Health Report (4:00 PM)")
            
            # 4. VWAP Updater — every 5 minutes in 9–15 IST (gated to 9:15–15:35 weekdays) so exits track UI
            self.scheduler.add_job(
                run_smart_future_vwap_update_gated,
                trigger=CronTrigger(
                    minute="0,5,10,15,20,25,30,35,40,45,50,55",
                    hour="9-15",
                    timezone="Asia/Kolkata",
                ),
                id="smart_future_vwap_update_every5",
                name="Update VWAP + exits (every 5 min, session gated)",
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            logger.info("✅ Scheduled: VWAP Updates (every 5 min 9:00–15:55 IST clock, work 9:15–15:35 weekdays)")
            
            # EOD Close at 3:25 PM
            def run_eod_close():
                if _skip_ist_non_trading_job("EOD close all trades"):
                    return
                logger.info("🔧 Triggering EOD Close All Trades job...")
                try:
                    close_all_open_trades()
                    logger.info("✅ EOD Close All Trades job completed")
                except Exception as e:
                    logger.error(f"❌ EOD Close All Trades job failed: {e}", exc_info=True)
            
            self.scheduler.add_job(
                run_eod_close,
                trigger=CronTrigger(hour=15, minute=25, timezone='Asia/Kolkata'),
                id='smart_future_close_all_trades_eod',
                name='Close All Open Trades (3:25 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            logger.info("✅ Scheduled: EOD Close All Trades (3:25 PM)")

            # Final reconciliation: broker fill alignment + Exit-TM tagging
            def run_final_reconciliation_job():
                if _skip_ist_non_trading_job("final reconciliation"):
                    return
                logger.info("🔧 Triggering Final Reconciliation...")
                try:
                    run_final_reconciliation()
                    logger.info("✅ Final Reconciliation completed")
                except Exception as e:
                    logger.error(f"❌ Final Reconciliation failed: {e}", exc_info=True)

            self.scheduler.add_job(
                run_final_reconciliation_job,
                trigger=CronTrigger(hour=15, minute=45, timezone='Asia/Kolkata'),
                id='smart_future_final_reconciliation_1545',
                name='Final Reconciliation (3:45 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            self.scheduler.add_job(
                run_final_reconciliation_job,
                trigger=CronTrigger(hour=16, minute=0, timezone='Asia/Kolkata'),
                id='smart_future_final_reconciliation_1600',
                name='Final Reconciliation (4:00 PM)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            logger.info("✅ Scheduled: Final Reconciliation (3:45 PM & 4:00 PM)")
            
            # VWAP Slope Cycles
            def run_cycle_1():
                if _skip_ist_non_trading_job("cycle 1 VWAP slope"):
                    return
                logger.info("🔧 Triggering Cycle 1 VWAP Slope calculation...")
                try:
                    import asyncio
                    asyncio.run(calculate_vwap_slope_for_cycle(1, datetime.now(pytz.timezone('Asia/Kolkata'))))
                    logger.info("✅ Cycle 1 VWAP Slope calculation completed")
                except Exception as e:
                    logger.error(f"❌ Cycle 1 VWAP Slope calculation failed: {e}", exc_info=True)
            
            def run_cycle_2():
                if _skip_ist_non_trading_job("cycle 2 VWAP slope"):
                    return
                logger.info("🔧 Triggering Cycle 2 VWAP Slope calculation...")
                try:
                    import asyncio
                    asyncio.run(calculate_vwap_slope_for_cycle(2, datetime.now(pytz.timezone('Asia/Kolkata'))))
                    logger.info("✅ Cycle 2 VWAP Slope calculation completed")
                except Exception as e:
                    logger.error(f"❌ Cycle 2 VWAP Slope calculation failed: {e}", exc_info=True)
            
            def run_cycle_3():
                if _skip_ist_non_trading_job("cycle 3 VWAP slope"):
                    return
                logger.info("🔧 Triggering Cycle 3 VWAP Slope calculation...")
                try:
                    import asyncio
                    asyncio.run(calculate_vwap_slope_for_cycle(3, datetime.now(pytz.timezone('Asia/Kolkata'))))
                    logger.info("✅ Cycle 3 VWAP Slope calculation completed")
                except Exception as e:
                    logger.error(f"❌ Cycle 3 VWAP Slope calculation failed: {e}", exc_info=True)
            
            def run_cycle_4():
                if _skip_ist_non_trading_job("cycle 4 VWAP slope"):
                    return
                logger.info("🔧 Triggering Cycle 4 VWAP Slope calculation...")
                try:
                    import asyncio
                    asyncio.run(calculate_vwap_slope_for_cycle(4, datetime.now(pytz.timezone('Asia/Kolkata'))))
                    logger.info("✅ Cycle 4 VWAP Slope calculation completed")
                except Exception as e:
                    logger.error(f"❌ Cycle 4 VWAP Slope calculation failed: {e}", exc_info=True)
            
            def run_cycle_5():
                if _skip_ist_non_trading_job("cycle 5 VWAP slope"):
                    return
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
                id='smart_future_cycle_1_vwap_slope',
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
                id='smart_future_cycle_2_vwap_slope',
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
                id='smart_future_cycle_3_vwap_slope',
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
                id='smart_future_cycle_4_vwap_slope',
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
                id='smart_future_cycle_5_vwap_slope',
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
                if _skip_ist_non_trading_job("index price check", now):
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
                id='smart_future_index_price_check',
                name='Index Price Check (Every 5 minutes)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            logger.info("✅ Scheduled: Index Price Check (Every 5 minutes)")
            
            # Special jobs for 9:15 AM and 3:30 PM
            def run_index_price_9_15():
                if _skip_ist_non_trading_job("index price 9:15"):
                    return
                logger.info("🔧 Triggering Index Price at 9:15 AM (Market Open) job...")
                try:
                    index_price_scheduler.fetch_and_store_index_prices()
                    logger.info("✅ Index Price at 9:15 AM job completed")
                except Exception as e:
                    logger.error(f"❌ Index Price at 9:15 AM job failed: {e}", exc_info=True)
            
            def run_index_price_15_30():
                if _skip_ist_non_trading_job("index price 15:30"):
                    return
                logger.info("🔧 Triggering Index Price at 3:30 PM (Market Close) job...")
                try:
                    index_price_scheduler.fetch_and_store_index_prices()
                    logger.info("✅ Index Price at 3:30 PM job completed")
                except Exception as e:
                    logger.error(f"❌ Index Price at 3:30 PM job failed: {e}", exc_info=True)
            
            self.scheduler.add_job(
                run_index_price_9_15,
                trigger=CronTrigger(hour=9, minute=15, timezone='Asia/Kolkata'),
                id='smart_future_index_price_9_15',
                name='Index Price at 9:15 AM (Market Open)',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True
            )
            
            self.scheduler.add_job(
                run_index_price_15_30,
                trigger=CronTrigger(hour=15, minute=30, timezone='Asia/Kolkata'),
                id='smart_future_index_price_15_30',
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
                if _skip_ist_non_trading_job("entry slip monitor"):
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
                id='smart_future_entry_slip_monitor',
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
                    if _skip_ist_non_trading_job(f"fin sentiment {h:02d}:{m:02d}", now):
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
                    id=f"smart_future_fin_sentiment_{hour}_{minute:02d}",
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

            # Pre-market F&O Top N watchlist — configurable IST (default 9:14), before cash open pickers
            pm_h, pm_m = _parse_hh_mm(getattr(settings, "PREMKET_RUN_TIME", "09:00"))

            def run_premarket_watchlist_scheduled():
                if not getattr(settings, "PREMKET_ENABLED", True):
                    return
                if not is_allowed_scheduler_window_ist():
                    logger.debug("Outside 08:30–21:00 IST — skip Pre-market watchlist")
                    return
                ist = pytz.timezone("Asia/Kolkata")
                if _skip_ist_non_trading_job("pre-market watchlist", datetime.now(ist)):
                    return
                logger.info("🔧 Pre-market F&O watchlist job (Top 200 → Top %s)...", getattr(settings, "PREMKET_TOP_N", 10))
                try:
                    out = run_premarket_watchlist_job()
                    logger.info(
                        "✅ Pre-market watchlist completed: top_n=%s",
                        len(out.get("top") or []),
                    )
                except Exception as e:
                    logger.error("❌ Pre-market watchlist failed: %s", e, exc_info=True)

            if getattr(settings, "PREMKET_ENABLED", True):
                self.scheduler.add_job(
                    run_premarket_watchlist_scheduled,
                    trigger=CronTrigger(day_of_week="mon-fri", hour=pm_h, minute=pm_m, timezone="Asia/Kolkata"),
                    id="smart_future_premarket_watchlist",
                    name="Pre-market F&O watchlist (config IST)",
                    replace_existing=True,
                    max_instances=1,
                    misfire_grace_time=600,
                    coalesce=True,
                )
                logger.info(
                    "✅ Scheduled: Pre-market F&O watchlist (%02d:%02d IST, Mon–Fri)",
                    pm_h,
                    pm_m,
                )
            else:
                logger.info("⏭️ Pre-market watchlist not scheduled (PREMKET_ENABLED=false)")

            # Live OI heatmap (Top ~200 stock FUT) — Upstox batch quotes every 15 min, 9:15–15:15 IST Mon–Fri
            def run_oi_heatmap_tick():
                if not getattr(settings, "UPSTOX_OI_ENABLED", True):
                    return
                ist = pytz.timezone("Asia/Kolkata")
                now = datetime.now(ist)
                if _skip_ist_non_trading_job("OI heatmap live", now):
                    return
                h, m = now.hour, now.minute
                # Cron fires at :00,:15,:30,:45 for hours 9–15; restrict to 9:15–15:15 only
                if h < 9 or (h == 9 and m < 15):
                    return
                if h > 15 or (h == 15 and m > 15):
                    return
                if h == 9 and m == 0:
                    return  # skip 9:00 — first run is 9:15
                try:
                    from backend.services.oi_heatmap import refresh_oi_heatmap_live

                    refresh_oi_heatmap_live()
                except Exception as e:
                    logger.error("❌ OI heatmap refresh failed: %s", e, exc_info=True)

            self.scheduler.add_job(
                run_oi_heatmap_tick,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour="9-15",
                    minute="0,15,30,45",
                    timezone="Asia/Kolkata",
                ),
                id="smart_future_oi_heatmap_live",
                name="OI heatmap live (Upstox, 15 min 9:15–15:15 IST)",
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,
                coalesce=True,
            )
            logger.info(
                "✅ Scheduled: OI heatmap refresh every 15 min (9:15–15:15 IST, Mon–Fri); "
                "dashboard falls back to oi_heatmap_latest when live fetch fails"
            )

            # Smart Futures picker — 9:15 (first bar after cash open), 9:30, then 10:00–15:00 every 30 min (weekdays)
            _sf_picker_slots = [(9, 15), (9, 30)]
            for _h in range(10, 15):
                for _m in (0, 30):
                    _sf_picker_slots.append((_h, _m))
            _sf_picker_slots.append((15, 0))

            def _create_sf_picker_wrapper(_hh: int, _mm: int):
                _label = f"{_hh:02d}:{_mm:02d}"

                def _run_sf_picker():
                    if not is_allowed_scheduler_window_ist():
                        logger.debug("Outside 08:30–21:00 IST — skip Smart Futures picker %s", _label)
                        return
                    ist = pytz.timezone("Asia/Kolkata")
                    if _skip_ist_non_trading_job(f"Smart Futures picker {_label}", datetime.now(ist)):
                        return
                    logger.info("🔧 Smart Futures picker (%s IST)...", _label)
                    try:
                        run_smart_futures_picker_job(scan_trigger=_label)
                        logger.info("✅ Smart Futures picker (%s) completed", _label)
                    except Exception as e:
                        logger.error("❌ Smart Futures picker (%s) failed: %s", _label, e, exc_info=True)

                return _run_sf_picker

            for _hh, _mm in _sf_picker_slots:
                self.scheduler.add_job(
                    _create_sf_picker_wrapper(_hh, _mm),
                    trigger=CronTrigger(
                        day_of_week="mon-fri",
                        hour=_hh,
                        minute=_mm,
                        timezone="Asia/Kolkata",
                    ),
                    id=f"smart_future_futures_picker_{_hh}_{_mm:02d}",
                    name=f"Smart Futures picker ({_hh:02d}:{_mm:02d} IST)",
                    replace_existing=True,
                    max_instances=1,
                    misfire_grace_time=300,
                    coalesce=True,
                )
            logger.info(
                "✅ Scheduled: Smart Futures picker (%s weekday slots 9:15/9:30 + 30 min to 15:00 IST)",
                len(_sf_picker_slots),
            )

            # 6. CAR NIFTY200 Updater - Every 3 hours (Yahoo first, Upstox fallback)
            def run_car_nifty200_update():
                if not is_allowed_scheduler_window_ist():
                    logger.debug("Outside 08:30–21:00 IST — skip CAR NIFTY200 Update tick")
                    return
                if _skip_ist_non_trading_job("CAR NIFTY200 update"):
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
                id='smart_future_car_nifty200_update',
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
                    if _skip_ist_non_trading_job("CAR NIFTY200 startup run"):
                        return
                    logger.info("🔧 Running CAR NIFTY200 Update once on startup...")
                    run_car_nifty200_update_job()
                    logger.info("✅ CAR NIFTY200 startup run completed")
                except Exception as e:
                    logger.error(f"❌ CAR NIFTY200 startup run failed: {e}", exc_info=True)

            threading.Thread(target=_car_nifty_startup_bg, name="car_nifty200_startup", daemon=True).start()
            
            total_jobs = len(self.scheduler.get_jobs())
            logger.info("=" * 60)
            logger.info(f"✅ Smart Future Algo Scheduler Controller STARTED")
            logger.info(f"   Status: Running as independent scheduled job")
            logger.info(f"   Total Jobs: {total_jobs}")
            logger.info(f"   Log File: {log_file}")
            logger.info(f"   Scheduler State: {self.scheduler.state if hasattr(self.scheduler, 'state') else 'RUNNING'}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ Failed to start Smart Future Algo Scheduler: {e}", exc_info=True)
            raise
    
    def stop(self):
        """Stop all scheduled jobs"""
        if not self.is_running:
            logger.warning("⚠️ Smart Future Algo Scheduler is not running")
            return
        
        try:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("✅ Smart Future Algo Scheduler Controller STOPPED")
        except Exception as e:
            logger.error(f"❌ Error stopping Smart Future Algo Scheduler: {e}", exc_info=True)
    
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
smart_future_algo_scheduler = SmartFutureAlgoScheduler()


def start_smart_future_algo():
    """Start the smart future algo scheduler."""
    smart_future_algo_scheduler.start()


def stop_smart_future_algo():
    """Stop the smart future algo scheduler."""
    smart_future_algo_scheduler.stop()
