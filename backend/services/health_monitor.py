"""
Self-Healing Health Monitor for TradeManthan
Monitors critical services, detects failures, and attempts auto-recovery
Sends alerts for issues requiring manual intervention (Email + WhatsApp)
"""

import logging
import os
import smtplib
import urllib.request
import urllib.parse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

# Get logger - will inherit from root logger configured in main.py
logger = logging.getLogger(__name__)

# Ensure logger propagates to root logger (default behavior, but make it explicit)
logger.propagate = True
# Set level to ensure logs pass through
logger.setLevel(logging.INFO)

class HealthMonitor:
    """Monitors system health and sends alerts on critical failures"""
    
    def __init__(self):
        # BackgroundScheduler runs jobs synchronously in background threads
        # This ensures jobs run sequentially and don't interfere with webhook processing
        self.scheduler = BackgroundScheduler(timezone='Asia/Kolkata')
        self.is_running = False
        
        # Track consecutive failures for each component
        self.webhook_failures = 0
        self.api_token_failures = 0
        self.database_failures = 0
        
        # Alert thresholds
        self.MAX_CONSECUTIVE_FAILURES = 3
        self.alert_sent_for = set()  # Track what we've already alerted about
        
    def start(self):
        """Start the health monitor scheduler"""
        if not self.is_running:
            # Health check every 30 minutes from 8:30 AM until 4:00 PM IST (kept in sync with scan_st1_algo)
            health_check_times = [
                (8, 30), (9, 0), (9, 30), (10, 0), (10, 30), (11, 0), (11, 30), (12, 0),
                (12, 30), (13, 0), (13, 30), (14, 0), (14, 30), (15, 0), (15, 30), (16, 0),
            ]
            
            for hour, minute in health_check_times:
                self.scheduler.add_job(
                    self.perform_health_check,
                    trigger=CronTrigger(hour=hour, minute=minute, timezone='Asia/Kolkata'),
                    id=f'health_check_{hour}_{minute}',
                    name=f'Health Check {hour:02d}:{minute:02d}',
                    replace_existing=True,
                    max_instances=1,
                    misfire_grace_time=300,  # Allow running up to 5 minutes after scheduled time
                    coalesce=True  # Combine multiple missed runs into one
                )
            
            # Daily health report at 4:00 PM (after market close)
            self.scheduler.add_job(
                self.send_daily_health_report,
                trigger=CronTrigger(hour=16, minute=0, timezone='Asia/Kolkata'),
                id='daily_health_report',
                name='Daily Health Report',
                replace_existing=True,
                max_instances=1,
                misfire_grace_time=300,  # Allow running up to 5 minutes after scheduled time
                coalesce=True  # Combine multiple missed runs into one
            )
            
            try:
                self.scheduler.start()
                self.is_running = True
                logger.info("✅ Health Monitor started - Checking every 30 min from 8:30 AM to 4:00 PM IST")
                print(f"✅ Health Monitor scheduler started - Jobs: {len(self.scheduler.get_jobs())}", flush=True)
            except Exception as e:
                logger.error(f"❌ Failed to start Health Monitor scheduler: {e}", exc_info=True)
                print(f"❌ Failed to start Health Monitor scheduler: {e}", flush=True)
                raise
    
    def stop(self):
        """Stop the health monitor"""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("Health Monitor stopped")
    
    def perform_health_check(self):
        """Perform comprehensive health check"""
        try:
            logger.info("🔍 Starting health check...")
            from backend.database import SessionLocal
            from backend.models.trading import IntradayStockOption
            import pytz
            
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            today = now.date()
            
            logger.info(f"🔍 Health check time: {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
            issues = []
            
            # 1. Check database connectivity
            try:
                from sqlalchemy import text
                logger.info("🔍 Checking database connectivity...")
                db = SessionLocal()
                db.execute(text("SELECT 1"))
                db.close()
                logger.info("✅ Database: OK")
                self.database_failures = 0
            except Exception as e:
                self.database_failures += 1
                issues.append(f"❌ Database connection failed: {str(e)}")
                logger.error(f"Database health check failed: {e}")
            
            # 2. Check if webhooks are being received today
            try:
                logger.info("🔍 Checking webhook status...")
                db = SessionLocal()
                today_alerts = db.query(IntradayStockOption).filter(
                    IntradayStockOption.trade_date >= datetime.combine(today, datetime.min.time())
                ).count()
                logger.info(f"🔍 Found {today_alerts} alerts today")
                
                # Check if it's a trading day (not weekend or holiday)
                is_trading_day = False
                try:
                    from backend.services.upstox_service import upstox_service
                    is_trading_day = upstox_service.is_trading_day(now)
                except Exception as trading_day_error:
                    logger.warning(f"Could not check if trading day: {str(trading_day_error)}")
                    # Fallback: assume it's a trading day if it's a weekday
                    is_trading_day = now.weekday() < 5
                
                # Only alert if it's a trading day and market hours
                is_weekday = now.weekday() < 5  # 0=Monday, 4=Friday
                
                if is_trading_day and is_weekday and now.hour >= 11 and today_alerts == 0:
                    # After 11 AM on a trading day, we should have received some webhooks
                    self.webhook_failures += 1
                    issues.append(f"⚠️ No webhooks received today (after 11 AM on trading day)")
                    logger.warning(f"No webhooks received today after 11 AM (trading day)")
                else:
                    self.webhook_failures = 0
                    if is_trading_day and is_weekday:
                        logger.info(f"✅ Webhooks: {today_alerts} alerts today")
                    elif not is_trading_day:
                        logger.info(f"ℹ️ Market holiday - No webhooks expected ({today_alerts} alerts)")
                    else:
                        logger.info(f"ℹ️ Weekend - No webhooks expected ({today_alerts} alerts)")
                
                db.close()
            except Exception as e:
                issues.append(f"❌ Webhook check failed: {str(e)}")
                logger.error(f"Webhook health check failed: {e}")
            
            # 3. Check Upstox token status
            try:
                logger.info("🔍 Checking Upstox API status...")
                from backend.services.upstox_service import upstox_service
                
                # First, try to reload token from storage (in case it was updated via OAuth)
                try:
                    logger.info("🔍 Attempting to reload Upstox token...")
                    upstox_service.reload_token_from_storage()
                    logger.info("✅ Token reloaded successfully")
                except Exception as reload_error:
                    logger.debug(f"Token reload attempt: {reload_error}")
                
                # Try to fetch index prices (quick API call)
                logger.info("🔍 Fetching index trends from Upstox API...")
                result = upstox_service.check_index_trends()
                logger.info(f"🔍 Upstox API result: {result is not None}")
                if result and result.get('nifty_data'):
                    self.api_token_failures = 0
                    logger.info("✅ Upstox API: OK")
                else:
                    self.api_token_failures += 1
                    issues.append("⚠️ Upstox API token may be expired")
                    logger.warning(f"⚠️ Upstox API token check failed - Result: {result}")
            except Exception as e:
                self.api_token_failures += 1
                if "401" in str(e) or "token" in str(e).lower() or "unauthorized" in str(e).lower():
                    issues.append(f"❌ Upstox API token expired/invalid - Please refresh via OAuth")
                else:
                    issues.append(f"⚠️ Upstox API error: {str(e)}")
                logger.error(f"Upstox API health check failed: {e}")
            
            # 4. Check instruments file exists and is recent
            try:
                logger.info("🔍 Checking instruments file...")
                instruments_file = "/home/ubuntu/trademanthan/data/instruments/nse_instruments.json"
                if os.path.exists(instruments_file):
                    file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(instruments_file))
                    if file_age.days > 7:
                        issues.append(f"⚠️ Instruments file is {file_age.days} days old")
                    else:
                        logger.info(f"✅ Instruments file: {file_age.days} days old")
                else:
                    issues.append("❌ Instruments file missing")
                    logger.warning("⚠️ Instruments file not found at: " + instruments_file)
            except Exception as e:
                issues.append(f"⚠️ Instruments file check failed: {str(e)}")
                logger.error(f"Instruments file check error: {e}")
            
            # Send alert if critical issues detected
            if issues:
                logger.info(f"⚠️ Health check found {len(issues)} issue(s)")
                self.handle_health_issues(issues, now)
            else:
                logger.info("✅ Health check completed with no issues")
            
        except Exception as e:
            logger.error(f"❌ Health check failed: {str(e)}")
            import traceback
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
    
    def handle_health_issues(self, issues: List[str], check_time: datetime):
        """Handle detected health issues with appropriate responses"""
        
        critical_issues = [issue for issue in issues if "❌" in issue]
        warnings = [issue for issue in issues if "⚠️" in issue]
        
        # Log all issues
        logger.warning("=" * 60)
        logger.warning(f"🏥 HEALTH CHECK at {check_time.strftime('%Y-%m-%d %H:%M:%S')}")
        for issue in issues:
            logger.warning(issue)
        logger.warning("=" * 60)
        
        # Alert if we have consecutive failures
        if self.webhook_failures >= self.MAX_CONSECUTIVE_FAILURES:
            alert_key = "webhook_failures"
            if alert_key not in self.alert_sent_for:
                self.send_critical_alert(
                    "⚠️ WEBHOOK FAILURES DETECTED",
                    f"No webhooks processed for {self.webhook_failures} consecutive checks.\n" +
                    "Possible issues:\n" +
                    "- Chartink not sending webhooks\n" +
                    "- Backend processing errors\n" +
                    "- Database connection issues\n\n" +
                    f"Time: {check_time.strftime('%Y-%m-%d %H:%M:%S IST')}"
                )
                self.alert_sent_for.add(alert_key)
        
        if self.api_token_failures >= self.MAX_CONSECUTIVE_FAILURES:
            alert_key = "api_token_failures"
            if alert_key not in self.alert_sent_for:
                self.send_critical_alert(
                    "❌ UPSTOX TOKEN EXPIRED",
                    f"Upstox API token has been failing for {self.api_token_failures} consecutive checks.\n\n" +
                    "ACTION REQUIRED:\n" +
                    "1. Go to: https://trademanthan.in/scan.html\n" +
                    "2. Click 'Login with Upstox'\n" +
                    "3. Complete OAuth authorization\n\n" +
                    f"Time: {check_time.strftime('%Y-%m-%d %H:%M:%S IST')}"
                )
                self.alert_sent_for.add(alert_key)
        
        # Reset alert flags if issues resolved
        if self.webhook_failures == 0 and "webhook_failures" in self.alert_sent_for:
            self.alert_sent_for.remove("webhook_failures")
        if self.api_token_failures == 0 and "api_token_failures" in self.alert_sent_for:
            self.alert_sent_for.remove("api_token_failures")
    
    def send_daily_report_email(self, subject: str, body: str) -> bool:
        """
        Send routine daily report via SMTP only (no WhatsApp/Telegram).
        Subject is used as-is (e.g. 'TradeManthan Daily Report - Mar 27, 2026').
        """
        try:
            email_to = os.getenv("ALERT_EMAIL")
            email_from = os.getenv("SMTP_FROM_EMAIL", "alerts@trademanthan.in")
            smtp_server = os.getenv("SMTP_SERVER", "localhost")
            smtp_port = int(os.getenv("SMTP_PORT", "25"))
            smtp_user = os.getenv("SMTP_USER", "")
            smtp_password = os.getenv("SMTP_PASSWORD", "")

            if not email_to:
                logger.warning("Daily report email skipped: ALERT_EMAIL not set")
                return False

            msg = MIMEMultipart()
            msg['From'] = email_from
            msg['To'] = email_to
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            if smtp_user:
                server = smtplib.SMTP(smtp_server, smtp_port)
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
                server.quit()
            else:
                server = smtplib.SMTP(smtp_server, smtp_port)
                server.send_message(msg)
                server.quit()
            logger.info(f"✅ Daily report email sent to {email_to}")
            return True
        except Exception as e:
            logger.warning(f"Could not send daily report email: {str(e)}", exc_info=True)
            return False

    def send_daily_health_report(self):
        """Send daily health report at 4 PM after market close"""
        try:
            from backend.database import SessionLocal
            from backend.models.trading import IntradayStockOption
            import pytz
            
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            today = now.date()
            day_start = datetime.combine(today, datetime.min.time())
            day_end = day_start + timedelta(days=1)
            
            db = SessionLocal()
            
            # Get today's stats (same calendar day as trade_date)
            total_alerts = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= day_start,
                IntradayStockOption.trade_date < day_end,
            ).count()
            
            bullish_count = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= day_start,
                IntradayStockOption.trade_date < day_end,
                IntradayStockOption.alert_type == 'Bullish'
            ).count()
            
            bearish_count = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= day_start,
                IntradayStockOption.trade_date < day_end,
                IntradayStockOption.alert_type == 'Bearish'
            ).count()
            
            trades_entered = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= day_start,
                IntradayStockOption.trade_date < day_end,
                IntradayStockOption.status == 'bought'
            ).count()
            
            no_entry = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= day_start,
                IntradayStockOption.trade_date < day_end,
                IntradayStockOption.status == 'no_entry'
            ).count()
            
            db.close()
            
            report = f"""
📊 DAILY HEALTH REPORT - {today.strftime('%B %d, %Y')}
{'=' * 60}

WEBHOOK ALERTS:
• Total Alerts: {total_alerts}
• Bullish: {bullish_count}
• Bearish: {bearish_count}

TRADE EXECUTION:
• Trades Entered: {trades_entered}
• No Entry (Opposite Trends): {no_entry}

SYSTEM HEALTH:
• Database: {'✅ OK' if self.database_failures == 0 else f'❌ {self.database_failures} failures'}
• Upstox API: {'✅ OK' if self.api_token_failures == 0 else f'❌ {self.api_token_failures} failures'}
• Webhooks: {'✅ OK' if self.webhook_failures == 0 else f'⚠️ {self.webhook_failures} checks without data'}

{'=' * 60}
Generated: {now.strftime('%Y-%m-%d %H:%M:%S IST')}
"""
            
            logger.info(report)
            
            # Send email on weekdays when ALERT_EMAIL is set (even if zero alerts — routine heartbeat)
            is_weekday = now.weekday() < 5  # Mon-Fri
            
            if os.getenv("ALERT_EMAIL") and is_weekday:
                self.send_daily_report_email(
                    f"TradeManthan Daily Report - {today.strftime('%b %d, %Y')}",
                    report.strip(),
                )
            
            # Only send critical alert if NO webhooks AND consecutive failures
            # This helps avoid false alarms on single-day holidays
            if total_alerts == 0 and is_weekday and self.webhook_failures >= 2:
                self.send_critical_alert(
                    "⚠️ NO WEBHOOKS - Multiple Days",
                    f"No webhook alerts received on {today.strftime('%B %d, %Y')} (weekday).\n\n" +
                    f"This is the {self.webhook_failures}th consecutive check without data.\n\n" +
                    "This may indicate:\n" +
                    "- Multi-day market closure\n" +
                    "- Chartink not sending webhooks\n" +
                    "- Backend processing failures\n\n" +
                    "Please investigate."
                )
            
        except Exception as e:
            logger.error(f"Daily health report failed: {str(e)}", exc_info=True)
    
    def send_whatsapp_message(self, message: str) -> bool:
        """Send WhatsApp message via CallMeBot API"""
        try:
            whatsapp_phone = os.getenv("WHATSAPP_PHONE")  # Format: +919876543210
            whatsapp_apikey = os.getenv("WHATSAPP_APIKEY")  # From CallMeBot registration
            
            if not whatsapp_phone or not whatsapp_apikey:
                logger.debug("WhatsApp not configured, skipping")
                return False
            
            # Truncate message to 1000 chars (CallMeBot limit)
            if len(message) > 1000:
                message = message[:997] + "..."
            
            # Format phone number (remove + and spaces)
            phone = whatsapp_phone.replace("+", "").replace(" ", "").replace("-", "")
            
            # Build API URL
            url = f"https://api.callmebot.com/whatsapp.php?phone={phone}&text={urllib.parse.quote(message)}&apikey={whatsapp_apikey}"
            
            # Send request
            response = urllib.request.urlopen(url, timeout=10)
            
            if response.status == 200:
                logger.info(f"✅ WhatsApp alert sent to {whatsapp_phone}")
                return True
            else:
                logger.warning(f"WhatsApp API returned status {response.status}")
                return False
                
        except Exception as e:
            logger.warning(f"Could not send WhatsApp alert: {str(e)}")
            return False
    
    def send_telegram_message(self, message: str) -> bool:
        """Send Telegram message via CallMeBot API"""
        try:
            telegram_username = os.getenv("TELEGRAM_USERNAME")  # Format: bipulsahay (without @)
            
            if not telegram_username:
                logger.debug("Telegram not configured, skipping")
                return False
            
            # Truncate message to 1000 chars (CallMeBot limit)
            if len(message) > 1000:
                message = message[:997] + "..."
            
            # Build API URL - CallMeBot Telegram API (no API key needed)
            # Format: https://api.callmebot.com/text.php?user=@bipulsahay&text=<message>
            url = f"https://api.callmebot.com/text.php?user=@{telegram_username}&text={urllib.parse.quote(message)}"
            
            # Send request
            response = urllib.request.urlopen(url, timeout=10)
            
            if response.status == 200:
                logger.info(f"✅ Telegram alert sent to @{telegram_username}")
                return True
            else:
                logger.warning(f"Telegram CallMeBot API returned status {response.status}")
                return False
                
        except Exception as e:
            logger.warning(f"Could not send Telegram alert: {str(e)}")
            return False
    
    def send_critical_alert(self, subject: str, message: str):
        """Send critical alert via email + WhatsApp + Telegram + logging"""
        
        # Always log to console/journald
        logger.critical("=" * 60)
        logger.critical(f"🚨 CRITICAL ALERT: {subject}")
        logger.critical(message)
        logger.critical("=" * 60)
        
        # Try to send email if configured
        email_sent = False
        try:
            email_to = os.getenv("ALERT_EMAIL")
            email_from = os.getenv("SMTP_FROM_EMAIL", "alerts@trademanthan.in")
            smtp_server = os.getenv("SMTP_SERVER", "localhost")
            smtp_port = int(os.getenv("SMTP_PORT", "25"))
            smtp_user = os.getenv("SMTP_USER", "")
            smtp_password = os.getenv("SMTP_PASSWORD", "")
            
            if email_to:
                msg = MIMEMultipart()
                msg['From'] = email_from
                msg['To'] = email_to
                msg['Subject'] = f"🚨 TradeManthan Alert: {subject}"
                
                body = f"""
TradeManthan System Alert
{'=' * 60}

{message}

{'=' * 60}
System: TradeManthan Scan Service
Server: https://trademanthan.in
Time: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S IST')}

This is an automated alert. Please check the system immediately.
"""
                msg.attach(MIMEText(body, 'plain'))
                
                # Send email
                if smtp_user:
                    server = smtplib.SMTP(smtp_server, smtp_port)
                    server.starttls()
                    server.login(smtp_user, smtp_password)
                    server.send_message(msg)
                    server.quit()
                    logger.info(f"✅ Alert email sent to {email_to}")
                    email_sent = True
                else:
                    # Try without authentication for local SMTP
                    server = smtplib.SMTP(smtp_server, smtp_port)
                    server.send_message(msg)
                    server.quit()
                    logger.info(f"✅ Alert email sent to {email_to}")
                    email_sent = True
                    
        except Exception as e:
            logger.warning(f"Could not send email alert: {str(e)}")
        
        # Try to send WhatsApp alert
        whatsapp_message = f"🚨 *TradeManthan Alert*\n\n*{subject}*\n\n{message}\n\n_Time: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M IST')}_"
        whatsapp_sent = self.send_whatsapp_message(whatsapp_message)
        
        # Try to send Telegram alert
        # Format message with markdown for Telegram
        telegram_message = f"🚨 *TradeManthan Alert*\n\n*{subject}*\n\n{message}\n\n_Time: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M IST')}_"
        telegram_sent = self.send_telegram_message(telegram_message)
        
        # Log notification status
        sent_channels = []
        if email_sent:
            sent_channels.append("Email")
        if whatsapp_sent:
            sent_channels.append("WhatsApp")
        if telegram_sent:
            sent_channels.append("Telegram")
        
        if sent_channels:
            logger.info(f"✅ Alerts sent via: {', '.join(sent_channels)}")
        else:
            logger.warning("⚠️ Alert sent via logs only (Email, WhatsApp, and Telegram failed/not configured)")
    
    def record_webhook_success(self):
        """Record successful webhook processing"""
        self.webhook_failures = 0
    
    def record_webhook_failure(self):
        """Record webhook processing failure"""
        self.webhook_failures += 1
        logger.warning(f"Webhook failure recorded ({self.webhook_failures} consecutive)")
    
    def record_token_success(self):
        """Record successful API token usage"""
        self.api_token_failures = 0
    
    def record_token_failure(self):
        """Record API token failure"""
        self.api_token_failures += 1
        logger.warning(f"API token failure recorded ({self.api_token_failures} consecutive)")


# Global health monitor instance
health_monitor = HealthMonitor()


def start_health_monitor():
    """Start the health monitor"""
    health_monitor.start()


def stop_health_monitor():
    """Stop the health monitor"""
    health_monitor.stop()


# Webhook retry queue for failed webhooks
class WebhookRetryQueue:
    """Stores failed webhooks for retry attempts"""
    
    def __init__(self):
        self.queue = []
        self.max_retries = 3
    
    def add(self, webhook_data: Dict, attempt: int = 1):
        """Add failed webhook to retry queue"""
        self.queue.append({
            "data": webhook_data,
            "attempt": attempt,
            "failed_at": datetime.now(pytz.timezone('Asia/Kolkata')),
            "error": None
        })
        logger.info(f"Added webhook to retry queue (attempt {attempt}/{self.max_retries})")
    
    def retry_all(self, process_function):
        """Retry all failed webhooks"""
        if not self.queue:
            return
        
        logger.info(f"🔄 Retrying {len(self.queue)} failed webhooks...")
        
        retry_queue = self.queue.copy()
        self.queue = []
        
        for item in retry_queue:
            if item['attempt'] >= self.max_retries:
                logger.error(f"❌ Webhook exceeded max retries, discarding: {item['data'].get('scan_name', 'Unknown')}")
                continue
            
            try:
                # Handle both sync and async process functions
                import asyncio
                if asyncio.iscoroutinefunction(process_function):
                    # If async, run in event loop
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(process_function(item['data']))
                else:
                    # If sync, call directly
                    process_function(item['data'])
                logger.info(f"✅ Webhook retry successful on attempt {item['attempt']}")
            except Exception as e:
                logger.error(f"Webhook retry failed (attempt {item['attempt']}): {str(e)}")
                # Re-add to queue with incremented attempt count
                self.add(item['data'], item['attempt'] + 1)


# Global retry queue
webhook_retry_queue = WebhookRetryQueue()

