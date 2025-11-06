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
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

class HealthMonitor:
    """Monitors system health and sends alerts on critical failures"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler(timezone='Asia/Kolkata')
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
            # Health check every 15 minutes during market hours (9 AM - 4 PM)
            for hour in range(9, 16):
                for minute in [0, 15, 30, 45]:
                    self.scheduler.add_job(
                        self.perform_health_check,
                        trigger=CronTrigger(hour=hour, minute=minute, timezone='Asia/Kolkata'),
                        id=f'health_check_{hour}_{minute}',
                        name=f'Health Check {hour:02d}:{minute:02d}',
                        replace_existing=True
                    )
            
            # Daily health report at 4:00 PM (after market close)
            self.scheduler.add_job(
                self.send_daily_health_report,
                trigger=CronTrigger(hour=16, minute=0, timezone='Asia/Kolkata'),
                id='daily_health_report',
                name='Daily Health Report',
                replace_existing=True
            )
            
            self.scheduler.start()
            self.is_running = True
            logger.info("✅ Health Monitor started - Checking every 15 min during market hours")
    
    def stop(self):
        """Stop the health monitor"""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("Health Monitor stopped")
    
    async def perform_health_check(self):
        """Perform comprehensive health check"""
        try:
            from database import SessionLocal
            from models.trading import IntradayStockOption
            import pytz
            
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            today = now.date()
            
            issues = []
            
            # 1. Check database connectivity
            try:
                from sqlalchemy import text
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
                db = SessionLocal()
                today_alerts = db.query(IntradayStockOption).filter(
                    IntradayStockOption.trade_date >= datetime.combine(today, datetime.min.time())
                ).count()
                
                # Only alert if it's a weekday (Mon-Fri) and market hours
                is_weekday = now.weekday() < 5  # 0=Monday, 4=Friday
                
                if is_weekday and now.hour >= 11 and today_alerts == 0:
                    # After 11 AM on weekday, we should have received some webhooks
                    # (Unless it's a trading holiday - but we can't detect all holidays)
                    self.webhook_failures += 1
                    issues.append(f"⚠️ No webhooks received today (after 11 AM on weekday)")
                    logger.warning(f"No webhooks received today after 11 AM (weekday)")
                else:
                    self.webhook_failures = 0
                    if is_weekday:
                        logger.info(f"✅ Webhooks: {today_alerts} alerts today")
                    else:
                        logger.info(f"ℹ️ Weekend - No webhooks expected ({today_alerts} alerts)")
                
                db.close()
            except Exception as e:
                issues.append(f"❌ Webhook check failed: {str(e)}")
                logger.error(f"Webhook health check failed: {e}")
            
            # 3. Check Upstox token status
            try:
                from services.upstox_service import upstox_service
                
                # First, try to reload token from storage (in case it was updated via OAuth)
                try:
                    upstox_service.reload_token_from_storage()
                except Exception as reload_error:
                    logger.debug(f"Token reload attempt: {reload_error}")
                
                # Try to fetch index prices (quick API call)
                result = upstox_service.check_index_trends()
                if result and result.get('nifty'):
                    self.api_token_failures = 0
                    logger.info("✅ Upstox API: OK")
                else:
                    self.api_token_failures += 1
                    issues.append("⚠️ Upstox API token may be expired")
                    logger.warning("Upstox API token check failed")
            except Exception as e:
                self.api_token_failures += 1
                if "401" in str(e) or "token" in str(e).lower() or "unauthorized" in str(e).lower():
                    issues.append(f"❌ Upstox API token expired/invalid - Please refresh via OAuth")
                else:
                    issues.append(f"⚠️ Upstox API error: {str(e)}")
                logger.error(f"Upstox API health check failed: {e}")
            
            # 4. Check instruments file exists and is recent
            try:
                instruments_file = "/home/ubuntu/trademanthan/data/instruments/nse_instruments.json"
                if os.path.exists(instruments_file):
                    file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(instruments_file))
                    if file_age.days > 7:
                        issues.append(f"⚠️ Instruments file is {file_age.days} days old")
                    else:
                        logger.info(f"✅ Instruments file: {file_age.days} days old")
                else:
                    issues.append("❌ Instruments file missing")
            except Exception as e:
                issues.append(f"⚠️ Instruments file check failed: {str(e)}")
            
            # Send alert if critical issues detected
            if issues:
                await self.handle_health_issues(issues, now)
            
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
    
    async def handle_health_issues(self, issues: List[str], check_time: datetime):
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
                await self.send_critical_alert(
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
                await self.send_critical_alert(
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
    
    async def send_daily_health_report(self):
        """Send daily health report at 4 PM after market close"""
        try:
            from database import SessionLocal
            from models.trading import IntradayStockOption
            import pytz
            
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            today = now.date()
            
            db = SessionLocal()
            
            # Get today's stats
            total_alerts = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= datetime.combine(today, datetime.min.time())
            ).count()
            
            bullish_count = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= datetime.combine(today, datetime.min.time()),
                IntradayStockOption.alert_type == 'Bullish'
            ).count()
            
            bearish_count = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= datetime.combine(today, datetime.min.time()),
                IntradayStockOption.alert_type == 'Bearish'
            ).count()
            
            trades_entered = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= datetime.combine(today, datetime.min.time()),
                IntradayStockOption.status == 'bought'
            ).count()
            
            no_entry = db.query(IntradayStockOption).filter(
                IntradayStockOption.trade_date >= datetime.combine(today, datetime.min.time()),
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
            
            # Send email if configured (only on weekdays with no alerts)
            # Skip alert if it's likely a holiday or weekend
            is_weekday = now.weekday() < 5  # Mon-Fri
            
            # Send daily report summary (non-critical) on weekdays
            alert_email = os.getenv("ALERT_EMAIL")
            if alert_email and is_weekday and total_alerts > 0:
                # Normal trading day - send summary
                await self.send_critical_alert(
                    f"📊 TradeManthan Daily Report - {today.strftime('%b %d, %Y')}",
                    report
                )
            
            # Only send critical alert if NO webhooks AND consecutive failures
            # This helps avoid false alarms on single-day holidays
            if total_alerts == 0 and is_weekday and self.webhook_failures >= 2:
                await self.send_critical_alert(
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
            logger.error(f"Daily health report failed: {str(e)}")
    
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
    
    async def send_critical_alert(self, subject: str, message: str):
        """Send critical alert via email + WhatsApp + logging"""
        
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
        
        # Log notification status
        if email_sent and whatsapp_sent:
            logger.info("✅ Alerts sent via Email + WhatsApp")
        elif email_sent:
            logger.info("✅ Alert sent via Email (WhatsApp not configured/failed)")
        elif whatsapp_sent:
            logger.info("✅ Alert sent via WhatsApp (Email failed)")
        else:
            logger.warning("⚠️ Alert sent via logs only (Email and WhatsApp failed)")
    
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
    
    async def retry_all(self, process_function):
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
                await process_function(item['data'])
                logger.info(f"✅ Webhook retry successful on attempt {item['attempt']}")
            except Exception as e:
                logger.error(f"Webhook retry failed (attempt {item['attempt']}): {str(e)}")
                # Re-add to queue with incremented attempt count
                self.add(item['data'], item['attempt'] + 1)


# Global retry queue
webhook_retry_queue = WebhookRetryQueue()

