"""
Async email of ChartInk ranking CSV — does not block webhook / order processing.
Uses same SMTP env vars as other alerts (SMTP_SERVER, SMTP_USER, SMTP_PASSWORD, etc.).
"""

from __future__ import annotations

import logging
import os
import smtplib
import threading
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

import pytz

logger = logging.getLogger(__name__)

# Used only if CHARTINK_RANKING_EMAIL and ALERT_EMAIL are both unset
DEFAULT_RANKING_EMAIL_TO = "tradentical@gmail.com"
IST = pytz.timezone("Asia/Kolkata")


def _ranking_email_to() -> str:
    """Prefer explicit CHARTINK_RANKING_EMAIL, else same inbox as daily alerts (ALERT_EMAIL), else legacy default."""
    explicit = (os.getenv("CHARTINK_RANKING_EMAIL") or "").strip()
    if explicit:
        return explicit
    alert = (os.getenv("ALERT_EMAIL") or "").strip()
    if alert:
        return alert
    return DEFAULT_RANKING_EMAIL_TO


def _send_chartink_ranking_email_sync(csv_path: Path) -> bool:
    """
    Send CSV as attachment. Subject/body use system time at send (IST).
    Returns True if SMTP send succeeded.
    """
    csv_path = Path(csv_path)
    if not csv_path.is_file():
        logger.warning("ChartInk ranking email: missing file %s", csv_path)
        return False

    email_to = _ranking_email_to()
    email_from = os.getenv("SMTP_FROM_EMAIL", "alerts@trademanthan.in")
    smtp_server = os.getenv("SMTP_SERVER", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")

    now_ist = datetime.now(IST)
    ts = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")
    subject = f"ChartInk webhook stock score for {ts}"

    msg = MIMEMultipart()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject

    body = f"""ChartInk webhook — stock ranking scores (CSV attached).

Email sent at (system / IST): {ts}
File: {csv_path.name}

This is an automated message from TradeManthan scan processing.
"""
    msg.attach(MIMEText(body, "plain", "utf-8"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(csv_path.read_bytes())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{csv_path.name}"')
    msg.attach(part)

    try:
        server = smtplib.SMTP(smtp_server, smtp_port, timeout=45)
        try:
            if smtp_user:
                server.starttls()
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
        finally:
            server.quit()
        logger.info("ChartInk ranking CSV emailed to %s (subject: %s)", email_to, subject)
        return True
    except Exception as e:
        logger.warning("ChartInk ranking email failed: %s", e, exc_info=True)
        return False


def schedule_chartink_ranking_email(csv_path: Optional[Path]) -> None:
    """
    Fire-and-forget: send email in a daemon thread so webhook/order path is not blocked.
    """
    if not csv_path:
        return
    p = Path(csv_path)
    if not p.is_file():
        return

    def _run():
        try:
            _send_chartink_ranking_email_sync(p)
        except Exception as e:
            logger.warning("ChartInk ranking email thread error: %s", e, exc_info=True)

    t = threading.Thread(target=_run, name="chartink-ranking-email", daemon=True)
    t.start()
