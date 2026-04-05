"""Send messages to the TradeWithCTO Telegram channel (same env as auth router)."""

from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)


def send_trade_with_cto_channel_message(text: str) -> bool:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_NOTIFY_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_TRADEWITHCTO_CHAT_ID", "@TradeWithCTO")
    if not bot_token or not chat_id:
        logger.warning("Telegram TradeWithCTO: missing TELEGRAM_BOT_TOKEN or TELEGRAM_TRADEWITHCTO_CHAT_ID")
        return False
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        r = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=15)
        if r.status_code != 200:
            logger.warning("Telegram TradeWithCTO: sendMessage HTTP %s", r.status_code)
        return r.status_code == 200
    except Exception as e:
        logger.warning("Telegram TradeWithCTO: send failed: %s", e)
        return False
