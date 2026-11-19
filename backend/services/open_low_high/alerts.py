"""OLH Telegram/email alerts (best-effort; never raise into the screen path)."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from backend.services.open_low_high.config import OLH_ALERTS_ENABLED
from backend.services.open_low_high.ohl_log import ohl_log

logger = logging.getLogger(__name__)


def send_olh_alert(subject: str, body: str, *, meta: Optional[Dict[str, Any]] = None) -> bool:
    """Send OLH alert via TradeWithCTO Telegram when configured."""
    if not OLH_ALERTS_ENABLED:
        ohl_log("skipped", action="alert", detail=f"disabled|{subject[:80]}")
        return False
    text = f"[OLH] {subject}\n{body}".strip()
    if meta:
        extras = " ".join(f"{k}={v}" for k, v in list(meta.items())[:12])
        if extras:
            text = f"{text}\n{extras}"
    text = text[:3500]
    sent = False
    try:
        from backend.services.telegram_trade_channel import send_trade_with_cto_channel_message

        sent = bool(send_trade_with_cto_channel_message(text))
    except Exception as exc:
        logger.warning("OLH alert telegram failed: %s", exc)
        ohl_log("failed", action="alert", detail=f"telegram|{str(exc)[:120]}")
        return False
    ohl_log(
        "success" if sent else "failed",
        action="alert",
        detail=f"{'sent' if sent else 'not_sent'}|{subject[:100]}",
    )
    return sent


def alert_phase_stats(
    phase: str,
    *,
    trade_date: str,
    phase_a: int = 0,
    active: int = 0,
    wait: int = 0,
    coverage: Optional[float] = None,
    nifty_ready: Optional[bool] = None,
    status: str = "",
    extra: str = "",
) -> bool:
    cov = f"{coverage:.0%}" if coverage is not None else "n/a"
    subject = f"{phase} {status or 'update'} {trade_date}"
    body = (
        f"phase_a={phase_a} active={active} wait={wait} "
        f"coverage={cov} nifty_ready={nifty_ready}"
    )
    if extra:
        body = f"{body}\n{extra}"
    return send_olh_alert(subject, body)


def alert_degraded(reason: str, *, trade_date: str, **kwargs: Any) -> bool:
    return send_olh_alert(
        f"DEGRADED {trade_date}",
        reason,
        meta={k: kwargs[k] for k in list(kwargs)[:10]},
    )
