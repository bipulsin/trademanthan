"""Forward ChartInk payloads received on Daily Futures URLs into the Scan (intraday) pipeline."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def chartink_payload_for_scan(inner: Any, *, direction: str) -> Dict[str, Any]:
    """Build a dict compatible with scan.process_webhook_data."""
    direction_norm = (direction or "bullish").strip().lower()
    label = "Bullish" if direction_norm == "bullish" else "Bearish"

    if isinstance(inner, str):
        stocks = inner.strip()
        return {
            "stocks": stocks,
            "trigger_prices": "",
            "triggered_at": "",
            "scan_name": f"{label} Intraday (Daily Futures bridge)",
            "alert_name": f"{label} Alert",
            "scan_url": "",
        }

    if not isinstance(inner, dict):
        return {
            "stocks": "",
            "triggered_at": "",
            "scan_name": f"{label} Intraday (Daily Futures bridge)",
        }

    stocks = inner.get("stocks") or inner.get("stock") or inner.get("symbol") or ""
    if isinstance(stocks, list):
        stocks = ",".join(str(x).strip() for x in stocks if str(x).strip())
    else:
        stocks = str(stocks).strip()

    trig = inner.get("trigger_prices") or inner.get("trigger_price") or ""
    if isinstance(trig, list):
        trig = ",".join(str(x).strip() for x in trig)

    triggered = (
        inner.get("triggered_at")
        or inner.get("trigger_time")
        or inner.get("alert_time")
        or inner.get("time")
        or ""
    )

    return {
        "stocks": stocks,
        "trigger_prices": str(trig).strip() if trig is not None else "",
        "triggered_at": str(triggered).strip() if triggered else "",
        "scan_name": inner.get("scan_name") or f"{label} Intraday",
        "scan_url": inner.get("scan_url") or "",
        "alert_name": inner.get("alert_name") or inner.get("scan_name") or f"{label} Alert",
    }


def run_scan_chartink_ingest(payload: Dict[str, Any], forced_type: str) -> None:
    """Sync ingest into intraday_stock_options (same thread pool worker as /scan/chartink-webhook-*)."""
    stocks = (payload.get("stocks") or "").strip()
    if not stocks:
        logger.warning("chartink_scan_bridge: skip empty stocks (forced_type=%s)", forced_type)
        return
    try:
        from backend.routers.scan import _run_webhook_worker

        _run_webhook_worker(payload, forced_type)
        logger.info(
            "chartink_scan_bridge: scan ingest done forced_type=%s stock_count=%s",
            forced_type,
            len(stocks.split(",")),
        )
    except Exception as exc:
        logger.error(
            "chartink_scan_bridge: scan ingest failed forced_type=%s: %s",
            forced_type,
            exc,
            exc_info=True,
        )
