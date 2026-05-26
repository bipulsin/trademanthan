"""Market data health summary for admin dashboards."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pytz

from backend.services.market_data.reads import is_market_data_fresh
from backend.services.market_data.repository import load_universe_rows

IST = pytz.timezone("Asia/Kolkata")


def get_market_data_health() -> Dict[str, Any]:
    rows = load_universe_rows()
    now = datetime.now(IST)
    stale: List[str] = []
    failed: List[str] = []
    fresh_ltp = 0

    for row in rows:
        stock = row.get("stock")
        st = row.get("market_data_refresh_status")
        if st == "failed":
            failed.append(stock)
        lu = row.get("market_data_last_updated") or row.get("currmth_future_last_updated")
        if is_market_data_fresh(lu):
            fresh_ltp += 1
        elif stock:
            stale.append(stock)

    ws_status = "unknown"
    try:
        from backend.services import upstox_market_feed as umf

        if getattr(umf, "_feed_thread", None) and getattr(umf._feed_thread, "is_alive", lambda: False)():
            ws_status = "running"
        else:
            ws_status = "stopped"
    except Exception:
        pass

    last_global = None
    for row in rows:
        lu = row.get("market_data_last_updated")
        if lu and (last_global is None or str(lu) > str(last_global)):
            last_global = lu

    return {
        "rows_total": len(rows),
        "fresh_rows": fresh_ltp,
        "stale_count": len(stale),
        "failed_count": len(failed),
        "stale_sample": stale[:20],
        "failed_sample": failed[:20],
        "last_refresh": str(last_global) if last_global else None,
        "websocket_status": ws_status,
        "checked_at_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
    }
