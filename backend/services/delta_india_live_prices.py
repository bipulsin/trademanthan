"""Desktop Live Crypto Prices from Delta Exchange India public tickers.

Product symbols are live USD perpetuals on ``api.india.delta.exchange``.
``MATICUSD`` is expired; the live Polygon perpetual is ``POLUSD``.
No API key is used. A failed Delta call does not reuse an older price.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.services.crypto_options_report.delta_client import fetch_public_tickers

REFRESH_SECONDS = 300
SOURCE = "delta_india"

# card: DOM id prefix on desktop.html (unchanged). product_symbol: live Delta contract.
DESKTOP_CARDS: List[Dict[str, str]] = [
    {"card": "BTCUSDT", "name": "Bitcoin", "product_symbol": "BTCUSD", "underlying": "BTC"},
    {"card": "ETHUSDT", "name": "Ethereum", "product_symbol": "ETHUSD", "underlying": "ETH"},
    {"card": "BNBUSDT", "name": "Binance Coin", "product_symbol": "BNBUSD", "underlying": "BNB"},
    {"card": "ADAUSDT", "name": "Cardano", "product_symbol": "ADAUSD", "underlying": "ADA"},
    {"card": "SOLUSDT", "name": "Solana", "product_symbol": "SOLUSD", "underlying": "SOL"},
    {"card": "DOTUSDT", "name": "Polkadot", "product_symbol": "DOTUSD", "underlying": "DOT"},
    {"card": "MATICUSDT", "name": "Polygon", "product_symbol": "POLUSD", "underlying": "POL"},
    {"card": "LINKUSDT", "name": "Chainlink", "product_symbol": "LINKUSD", "underlying": "LINK"},
]


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _unavailable_card(spec: Dict[str, str]) -> Dict[str, Any]:
    return {
        "card": spec["card"],
        "name": spec["name"],
        "product_symbol": spec["product_symbol"],
        "underlying": spec["underlying"],
        "available": False,
        "price": None,
        "change_percent": None,
    }


def _index_live_perpetuals(rows: List[Any]) -> Dict[str, Dict[str, Any]]:
    wanted = {spec["product_symbol"] for spec in DESKTOP_CARDS}
    indexed: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "")
        if symbol not in wanted:
            continue
        if str(row.get("contract_type") or "") != "perpetual_futures":
            continue
        status = row.get("product_trading_status")
        if status is not None and str(status) != "operational":
            continue
        indexed[symbol] = row
    return indexed


def build_desktop_crypto_prices() -> Dict[str, Any]:
    """One public ticker read for the desktop cards. Fail closed when Delta is down."""
    underlyings = ",".join(spec["underlying"] for spec in DESKTOP_CARDS)
    fetched = fetch_public_tickers(
        contract_types="perpetual_futures",
        underlying_asset_symbols=underlyings,
    )
    if not fetched.get("ok"):
        return {
            "ok": False,
            "source": SOURCE,
            "refresh_seconds": REFRESH_SECONDS,
            "updated_at": None,
            "error": fetched.get("error") or "unavailable",
            "prices": [_unavailable_card(spec) for spec in DESKTOP_CARDS],
        }

    by_symbol = _index_live_perpetuals(fetched.get("result") or [])
    prices: List[Dict[str, Any]] = []
    for spec in DESKTOP_CARDS:
        row = by_symbol.get(spec["product_symbol"])
        price = _as_float(row.get("close")) if row else None
        if row is None or price is None:
            prices.append(_unavailable_card(spec))
            continue
        prices.append(
            {
                "card": spec["card"],
                "name": spec["name"],
                "product_symbol": spec["product_symbol"],
                "underlying": spec["underlying"],
                "available": True,
                "price": price,
                "change_percent": _as_float(row.get("ltp_change_24h")),
            }
        )
    return {
        "ok": True,
        "source": SOURCE,
        "refresh_seconds": REFRESH_SECONDS,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
        "prices": prices,
    }
