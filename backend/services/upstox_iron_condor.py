"""
Upstox integration helpers for Iron Condor / short-vol workflows.

Uses the workspace :class:`backend.services.upstox_service.UpstoxService` with
credentials from ``backend.config.settings`` (``UPSTOX_*`` env vars); the access
token is resolved the same way as elsewhere (token manager inside the service).

Suggested poll cadence (enforce in your scheduler, not here):

- Equity spot LTP / per-option LTP: about every 5 minutes during the session.
- India VIX: about every 15 minutes.

Python path: ``backend/services/upstox_iron_condor.py`` (there is no separate
TypeScript SDK in this repo).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time as time_of_day
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import pytz

from backend.services.iron_condor_service import option_chain_underlying

if TYPE_CHECKING:
    from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

_IST = pytz.timezone("Asia/Kolkata")
_svc: Optional["UpstoxService"] = None


def get_iron_condor_upstox() -> "UpstoxService":
    """Lazy singleton built from ``settings`` (same pattern as other jobs)."""
    global _svc
    if _svc is None:
        from backend.config import settings
        from backend.services.upstox_service import UpstoxService

        _svc = UpstoxService(
            api_key=settings.UPSTOX_API_KEY,
            api_secret=settings.UPSTOX_API_SECRET,
            access_token=None,
        )
    return _svc


@dataclass(frozen=True)
class OptionLegRef:
    underlying: str
    expiry: date
    strike: float
    option_type: str  # CE or PE


def spot_equity_quote(
    symbol: str,
    *,
    svc: Optional["UpstoxService"] = None,
) -> Optional[Dict[str, Any]]:
    """NSE equity market quote (LTP, day OHLC) for underlying spot."""
    ux = svc or get_iron_condor_upstox()
    sym = option_chain_underlying(symbol.strip().upper())
    return ux.get_market_quote(sym)


def spot_equity_ltp(
    symbol: str, *, svc: Optional["UpstoxService"] = None
) -> Optional[float]:
    q = spot_equity_quote(symbol, svc=svc)
    if not q:
        return None
    try:
        lp = float(q.get("last_price") or 0)
        return lp if lp > 0 else None
    except (TypeError, ValueError):
        return None


def option_chain_for_expiry(
    symbol: str,
    expiry: Union[date, datetime],
    *,
    svc: Optional["UpstoxService"] = None,
) -> Optional[Dict[str, Any]]:
    """Option chain for API-mapped underlying and explicit expiry."""
    ux = svc or get_iron_condor_upstox()
    sym = option_chain_underlying(symbol.strip().upper())
    return ux.get_option_chain(sym, expiry_date=expiry)


def _strike_row_ltp_oi(strike_data: Dict[str, Any], ce: bool) -> Tuple[float, float]:
    key = "call_options" if ce else "put_options"
    node = strike_data.get(key)
    option_data = None
    if isinstance(node, dict):
        option_data = node.get("market_data", node)
    elif isinstance(node, list) and node and isinstance(node[0], dict):
        option_data = node[0]
    if not option_data or not isinstance(option_data, dict):
        return 0.0, 0.0
    ltp = float(option_data.get("ltp") or option_data.get("last_price") or 0.0)
    oi = float(option_data.get("oi") or option_data.get("open_interest") or 0.0)
    return ltp, oi


def _extract_chain_strikes(chain: Any) -> Optional[List[Dict[str, Any]]]:
    strike_list = None
    if isinstance(chain, dict):
        if isinstance(chain.get("strikes"), list):
            strike_list = chain["strikes"]
        elif isinstance(chain.get("data"), dict) and isinstance(
            chain["data"].get("strikes"), list
        ):
            strike_list = chain["data"]["strikes"]
    elif isinstance(chain, list):
        strike_list = chain
    return strike_list


def strike_oi_grid_from_chain(chain_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Per-strike CE/PE LTP and open interest from one chain snapshot (hedge selection).
    """
    strike_list = _extract_chain_strikes(chain_payload)
    if not strike_list:
        return []
    rows: List[Dict[str, Any]] = []
    for sd in strike_list:
        if not isinstance(sd, dict):
            continue
        sp_raw = sd.get("strike_price")
        if sp_raw is None:
            continue
        sp = float(sp_raw)
        cel, ceoi = _strike_row_ltp_oi(sd, True)
        pel, peoi = _strike_row_ltp_oi(sd, False)
        rows.append(
            {
                "strike": sp,
                "ce_ltp": cel,
                "ce_oi": ceoi,
                "pe_ltp": pel,
                "pe_oi": peoi,
            }
        )
    rows.sort(key=lambda r: r["strike"])
    return rows


def batch_option_ltps(
    legs: List[OptionLegRef],
    *,
    svc: Optional[UpstoxService] = None,
) -> Dict[str, float]:
    """
    Batch last prices for many legs (e.g. four legs × several positions).

    Returns requested ``instrument_key`` -> positive ``last_price`` only for keys
    returned by Upstox.
    """
    ux = svc or get_iron_condor_upstox()
    keys: List[str] = []
    for leg in legs:
        u = option_chain_underlying(leg.underlying.strip().upper())
        exp_dt = _IST.localize(datetime.combine(leg.expiry, time_of_day(0, 0)))
        ik = ux.get_option_instrument_key(
            u, exp_dt, float(leg.strike), leg.option_type.upper()
        )
        if ik:
            keys.append(ik)
    return ux.get_market_quotes_batch_by_keys(keys)


def monthly_ohlc_for_atr(
    symbol: str,
    *,
    months_back: int = 18,
    svc: Optional["UpstoxService"] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Monthly OHLC candles on the cash underlying for monthly ATR(14).

    Default window is ~18 months of calendar span so ≥15 monthly bars survive
    after API alignment.
    """
    ux = svc or get_iron_condor_upstox()
    sym = option_chain_underlying(symbol.strip().upper())
    ik = ux.get_instrument_key(sym)
    if not ik:
        logger.warning("monthly_ohlc_for_atr: no instrument key for %s", sym)
        return None
    return ux.get_monthly_candles_by_instrument_key(ik, months_back=months_back)


def india_vix_quote(
    *, svc: Optional["UpstoxService"] = None
) -> Optional[Dict[str, Any]]:
    ux = svc or get_iron_condor_upstox()
    return ux.get_market_quote_by_key(ux.INDIA_VIX_KEY)


def india_vix_last_price(*, svc: Optional["UpstoxService"] = None) -> Optional[float]:
    q = india_vix_quote(svc=svc)
    if not q:
        return None
    try:
        lp = float(q.get("last_price") or 0)
        return lp if lp > 0 else None
    except (TypeError, ValueError):
        return None


def equity_daily_ohlc_last_trading_sessions(
    symbol: str,
    *,
    sessions: int = 5,
    svc: Optional["UpstoxService"] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Last ``sessions`` daily equity bars, oldest first (gap / recent-range filters).

    Pulls extra calendar history so five trading days are likely present across
    weekends and short holidays.
    """
    ux = svc or get_iron_condor_upstox()
    sym = option_chain_underlying(symbol.strip().upper())
    days_fetch = max(21, sessions * 5)
    raw = ux.get_historical_candles(sym, interval="days/1", days_back=days_fetch)
    if not raw:
        return None
    bars = sorted(raw, key=lambda x: str(x.get("timestamp") or ""))
    return bars[-sessions:] if len(bars) >= sessions else bars
