#!/usr/bin/env python3
"""
Test run: simulate webhook sending one stock (BANKBARODA, LTP 324),
use scan algo logic to call get_option_chain, and print raw response + expiry.

Usage (from project root):
    python backend/scripts/test_option_chain_webhook.py
    python backend/scripts/test_option_chain_webhook.py --stock RELIANCE --ltp 1000
"""

import sys
import os
import json
import argparse
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Optional: reduce log noise during test
import logging
logging.getLogger("backend").setLevel(logging.WARNING)


def json_serial(obj):
    """JSON serialize datetime and other common types."""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def main():
    parser = argparse.ArgumentParser(description="Test get_option_chain as used by scan algo (webhook flow)")
    parser.add_argument("--stock", type=str, default="BANKBARODA", help="Stock symbol (default: BANKBARODA)")
    parser.add_argument("--ltp", type=float, default=324.0, help="LTP for context (default: 324)")
    args = parser.parse_args()

    stock_name = args.stock.strip().upper()
    ltp = args.ltp

    print("=" * 60)
    print("Test run: webhook stock with LTP (scan algo logic)")
    print("=" * 60)
    print(f"  Stock (webhook): {stock_name}")
    print(f"  LTP:            {ltp}")
    print()

    # Same symbol resolution as scan router
    from backend.routers.scan import _symbol_for_option_chain_api
    from backend.services.upstox_service import upstox_service

    api_symbol = _symbol_for_option_chain_api(stock_name)
    print(f"  API symbol (after _symbol_for_option_chain_api): {api_symbol}")
    print()

    # Use next month expiry (for this test run)
    use_next_month = True
    if use_next_month:
        import pytz
        ist = pytz.timezone("Asia/Kolkata")
        ref = datetime.now(ist).replace(day=19)
        expiry = upstox_service.get_monthly_expiry(reference_date=ref)
    else:
        expiry = upstox_service.get_monthly_expiry()
    expiry_str = expiry.strftime("%Y-%m-%d")
    print("  Expiry date used for option chain request (next month):", expiry_str)
    print()

    # Raw option chain with next month expiry
    print("  Calling upstox_service.get_option_chain(%r, use_next_month_expiry=True) ..." % api_symbol)
    raw_data = upstox_service.get_option_chain(api_symbol, use_next_month_expiry=True)
    print()

    print("-" * 60)
    print("Raw data returned by get_option_chain:")
    print("-" * 60)
    if raw_data is None:
        print("  None")
    else:
        try:
            # Pretty-print; handle list or dict
            if isinstance(raw_data, (list, dict)):
                out = json.dumps(raw_data, indent=2, default=json_serial)
                # Truncate if huge
                if len(out) > 8000:
                    out = out[:8000] + "\n  ... (truncated, total bytes: %d)" % len(out)
                print(out)
            else:
                print(f"  type: {type(raw_data).__name__}")
                print(f"  repr: {raw_data!r}")
        except Exception as e:
            print(f"  (could not serialize: {e})")
            print(f"  type: {type(raw_data).__name__}, repr: {raw_data!r}")
    print("-" * 60)
    print("Done.")


if __name__ == "__main__":
    main()
