#!/usr/bin/env python3
"""Check option daily candles and candle size ratio for GMRAIRPORT & ONGC"""
import sys
import os
from datetime import datetime
import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.upstox_service import upstox_service

INSTRUMENTS = {
    "GMRAIRPORT": "NSE_FO|102399",  # GMRAIRPORT-Dec2025-110-CE
    "ONGC": "NSE_FO|52956",         # ONGC-Dec2025-230-PE
}


def print_candle_info(name: str, data: dict):
    if not data:
        print(f"❌ No candle data for {name}")
        return

    current = data.get("current_day_candle") or {}
    prev = data.get("previous_day_candle") or {}

    print(f"Stock: {name}")
    print("  Current Day Candle:")
    print(f"    O={current.get('open')} H={current.get('high')} L={current.get('low')} C={current.get('close')} T={current.get('time')}")
    print("  Previous Day Candle:")
    print(f"    O={prev.get('open')} H={prev.get('high')} L={prev.get('low')} C={prev.get('close')} T={prev.get('time')}")

    try:
        ch = float(current.get('high') or 0)
        cl = float(current.get('low') or 0)
        ph = float(prev.get('high') or 0)
        pl = float(prev.get('low') or 0)
        current_size = abs(ch - cl)
        prev_size = abs(ph - pl)
        if prev_size > 0:
            ratio = current_size / prev_size
            passed = ratio < 7.5
            print(f"  Sizes: current={current_size:.2f}, previous={prev_size:.2f}, ratio={ratio:.2f}x -> {'PASS' if passed else 'FAIL'}")
        else:
            print("  Previous candle size is zero or missing; cannot compute ratio")
    except Exception as e:
        print(f"  Error computing sizes: {e}")


def main():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    print("=" * 80)
    print("Option daily candles & candle size ratio for GMRAIRPORT and ONGC")
    print("Now:", now.strftime("%Y-%m-%d %H:%M:%S %Z"))
    print("=" * 80)

    for name, key in INSTRUMENTS.items():
        print("-" * 80)
        print(f"Instrument: {name} | Key: {key}")
        try:
            candles = upstox_service.get_option_daily_candles_current_and_previous(key)
        except Exception as e:
            print(f"❌ Error fetching candles: {e}")
            continue
        print_candle_info(name, candles)


if __name__ == "__main__":
    main()
