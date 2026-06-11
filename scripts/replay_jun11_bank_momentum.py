#!/usr/bin/env python3
"""
Replay Jun-11-2026 bank momentum checks against live Upstox candles (production or local).

Usage (on paperclip app container):
  python3 scripts/replay_jun11_bank_momentum.py

Prints whether ICICIBANK/KOTAKBANK would pass new VM momentum paths and SF opening gates.
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime

import pytz

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

IST = pytz.timezone("Asia/Kolkata")
TRADE_DATE = date(2026, 6, 11)
BANKS = ("ICICIBANK", "KOTAKBANK")


def main() -> int:
    from backend.config import settings
    from backend.services.smart_futures_picker.indicators import (
        opening_long_sector_waiver,
        opening_momentum_regime_ok,
    )
    from backend.services.upstox_service import UpstoxService
    from backend.services.vajra.pipeline import rate_symbol_transition
    from backend.services.vajra.transition import opening_session_5m_bull_bias
    from backend.services.volume_mismatch.candles import (
        batch_fetch_candles,
        first_15m_bar_for_session,
        previous_day_close,
    )
    from backend.services.volume_mismatch.signal_rules import (
        bollinger_bands_as_of_session,
        compute_relative_volume,
        evaluate_vm_signal,
    )
    from backend.services.volume_mismatch.universe import load_volume_mismatch_universe

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    uni = {u["symbol"]: u for u in load_volume_mismatch_universe()}

    print(f"=== Replay {TRADE_DATE} bank momentum ===")
    for sym in BANKS:
        u = uni.get(sym)
        if not u:
            print(f"{sym}: not in universe")
            continue
        ik = u["instrument_key"]
        bars_15 = batch_fetch_candles(upstox, [ik], "minutes/15", days_back=35, range_end_date=TRADE_DATE).get(ik, [])
        bars_1d = batch_fetch_candles(upstox, [ik], "days/1", days_back=45, range_end_date=TRADE_DATE).get(ik, [])
        bars_5 = batch_fetch_candles(upstox, [ik], "minutes/5", days_back=5, range_end_date=TRADE_DATE).get(ik, [])
        bars_30 = batch_fetch_candles(upstox, [ik], "minutes/30", days_back=30, range_end_date=TRADE_DATE).get(ik, [])
        bars_1h = batch_fetch_candles(upstox, [ik], "hours/1", days_back=30, range_end_date=TRADE_DATE).get(ik, [])

        first = first_15m_bar_for_session(bars_15, TRADE_DATE)
        prev = previous_day_close(bars_1d, TRADE_DATE)
        bb = bollinger_bands_as_of_session(bars_1d, TRADE_DATE)
        rel = compute_relative_volume(first, bars_15, TRADE_DATE) if first else None
        vm_sig = None
        if first and prev and bb:
            vm_sig = evaluate_vm_signal(
                symbol=sym,
                future_symbol=u.get("future_symbol") or sym,
                instrument_key=ik,
                first_bar=first,
                previous_close=prev,
                bb=bb,
                relative_volume=rel,
            )

        print(f"\n--- {sym} ---")
        if vm_sig:
            print(
                f"  VM scan: LONG via {vm_sig.get('signal_path')} "
                f"gap={vm_sig.get('gap_percent')}% rel_vol={vm_sig.get('relative_volume')} score={vm_sig.get('score')}"
            )
        else:
            print(f"  VM scan: no signal (rel_vol={rel}, first_bar={'yes' if first else 'no'})")

        for label, minute in [("10:00", 10 * 60), ("11:15", 11 * 60 + 15), ("11:30", 11 * 60 + 30)]:
            ts = IST.localize(datetime(2026, 6, 11, minute // 60, minute % 60))
            bias = opening_session_5m_bull_bias(bars_5) if bars_5 else False
            print(f"  Vajra 5m bias @ {label}: {bias}")

        for at in ["10:00", "11:15", "11:30"]:
            hh, mm = map(int, at.split(":"))
            ts = IST.localize(datetime(2026, 6, 11, hh, mm))
            row = rate_symbol_transition(
                stock=sym,
                fut_sym=u.get("future_symbol") or sym,
                instrument_key=ik,
                candles_30m=bars_30,
                candles_1hr=bars_1h,
                candles_5m=bars_5,
                computed_at=ts,
                run_execution=True,
            )
            if row:
                print(
                    f"  Vajra @ {at}: {row.get('trade_type')} conf={row.get('confidence')} "
                    f"tps={row.get('tps_score')} qual={row.get('qualification_state')}"
                )
            else:
                print(f"  Vajra @ {at}: no row")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
