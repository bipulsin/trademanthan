"""Unit tests: Tarang follow-up (snapshots, calendar, fees, mini sizing)."""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from backend.services.tarang.calendar import (
    in_crypto_weekend_window,
    ist_clock,
    should_run_delta_snapshot,
)
from backend.services.tarang.chain_snapshots import pack_chain_payload, unpack_chain_payload
from backend.services.tarang.fees import delta_india_option_fee_inr, upstox_mcx_option_fee_inr
from backend.services.tarang.sizing import min_max_loss_table

IST = ZoneInfo("Asia/Kolkata")


def test_gzip_chain_payload_roundtrip():
    payload = {
        "underlying_price": 100.0,
        "quotes": [{"strike": 100, "right": "CE", "bid": 1.0, "ask": 1.2, "iv": 0.4, "delta": 0.5}],
        "ist_dow": 5,
        "ist_hour": 18,
    }
    blob = pack_chain_payload(payload)
    assert isinstance(blob, (bytes, bytearray))
    assert len(blob) < 5000
    back = unpack_chain_payload(blob)
    assert back["underlying_price"] == 100.0
    assert back["quotes"][0]["delta"] == 0.5


def test_weekend_window_and_delta_cadence():
    fri_19 = datetime(2026, 9, 18, 19, 0, tzinfo=IST)
    sat = datetime(2026, 9, 19, 12, 0, tzinfo=IST)
    sun_16 = datetime(2026, 9, 20, 16, 0, tzinfo=IST)
    sun_18 = datetime(2026, 9, 20, 18, 0, tzinfo=IST)
    mon = datetime(2026, 9, 21, 10, 0, tzinfo=IST)
    assert in_crypto_weekend_window(fri_19)
    assert in_crypto_weekend_window(sat)
    assert in_crypto_weekend_window(sun_16)
    assert not in_crypto_weekend_window(sun_18)
    assert not in_crypto_weekend_window(mon)
    # Weekend: 15-min. Weekday: :00/:30 only.
    assert should_run_delta_snapshot(datetime(2026, 9, 19, 12, 15, tzinfo=IST))
    assert not should_run_delta_snapshot(datetime(2026, 9, 21, 12, 15, tzinfo=IST))
    assert should_run_delta_snapshot(datetime(2026, 9, 21, 12, 30, tzinfo=IST))


def test_ist_clock_tags():
    clock = ist_clock(datetime(2026, 9, 19, 18, 5, tzinfo=IST))
    assert clock["ist_dow"] == 5  # Saturday
    assert clock["ist_hour"] == 18
    assert clock["ist_weekend_window"] is True


def test_upstox_mcx_fees_have_brokerage_and_gst():
    buy = upstox_mcx_option_fee_inr(premium_pts=10.0, lot_size=10, qty=1, side="BUY")
    sell = upstox_mcx_option_fee_inr(premium_pts=10.0, lot_size=10, qty=1, side="SELL")
    assert buy["brokerage"] == 20.0
    assert buy["stamp"] > 0
    assert sell["ctt"] > 0
    assert buy["gst"] > 0
    assert buy["total_inr"] > 20
    assert sell["total_inr"] > buy["total_inr"] - buy["stamp"]


def test_delta_option_fee_capped_and_gst():
    fee = delta_india_option_fee_inr(
        premium_pts=100.0,
        contract_value=0.001,
        qty=1,
        underlying_price=100000.0,
        usd_inr=83.0,
        taker=True,
    )
    assert fee["total_inr"] > 0
    assert fee["gst_usd"] > 0
    # Cap at 3.5% of premium
    assert fee["fee_usd"] <= fee["premium_usd"] * 0.035 + 1e-9


def test_min_max_loss_table_shows_mini_and_full():
    table = min_max_loss_table()
    families = {r.get("contract_family") for r in table["rows"] if r.get("currency") == "INR"}
    assert "mini" in families
    assert "full" in families
    assert table["energy_budget"]["per_trade_budget_inr"] == 5000
    assert table["energy_budget"]["hard_cap_inr"] == 10000
    mini_ng_max = [
        r
        for r in table["rows"]
        if r.get("underlying") == "NATGASMINI" and "profile max" in str(r.get("width_label"))
    ]
    if mini_ng_max:
        row = mini_ng_max[0]
        # 5-step × 5 pts × lot 250 = 6250 with fallback specs
        if row["lot_or_cv"] == 250 and row["strike_step"] == 5:
            assert row["max_loss_zero_credit"] == 6250
            assert row["fits_energy_budget"] is False
            assert row["fits_hard_cap"] is True
            assert not table["mini_cannot_fit_hard_cap"]
