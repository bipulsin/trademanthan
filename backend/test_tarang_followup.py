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


def test_delta_window_never_narrower_than_atm_pm_10():
    from backend.services.tarang.strike_window import delta_snapshot_expiries, select_strikes

    strikes = list(range(80, 121))
    keep, meta = select_strikes(
        strikes,
        F=100.0,
        atm_iv=0.45,
        years=26 / 365.25,
        delta_abs_min=0.03,
        delta_abs_max=0.97,
        sigma_mult=2.5,
        min_atm_window=10,
    )
    atm_pm = {s for s in strikes if abs(s - 100) <= 10}
    assert atm_pm.issubset(keep)
    assert len(keep) >= 21
    assert meta["window_kind"] == "delta_window"
    listed = ["2026-09-19", "2026-09-20", "2026-09-21", "2026-09-25", "2026-10-02", "2026-10-09"]
    got = delta_snapshot_expiries(listed, today=datetime(2026, 9, 19).date())
    assert got[:3] == ["2026-09-19", "2026-09-20", "2026-09-21"]
    assert "2026-09-25" in got and "2026-10-02" in got
    assert "2026-10-09" not in got


def test_validate_profiles_entry_min_exceeds_time_stop():
    from backend.services.tarang.config import validate_profiles

    assert validate_profiles() == []


def test_crudeoil_15_oct_entered_26_dte_does_not_time_stop():
    from backend.services.tarang.exit_engine import evaluate_exits

    now = datetime(2026, 9, 19, 10, 0, tzinfo=IST)
    ev = evaluate_exits(
        entry_credit_pts=1.0,
        debit_to_close_pts=1.0,
        unrealized_pnl_inr=0.0,
        max_profit_inr=1000,
        max_loss_inr=4000,
        budget_inr=4000,
        short_deltas=[0.12],
        venue="upstox_mcx",
        profile_id="CL",
        holding_mode="POSITIONAL",
        expiry="2026-10-15",
        exit_levels={"profit_take_frac": 0.40, "credit_stop_multiple": 2.0, "time_stop_dte": 5},
        now=now,
    )
    ts = next(t for t in ev.triggers if t.reason == "TIME_STOP")
    assert ts.value == 26
    assert ts.hit is False
    assert ev.reason != "TIME_STOP"


def test_fee_gate_rejects_when_fees_exceed_15pct_of_credit():
    from backend.services.tarang.fee_gate import gate_fee_and_limits
    from backend.services.tarang.gates import gate_two_sided_quotes

    legs = [
        {"side": "SELL", "right": "CE", "strike": 100, "bid": 0.10, "ask": 0.12, "mid": 0.11},
        {"side": "BUY", "right": "CE", "strike": 105, "bid": 0.04, "ask": 0.06, "mid": 0.05},
        {"side": "SELL", "right": "PE", "strike": 95, "bid": 0.10, "ask": 0.12, "mid": 0.11},
        {"side": "BUY", "right": "PE", "strike": 90, "bid": 0.04, "ask": 0.06, "mid": 0.05},
    ]
    info = gate_fee_and_limits(
        venue="upstox_mcx",
        legs=legs,
        net_credit_pts=0.12,
        units=1,
        lot_size=10,
        contract_value=None,
        underlying_price=100.0,
    )
    # Gross credit = 0.12 * 10 = ₹1.2; round-trip MCX brokerage alone dwarfs that
    assert info["gate"].passed is False
    assert info["fees_frac_of_credit"] is None or info["fees_frac_of_credit"] > 0.15
    assert info["max_contracts_per_order"] == 50
    assert info["max_contracts_per_trade"] == 50
    bad = [{"side": "SELL", "right": "CE", "strike": 100, "bid": 1.0, "ask": None, "mid": None}]
    g = gate_two_sided_quotes(bad)
    assert g.passed is False


def test_ng_23_sep_excluded_from_eligibility():
    from datetime import date

    from backend.services.tarang.expiry_eligibility import expiry_eligibility

    class FakeBuilder:
        def snapshot_expiries(self, pid):
            return {
                "NG": ["2026-09-23", "2026-10-23"],
                "CL": ["2026-10-15"],
                "BTC": ["2026-09-25"],
                "ETH": ["2026-09-25"],
            }.get(pid, [])

    out = expiry_eligibility(today=date(2026, 9, 19), builder=FakeBuilder())
    ng_sep = [r for r in out["rows"] if r.get("expiry") == "2026-09-23"]
    assert ng_sep
    assert ng_sep[0]["tradable"] is False
    assert ng_sep[0]["dte"] == 4
    assert any("below_min" in x for x in ng_sep[0]["reasons"])
    ng_oct = [r for r in out["rows"] if r.get("expiry") == "2026-10-23"]
    assert ng_oct and ng_oct[0]["tradable"] is True
    assert ng_oct[0]["dte"] == 34
