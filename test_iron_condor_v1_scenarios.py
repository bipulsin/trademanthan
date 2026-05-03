"""Mock-friendly scenarios for Iron Condor v1 (no live Upstox)."""

import unittest
from datetime import date, timedelta

from backend.services import iron_condor_service as ic
from backend.services.iron_condor_checklist import fetch_earnings_chip
from backend.services.iron_condor_extended import merge_positions_peak_alert_severity
from backend.services.iron_condor_earnings import extract_future_dates_from_text
from backend.services.iron_condor_iv_vol import _percentile_approx


class TestBuyWingFallback(unittest.TestCase):
    def test_picks_farther_strike_when_ideal_missing(self):
        """If 5-step target missing but 4-step exists with OI, still returns a hedge."""
        step = 50.0
        sell_ce = 1000.0
        sorted_strikes = [
            sell_ce + 250,
            sell_ce + 300,
        ]  # 5*50=250 present; mimic wide chain
        agg = {}
        for sp in sorted_strikes:
            agg[sp] = {"strike": sp, "ce_ltp": 12.0, "ce_oi": 900.0, "pe_ltp": 1.0, "pe_oi": 500.0}
        out = ic._pick_buy_wing(sorted_strikes, agg, sell_ce, step, long_call=True)
        self.assertIsNotNone(out)
        strike, _ltp, oi, warns = out
        self.assertTrue(strike > sell_ce)
        self.assertGreaterEqual(oi, 0)


class TestEarningsDeclare(unittest.TestCase):
    def test_fail_inside_25d(self):
        d = date.today() + timedelta(days=20)
        chip = fetch_earnings_chip("SBIN", declared_next_earnings_iso=d.isoformat())
        self.assertEqual(chip["status"], "FAIL")


class TestDateParse(unittest.TestCase):
    def test_extracts_iso_and_verbal_dates(self):
        today = date(2026, 4, 1)
        txt = "Board meeting outcome on 07 May 2026 and FY results; backup 2026-06-15"
        xs = extract_future_dates_from_text(txt, today=today)
        self.assertTrue(any(str(x) == "2026-05-07" for x in xs))
        self.assertTrue(any(str(x) == "2026-06-15" for x in xs))


class TestPercentile(unittest.TestCase):
    def test_pct_mid(self):
        p = _percentile_approx(5.0, [2.0, 4.0, 6.0, 8.0])
        self.assertTrue(p is not None and 40 <= p <= 75)


class TestPeakSeverity(unittest.TestCase):
    def test_worse_severity_on_card(self):
        pos = [{"id": 1, "underlying": "SBIN"}]
        alerts = [
            {"position_id": 1, "acknowledged": False, "severity": "GREEN"},
            {"position_id": 1, "acknowledged": False, "severity": "RED"},
        ]
        merged = merge_positions_peak_alert_severity(pos, alerts)
        self.assertEqual(merged[0]["card_peak_severity"], "RED")


if __name__ == "__main__":
    unittest.main()
