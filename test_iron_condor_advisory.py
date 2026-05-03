"""Unit tests for Iron Condor hedge gate and ATR helper (no live API)."""

import unittest


class TestIronCondorMath(unittest.TestCase):
    def test_classify_hedge_ratio(self):
        from backend.services.iron_condor_service import classify_hedge_ratio

        g, _ = classify_hedge_ratio(0.30)
        self.assertEqual(g, "VALID")
        g, _ = classify_hedge_ratio(0.20)
        self.assertEqual(g, "WARN")
        g, _ = classify_hedge_ratio(0.40)
        self.assertEqual(g, "BLOCK")

    def test_monthly_atr_wilder(self):
        from backend.services import iron_condor_service as ic

        # Synthetic 20 months: flat-ish with small noise
        rows = []
        for i in range(20):
            o, h, l, c = 100.0 + i, 102.0 + i, 99.0 + i, 101.0 + i
            rows.append({"timestamp": f"2024-{i+1:02d}-01", "open": o, "high": h, "low": l, "close": c})
        atr = ic._monthly_atr_wilder_14(rows)
        self.assertIsNotNone(atr)
        self.assertGreater(atr, 0)

    def test_position_suggestion_slots(self):
        from backend.services.iron_condor_service import compute_position_suggestion

        p, err = compute_position_suggestion(trading_capital=100000, target_slots=5, new_sector="IT", open_sectors=["Banking"])
        self.assertEqual(p, 3.0)
        self.assertIsNone(err)
        p2, err2 = compute_position_suggestion(trading_capital=100000, target_slots=3, new_sector="IT", open_sectors=[])
        self.assertEqual(p2, 5.0)
        self.assertIsNone(err2)


if __name__ == "__main__":
    unittest.main()
