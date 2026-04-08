"""Unit tests: Renko builder, exit rule, scoring (trend vs chop simulation)."""
import unittest

from backend.services.smart_futures.renko_engine import (
    RenkoBrick,
    build_traditional_renko,
    exit_two_opposite_bricks,
    renko_structure_filter_long,
)
from backend.services.smart_futures.scanner import compute_score


class TestSmartFuturesRenko(unittest.TestCase):
    def test_trend_day_long_stack_green(self):
        brick = 10.0
        closes = [100 + i * 3 for i in range(40)]
        bricks = build_traditional_renko(closes, brick)
        self.assertGreaterEqual(len(bricks), 5)
        ok, _ = renko_structure_filter_long(bricks)
        self.assertTrue(ok)

    def test_chop_day_has_many_bricks(self):
        brick = 5.0
        closes = [100.0]
        x = 100.0
        for i in range(50):
            x += 6 if i % 2 == 0 else -6
            closes.append(x)
        bricks = build_traditional_renko(closes, brick)
        self.assertGreater(len(bricks), 4)

    def test_exit_long_two_reds(self):
        bricks = [
            RenkoBrick("GREEN", 0, 1),
            RenkoBrick("GREEN", 1, 2),
            RenkoBrick("RED", 2, 1),
            RenkoBrick("RED", 1, 0),
        ]
        self.assertTrue(exit_two_opposite_bricks(bricks, "LONG"))

    def test_score_range(self):
        bricks = [RenkoBrick("GREEN", float(i), float(i + 1)) for i in range(10)]
        s = compute_score("LONG", bricks, vol_spike=True, momentum_ok=True)
        self.assertGreaterEqual(s, 0)
        self.assertLessEqual(s, 6)


if __name__ == "__main__":
    unittest.main()
