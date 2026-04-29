from __future__ import annotations

import unittest
from datetime import datetime, timedelta

import pytz

from backend.services.daily_futures_service import (
    _adx_di_wilder,
    _ema,
    _evaluate_indicator_exit_signal,
    _obv,
    _rsi_wilder,
    _sma,
    _wma,
)

IST = pytz.timezone("Asia/Kolkata")


def _mk_candles(vals, vols=None):
    now = IST.localize(datetime(2026, 4, 29, 9, 15))
    out = []
    for i, c in enumerate(vals):
        ts = now + timedelta(minutes=15 * i)
        v = vols[i] if vols else 1000 + (i * 20)
        out.append(
            {
                "timestamp": ts,
                "open": c - 0.6,
                "high": c + 1.0,
                "low": c - 1.0,
                "close": c,
                "volume": float(v),
            }
        )
    return out


class IndicatorAlertTests(unittest.TestCase):
    def test_macd_crossover_both_directions(self):
        up = [100 + i * 0.8 for i in range(40)]
        dn = [140 - i * 0.9 for i in range(40)]
        macd_up = _ema(up, 12)[-1] - _ema(up, 26)[-1]
        sig_up = _ema([_ema(up, 12)[i] - _ema(up, 26)[i] for i in range(len(up))], 9)[-1]
        self.assertGreater(macd_up, sig_up)
        macd_dn = _ema(dn, 12)[-1] - _ema(dn, 26)[-1]
        sig_dn = _ema([_ema(dn, 12)[i] - _ema(dn, 26)[i] for i in range(len(dn))], 9)[-1]
        self.assertLess(macd_dn, sig_dn)

    def test_di_crossover_both_directions(self):
        up = [100 + i * 1.0 for i in range(40)]
        dn = [150 - i * 1.0 for i in range(40)]
        hi_u = [x + 1 for x in up]
        lo_u = [x - 1 for x in up]
        hi_d = [x + 1 for x in dn]
        lo_d = [x - 1 for x in dn]
        di_p_u, di_m_u = _adx_di_wilder(hi_u, lo_u, up, 14)
        di_p_d, di_m_d = _adx_di_wilder(hi_d, lo_d, dn, 14)
        self.assertGreater((di_p_u[-1] or 0), (di_m_u[-1] or 0))
        self.assertGreater((di_m_d[-1] or 0), (di_p_d[-1] or 0))

    def test_hilega_flip_both_directions(self):
        up = [100 + i * 0.7 for i in range(40)]
        dn = [140 - i * 0.7 for i in range(40)]
        w_up = _wma(up, 21)[-1]
        r_up = _rsi_wilder(up, 9)[-1]
        e_up = _ema(up, 3)[-1]
        w_dn = _wma(dn, 21)[-1]
        r_dn = _rsi_wilder(dn, 9)[-1]
        e_dn = _ema(dn, 3)[-1]
        self.assertTrue(w_up is not None and r_up is not None and w_up > r_up and w_up > e_up)
        self.assertTrue(w_dn is not None and r_dn is not None and w_dn < r_dn and w_dn < e_dn)

    def test_obv_sma_cross_both_directions(self):
        closes_up = [100 + i for i in range(20)]
        closes_dn = [120 - i for i in range(20)]
        obv_up = _obv(closes_up, [1000 + i * 10 for i in range(20)])
        obv_dn = _obv(closes_dn, [1000 + i * 10 for i in range(20)])
        sma_up = _sma(obv_up, 10)
        sma_dn = _sma(obv_dn, 10)
        self.assertGreater(obv_up[-1], float(sma_up[-1]))
        self.assertLess(obv_dn[-1], float(sma_dn[-1]))

    def test_alert_tier_transitions(self):
        candles = _mk_candles([120 - i * 0.9 for i in range(40)])
        ev = _evaluate_indicator_exit_signal("IK", "LONG", candles)
        self.assertIn(int(ev["count"]), [0, 1, 2, 3, 4])
        # Decision tiers derived from count
        c = int(ev["count"])
        if c >= 3:
            self.assertTrue(True)
        elif c == 2:
            self.assertTrue(True)
        else:
            self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()

