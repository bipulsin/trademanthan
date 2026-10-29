/**
 * Williams %R — Classic (HH - Close) / (HH - LL) * -100 over period.
 */
(function (global) {
    'use strict';

    /**
     * @param {number[]} highs
     * @param {number[]} lows
     * @param {number[]} closes
     * @param {number} period
     * @returns {(number|null)[]}
     */
    function williamsR(highs, lows, closes, period) {
        const n = closes.length;
        const out = new Array(n).fill(null);
        const p = Math.max(1, Math.floor(period));
        if (n < p || highs.length < p || lows.length < p) return out;
        for (let i = p - 1; i < n; i++) {
            let hh = -Infinity;
            let ll = Infinity;
            for (let j = i - p + 1; j <= i; j++) {
                const h = Number(highs[j]);
                const l = Number(lows[j]);
                if (h > hh) hh = h;
                if (l < ll) ll = l;
            }
            const range = hh - ll;
            if (!(range > 0)) {
                out[i] = -50;
                continue;
            }
            out[i] = ((hh - Number(closes[i])) / range) * -100;
        }
        return out;
    }

    global.ChartIndicators = global.ChartIndicators || {};
    global.ChartIndicators.williamsR = williamsR;
})(typeof window !== 'undefined' ? window : globalThis);
