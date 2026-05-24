/**
 * Wilder RSI — reusable for chart indicators.
 */
(function (global) {
    'use strict';

    /**
     * @param {number[]} closes
     * @param {number} period
     * @returns {(number|null)[]}
     */
    function rsiWilder(closes, period) {
        const n = closes.length;
        const out = new Array(n).fill(null);
        const p = Math.max(1, Math.floor(period));
        if (n < p + 1) return out;

        const gains = [0];
        const losses = [0];
        for (let i = 1; i < n; i++) {
            const d = closes[i] - closes[i - 1];
            gains.push(d > 0 ? d : 0);
            losses.push(d < 0 ? -d : 0);
        }

        let avgG = 0;
        let avgL = 0;
        for (let i = 1; i <= p; i++) {
            avgG += gains[i];
            avgL += losses[i];
        }
        avgG /= p;
        avgL /= p;
        out[p] = avgL === 0 ? 100 : 100 - 100 / (1 + avgG / avgL);

        for (let i = p + 1; i < n; i++) {
            avgG = (avgG * (p - 1) + gains[i]) / p;
            avgL = (avgL * (p - 1) + losses[i]) / p;
            if (avgL === 0) out[i] = 100;
            else out[i] = 100 - 100 / (1 + avgG / avgL);
        }
        return out;
    }

    global.ChartIndicators = global.ChartIndicators || {};
    global.ChartIndicators.rsiWilder = rsiWilder;
})(typeof window !== 'undefined' ? window : globalThis);
