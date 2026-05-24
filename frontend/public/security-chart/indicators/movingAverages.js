/**
 * EMA / WMA helpers over numeric series (e.g. RSI values).
 */
(function (global) {
    'use strict';

    /**
     * EMA over a nullable series; skips null gaps but preserves indices.
     * @param {(number|null)[]} values
     * @param {number} period
     * @returns {(number|null)[]}
     */
    function emaOverSeries(values, period) {
        const n = values.length;
        const out = new Array(n).fill(null);
        const p = Math.max(1, Math.floor(period));
        const k = 2 / (p + 1);
        let ema = null;
        let seedCount = 0;
        let seedSum = 0;

        for (let i = 0; i < n; i++) {
            const v = values[i];
            if (v == null || !Number.isFinite(v)) continue;

            if (ema == null) {
                seedSum += v;
                seedCount += 1;
                if (seedCount < p) continue;
                ema = seedSum / p;
                out[i] = ema;
                continue;
            }
            ema = (v - ema) * k + ema;
            out[i] = ema;
        }
        return out;
    }

    /**
     * WMA over a nullable series (most recent bar gets highest weight).
     * @param {(number|null)[]} values
     * @param {number} period
     * @returns {(number|null)[]}
     */
    function wmaOverSeries(values, period) {
        const n = values.length;
        const out = new Array(n).fill(null);
        const p = Math.max(1, Math.floor(period));
        const buf = [];

        for (let i = 0; i < n; i++) {
            const v = values[i];
            if (v == null || !Number.isFinite(v)) continue;
            buf.push({ idx: i, v: v });
            if (buf.length > p) buf.shift();
            if (buf.length < p) continue;

            let sum = 0;
            let wSum = 0;
            for (let j = 0; j < buf.length; j++) {
                const w = j + 1;
                sum += buf[j].v * w;
                wSum += w;
            }
            out[buf[buf.length - 1].idx] = sum / wSum;
        }
        return out;
    }

    global.ChartIndicators = global.ChartIndicators || {};
    global.ChartIndicators.emaOverSeries = emaOverSeries;
    global.ChartIndicators.wmaOverSeries = wmaOverSeries;
})(typeof window !== 'undefined' ? window : globalThis);
