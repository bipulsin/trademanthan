/**
 * Centralized IST crosshair / localization time formatting for chart modals.
 */
(function (global) {
    'use strict';

    const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    function isDailyTimeframe(tf) {
        return String(tf || '').toLowerCase() === '1d';
    }

    function isBusinessDayTime(time) {
        return (
            time &&
            typeof time === 'object' &&
            typeof time.year === 'number' &&
            typeof time.month === 'number' &&
            typeof time.day === 'number' &&
            typeof time !== 'number'
        );
    }

    function partsFromChartTime(time) {
        if (isBusinessDayTime(time)) {
            return {
                day: time.day,
                month: time.month,
                year: time.year,
                hour: 0,
                minute: 0,
                isDaily: true,
            };
        }
        const sec = typeof time === 'number' ? time : time && time.timestamp;
        if (!Number.isFinite(sec)) return null;
        const d = new Date(sec * 1000);
        return {
            day: d.getUTCDate(),
            month: d.getUTCMonth() + 1,
            year: d.getUTCFullYear(),
            hour: d.getUTCHours(),
            minute: d.getUTCMinutes(),
            isDaily: false,
        };
    }

    /**
     * Full crosshair label: e.g. "26 May 2026 09:15 IST"
     * @param {*} time LWC time
     * @param {string} timeframe
     * @returns {string}
     */
    function formatCrosshair(time, timeframe) {
        const p = partsFromChartTime(time);
        if (!p) return '';
        const mon = MONTHS[p.month - 1] || '';
        const datePart = p.day + ' ' + mon + ' ' + p.year;
        if (isDailyTimeframe(timeframe) || p.isDaily) {
            return datePart + ' IST';
        }
        const hh = String(p.hour).padStart(2, '0');
        const mi = String(p.minute).padStart(2, '0');
        return datePart + ' ' + hh + ':' + mi + ' IST';
    }

    /**
     * Compact axis tick label (intraday: HH:MM, daily: date).
     * @param {*} time
     * @param {string} timeframe
     * @returns {string}
     */
    function formatAxisTick(time, timeframe) {
        const p = partsFromChartTime(time);
        if (!p) return '';
        if (isDailyTimeframe(timeframe) || p.isDaily) {
            return p.day + ' ' + (MONTHS[p.month - 1] || '');
        }
        return String(p.hour).padStart(2, '0') + ':' + String(p.minute).padStart(2, '0');
    }

    global.ChartCrosshairFormat = {
        formatCrosshair: formatCrosshair,
        formatAxisTick: formatAxisTick,
        isDailyTimeframe: isDailyTimeframe,
    };
})(typeof window !== 'undefined' ? window : globalThis);
