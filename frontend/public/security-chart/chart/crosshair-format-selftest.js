'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const ctx = { global: {}, window: {} };
ctx.global = ctx.global;
ctx.window = ctx.global;
vm.runInNewContext(fs.readFileSync(path.join(__dirname, 'crosshair-format.js'), 'utf8'), ctx, {
    filename: 'crosshair-format.js',
});

const F = ctx.global.ChartCrosshairFormat;
const intraday = F.formatCrosshair(
    Date.UTC(2026, 4, 26, 9, 15, 0) / 1000,
    '5m'
);
if (intraday.indexOf('26 May 2026') < 0 || intraday.indexOf('09:15') < 0) {
    throw new Error('intraday format failed: ' + intraday);
}
const daily = F.formatCrosshair({ year: 2026, month: 5, day: 26 }, '1d');
if (daily.indexOf('26 May 2026') < 0 || daily.indexOf('09:15') >= 0) {
    throw new Error('daily format failed: ' + daily);
}
console.log('crosshair-format-selftest ok', { intraday: intraday, daily: daily });
