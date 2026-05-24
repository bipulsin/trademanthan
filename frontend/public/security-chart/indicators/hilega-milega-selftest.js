/**
 * Node self-test for Hilega-Milega math (run: node hilega-milega-selftest.js).
 * Loads rsi + movingAverages via vm; no browser/LWC required.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = __dirname;
const ctx = { global: {}, window: {} };
ctx.global = ctx.window;
ctx.global.ChartIndicators = {};

function load(file) {
    const code = fs.readFileSync(path.join(root, file), 'utf8');
    vm.runInNewContext(code, ctx, { filename: file });
}

load('rsi.js');
load('movingAverages.js');

const CI = ctx.global.ChartIndicators;
const closes = [];
for (let i = 0; i < 40; i++) closes.push(100 + Math.sin(i / 4) * 5 + i * 0.1);
const rsi = CI.rsiWilder(closes, 9);
const ema = CI.emaOverSeries(rsi, 3);
const wma = CI.wmaOverSeries(rsi, 21);

let rsiCount = 0;
let emaCount = 0;
let wmaCount = 0;
for (let i = 0; i < closes.length; i++) {
    if (rsi[i] != null) rsiCount++;
    if (ema[i] != null) emaCount++;
    if (wma[i] != null) wmaCount++;
}

if (rsiCount < 20) throw new Error('RSI output too short: ' + rsiCount);
if (emaCount < 15) throw new Error('EMA on RSI too short: ' + emaCount);
if (wmaCount < 10) throw new Error('WMA on RSI too short: ' + wmaCount);

const lastRsi = rsi[rsi.length - 1];
if (lastRsi < 0 || lastRsi > 100) throw new Error('RSI out of range: ' + lastRsi);

console.log('hilega-milega-selftest ok', { rsiCount: rsiCount, emaCount: emaCount, wmaCount: wmaCount, lastRsi: lastRsi.toFixed(2) });
