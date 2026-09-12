/**
 * Shared configuration for divtest.
 * Override via env vars or POST /api/settings.
 */
'use strict';

const path = require('path');

const ROOT = path.resolve(__dirname, '..');

const defaults = {
  // Strategy
  macdFast: 12,
  macdSlow: 26,
  macdSignal: 9,
  divergenceLookback: 60,
  histogramFlipWindow: 10,
  entryMode: 'next_open', // next_open | same_close
  exitOppositeFlip: true,
  exitStopLoss: true,
  exitTimeBased: true,
  atrPeriod: 14,
  atrStopMultiplier: 1.5,
  atrTargetMultiplier: 2.5,
  maxHoldingBars: 40,
  // Costs
  brokeragePerSide: 20,
  brokerageEnabled: true,
  equityDefaultQty: 1,
  // Upstox throttle (conservative vs documented 50/s, 500/min, 2000/30min)
  maxRequestsPerSecond: 20,
  maxRequestsPerMinute: 200,
  maxRequestsPer30Min: 900,
  requestTimeoutMs: 30000,
  // Paths
  dataDir: path.join(ROOT, 'data'),
  cacheDir: path.join(ROOT, 'cache'),
  instrumentMasterUrl:
    process.env.UPSTOX_INSTRUMENTS_URL ||
    'https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz',
  instrumentMasterMaxAgeHours: 24,
  // Auth / server
  port: Number(process.env.DIVTEST_PORT || process.env.PORT || 3847),
  basicAuthUser: process.env.DIVTEST_BASIC_AUTH_USER || '',
  basicAuthPass: process.env.DIVTEST_BASIC_AUTH_PASS || '',
  ipAllowlist: (process.env.DIVTEST_IP_ALLOWLIST || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean),
  demoMode: process.env.DIVTEST_DEMO_MODE === '1' || process.env.DIVTEST_DEMO_MODE === 'true',
  // Timeframes exposed to UI / runner
  timeframes: [
    { id: '10min', unit: 'minutes', interval: 10, label: '10 min' },
    { id: '15min', unit: 'minutes', interval: 15, label: '15 min' },
    { id: '1hr', unit: 'hours', interval: 1, label: '1 hour' },
  ],
};

/** Runtime mutable settings (strategy + costs). */
const runtime = { ...defaults };

function getConfig() {
  return { ...runtime };
}

function updateConfig(partial = {}) {
  const allowed = [
    'macdFast',
    'macdSlow',
    'macdSignal',
    'divergenceLookback',
    'histogramFlipWindow',
    'entryMode',
    'exitOppositeFlip',
    'exitStopLoss',
    'exitTimeBased',
    'atrPeriod',
    'atrStopMultiplier',
    'atrTargetMultiplier',
    'maxHoldingBars',
    'brokeragePerSide',
    'brokerageEnabled',
    'equityDefaultQty',
  ];
  for (const key of allowed) {
    if (partial[key] !== undefined && partial[key] !== null && partial[key] !== '') {
      const numKeys = [
        'macdFast',
        'macdSlow',
        'macdSignal',
        'divergenceLookback',
        'histogramFlipWindow',
        'atrPeriod',
        'atrStopMultiplier',
        'atrTargetMultiplier',
        'maxHoldingBars',
        'brokeragePerSide',
        'equityDefaultQty',
      ];
      if (numKeys.includes(key)) {
        runtime[key] = Number(partial[key]);
      } else if (typeof defaults[key] === 'boolean') {
        runtime[key] = Boolean(partial[key]);
      } else {
        runtime[key] = partial[key];
      }
    }
  }
  return getConfig();
}

function resetConfig() {
  Object.assign(runtime, defaults);
  return getConfig();
}

module.exports = {
  ROOT,
  defaults,
  getConfig,
  updateConfig,
  resetConfig,
};
