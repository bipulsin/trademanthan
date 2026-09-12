'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  computeMACD,
  histogramFlipSignal,
  findSwingPoints,
  runStrategy,
  analyzeTrades,
} = require('../lib/strategyEngine');
const { generateDemoCandles } = require('../lib/upstoxClient');
const { monthsBack } = require('../lib/backtestRunner');

describe('MACD', () => {
  it('computes macd/signal/histogram lengths', () => {
    const closes = Array.from({ length: 80 }, (_, i) => 100 + Math.sin(i / 5) * 5 + i * 0.1);
    const { macdLine, signal, histogram } = computeMACD(closes, 12, 26, 9);
    assert.equal(macdLine.length, 80);
    assert.equal(signal.length, 80);
    assert.equal(histogram.length, 80);
    const ready = histogram.filter((v) => v != null).length;
    assert.ok(ready > 30);
  });
});

describe('histogramFlipSignal', () => {
  it('detects zero cross bullish', () => {
    const h = [null, null, -1, -0.5, 0.2];
    assert.equal(histogramFlipSignal(h, 4), 'bullish_flip');
  });
  it('detects zero cross bearish', () => {
    const h = [null, null, 1, 0.5, -0.1];
    assert.equal(histogramFlipSignal(h, 4), 'bearish_flip');
  });
  it('ignores soft contraction without zero cross', () => {
    const h = [null, -3, -5, -4];
    assert.equal(histogramFlipSignal(h, 3), null);
  });
});

describe('swing points', () => {
  it('finds local highs and lows', () => {
    const values = [1, 2, 3, 2, 1, 0, 1, 2, 5, 2, 1];
    const { highs, lows } = findSwingPoints(values, 2);
    assert.ok(highs.some((h) => h.index === 2));
    assert.ok(lows.some((l) => l.index === 5));
  });
});

describe('runStrategy', () => {
  it('runs on demo candles without throwing and returns trade shape', () => {
    const candles = generateDemoCandles('2026-08', '15min', 7);
    assert.ok(candles.length > 50);
    const { trades, meta } = runStrategy(candles, {
      instrument: 'DEMO',
      timeframe: '15min',
      qtyPerLot: 1,
      brokerageEnabled: true,
      brokeragePerSide: 20,
      divergenceLookback: 25,
      histogramFlipWindow: 12,
    });
    assert.ok(meta.candleCount > 0);
    for (const t of trades) {
      assert.ok(['LONG', 'SHORT'].includes(t.side));
      assert.ok(['bullish', 'bearish'].includes(t.divergence_type));
      assert.ok(['opposite_flip', 'stop_loss', 'time_exit'].includes(t.exit_reason));
      assert.equal(typeof t.pnl_inr, 'number');
      assert.equal(t.instrument, 'DEMO');
    }
  });

  it('analyzeTrades computes summary fields', () => {
    const trades = [
      {
        pnl_inr: 100,
        exit_datetime: '2026-01-02',
        holding_bars: 5,
      },
      {
        pnl_inr: -40,
        exit_datetime: '2026-01-03',
        holding_bars: 3,
      },
      {
        pnl_inr: 50,
        exit_datetime: '2026-01-04',
        holding_bars: 4,
      },
    ];
    const a = analyzeTrades(trades);
    assert.equal(a.total_trades, 3);
    assert.equal(a.total_pnl, 110);
    assert.ok(a.win_rate_pct > 50);
    assert.ok(a.equity_curve.length === 3);
    assert.ok(a.profit_factor > 0);
  });
});

describe('monthsBack', () => {
  it('returns N months ending with current month', () => {
    const m = monthsBack(6);
    assert.equal(m.length, 6);
    const now = new Date();
    const cur = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}`;
    assert.equal(m[0], cur);
  });
});
