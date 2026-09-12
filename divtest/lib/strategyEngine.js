/**
 * MACD Divergence + Histogram Flip strategy engine.
 */
'use strict';

/**
 * EMA series.
 */
function ema(values, period) {
  const out = new Array(values.length).fill(null);
  if (!values.length || period <= 0) return out;
  const k = 2 / (period + 1);
  let prev = null;
  let sum = 0;
  let count = 0;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (v == null || Number.isNaN(v)) {
      out[i] = prev;
      continue;
    }
    if (prev == null) {
      sum += v;
      count += 1;
      if (count === period) {
        prev = sum / period;
        out[i] = prev;
      }
    } else {
      prev = v * k + prev * (1 - k);
      out[i] = prev;
    }
  }
  return out;
}

function computeMACD(closes, fast = 12, slow = 26, signalPeriod = 9) {
  const emaFast = ema(closes, fast);
  const emaSlow = ema(closes, slow);
  const macdLine = closes.map((_, i) =>
    emaFast[i] != null && emaSlow[i] != null ? emaFast[i] - emaSlow[i] : null
  );
  const signal = ema(
    macdLine.map((v) => (v == null ? NaN : v)),
    signalPeriod
  );
  const histogram = macdLine.map((v, i) =>
    v != null && signal[i] != null ? v - signal[i] : null
  );
  return { macdLine, signal, histogram };
}

function computeATR(candles, period = 14) {
  const trs = new Array(candles.length).fill(null);
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    if (i === 0) {
      trs[i] = c.high - c.low;
      continue;
    }
    const prevClose = candles[i - 1].close;
    trs[i] = Math.max(
      c.high - c.low,
      Math.abs(c.high - prevClose),
      Math.abs(c.low - prevClose)
    );
  }
  // Wilder ATR via RMA
  const atr = new Array(candles.length).fill(null);
  let prev = null;
  let sum = 0;
  for (let i = 0; i < trs.length; i++) {
    if (trs[i] == null) continue;
    if (prev == null) {
      sum += trs[i];
      if (i === period - 1) {
        prev = sum / period;
        atr[i] = prev;
      }
    } else {
      prev = (prev * (period - 1) + trs[i]) / period;
      atr[i] = prev;
    }
  }
  return atr;
}

/**
 * Local swing highs/lows using a symmetric window of `order` bars each side.
 */
function findSwingPoints(values, order = 3) {
  const highs = [];
  const lows = [];
  for (let i = order; i < values.length - order; i++) {
    const v = values[i];
    if (v == null) continue;
    let isHigh = true;
    let isLow = true;
    for (let j = 1; j <= order; j++) {
      if (values[i - j] == null || values[i + j] == null) {
        isHigh = false;
        isLow = false;
        break;
      }
      if (!(v > values[i - j] && v >= values[i + j])) isHigh = false;
      if (!(v < values[i - j] && v <= values[i + j])) isLow = false;
    }
    if (isHigh) highs.push({ index: i, value: v });
    if (isLow) lows.push({ index: i, value: v });
  }
  return { highs, lows };
}

/**
 * Detect regular divergences within lookback of bar `i` (confirmed at swing confirmation).
 * Returns divergences whose second swing is confirmed at index `confirmIndex`.
 */
function detectDivergencesAt(priceSwings, macdSwings, confirmIndex, lookback) {
  const results = [];
  const minIdx = Math.max(0, confirmIndex - lookback);

  // Bearish: price HH, MACD LH — among swing highs confirmed at confirmIndex
  const priceHighs = priceSwings.highs.filter(
    (s) => s.index >= minIdx && s.index <= confirmIndex
  );
  const macdHighs = macdSwings.highs.filter(
    (s) => s.index >= minIdx && s.index <= confirmIndex
  );
  if (priceHighs.length >= 2 && macdHighs.length >= 2) {
    const p2 = priceHighs[priceHighs.length - 1];
    const p1 = priceHighs[priceHighs.length - 2];
    // Require the second price swing to be near confirmIndex (within order slack)
    if (p2.index >= confirmIndex - 2) {
      const m2 = nearestSwing(macdHighs, p2.index, 5);
      const m1 = nearestSwing(
        macdHighs.filter((s) => s.index < (m2 ? m2.index : p2.index)),
        p1.index,
        8
      );
      if (m1 && m2 && p2.value > p1.value && m2.value < m1.value) {
        results.push({
          type: 'bearish',
          confirmIndex: p2.index,
          priceSwing1: p1,
          priceSwing2: p2,
          macdSwing1: m1,
          macdSwing2: m2,
        });
      }
    }
  }

  // Bullish: price LL, MACD HL
  const priceLows = priceSwings.lows.filter(
    (s) => s.index >= minIdx && s.index <= confirmIndex
  );
  const macdLows = macdSwings.lows.filter(
    (s) => s.index >= minIdx && s.index <= confirmIndex
  );
  if (priceLows.length >= 2 && macdLows.length >= 2) {
    const p2 = priceLows[priceLows.length - 1];
    const p1 = priceLows[priceLows.length - 2];
    if (p2.index >= confirmIndex - 2) {
      const m2 = nearestSwing(macdLows, p2.index, 5);
      const m1 = nearestSwing(
        macdLows.filter((s) => s.index < (m2 ? m2.index : p2.index)),
        p1.index,
        8
      );
      if (m1 && m2 && p2.value < p1.value && m2.value > m1.value) {
        results.push({
          type: 'bullish',
          confirmIndex: p2.index,
          priceSwing1: p1,
          priceSwing2: p2,
          macdSwing1: m1,
          macdSwing2: m2,
        });
      }
    }
  }

  return results;
}

function nearestSwing(swings, targetIdx, maxDist) {
  let best = null;
  let bestDist = Infinity;
  for (const s of swings) {
    const d = Math.abs(s.index - targetIdx);
    if (d <= maxDist && d < bestDist) {
      best = s;
      bestDist = d;
    }
  }
  return best;
}

/**
 * Histogram flip:
 * - zero cross (sign change)
 * - OR reverse from expanding-negative to contracting (and vice versa for positive)
 */
function histogramFlipSignal(histogram, i) {
  if (i < 2) return null;
  const a = histogram[i - 2];
  const b = histogram[i - 1];
  const c = histogram[i];
  if (a == null || b == null || c == null) return null;

  // Zero cross
  if (b < 0 && c >= 0) return 'bullish_flip';
  if (b > 0 && c <= 0) return 'bearish_flip';

  // Expanding negative → contracting (magnitude shrinking) toward zero / positive
  if (a < 0 && b < 0 && c < 0) {
    const expanding = Math.abs(b) > Math.abs(a);
    const contracting = Math.abs(c) < Math.abs(b);
    if (expanding && contracting) return 'bullish_flip';
  }
  if (a > 0 && b > 0 && c > 0) {
    const expanding = Math.abs(b) > Math.abs(a);
    const contracting = Math.abs(c) < Math.abs(b);
    if (expanding && contracting) return 'bearish_flip';
  }

  // Negative to less-negative / positive already covered; also catch b negative, c less negative without cross
  if (b < 0 && c > b && a <= b) return 'bullish_flip';
  if (b > 0 && c < b && a >= b) return 'bearish_flip';

  return null;
}

function oppositeFlip(side, flip) {
  if (side === 'LONG') return flip === 'bearish_flip';
  if (side === 'SHORT') return flip === 'bullish_flip';
  return false;
}

/**
 * Run strategy over a candle series.
 * @returns {{ trades: Array, meta: Object }}
 */
function runStrategy(candles, options = {}) {
  const opts = {
    macdFast: 12,
    macdSlow: 26,
    macdSignal: 9,
    divergenceLookback: 25,
    histogramFlipWindow: 10,
    entryMode: 'next_open', // next_open | same_close
    exitOppositeFlip: true,
    exitStopLoss: true,
    exitTimeBased: true,
    atrPeriod: 14,
    atrStopMultiplier: 1.5,
    atrTargetMultiplier: 2.5,
    maxHoldingBars: 40,
    swingOrder: 3,
    instrument: 'UNKNOWN',
    timeframe: '15min',
    qtyPerLot: 1,
    brokeragePerSide: 20,
    brokerageEnabled: true,
    ...options,
  };

  if (!candles || candles.length < opts.macdSlow + opts.macdSignal + 10) {
    return { trades: [], meta: { reason: 'insufficient_candles', count: candles?.length || 0 } };
  }

  const closes = candles.map((c) => c.close);
  const { macdLine, signal, histogram } = computeMACD(
    closes,
    opts.macdFast,
    opts.macdSlow,
    opts.macdSignal
  );
  const atr = computeATR(candles, opts.atrPeriod);
  const priceSwings = findSwingPoints(closes, opts.swingOrder);
  const macdSwings = findSwingPoints(macdLine, opts.swingOrder);

  const trades = [];
  let pending = null; // divergence waiting for flip
  let openTrade = null;
  const seenDivKeys = new Set();

  for (let i = 0; i < candles.length; i++) {
    // Manage open trade exits first (only after entry bar)
    if (openTrade && i > openTrade.entryIndex) {
      const barsHeld = i - openTrade.entryIndex;
      const c = candles[i];
      let exitReason = null;
      let exitPrice = null;

      // Intrabar ATR stop / target (R-multiple). Protective stop checked before target.
      if (opts.exitStopLoss && openTrade.stop != null) {
        if (openTrade.side === 'LONG') {
          if (c.low <= openTrade.stop) {
            exitReason = 'stop_loss';
            exitPrice = openTrade.stop;
          } else if (openTrade.target != null && c.high >= openTrade.target) {
            exitReason = 'stop_loss';
            exitPrice = openTrade.target;
          }
        } else if (c.high >= openTrade.stop) {
          exitReason = 'stop_loss';
          exitPrice = openTrade.stop;
        } else if (openTrade.target != null && c.low <= openTrade.target) {
          exitReason = 'stop_loss';
          exitPrice = openTrade.target;
        }
      }

      if (!exitReason && opts.exitOppositeFlip) {
        const flip = histogramFlipSignal(histogram, i);
        if (flip && oppositeFlip(openTrade.side, flip)) {
          exitReason = 'opposite_flip';
          exitPrice = c.close;
        }
      }

      if (!exitReason && opts.exitTimeBased && barsHeld >= opts.maxHoldingBars) {
        exitReason = 'time_exit';
        exitPrice = c.close;
      }

      // Default: opposite flip OR stop-loss/target, whichever first (+ optional time exit)
      if (exitReason) {
        trades.push(
          finalizeTrade(openTrade, {
            exitIndex: i,
            exitDatetime: c.timestamp,
            exitPrice,
            exitReason,
            exitDetail: exitReason,
            opts,
          })
        );
        openTrade = null;
        continue;
      }
    }

    // Detect new divergences at this bar
    if (!openTrade) {
      const divs = detectDivergencesAt(priceSwings, macdSwings, i, opts.divergenceLookback);
      for (const d of divs) {
        const key = `${d.type}:${d.confirmIndex}`;
        if (seenDivKeys.has(key)) continue;
        seenDivKeys.add(key);
        pending = {
          divergence: d,
          armedIndex: d.confirmIndex,
          expiresAt: d.confirmIndex + opts.histogramFlipWindow,
        };
      }
    }

    // Wait for histogram flip after divergence
    if (!openTrade && pending && i >= pending.armedIndex && i <= pending.expiresAt) {
      const flip = histogramFlipSignal(histogram, i);
      const wantBull = pending.divergence.type === 'bullish';
      const ok =
        (wantBull && flip === 'bullish_flip') || (!wantBull && flip === 'bearish_flip');
      if (ok) {
        const side = wantBull ? 'LONG' : 'SHORT';
        let entryIndex;
        let entryPrice;
        if (opts.entryMode === 'same_close') {
          entryIndex = i;
          entryPrice = candles[i].close;
        } else {
          entryIndex = i + 1;
          if (entryIndex >= candles.length) {
            pending = null;
            continue;
          }
          entryPrice = candles[entryIndex].open;
        }
        const entryAtr = atr[Math.min(i, atr.length - 1)] || atr[i] || 0;
        const stopDist = entryAtr * opts.atrStopMultiplier;
        const tgtDist = entryAtr * opts.atrTargetMultiplier;
        openTrade = {
          side,
          divergence_type: pending.divergence.type,
          entryIndex,
          entryDatetime: candles[entryIndex].timestamp,
          entryPrice,
          stop:
            opts.exitStopLoss && entryAtr
              ? side === 'LONG'
                ? entryPrice - stopDist
                : entryPrice + stopDist
              : null,
          target:
            opts.exitStopLoss && entryAtr
              ? side === 'LONG'
                ? entryPrice + tgtDist
                : entryPrice - tgtDist
              : null,
          flipIndex: i,
        };
        pending = null;
        // If entry is next bar, skip exit checks until that bar
        if (entryIndex > i) {
          // jump loop to entry bar without double processing — handled naturally
        }
      }
    }

    if (pending && i > pending.expiresAt) {
      pending = null;
    }
  }

  // Force close at end of series
  if (openTrade) {
    const last = candles[candles.length - 1];
    trades.push(
      finalizeTrade(openTrade, {
        exitIndex: candles.length - 1,
        exitDatetime: last.timestamp,
        exitPrice: last.close,
        exitReason: 'time_exit',
        exitDetail: 'series_end',
        opts,
      })
    );
  }

  return {
    trades,
    meta: {
      candleCount: candles.length,
      macdReady: macdLine.filter((v) => v != null).length,
      divergenceCandidates: seenDivKeys.size,
    },
  };
}

function finalizeTrade(openTrade, { exitIndex, exitDatetime, exitPrice, exitReason, exitDetail, opts }) {
  const dir = openTrade.side === 'LONG' ? 1 : -1;
  const rawPnl = (exitPrice - openTrade.entryPrice) * opts.qtyPerLot * dir;
  const fees = opts.brokerageEnabled ? opts.brokeragePerSide * 2 : 0;
  const pnl = rawPnl - fees;
  return {
    instrument: opts.instrument,
    timeframe: opts.timeframe,
    entry_datetime: openTrade.entryDatetime,
    entry_price: round2(openTrade.entryPrice),
    side: openTrade.side,
    exit_datetime: exitDatetime,
    exit_price: round2(exitPrice),
    qty_per_lot: opts.qtyPerLot,
    pnl_inr: round2(pnl),
    raw_pnl_inr: round2(rawPnl),
    fees_inr: fees,
    divergence_type: openTrade.divergence_type,
    exit_reason: exitReason,
    exit_detail: exitDetail || exitReason,
    stop: openTrade.stop != null ? round2(openTrade.stop) : null,
    target: openTrade.target != null ? round2(openTrade.target) : null,
    entry_index: openTrade.entryIndex,
    exit_index: exitIndex,
    holding_bars: exitIndex - openTrade.entryIndex,
  };
}

function round2(n) {
  return Math.round(Number(n) * 100) / 100;
}

/**
 * Aggregate trade stats for analysis cards.
 */
function analyzeTrades(trades) {
  const empty = {
    total_trades: 0,
    win_rate_pct: 0,
    total_pnl: 0,
    avg_pnl: 0,
    avg_win: 0,
    avg_loss: 0,
    max_drawdown_inr: 0,
    max_drawdown_pct: 0,
    profit_factor: 0,
    largest_win: 0,
    largest_loss: 0,
    avg_holding_bars: 0,
    equity_curve: [],
  };
  if (!trades || !trades.length) return empty;

  const sorted = [...trades].sort((a, b) =>
    String(a.exit_datetime).localeCompare(String(b.exit_datetime))
  );
  let equity = 0;
  let peak = 0;
  let maxDd = 0;
  const curve = [];
  let wins = 0;
  let grossProfit = 0;
  let grossLoss = 0;
  let winSum = 0;
  let lossSum = 0;
  let largestWin = -Infinity;
  let largestLoss = Infinity;
  let holdSum = 0;

  for (const t of sorted) {
    const pnl = Number(t.pnl_inr) || 0;
    equity += pnl;
    peak = Math.max(peak, equity);
    maxDd = Math.max(maxDd, peak - equity);
    curve.push({ t: t.exit_datetime, equity: round2(equity), pnl });
    holdSum += Number(t.holding_bars) || 0;
    if (pnl > 0) {
      wins += 1;
      grossProfit += pnl;
      winSum += pnl;
      largestWin = Math.max(largestWin, pnl);
    } else if (pnl < 0) {
      grossLoss += -pnl;
      lossSum += pnl;
      largestLoss = Math.min(largestLoss, pnl);
    } else {
      largestWin = Math.max(largestWin, pnl);
      largestLoss = Math.min(largestLoss, pnl);
    }
  }

  const n = sorted.length;
  const lossCount = sorted.filter((t) => Number(t.pnl_inr) < 0).length;
  const winCount = wins;
  return {
    total_trades: n,
    win_rate_pct: round2((winCount / n) * 100),
    total_pnl: round2(equity),
    avg_pnl: round2(equity / n),
    avg_win: winCount ? round2(winSum / winCount) : 0,
    avg_loss: lossCount ? round2(lossSum / lossCount) : 0,
    max_drawdown_inr: round2(maxDd),
    max_drawdown_pct: peak > 0 ? round2((maxDd / peak) * 100) : round2(maxDd),
    profit_factor: grossLoss > 0 ? round2(grossProfit / grossLoss) : grossProfit > 0 ? 999 : 0,
    largest_win: largestWin === -Infinity ? 0 : round2(largestWin),
    largest_loss: largestLoss === Infinity ? 0 : round2(largestLoss),
    avg_holding_bars: round2(holdSum / n),
    equity_curve: curve,
  };
}

module.exports = {
  ema,
  computeMACD,
  computeATR,
  findSwingPoints,
  detectDivergencesAt,
  histogramFlipSignal,
  runStrategy,
  analyzeTrades,
};
