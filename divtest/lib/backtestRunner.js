/**
 * Orchestrates month-by-month fetch → strategy → JSON write with live progress events.
 */
'use strict';

const { EventEmitter } = require('events');
const { v4: uuidv4 } = require('uuid');
const { getConfig } = require('./config');
const { UpstoxClient, generateDemoCandles, startOfMonth } = require('./upstoxClient');
const { runStrategy } = require('./strategyEngine');
const {
  monthExists,
  writeMonth,
  appendMonthTrades,
  getResultsSummary,
} = require('./resultsStore');

function monthsBack(count) {
  const out = [];
  const now = new Date();
  // Current calendar month backwards
  let y = now.getUTCFullYear();
  let m = now.getUTCMonth(); // 0-based
  for (let i = 0; i < count; i++) {
    const mm = String(m + 1).padStart(2, '0');
    out.push(`${y}-${mm}`);
    m -= 1;
    if (m < 0) {
      m = 11;
      y -= 1;
    }
  }
  return out;
}

class BacktestRunner extends EventEmitter {
  constructor(upstoxClient) {
    super();
    this.upstox = upstoxClient || new UpstoxClient();
    this.jobs = new Map();
  }

  getJob(id) {
    return this.jobs.get(id) || null;
  }

  _log(job, message, level = 'info') {
    const entry = { ts: new Date().toISOString(), level, message };
    job.logs.push(entry);
    if (job.logs.length > 2000) job.logs.shift();
    this.emit('log', job.id, entry);
  }

  async run({
    instruments,
    periodMonths = 6,
    forceRefresh = false,
    settings = {},
  }) {
    const cfg = { ...getConfig(), ...settings };
    const jobId = uuidv4();
    const symbols = String(instruments || '')
      .split(',')
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);

    if (!symbols.length) {
      throw new Error('At least one instrument is required');
    }

    const months = monthsBack(periodMonths);
    const timeframes = cfg.timeframes;
    const totalSteps = symbols.length * months.length * timeframes.length;

    const job = {
      id: jobId,
      status: 'running',
      started_at: new Date().toISOString(),
      finished_at: null,
      symbols,
      months,
      forceRefresh,
      progress: {
        total: totalSteps,
        done: 0,
        current: null,
        processed_months: [],
        message: 'Starting…',
      },
      logs: [],
      partial_trades: 0,
      error: null,
    };
    this.jobs.set(jobId, job);

    // Fire async — return job id immediately
    setImmediate(() => this._execute(job, cfg, timeframes));
    return job;
  }

  async _execute(job, cfg, timeframes) {
    const log = (msg, level) => this._log(job, msg, level);
    this.upstox.setLogFn((m) => log(m, 'info'));

    try {
      if (!cfg.demoMode) {
        log('Ensuring instrument master is loaded…');
        await this.upstox.ensureInstrumentMaster();
      } else {
        log('DEMO MODE: using synthetic candles (no Upstox calls)');
      }

      for (const symbol of job.symbols) {
        let resolved = null;
        if (!cfg.demoMode) {
          resolved = await this.upstox.resolveInstrument(symbol);
          if (!resolved.found) {
            log(`Instrument not found: ${symbol} — skipping`, 'warn');
            job.progress.done += job.months.length * timeframes.length;
            this._emitProgress(job);
            continue;
          }
          log(
            `Resolved ${symbol}: cash=${resolved.cash?.instrument_key || 'none'}, ` +
              `futures=${resolved.futures.length}, commodity=${resolved.isCommodity}, lot=${resolved.lotSize}`
          );
        } else {
          resolved = {
            symbol,
            found: true,
            cash: { instrument_key: `DEMO|${symbol}`, lot_size: 1 },
            futures: [],
            isCommodity: ['CRUDEOIL', 'GOLD', 'SILVER', 'NATGAS'].includes(symbol),
            lotSize: symbol === 'NIFTY' ? 65 : 1,
            notes: [],
          };
        }

        for (const yyyyMm of job.months) {
          job.progress.current = `${symbol} ${yyyyMm}`;
          job.progress.message = `Processed: ${job.progress.processed_months.join(', ') || '—'}` +
            ` … fetching ${yyyyMm}`;
          this._emitProgress(job);

          for (const tf of timeframes) {
            const stepLabel = `${symbol} ${tf.id} ${yyyyMm}`;
            try {
              if (!job.forceRefresh && monthExists(symbol, tf.id, yyyyMm)) {
                log(`Skip ${stepLabel} (cached; use Force Refresh to re-run)`);
                job.progress.done += 1;
                this._emitProgress(job);
                continue;
              }

              const { candles, source, instrument_key, notes } = await this._fetchMonthData({
                symbol,
                resolved,
                yyyyMm,
                tf,
                demo: cfg.demoMode,
                log,
              });

              if (!candles.length) {
                log(`No candle data for ${stepLabel}`, 'warn');
                writeMonth(symbol, tf.id, yyyyMm, {
                  trades: [],
                  candles: 0,
                  source,
                  instrument_key,
                  notes: [...notes, 'no_data'],
                  meta: { empty: true },
                });
                job.progress.done += 1;
                this._emitProgress(job);
                continue;
              }

              const qty =
                resolved.lotSize ||
                (resolved.cash && resolved.cash.lot_size) ||
                cfg.equityDefaultQty;

              const { trades, meta } = runStrategy(candles, {
                macdFast: cfg.macdFast,
                macdSlow: cfg.macdSlow,
                macdSignal: cfg.macdSignal,
                divergenceLookback: cfg.divergenceLookback,
                histogramFlipWindow: cfg.histogramFlipWindow,
                entryMode: cfg.entryMode,
                exitOppositeFlip: cfg.exitOppositeFlip,
                exitStopLoss: cfg.exitStopLoss,
                exitTimeBased: cfg.exitTimeBased,
                atrPeriod: cfg.atrPeriod,
                atrStopMultiplier: cfg.atrStopMultiplier,
                atrTargetMultiplier: cfg.atrTargetMultiplier,
                maxHoldingBars: cfg.maxHoldingBars,
                instrument: symbol,
                timeframe: tf.id,
                qtyPerLot: qty,
                brokeragePerSide: cfg.brokeragePerSide,
                brokerageEnabled: cfg.brokerageEnabled,
              });

              if (job.forceRefresh) {
                writeMonth(symbol, tf.id, yyyyMm, {
                  trades,
                  candles: candles.length,
                  source,
                  instrument_key,
                  notes,
                  meta,
                });
              } else {
                appendMonthTrades(symbol, tf.id, yyyyMm, trades, {
                  candles: candles.length,
                  source,
                  instrument_key,
                  notes,
                  meta,
                });
              }

              job.partial_trades += trades.length;
              log(
                `${stepLabel}: ${candles.length} candles via ${source}, ${trades.length} trades`
              );
            } catch (err) {
              log(`Error on ${stepLabel}: ${err.message} — continuing`, 'error');
              writeMonth(symbol, tf.id, yyyyMm, {
                trades: [],
                candles: 0,
                source: 'error',
                notes: [err.message],
                meta: { error: true },
              });
            }

            job.progress.done += 1;
            this._emitProgress(job);
          }

          if (!job.progress.processed_months.includes(yyyyMm)) {
            job.progress.processed_months.push(yyyyMm);
          }
          job.progress.message = `Processed: ${job.progress.processed_months.join(', ')}`;
          this._emitProgress(job);
        }
      }

      job.status = 'completed';
      job.finished_at = new Date().toISOString();
      job.progress.message = `Done. ${job.partial_trades} new trades this run.`;
      this._emitProgress(job);
      this.emit('done', job.id, getResultsSummary());
      log('Backtest job completed');
    } catch (err) {
      job.status = 'failed';
      job.error = err.message;
      job.finished_at = new Date().toISOString();
      log(`Job failed: ${err.message}`, 'error');
      this.emit('error', job.id, err);
    }
  }

  _emitProgress(job) {
    this.emit('progress', job.id, {
      ...job.progress,
      status: job.status,
      partial_trades: job.partial_trades,
    });
  }

  async _fetchMonthData({ symbol, resolved, yyyyMm, tf, demo, log }) {
    const notes = [];
    if (demo) {
      const seed = symbol.split('').reduce((a, c) => a + c.charCodeAt(0), 0);
      return {
        candles: generateDemoCandles(yyyyMm, tf.id, seed),
        source: 'demo',
        instrument_key: `DEMO|${symbol}`,
        notes: ['demo_synthetic'],
      };
    }

    // Commodities: prioritize futures stitch
    const preferFutures = resolved.isCommodity || (resolved.futures && resolved.futures.length);

    if (preferFutures) {
      const fut = this.upstox.pickFutureForMonth(resolved, yyyyMm);
      if (fut) {
        try {
          const candles = await this.upstox.fetchMonthCandles({
            instrumentKey: fut.instrument_key,
            unit: tf.unit,
            interval: tf.interval,
            yyyyMm,
            tryExpired: true,
          });
          if (candles.length) {
            return {
              candles,
              source: `future:${fut.trading_symbol}`,
              instrument_key: fut.instrument_key,
              notes,
            };
          }
          notes.push(`Empty future candles for ${fut.trading_symbol} ${yyyyMm}`);
          log(
            `No future contract data for ${symbol} ${yyyyMm} (${fut.trading_symbol}), falling back to cash segment`,
            'warn'
          );
        } catch (err) {
          notes.push(`Future fetch failed: ${err.message}`);
          log(
            `No future contract data for ${symbol} ${yyyyMm}, falling back to cash segment (${err.message})`,
            'warn'
          );
        }
      } else {
        notes.push(`No future contract mapped for ${yyyyMm}`);
        log(
          `No future contract data for ${symbol} ${yyyyMm}, falling back to cash segment`,
          'warn'
        );
      }
    }

    if (resolved.cash && resolved.cash.instrument_key) {
      try {
        const candles = await this.upstox.fetchMonthCandles({
          instrumentKey: resolved.cash.instrument_key,
          unit: tf.unit,
          interval: tf.interval,
          yyyyMm,
        });
        return {
          candles,
          source: `cash:${resolved.cash.trading_symbol || resolved.cash.instrument_key}`,
          instrument_key: resolved.cash.instrument_key,
          notes,
        };
      } catch (err) {
        notes.push(`Cash fetch failed: ${err.message}`);
        log(`Cash segment fetch failed for ${symbol} ${yyyyMm}: ${err.message}`, 'error');
      }
    } else {
      notes.push('No cash instrument available');
      log(`No cash/equity fallback for ${symbol} ${yyyyMm}`, 'warn');
    }

    return { candles: [], source: 'none', instrument_key: null, notes };
  }
}

module.exports = {
  BacktestRunner,
  monthsBack,
};
