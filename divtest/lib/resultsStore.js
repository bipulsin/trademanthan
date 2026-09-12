/**
 * Persist backtest results as one JSON file per instrument/timeframe/month.
 * Layout: data/{instrument}/{timeframe}/{YYYY-MM}.json
 */
'use strict';

const fs = require('fs');
const path = require('path');
const { getConfig } = require('./config');
const { analyzeTrades } = require('./strategyEngine');

function safeName(s) {
  return String(s || 'UNKNOWN')
    .toUpperCase()
    .replace(/[^A-Z0-9._-]+/g, '_');
}

function safeTimeframe(tf) {
  return String(tf || '15min')
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '_');
}

function monthFile(instrument, timeframe, yyyyMm) {
  const root = getConfig().dataDir;
  return path.join(root, safeName(instrument), safeTimeframe(timeframe), `${yyyyMm}.json`);
}

function ensureDir(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function readMonth(instrument, timeframe, yyyyMm) {
  const fp = monthFile(instrument, timeframe, yyyyMm);
  if (!fs.existsSync(fp)) return null;
  try {
    return JSON.parse(fs.readFileSync(fp, 'utf8'));
  } catch {
    return null;
  }
}

function monthExists(instrument, timeframe, yyyyMm) {
  return fs.existsSync(monthFile(instrument, timeframe, yyyyMm));
}

function writeMonth(instrument, timeframe, yyyyMm, payload) {
  const fp = monthFile(instrument, timeframe, yyyyMm);
  ensureDir(fp);
  const doc = {
    instrument: safeName(instrument),
    timeframe,
    month: yyyyMm,
    updated_at: new Date().toISOString(),
    source: payload.source || null,
    instrument_key: payload.instrument_key || null,
    candles: payload.candles || 0,
    notes: payload.notes || [],
    trades: payload.trades || [],
    meta: payload.meta || {},
  };
  fs.writeFileSync(fp, JSON.stringify(doc, null, 2));
  return doc;
}

/**
 * Append trades into a month file (idempotent by entry+exit+side key when merging runs).
 */
function appendMonthTrades(instrument, timeframe, yyyyMm, trades, extra = {}) {
  const existing = readMonth(instrument, timeframe, yyyyMm) || {
    trades: [],
    notes: [],
    candles: 0,
  };
  const keyOf = (t) =>
    `${t.side}|${t.entry_datetime}|${t.exit_datetime}|${t.entry_price}|${t.exit_price}`;
  const seen = new Set((existing.trades || []).map(keyOf));
  const merged = [...(existing.trades || [])];
  for (const t of trades || []) {
    const k = keyOf(t);
    if (!seen.has(k)) {
      seen.add(k);
      merged.push(t);
    }
  }
  return writeMonth(instrument, timeframe, yyyyMm, {
    ...existing,
    ...extra,
    trades: merged,
    notes: [...new Set([...(existing.notes || []), ...(extra.notes || [])])],
  });
}

function listInstruments() {
  const root = getConfig().dataDir;
  if (!fs.existsSync(root)) return [];
  return fs
    .readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name)
    .sort();
}

function listMonths(instrument, timeframe) {
  const dir = path.join(getConfig().dataDir, safeName(instrument), safeTimeframe(timeframe));
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => /^\d{4}-\d{2}\.json$/.test(f))
    .map((f) => f.replace(/\.json$/, ''))
    .sort()
    .reverse();
}

/**
 * Load all stored trades with optional filters.
 */
function loadAllTrades({ instrument = null, timeframe = null } = {}) {
  const root = getConfig().dataDir;
  if (!fs.existsSync(root)) return [];
  const instruments = instrument ? [safeName(instrument)] : listInstruments();
  const tfs = timeframe && timeframe !== 'All' ? [timeframe] : ['10min', '15min', '1hr'];
  const trades = [];
  const files = [];

  for (const inst of instruments) {
    for (const tf of tfs) {
      const dir = path.join(root, inst, safeTimeframe(tf));
      if (!fs.existsSync(dir)) continue;
      for (const f of fs.readdirSync(dir)) {
        if (!f.endsWith('.json')) continue;
        const fp = path.join(dir, f);
        files.push(fp);
        try {
          const doc = JSON.parse(fs.readFileSync(fp, 'utf8'));
          for (const t of doc.trades || []) {
            trades.push({ ...t, instrument: t.instrument || inst, timeframe: t.timeframe || tf, _month: doc.month });
          }
        } catch {
          /* skip corrupt */
        }
      }
    }
  }

  trades.sort((a, b) => String(a.entry_datetime).localeCompare(String(b.entry_datetime)));
  return { trades, files: files.length };
}

function getResultsSummary(filters = {}) {
  const { trades } = loadAllTrades(filters);
  const analysis = analyzeTrades(trades);
  const byTimeframe = {};
  for (const tf of ['10min', '15min', '1hr']) {
    const subset = trades.filter((t) => t.timeframe === tf);
    byTimeframe[tf] = analyzeTrades(subset);
  }
  return {
    filters,
    instruments: listInstruments(),
    trade_count: trades.length,
    trades,
    analysis,
    by_timeframe: byTimeframe,
  };
}

module.exports = {
  safeName,
  monthFile,
  readMonth,
  monthExists,
  writeMonth,
  appendMonthTrades,
  listInstruments,
  listMonths,
  loadAllTrades,
  getResultsSummary,
  safeTimeframe,
};
