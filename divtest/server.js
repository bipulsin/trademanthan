/**
 * divtest Express API server.
 * Routes: /api/run-backtest, /api/results, /api/instruments, settings, OAuth, SSE progress.
 */
'use strict';

require('dotenv').config({ path: require('path').join(__dirname, '.env') });

const path = require('path');
const fs = require('fs');
const express = require('express');
const cors = require('cors');
const basicAuth = require('express-basic-auth');
const { getConfig, updateConfig, resetConfig } = require('./lib/config');
const { UpstoxClient } = require('./lib/upstoxClient');
const { BacktestRunner } = require('./lib/backtestRunner');
const {
  listInstruments,
  getResultsSummary,
  loadAllTrades,
} = require('./lib/resultsStore');

const cfg0 = getConfig();
fs.mkdirSync(cfg0.dataDir, { recursive: true });
fs.mkdirSync(cfg0.cacheDir, { recursive: true });

const app = express();
const upstox = new UpstoxClient();
const runner = new BacktestRunner(upstox);

app.use(cors());
app.use(express.json({ limit: '2mb' }));

// IP allowlist (optional)
app.use((req, res, next) => {
  const allow = getConfig().ipAllowlist;
  if (!allow.length) return next();
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '')
    .toString()
    .split(',')[0]
    .trim()
    .replace(/^::ffff:/, '');
  if (allow.includes(ip) || allow.includes('*')) return next();
  return res.status(403).json({ error: 'IP not allowed' });
});

// Basic auth (optional — enabled when both user+pass set)
const authUser = getConfig().basicAuthUser;
const authPass = getConfig().basicAuthPass;
if (authUser && authPass) {
  app.use(
    basicAuth({
      users: { [authUser]: authPass },
      challenge: true,
      realm: 'divtest',
    })
  );
}

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (_req, res) => {
  res.redirect('/divtest.html');
});

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'divtest',
    demoMode: getConfig().demoMode,
    hasToken: Boolean(upstox.accessToken || process.env.UPSTOX_ACCESS_TOKEN),
  });
});

app.get('/api/settings', (_req, res) => {
  const c = getConfig();
  res.json({
    macdFast: c.macdFast,
    macdSlow: c.macdSlow,
    macdSignal: c.macdSignal,
    divergenceLookback: c.divergenceLookback,
    histogramFlipWindow: c.histogramFlipWindow,
    entryMode: c.entryMode,
    exitOppositeFlip: c.exitOppositeFlip,
    exitStopLoss: c.exitStopLoss,
    exitTimeBased: c.exitTimeBased,
    atrPeriod: c.atrPeriod,
    atrStopMultiplier: c.atrStopMultiplier,
    atrTargetMultiplier: c.atrTargetMultiplier,
    maxHoldingBars: c.maxHoldingBars,
    brokeragePerSide: c.brokeragePerSide,
    brokerageEnabled: c.brokerageEnabled,
    equityDefaultQty: c.equityDefaultQty,
    demoMode: c.demoMode,
  });
});

app.post('/api/settings', (req, res) => {
  const updated = updateConfig(req.body || {});
  res.json({ ok: true, settings: updated });
});

app.post('/api/settings/reset', (_req, res) => {
  res.json({ ok: true, settings: resetConfig() });
});

app.get('/api/instruments', async (req, res) => {
  try {
    const q = String(req.query.q || req.query.name || '').trim();
    if (getConfig().demoMode && !q) {
      return res.json({
        demoMode: true,
        stored: listInstruments(),
        resolved: [],
      });
    }
    if (!getConfig().demoMode) {
      await upstox.ensureInstrumentMaster();
    }
    let resolved = [];
    if (q) {
      const names = q.split(',').map((s) => s.trim()).filter(Boolean);
      for (const name of names) {
        if (getConfig().demoMode) {
          resolved.push({
            symbol: name.toUpperCase(),
            found: true,
            demo: true,
            lotSize: 1,
          });
        } else {
          resolved.push(await upstox.resolveInstrument(name));
        }
      }
    }
    res.json({
      demoMode: getConfig().demoMode,
      stored: listInstruments(),
      resolved,
      masterCount: upstox.instruments.length,
      masterLoadedAt: upstox.instrumentsLoadedAt || null,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/instruments/refresh', async (_req, res) => {
  try {
    if (getConfig().demoMode) {
      return res.json({ ok: true, demoMode: true, count: 0 });
    }
    await upstox.ensureInstrumentMaster({ force: true });
    res.json({ ok: true, count: upstox.instruments.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/run-backtest', async (req, res) => {
  try {
    const {
      instruments,
      periodMonths = 6,
      forceRefresh = false,
      settings = {},
    } = req.body || {};
    if (settings && Object.keys(settings).length) {
      updateConfig(settings);
    }
    const job = await runner.run({
      instruments,
      periodMonths: Number(periodMonths) === 12 ? 12 : 6,
      forceRefresh: Boolean(forceRefresh),
      settings: getConfig(),
    });
    res.status(202).json({
      ok: true,
      jobId: job.id,
      status: job.status,
      months: job.months,
      symbols: job.symbols,
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/api/jobs/:id', (req, res) => {
  const job = runner.getJob(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json({
    id: job.id,
    status: job.status,
    progress: job.progress,
    partial_trades: job.partial_trades,
    error: job.error,
    started_at: job.started_at,
    finished_at: job.finished_at,
    logs: job.logs.slice(-200),
  });
});

app.get('/api/jobs/:id/events', (req, res) => {
  const job = runner.getJob(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();

  const send = (event, data) => {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };

  send('progress', { ...job.progress, status: job.status, partial_trades: job.partial_trades });
  for (const entry of job.logs.slice(-50)) {
    send('log', entry);
  }

  const onProgress = (id, data) => {
    if (id === job.id) send('progress', data);
  };
  const onLog = (id, entry) => {
    if (id === job.id) send('log', entry);
  };
  const onDone = (id) => {
    if (id === job.id) {
      send('done', { status: 'completed', summary: getResultsSummary() });
      cleanup();
    }
  };
  const onErr = (id, err) => {
    if (id === job.id) {
      send('error', { message: err.message });
      cleanup();
    }
  };

  function cleanup() {
    runner.off('progress', onProgress);
    runner.off('log', onLog);
    runner.off('done', onDone);
    runner.off('error', onErr);
    res.end();
  }

  runner.on('progress', onProgress);
  runner.on('log', onLog);
  runner.on('done', onDone);
  runner.on('error', onErr);

  req.on('close', cleanup);
});

app.get('/api/results', (req, res) => {
  try {
    const instrument = req.query.instrument || null;
    const timeframe = req.query.timeframe || null;
    const page = Math.max(1, Number(req.query.page) || 1);
    const pageSize = Math.min(500, Math.max(10, Number(req.query.pageSize) || 50));
    const summary = getResultsSummary({
      instrument: instrument && instrument !== 'All' ? instrument : null,
      timeframe: timeframe && timeframe !== 'All' ? timeframe : null,
    });
    const start = (page - 1) * pageSize;
    const pageTrades = summary.trades.slice(start, start + pageSize);
    res.json({
      ...summary,
      trades: pageTrades,
      pagination: {
        page,
        pageSize,
        total: summary.trade_count,
        pages: Math.ceil(summary.trade_count / pageSize) || 1,
      },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/results/all', (req, res) => {
  const instrument = req.query.instrument || null;
  const timeframe = req.query.timeframe || null;
  res.json(
    getResultsSummary({
      instrument: instrument && instrument !== 'All' ? instrument : null,
      timeframe: timeframe && timeframe !== 'All' ? timeframe : null,
    })
  );
});

// OAuth helpers (token never sent to frontend beyond masked status)
app.get('/api/upstox/login', (_req, res) => {
  try {
    const url = upstox.getLoginUrl();
    res.json({ url });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/api/upstox/callback', async (req, res) => {
  try {
    const code = req.query.code;
    if (!code) return res.status(400).send('Missing code');
    await upstox.exchangeCodeForToken(code);
    res.send(
      '<html><body><h2>divtest: Upstox token saved</h2><p>You can close this window and return to the app.</p></body></html>'
    );
  } catch (err) {
    res.status(500).send(`Token exchange failed: ${err.message}`);
  }
});

app.get('/api/upstox/status', (_req, res) => {
  const token = upstox.accessToken || '';
  res.json({
    configured: Boolean(token),
    preview: token ? `${token.slice(0, 6)}…${token.slice(-4)}` : null,
    demoMode: getConfig().demoMode,
  });
});

const port = getConfig().port;
if (require.main === module) {
  app.listen(port, () => {
    console.log(`divtest listening on http://0.0.0.0:${port}`);
    console.log(`Demo mode: ${getConfig().demoMode}`);
    console.log(`Basic auth: ${authUser && authPass ? 'enabled' : 'disabled'}`);
  });
}

module.exports = { app, runner, upstox };
