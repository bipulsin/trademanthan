/**
 * Upstox API client: auth, rate-limited requests, instrument master, candles.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { promisify } = require('util');
const { getConfig } = require('./config');

const gunzip = promisify(zlib.gunzip);

const API_V2 = 'https://api.upstox.com/v2';
const API_V3 = 'https://api.upstox.com/v3';

class RateLimiter {
  constructor(opts = {}) {
    this.maxPerSecond = opts.maxPerSecond ?? 20;
    this.maxPerMinute = opts.maxPerMinute ?? 200;
    this.maxPer30Min = opts.maxPer30Min ?? 900;
    this.timestamps = [];
  }

  _prune(now) {
    const cutoff = now - 30 * 60 * 1000;
    this.timestamps = this.timestamps.filter((t) => t > cutoff);
  }

  async acquire(logFn) {
    for (;;) {
      const now = Date.now();
      this._prune(now);
      const last1s = this.timestamps.filter((t) => t > now - 1000).length;
      const last1m = this.timestamps.filter((t) => t > now - 60_000).length;
      const last30m = this.timestamps.length;

      if (
        last1s < this.maxPerSecond &&
        last1m < this.maxPerMinute &&
        last30m < this.maxPer30Min
      ) {
        this.timestamps.push(now);
        return;
      }

      let waitMs = 50;
      if (last1s >= this.maxPerSecond) waitMs = Math.max(waitMs, 1000 - (now - this.timestamps.filter((t) => t > now - 1000)[0]));
      if (last1m >= this.maxPerMinute) waitMs = Math.max(waitMs, 250);
      if (last30m >= this.maxPer30Min) waitMs = Math.max(waitMs, 1000);
      if (logFn) logFn(`Rate limit throttle: waiting ${waitMs}ms (${last1s}/s ${last1m}/min ${last30m}/30min)`);
      await sleep(waitMs);
    }
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function monthKey(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function parseExpiryMs(expiry) {
  if (expiry == null || expiry === '') return null;
  if (typeof expiry === 'number') {
    return expiry > 1e12 ? expiry : expiry * 1000;
  }
  const s = String(expiry).trim();
  if (/^\d+$/.test(s)) {
    const n = Number(s);
    return n > 1e12 ? n : n * 1000;
  }
  const t = Date.parse(s);
  return Number.isFinite(t) ? t : null;
}

function dateToYmd(d) {
  return d.toISOString().slice(0, 10);
}

function endOfMonth(year, monthIndex0) {
  return new Date(Date.UTC(year, monthIndex0 + 1, 0));
}

function startOfMonth(year, monthIndex0) {
  return new Date(Date.UTC(year, monthIndex0, 1));
}

class UpstoxClient {
  constructor(options = {}) {
    const cfg = getConfig();
    this.apiKey = options.apiKey || process.env.UPSTOX_API_KEY || '';
    this.apiSecret = options.apiSecret || process.env.UPSTOX_API_SECRET || '';
    this.redirectUri =
      options.redirectUri ||
      process.env.UPSTOX_REDIRECT_URI ||
      'http://localhost:3847/api/upstox/callback';
    this.tokenFile =
      options.tokenFile ||
      process.env.UPSTOX_TOKEN_FILE ||
      path.join(cfg.cacheDir, 'upstox_token.json');
    this.accessToken = options.accessToken || process.env.UPSTOX_ACCESS_TOKEN || '';
    this.logFn = options.logFn || ((msg) => console.log(`[upstox] ${msg}`));
    this.limiter = new RateLimiter({
      maxPerSecond: cfg.maxRequestsPerSecond,
      maxPerMinute: cfg.maxRequestsPerMinute,
      maxPer30Min: cfg.maxRequestsPer30Min,
    });
    this.instruments = [];
    this.instrumentsLoadedAt = 0;
    this.byName = new Map();
    this._loadTokenFromDisk();
  }

  setLogFn(fn) {
    this.logFn = fn;
  }

  _loadTokenFromDisk() {
    try {
      if (fs.existsSync(this.tokenFile)) {
        const raw = JSON.parse(fs.readFileSync(this.tokenFile, 'utf8'));
        if (raw && raw.access_token) {
          this.accessToken = raw.access_token;
          this.logFn('Loaded Upstox access token from disk');
        }
      }
    } catch (err) {
      this.logFn(`Failed to load token file: ${err.message}`);
    }
  }

  saveToken(tokenPayload) {
    const cfg = getConfig();
    fs.mkdirSync(path.dirname(this.tokenFile), { recursive: true });
    const data = {
      access_token: tokenPayload.access_token || tokenPayload.accessToken,
      saved_at: new Date().toISOString(),
      expires_at: tokenPayload.expires_at || null,
    };
    fs.writeFileSync(this.tokenFile, JSON.stringify(data, null, 2), { mode: 0o600 });
    this.accessToken = data.access_token;
    this.logFn('Saved Upstox access token securely to disk');
  }

  getLoginUrl() {
    if (!this.apiKey) throw new Error('UPSTOX_API_KEY not configured');
    const params = new URLSearchParams({
      client_id: this.apiKey,
      redirect_uri: this.redirectUri,
      response_type: 'code',
    });
    return `https://api.upstox.com/v2/login/authorization/dialog?${params.toString()}`;
  }

  async exchangeCodeForToken(code) {
    const body = new URLSearchParams({
      code: String(code),
      client_id: this.apiKey,
      client_secret: this.apiSecret,
      redirect_uri: this.redirectUri,
      grant_type: 'authorization_code',
    });
    const res = await fetch(`${API_V2}/login/authorization/token`, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body,
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(`Token exchange failed: ${res.status} ${JSON.stringify(json)}`);
    }
    const token = json.access_token || (json.data && json.data.access_token);
    if (!token) throw new Error(`No access_token in response: ${JSON.stringify(json)}`);
    this.saveToken({ access_token: token, expires_at: json.expires_at || null });
    return token;
  }

  /**
   * Upstox does not offer silent refresh without a browser auth-code.
   * On 401 we reload from disk (in case another process refreshed via OAuth).
   */
  reloadToken() {
    this._loadTokenFromDisk();
    if (process.env.UPSTOX_ACCESS_TOKEN) {
      this.accessToken = process.env.UPSTOX_ACCESS_TOKEN;
    }
    return Boolean(this.accessToken);
  }

  async request(url, { method = 'GET', body, headers = {}, retries = 4 } = {}) {
    if (!this.accessToken) {
      this.reloadToken();
    }
    if (!this.accessToken && !getConfig().demoMode) {
      throw new Error('Upstox access token missing. Set UPSTOX_ACCESS_TOKEN or complete OAuth.');
    }

    let attempt = 0;
    let backoff = 500;
    while (attempt <= retries) {
      await this.limiter.acquire((m) => this.logFn(m));
      const res = await fetch(url, {
        method,
        headers: {
          Accept: 'application/json',
          Authorization: `Bearer ${this.accessToken}`,
          ...headers,
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(getConfig().requestTimeoutMs),
      });

      if (res.status === 429) {
        const retryAfter = Number(res.headers.get('retry-after') || 0);
        const wait = Math.max(retryAfter * 1000, backoff);
        this.logFn(`429 rate limited — exponential backoff ${wait}ms (attempt ${attempt + 1})`);
        await sleep(wait);
        backoff = Math.min(backoff * 2, 30_000);
        attempt += 1;
        continue;
      }

      if (res.status === 401) {
        this.logFn('401 unauthorized — reloading token from disk/env');
        this.reloadToken();
        if (attempt >= retries) {
          const text = await res.text();
          throw new Error(`Upstox 401: ${text}`);
        }
        attempt += 1;
        continue;
      }

      const text = await res.text();
      let json;
      try {
        json = text ? JSON.parse(text) : {};
      } catch {
        json = { raw: text };
      }

      if (!res.ok) {
        const msg = json.message || json.error || text || res.statusText;
        const err = new Error(`Upstox ${res.status}: ${msg}`);
        err.status = res.status;
        err.payload = json;
        throw err;
      }
      return json;
    }
    throw new Error(`Upstox request failed after retries: ${url}`);
  }

  // ─── Instrument master ───────────────────────────────────────────────

  instrumentCachePath() {
    return path.join(getConfig().cacheDir, 'complete.json');
  }

  async ensureInstrumentMaster({ force = false } = {}) {
    const cfg = getConfig();
    const cachePath = this.instrumentCachePath();
    const maxAgeMs = cfg.instrumentMasterMaxAgeHours * 3600 * 1000;
    const freshOnDisk =
      !force &&
      fs.existsSync(cachePath) &&
      Date.now() - fs.statSync(cachePath).mtimeMs < maxAgeMs;

    if (freshOnDisk && this.instruments.length) {
      return this.instruments;
    }

    if (freshOnDisk) {
      this.logFn('Loading instrument master from cache');
      const raw = fs.readFileSync(cachePath, 'utf8');
      this.instruments = JSON.parse(raw);
      this.instrumentsLoadedAt = Date.now();
      this._indexInstruments();
      return this.instruments;
    }

    this.logFn(`Downloading instrument master from ${cfg.instrumentMasterUrl}`);
    fs.mkdirSync(cfg.cacheDir, { recursive: true });
    const res = await fetch(cfg.instrumentMasterUrl, {
      signal: AbortSignal.timeout(120_000),
    });
    if (!res.ok) throw new Error(`Instrument master download failed: ${res.status}`);
    const buf = Buffer.from(await res.arrayBuffer());
    let jsonText;
    if (cfg.instrumentMasterUrl.endsWith('.gz') || buf[0] === 0x1f) {
      jsonText = (await gunzip(buf)).toString('utf8');
    } else {
      jsonText = buf.toString('utf8');
    }
    this.instruments = JSON.parse(jsonText);
    fs.writeFileSync(cachePath, JSON.stringify(this.instruments));
    this.instrumentsLoadedAt = Date.now();
    this._indexInstruments();
    this.logFn(`Instrument master loaded: ${this.instruments.length} rows`);
    return this.instruments;
  }

  _indexInstruments() {
    this.byName = new Map();
    for (const row of this.instruments) {
      const names = [
        row.name,
        row.trading_symbol,
        row.short_name,
        row.underlying_symbol,
      ]
        .filter(Boolean)
        .map((s) => String(s).toUpperCase().trim());
      for (const n of names) {
        if (!this.byName.has(n)) this.byName.set(n, []);
        this.byName.get(n).push(row);
      }
    }
  }

  _matchesName(row, symbol) {
    const s = symbol.toUpperCase();
    const fields = [row.name, row.trading_symbol, row.short_name, row.underlying_symbol]
      .filter(Boolean)
      .map((x) => String(x).toUpperCase());
    return fields.some((f) => f === s || f.startsWith(`${s} `) || f.startsWith(`${s}-`));
  }

  findRows(symbol) {
    const s = String(symbol).toUpperCase().trim();
    const direct = this.byName.get(s) || [];
    if (direct.length) return direct;
    return this.instruments.filter((r) => this._matchesName(r, s));
  }

  /**
   * Resolve human name → cash + futures contracts (current/previous + historical list).
   */
  async resolveInstrument(symbol) {
    await this.ensureInstrumentMaster();
    const s = String(symbol).toUpperCase().trim();
    const rows = this.findRows(s);
    if (!rows.length) {
      return {
        symbol: s,
        found: false,
        cash: null,
        futures: [],
        isCommodity: false,
        lotSize: getConfig().equityDefaultQty,
        notes: [`No instrument master match for ${s}`],
      };
    }

    const cash =
      rows.find(
        (r) =>
          (r.segment === 'NSE_EQ' || r.instrument_type === 'EQ') &&
          String(r.trading_symbol || '').toUpperCase() === s
      ) ||
      rows.find((r) => r.segment === 'NSE_EQ' || r.instrument_type === 'EQ') ||
      rows.find((r) => r.segment === 'BSE_EQ') ||
      null;

    const futRows = rows.filter((r) => {
      const t = String(r.instrument_type || '').toUpperCase();
      const seg = String(r.segment || '').toUpperCase();
      return (
        (t === 'FUT' || t === 'FUTURES' || t.includes('FUT')) &&
        (seg.includes('FO') || seg.includes('MCX') || seg.includes('NFO') || seg.includes('CDS'))
      );
    });

    const isCommodity = futRows.some((r) => String(r.segment || '').toUpperCase().includes('MCX')) ||
      rows.some((r) => String(r.exchange || r.segment || '').toUpperCase().includes('MCX'));

    const now = Date.now();
    const futures = futRows
      .map((r) => {
        const expMs = parseExpiryMs(r.expiry);
        return {
          instrument_key: r.instrument_key,
          trading_symbol: r.trading_symbol,
          segment: r.segment,
          exchange: r.exchange,
          lot_size: Number(r.lot_size || r.minimum_lot || 1),
          expiry: expMs ? new Date(expMs).toISOString().slice(0, 10) : null,
          expiryMs: expMs,
          weekly: Boolean(r.weekly),
        };
      })
      .filter((f) => f.expiryMs && !f.weekly)
      .sort((a, b) => a.expiryMs - b.expiryMs);

    // Prefer monthly futures (near month first among those not yet expired, plus recent expired)
    const live = futures.filter((f) => f.expiryMs >= now - 7 * 86400_000);
    const current = live[0] || null;
    const previous =
      futures.filter((f) => f.expiryMs < (current ? current.expiryMs : now)).slice(-1)[0] || null;

    const lotSize =
      (current && current.lot_size) ||
      (cash && Number(cash.lot_size || 1)) ||
      getConfig().equityDefaultQty;

    return {
      symbol: s,
      found: true,
      cash: cash
        ? {
            instrument_key: cash.instrument_key,
            trading_symbol: cash.trading_symbol,
            segment: cash.segment,
            lot_size: Number(cash.lot_size || 1),
          }
        : null,
      futures,
      currentFuture: current,
      previousFuture: previous,
      isCommodity,
      lotSize,
      notes: [],
    };
  }

  /**
   * Pick the futures contract that was active during a calendar month (YYYY-MM).
   * Rule: nearest monthly expiry on/after mid-month, else nearest before month-end.
   */
  pickFutureForMonth(resolved, yyyyMm) {
    const [y, m] = yyyyMm.split('-').map(Number);
    const mid = Date.UTC(y, m - 1, 15);
    const monthEnd = endOfMonth(y, m - 1).getTime();
    const monthStart = startOfMonth(y, m - 1).getTime();
    const candidates = (resolved.futures || []).filter(
      (f) => f.expiryMs >= monthStart - 5 * 86400_000
    );
    if (!candidates.length) return null;
    // Contract typically active in the month before its expiry
    const byExpiry = candidates
      .filter((f) => f.expiryMs >= mid && f.expiryMs <= monthEnd + 45 * 86400_000)
      .sort((a, b) => a.expiryMs - b.expiryMs);
    if (byExpiry.length) return byExpiry[0];
    // Fallback: expiry closest after month start
    const after = candidates.filter((f) => f.expiryMs >= monthStart).sort((a, b) => a.expiryMs - b.expiryMs);
    return after[0] || null;
  }

  // ─── Historical candles ──────────────────────────────────────────────

  _normalizeCandles(payload) {
    const rows =
      (payload && payload.data && payload.data.candles) ||
      (payload && payload.candles) ||
      [];
    return rows
      .map((c) => ({
        timestamp: c[0],
        open: Number(c[1]),
        high: Number(c[2]),
        low: Number(c[3]),
        close: Number(c[4]),
        volume: Number(c[5] || 0),
        oi: c.length > 6 ? Number(c[6]) : undefined,
      }))
      .filter((c) => Number.isFinite(c.close))
      .sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));
  }

  async getHistoricalCandles(instrumentKey, unit, interval, fromDate, toDate) {
    const keyEnc = encodeURIComponent(instrumentKey);
    const url =
      `${API_V3}/historical-candle/${keyEnc}/${unit}/${interval}/` +
      `${toDate}/${fromDate}`;
    try {
      const json = await this.request(url);
      return this._normalizeCandles(json);
    } catch (err) {
      // Fallback to v2 limited intervals when v3 rejects
      if (unit === 'minutes' && (interval === 10 || interval === 15)) {
        this.logFn(`V3 candle fetch failed for ${instrumentKey} (${err.message}); trying 1-minute aggregate path`);
        return this._fetchAndAggregateMinutes(instrumentKey, interval, fromDate, toDate);
      }
      throw err;
    }
  }

  async _fetchAndAggregateMinutes(instrumentKey, n, fromDate, toDate) {
    // V3 1-minute max span ~1 month — caller already month-scoped
    const keyEnc = encodeURIComponent(instrumentKey);
    const url = `${API_V3}/historical-candle/${keyEnc}/minutes/1/${toDate}/${fromDate}`;
    const json = await this.request(url);
    const oneMin = this._normalizeCandles(json);
    return aggregateMinutes(oneMin, n);
  }

  /**
   * Try expired-instruments historical API (Upstox Plus) for a past future key.
   */
  async getExpiredHistoricalCandles(instrumentKey, intervalLabel, fromDate, toDate) {
    // intervalLabel e.g. "30minute" | "day" — expired API is v2-style
    const keyEnc = encodeURIComponent(instrumentKey);
    const url =
      `${API_V2}/expired-instruments/historical-candle/${keyEnc}/` +
      `${intervalLabel}/${toDate}/${fromDate}`;
    const json = await this.request(url);
    return this._normalizeCandles(json);
  }

  async fetchMonthCandles({ instrumentKey, unit, interval, yyyyMm, tryExpired = false }) {
    const [y, m] = yyyyMm.split('-').map(Number);
    const fromDate = dateToYmd(startOfMonth(y, m - 1));
    const toDate = dateToYmd(endOfMonth(y, m - 1));
    try {
      return await this.getHistoricalCandles(instrumentKey, unit, interval, fromDate, toDate);
    } catch (err) {
      if (tryExpired) {
        const map = { minutes: `${interval}minute`, hours: '30minute' };
        // best-effort: for hours use day fallback is wrong; skip if unsupported
        if (unit === 'minutes' && [1, 30].includes(interval)) {
          this.logFn(`Trying expired candle API for ${instrumentKey} ${yyyyMm}`);
          return this.getExpiredHistoricalCandles(
            instrumentKey,
            interval === 1 ? '1minute' : '30minute',
            fromDate,
            toDate
          );
        }
      }
      throw err;
    }
  }
}

function aggregateMinutes(candles, n) {
  if (n <= 1) return candles;
  const buckets = new Map();
  for (const c of candles) {
    const dt = new Date(c.timestamp);
    if (Number.isNaN(dt.getTime())) continue;
    // IST bucket
    const istMs = dt.getTime() + (dt.getTimezoneOffset() + 330) * 60_000;
    const ist = new Date(istMs);
    const mins = ist.getUTCHours() * 60 + ist.getUTCMinutes();
    const bucketMin = Math.floor(mins / n) * n;
    const bh = String(Math.floor(bucketMin / 60)).padStart(2, '0');
    const bm = String(bucketMin % 60).padStart(2, '0');
    const key = `${ist.toISOString().slice(0, 10)}T${bh}:${bm}:00+05:30`;
    if (!buckets.has(key)) {
      buckets.set(key, {
        timestamp: key,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
        volume: c.volume || 0,
        oi: c.oi,
      });
    } else {
      const b = buckets.get(key);
      b.high = Math.max(b.high, c.high);
      b.low = Math.min(b.low, c.low);
      b.close = c.close;
      b.volume += c.volume || 0;
      if (c.oi != null) b.oi = c.oi;
    }
  }
  return [...buckets.values()].sort((a, b) => a.timestamp.localeCompare(b.timestamp));
}

/**
 * Synthetic OHLC series for demo / offline tests (includes planted divergences).
 * Builds a repeatable close-path that yields regular bullish + bearish divergences.
 */
function generateDemoCandles(yyyyMm, timeframeId, seed = 1) {
  const [y, m] = yyyyMm.split('-').map(Number);
  const start = startOfMonth(y, m - 1);
  const end = endOfMonth(y, m - 1);
  const stepMin = timeframeId === '1hr' ? 60 : timeframeId === '15min' ? 15 : 10;
  const times = [];
  let t = new Date(start);
  t.setUTCHours(3, 45, 0, 0);
  while (t <= end) {
    const day = t.getUTCDay();
    if (day !== 0 && day !== 6) times.push(new Date(t.getTime()));
    t = new Date(t.getTime() + stepMin * 60_000);
    const utcH = t.getUTCHours();
    const utcM = t.getUTCMinutes();
    if (utcH > 10 || (utcH === 10 && utcM > 0)) {
      t.setUTCDate(t.getUTCDate() + 1);
      t.setUTCHours(3, 45, 0, 0);
    }
  }

  const pattern = [];
  let p = 100 + (seed % 17);
  const pushN = (n, delta) => {
    for (let i = 0; i < n; i++) {
      p += delta;
      pattern.push(p);
    }
  };
  // Warm-up
  pushN(25, 0.25);
  // Bullish divergence: LL in price, HL in MACD, then reclaim/flip
  pushN(12, -1.6);
  pushN(10, 1.3);
  pushN(15, -1.15);
  pushN(1, -2.2);
  pushN(18, 2.1);
  // Bearish divergence: HH in price, LH in MACD, then selloff/flip
  pushN(8, 0.9);
  pushN(10, -0.85);
  pushN(12, 1.35);
  pushN(16, -1.55);
  // Mild chop filler
  for (let i = 0; i < 20; i++) pushN(1, i % 2 === 0 ? 0.35 : -0.3);

  const scale = 1 + (seed % 5) * 0.05;
  const base = 900 + (seed % 40) * 5;
  const candles = [];
  for (let i = 0; i < times.length; i++) {
    const raw = pattern[i % pattern.length];
    const cycle = Math.floor(i / pattern.length);
    const close = base + raw * scale + cycle * 3;
    const prev =
      i === 0
        ? close
        : base + pattern[(i - 1) % pattern.length] * scale + Math.floor((i - 1) / pattern.length) * 3;
    const open = prev;
    candles.push({
      timestamp: times[i].toISOString(),
      open,
      high: Math.max(open, close) + 0.8,
      low: Math.min(open, close) - 0.8,
      close,
      volume: 1000 + (i % 50) * 10,
    });
  }
  return candles;
}

module.exports = {
  UpstoxClient,
  RateLimiter,
  aggregateMinutes,
  generateDemoCandles,
  monthKey,
  parseExpiryMs,
  dateToYmd,
  startOfMonth,
  endOfMonth,
};
