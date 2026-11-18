/**
 * Open Low High — live screener UI (polls /api/open-low-high/today).
 * Screen: two columns (Bullish | Bearish), 2-card grid, active golden / Phase A grey.
 * Chart: SecurityChartEngine 5m + VWAP; Entry/SL/T1/T2 lines for actives.
 */
(function () {
  'use strict';

  const POLL_MS = 20000;
  const LTP_POLL_MS = 15000;
  const ENGINE_SRC = 'security-chart/security-chart-engine.js?v=19';
  const BADGE_ORDER = { S1: 0, S2: 1, S3: 2, W1: 3, W2: 4, W3: 5 };

  let pollTimer = null;
  let ltpTimer = null;
  let logLoaded = false;
  /** @type {Record<string, object>} */
  let cardByKey = {};

  function token() {
    return localStorage.getItem('trademanthan_token') || '';
  }

  function authHeaders() {
    const t = token();
    return t ? { Authorization: 'Bearer ' + t } : {};
  }

  async function api(path, opts) {
    const res = await fetch(path, {
      ...opts,
      headers: {
        'Content-Type': 'application/json',
        ...authHeaders(),
        ...(opts && opts.headers),
      },
    });
    if (res.status === 401) {
      window.location.href = 'login.html';
      throw new Error('Unauthorized');
    }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error((data && (data.detail || data.message)) || res.statusText);
    }
    return data;
  }

  function fmt(n) {
    if (n == null || n === '') return '—';
    const x = Number(n);
    if (!Number.isFinite(x)) return '—';
    return x.toLocaleString('en-IN', { maximumFractionDigits: 2 });
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function cardKey(card) {
    return (
      String(card.symbol || '').toUpperCase() +
      '|' +
      String(card.side || '').toUpperCase() +
      '|' +
      String(card.trade_date || '')
    );
  }

  function badgeSortRank(card) {
    const raw = String((card && (card.sector_badge || card.sector_badge_display)) || '')
      .trim()
      .toUpperCase()
      .replace(/\*$/, '');
    return Object.prototype.hasOwnProperty.call(BADGE_ORDER, raw) ? BADGE_ORDER[raw] : 99;
  }

  function hasOlhStar(card) {
    const b = String((card && card.olh_badge) || '').trim().toUpperCase();
    return b === 'O=L' || b === 'O=H';
  }

  /** Actives first; non-actives: O=L/O=H star, then S1…W3, then symbol. */
  function sortCards(list) {
    return (list || []).slice().sort(function (a, b) {
      const aa = a.active ? 0 : 1;
      const ba = b.active ? 0 : 1;
      if (aa !== ba) return aa - ba;
      if (!a.active && !b.active) {
        const as = hasOlhStar(a) ? 0 : 1;
        const bs = hasOlhStar(b) ? 0 : 1;
        if (as !== bs) return as - bs;
      }
      const ab = badgeSortRank(a);
      const bb = badgeSortRank(b);
      if (ab !== bb) return ab - bb;
      return String(a.symbol || '').localeCompare(String(b.symbol || ''));
    });
  }

  function badgeHtml(card) {
    const raw = String(card.sector_badge_display || card.sector_badge || '').trim();
    if (!raw) return '';
    const base = raw.replace(/\*$/, '');
    const isStrong = /^S[123]$/i.test(base);
    const isWeak = /^W[123]$/i.test(base);
    const cls = isStrong ? 'is-strong' : isWeak ? 'is-weak' : '';
    return `<span class="olh-sector-badge ${cls}" title="${escapeHtml(card.nifty_sector_label || '')}">${escapeHtml(raw)}</span>`;
  }

  function olhBadgeHtml(card) {
    const raw = String(card.olh_badge || '').trim().toUpperCase();
    if (raw !== 'O=L' && raw !== 'O=H') return '';
    if (card.active) {
      return `<span class="olh-extreme-badge is-glow" title="Open equals ${raw === 'O=L' ? 'Low' : 'High'}">${escapeHtml(raw)}</span>`;
    }
    return `<span class="olh-extreme-badge is-star" title="Open equals ${raw === 'O=L' ? 'Low' : 'High'} (Phase A)">★</span>`;
  }

  function phaseBReason(card) {
    if (card.active || card.phase_b_ok) return 'Phase B OK';
    const r = String(card.phase_b_fail_reason || '').trim();
    return r || '';
  }

  function splitActiveGrey(list) {
    const active = [];
    const grey = [];
    (list || []).forEach(function (c) {
      if (c.active) active.push(c);
      else grey.push(c);
    });
    return { active: active, grey: grey };
  }

  function columnHtml(list) {
    const parts = splitActiveGrey(sortCards(list));
    const chunks = [];
    if (parts.active.length) {
      chunks.push('<div class="olh-grid olh-grid-active">' + parts.active.map(cardHtml).join('') + '</div>');
    }
    if (parts.active.length && parts.grey.length) {
      chunks.push('<div class="olh-row-divider" aria-hidden="true"></div>');
    }
    if (parts.grey.length) {
      chunks.push('<div class="olh-grid olh-grid-grey">' + parts.grey.map(cardHtml).join('') + '</div>');
    }
    return chunks.join('');
  }

  function usesEquityLogic(card) {
    return !!(card && (card.equity_instrument_key || card.data_source_logic === 'equity'));
  }

  function entryValue(card) {
    // Frozen screen Entry only — never live LTP. Equity first_*/second_* are audit-only.
    if (card.entry != null && Number.isFinite(Number(card.entry))) return Number(card.entry);
    if (usesEquityLogic(card)) return null;
    if (card.second_close != null) return Number(card.second_close);
    if (card.first_close != null) return Number(card.first_close);
    return null;
  }

  function slValue(card) {
    if (card.sl != null && Number.isFinite(Number(card.sl))) return Number(card.sl);
    // Legacy future-only rows: first_open was FUT. Equity-logic rows must not use equity open.
    if (usesEquityLogic(card)) return null;
    if (card.first_open != null) return Number(card.first_open);
    return null;
  }

  function cardHtml(card) {
    const side = String(card.side || '').toUpperCase();
    const bull = side === 'BULLISH';
    const active = !!card.active;
    const key = cardKey(card);
    const entry = entryValue(card);
    const sl = slValue(card);
    const stateCls = active ? 'is-active' : 'is-grey';
    return `
      <article class="olh-card ${bull ? 'is-bullish' : 'is-bearish'} ${stateCls}" data-olh-key="${escapeHtml(key)}">
        <div class="olh-card-row olh-card-row1">
          <button type="button" class="olh-sym-btn" data-olh-chart="${escapeHtml(key)}" title="Open chart">
            ${escapeHtml(card.symbol || '')}
          </button>
          <div class="olh-entry-wrap">
            <span class="olh-k">Entry</span>
            <span class="olh-v olh-entry" data-olh-entry>${fmt(entry)}</span>
          </div>
        </div>
        <div class="olh-card-row olh-card-row2">
          <div><span class="olh-k">SL</span> <span class="olh-v">${fmt(sl)}</span></div>
          <div><span class="olh-k">T1</span> <span class="olh-v">${fmt(card.t1)}</span></div>
          <div><span class="olh-k">T2</span> <span class="olh-v">${fmt(card.t2)}</span></div>
        </div>
        <div class="olh-card-meta">
          <span class="olh-side-chip ${bull ? 'bull' : 'bear'}">${bull ? 'BULL' : 'BEAR'}</span>
          ${active ? '<span class="olh-active-chip">ACTIVE</span>' : '<span class="olh-grey-chip">PHASE A</span>'}
          ${olhBadgeHtml(card)}
          ${badgeHtml(card)}
        </div>
      </article>`;
  }

  function setBanner(msg, show) {
    const el = document.getElementById('olhBanner');
    if (!el) return;
    if (!show || !msg) {
      el.hidden = true;
      el.textContent = '';
      return;
    }
    el.hidden = false;
    el.textContent = msg;
  }

  function indexCards(bull, bear) {
    cardByKey = {};
    (bull || []).concat(bear || []).forEach(function (c) {
      cardByKey[cardKey(c)] = c;
    });
  }

  function renderNiftyCue(data) {
    const showOn = String(data.nifty_show_on || '').toUpperCase();
    const bullEl = document.getElementById('olhNiftyBull');
    const bearEl = document.getElementById('olhNiftyBear');
    if (bullEl) {
      const on = showOn === 'BULLISH';
      bullEl.hidden = !on;
      bullEl.title = on
        ? 'NIFTY bullish + gap up (09:20 close > prev close; 09:15 open > prev close)'
        : '';
    }
    if (bearEl) {
      const on = showOn === 'BEARISH';
      bearEl.hidden = !on;
      bearEl.title = on
        ? 'NIFTY bearish + gap down (09:20 close < prev close; 09:15 open < prev close)'
        : '';
    }
  }

  function renderScreen(data) {
    const bull = sortCards(data.bullish || []);
    const bear = sortCards(data.bearish || []);
    indexCards(bull, bear);

    const bullGrid = document.getElementById('olhBullGrid');
    const bearGrid = document.getElementById('olhBearGrid');
    const bullEmpty = document.getElementById('olhBullEmpty');
    const bearEmpty = document.getElementById('olhBearEmpty');
    const bullActive = bull.filter(function (c) { return c.active; }).length;
    const bearActive = bear.filter(function (c) { return c.active; }).length;
    document.getElementById('olhBullCount').textContent =
      bull.length + (bull.length ? ` (${bullActive} active)` : '');
    document.getElementById('olhBearCount').textContent =
      bear.length + (bear.length ? ` (${bearActive} active)` : '');

    if (bullGrid) {
      bullGrid.outerHTML =
        '<div class="olh-col-body" id="olhBullGrid"' +
        (bull.length === 0 ? ' hidden' : '') +
        '>' +
        columnHtml(bull) +
        '</div>';
    }
    if (bearGrid) {
      bearGrid.outerHTML =
        '<div class="olh-col-body" id="olhBearGrid"' +
        (bear.length === 0 ? ' hidden' : '') +
        '>' +
        columnHtml(bear) +
        '</div>';
    }
    if (bullEmpty) bullEmpty.hidden = bull.length > 0;
    if (bearEmpty) bearEmpty.hidden = bear.length > 0;

    renderNiftyCue(data);

    const statusEl = document.getElementById('olhStatus');
    if (statusEl) {
      const st = String(data.status || '—');
      statusEl.textContent = st.toUpperCase();
      statusEl.classList.toggle('is-frozen', !!data.frozen || st === 'frozen');
      statusEl.classList.toggle('is-waiting', st === 'waiting');
    }
    const dateEl = document.getElementById('olhDate');
    if (dateEl) dateEl.textContent = data.trade_date || '—';
    const timeEl = document.getElementById('olhServerTime');
    if (timeEl) {
      const raw = data.server_time || '';
      timeEl.textContent = raw ? raw.slice(11, 19) + ' IST' : '—';
    }
    setBanner(data.message || '', true);
  }

  function patchLiveLtp(bySymbol) {
    // Keep LTP on the card object for reference; never mutate frozen Entry display.
    if (!bySymbol) return;
    Object.keys(cardByKey).forEach(function (key) {
      const card = cardByKey[key];
      const sym = String(card.symbol || '').toUpperCase();
      const live = bySymbol[sym];
      if (!live || live.ltp == null) return;
      card.ltp = live.ltp;
    });
  }

  async function loadToday() {
    try {
      const data = await api('/api/open-low-high/today');
      renderScreen(data);
    } catch (err) {
      setBanner('Failed to load: ' + (err && err.message ? err.message : err), true);
    }
  }

  async function pollLtp() {
    try {
      const data = await api('/api/open-low-high/ltp');
      patchLiveLtp(data.by_symbol || {});
    } catch (err) {
      /* soft-fail — full poll will refresh */
    }
  }

  async function forceRun() {
    try {
      setBanner('Queuing force screen…', true);
      const res = await api('/api/open-low-high/run', {
        method: 'POST',
        body: JSON.stringify({ force: true }),
      });
      setBanner(res.message || 'Queued. Polling…', true);
      setTimeout(loadToday, 4000);
      setTimeout(loadToday, 20000);
      setTimeout(loadToday, 45000);
    } catch (err) {
      setBanner('Force run failed: ' + (err && err.message ? err.message : err), true);
    }
  }

  function ensureChartEngine() {
    if (window.SecurityChartEngine) return Promise.resolve(window.SecurityChartEngine);
    return new Promise(function (resolve, reject) {
      const existing = document.querySelector('script[data-uscm-engine="1"]');
      if (existing) {
        existing.addEventListener('load', function () {
          if (window.SecurityChartEngine) resolve(window.SecurityChartEngine);
          else reject(new Error('Chart module unavailable'));
        });
        existing.addEventListener('error', function () {
          reject(new Error('Failed to load chart module'));
        });
        return;
      }
      const s = document.createElement('script');
      s.src = ENGINE_SRC;
      s.async = true;
      s.setAttribute('data-uscm-engine', '1');
      s.onload = function () {
        if (window.SecurityChartEngine) resolve(window.SecurityChartEngine);
        else reject(new Error('Chart module unavailable'));
      };
      s.onerror = function () {
        reject(new Error('Failed to load chart module'));
      };
      document.head.appendChild(s);
    });
  }

  function buildPriceLines(card) {
    if (!card || !card.active) return [];
    const lines = [];
    const entry = entryValue(card);
    const sl = slValue(card);
    if (entry != null) {
      lines.push({ price: entry, title: 'Entry', color: '#3dba7a', lineWidth: 2 });
    }
    if (sl != null) {
      lines.push({ price: sl, title: 'SL', color: '#e06b6b', lineWidth: 2 });
    }
    if (card.t1 != null && Number.isFinite(Number(card.t1))) {
      lines.push({ price: Number(card.t1), title: 'T1', color: '#d4a017', lineWidth: 1, dashed: true });
    }
    if (card.t2 != null && Number.isFinite(Number(card.t2))) {
      lines.push({ price: Number(card.t2), title: 'T2', color: '#c084fc', lineWidth: 1, dashed: true });
    }
    return lines;
  }

  function openOlhChart(card) {
    if (!card) return;
    const futIk = String(card.instrument_key || '').trim();
    const eqIk = String(card.equity_instrument_key || '').trim();
    if (!futIk && !eqIk) {
      window.alert('No instrument key for chart');
      return;
    }
    const sym = String(card.symbol || '').toUpperCase();
    const side = String(card.side || '').toUpperCase();
    const direction = side === 'BEARISH' ? 'SHORT' : 'LONG';
    const active = !!card.active;
    // Default = Future; Equity toggle available when equity key exists.
    const defaultIk = futIk || eqIk;
    const defaultType = futIk ? 'FUT' : 'EQ';
    ensureChartEngine()
      .then(function (eng) {
        if (!eng || typeof eng.openSecurityChart !== 'function') {
          throw new Error('Chart module unavailable');
        }
        return eng.openSecurityChart({
          symbol: sym,
          instrumentType: defaultType,
          instrumentKey: defaultIk,
          displaySymbol: defaultType === 'FUT' ? (card.contract || sym) : sym,
          exchange: 'NSE',
          timeframe: '5m',
          direction: direction,
          skipSavedPrefs: true,
          emaEnabled: false,
          ema2Enabled: false,
          ema3Enabled: false,
          vwapEnabled: true,
          vwapColor: '#9ca3af',
          vwapLineWidth: 1,
          volumeEnabled: true,
          rsiEnabled: false,
          williamsEnabled: false,
          hmEnabled: false,
          macdDivergenceEnabled: false,
          instrumentToggle: futIk && eqIk
            ? {
                future: {
                  instrumentType: 'FUT',
                  instrumentKey: futIk,
                  displaySymbol: card.contract || sym,
                },
                equity: {
                  instrumentType: 'EQ',
                  instrumentKey: eqIk,
                  displaySymbol: sym,
                },
                default: 'future',
              }
            : null,
          priceLines: active && defaultType === 'FUT' ? buildPriceLines(card) : [],
          screenerData: {
            direction: direction,
            insight: (active ? 'ACTIVE' : 'PHASE A') + ' · ' + side,
            sections: [
              {
                title: 'OPEN LOW HIGH',
                metrics: [
                  { key: 'entry', label: 'Entry', value: fmt(entryValue(card)) },
                  { key: 'sl', label: 'SL', value: fmt(slValue(card)) },
                  { key: 't1', label: 'T1', value: fmt(card.t1) },
                  { key: 't2', label: 'T2', value: fmt(card.t2) },
                  { key: 'state', label: 'State', value: active ? 'Active' : 'Phase A (grey)' },
                ],
              },
            ],
          },
          metadata: { algo: 'open_low_high' },
        });
      })
      .catch(function (err) {
        if (window.console && window.console.warn) window.console.warn('OLH chart:', err);
        window.alert((err && err.message) || 'Chart failed');
      });
  }

  function renderLog(data) {
    const host = document.getElementById('olhLogHost');
    const empty = document.getElementById('olhLogEmpty');
    if (!host) return;
    const sections = data.sections || [];
    if (!sections.length) {
      host.innerHTML = '';
      if (empty) empty.hidden = false;
      return;
    }
    if (empty) empty.hidden = true;

    // Index log rows for chart clicks (include trade_date in key).
    sections.forEach(function (sec) {
      (sec.rows || []).forEach(function (r) {
        const row = Object.assign({}, r, { trade_date: sec.date });
        cardByKey[cardKey(row)] = row;
      });
    });

    host.innerHTML = sections
      .map(function (sec) {
        const rows = sortCards(sec.rows || [])
          .map(function (r) {
            const row = Object.assign({}, r, { trade_date: sec.date });
            const key = cardKey(row);
            const active = !!r.active;
            return `<tr class="${active ? 'olh-log-active' : 'olh-log-grey'}">
            <td><button type="button" class="olh-sym-btn olh-sym-btn--table" data-olh-chart="${escapeHtml(key)}">${escapeHtml(r.symbol || '')}</button></td>
            <td>${escapeHtml(r.side || '')}</td>
            <td>${active ? 'Active' : 'Phase A'}</td>
            <td class="olh-log-reason">${escapeHtml(phaseBReason(r))}</td>
            <td>${escapeHtml(r.contract || '')}</td>
            <td>${fmt(entryValue(r))}</td>
            <td>${fmt(slValue(r))}</td>
            <td>${fmt(r.t1)}</td>
            <td>${fmt(r.t2)}</td>
            <td>${escapeHtml(r.olh_badge || '')}</td>
            <td>${escapeHtml(r.sector_badge_display || r.sector_badge || '')}</td>
          </tr>`;
          })
          .join('');
        return `<h3 class="olh-log-date">${escapeHtml(sec.date)}</h3>
          <div class="olh-table-wrap"><table class="olh-table">
            <thead><tr>
              <th>Symbol</th><th>Side</th><th>State</th><th>Reason</th><th>Contract</th>
              <th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>O=L/H</th><th>Tag</th>
            </tr></thead>
            <tbody>${rows}</tbody>
          </table></div>`;
      })
      .join('');
  }

  async function loadLog() {
    const side = (document.getElementById('olhLogSide') || {}).value || '';
    const q = side ? '?side=' + encodeURIComponent(side) : '';
    try {
      const data = await api('/api/open-low-high/log' + q);
      renderLog(data);
      logLoaded = true;
    } catch (err) {
      setBanner('Trade log failed: ' + (err && err.message ? err.message : err), true);
    }
  }

  function switchTab(name) {
    document.querySelectorAll('.olh-tab').forEach(function (btn) {
      const on = btn.getAttribute('data-olh-tab') === name;
      btn.classList.toggle('active', on);
      btn.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    document.querySelectorAll('[data-olh-panel]').forEach(function (panel) {
      panel.hidden = panel.getAttribute('data-olh-panel') !== name;
    });
    if (name === 'log' && !logLoaded) loadLog();
  }

  function onChartClick(ev) {
    const btn = ev.target.closest('[data-olh-chart]');
    if (!btn) return;
    ev.preventDefault();
    const key = btn.getAttribute('data-olh-chart');
    const card = cardByKey[key];
    if (card) openOlhChart(card);
  }

  function bind() {
    document.querySelectorAll('.olh-tab').forEach(function (btn) {
      btn.addEventListener('click', function () {
        switchTab(btn.getAttribute('data-olh-tab'));
      });
    });
    const refresh = document.getElementById('olhRefreshBtn');
    if (refresh) refresh.addEventListener('click', function () { loadToday(); });
    const force = document.getElementById('olhForceBtn');
    if (force) force.addEventListener('click', function () { forceRun(); });
    const logReload = document.getElementById('olhLogReload');
    if (logReload) logReload.addEventListener('click', function () { loadLog(); });
    const logSide = document.getElementById('olhLogSide');
    if (logSide) logSide.addEventListener('change', function () { loadLog(); });

    const screen = document.getElementById('olhPanelScreen');
    if (screen) screen.addEventListener('click', onChartClick);
    const logHost = document.getElementById('olhLogHost');
    if (logHost) logHost.addEventListener('click', onChartClick);
  }

  function startPoll() {
    if (pollTimer) clearInterval(pollTimer);
    if (ltpTimer) clearInterval(ltpTimer);
    pollTimer = setInterval(loadToday, POLL_MS);
    ltpTimer = setInterval(pollLtp, LTP_POLL_MS);
  }

  document.addEventListener('DOMContentLoaded', function () {
    bind();
    loadToday();
    startPoll();
  });
})();
