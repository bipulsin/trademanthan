/**
 * Open Low High — live screener UI (polls /api/open-low-high/today).
 * Screen: two columns (Bullish | Bearish), Active → Wait → Non-Active bands.
 * Chart: SecurityChartEngine 5m + VWAP; Entry/SL/T1/T2 lines for actives.
 */
(function () {
  'use strict';

  const POLL_MS = 20000;
  const LTP_POLL_MS = 15000;
  const ENGINE_SRC = 'security-chart/security-chart-engine.js?v=20';
  const BADGE_ORDER = { S1: 0, S2: 1, S3: 2, W1: 3, W2: 4, W3: 5 };
  const LOG_COLS = [
    { key: 'symbol', label: 'Symbol' },
    { key: 'side', label: 'Side' },
    { key: 'state', label: 'State' },
    { key: 'reason', label: 'Reason' },
    { key: 'contract', label: 'Contract' },
    { key: 'entry', label: 'Entry' },
    { key: 'sl', label: 'SL' },
    { key: 't1', label: 'T1' },
    { key: 't2', label: 'T2' },
    { key: 'olh_badge', label: 'O=L/H' },
    { key: 'tag', label: 'Tag' },
  ];

  let pollTimer = null;
  let ltpTimer = null;
  let logLoaded = false;
  /** Screen tab cards (replaced on each /today poll). */
  /** @type {Record<string, object>} */
  let screenByKey = {};
  /** Trade Log rows (must survive Screen poll wipes). */
  /** @type {Record<string, object>} */
  let logByKey = {};
  /** @type {{ sections: Array<{ date: string, rows: object[] }> } | null} */
  let logDataCache = null;
  /** @type {{ key: string, dir: 'asc' | 'desc' } | null} */
  let logSort = null;

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

  function isWaitList(card) {
    if (!card || card.active) return false;
    if (card.wait_list) return true;
    const wr = Number(card.wick_body_ratio);
    return Number.isFinite(wr) && wr > 0.35 && wr <= 0.9;
  }

  function bandRank(card) {
    if (card && card.active) return 0;
    if (isWaitList(card)) return 1;
    return 2;
  }

  /** Active → Wait → Non-Active; within band: O=L/O=H star, S1…W3, symbol. */
  function sortCards(list) {
    return (list || []).slice().sort(function (a, b) {
      const aa = bandRank(a);
      const ba = bandRank(b);
      if (aa !== ba) return aa - ba;
      if (aa > 0) {
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

  function stateLabel(card) {
    if (card.active) return 'Active';
    if (isWaitList(card)) return 'Wait';
    return 'Non-Active';
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

  function formatWickPct(ratio) {
    const n = Number(ratio);
    if (!Number.isFinite(n)) return '';
    const pct = n * 100;
    const r1 = Math.round(pct * 10) / 10;
    if (Math.abs(r1 - Math.round(r1)) < 1e-9) return String(Math.round(r1)) + '%';
    return r1.toFixed(1) + '%';
  }

  function phaseBReason(card) {
    if (card.active || card.phase_b_ok) return 'Phase B OK';
    let r = String(card.phase_b_fail_reason || '').trim();
    if (!r) return '';
    // Legacy rows: append wick % when ratio is stored but not yet in the reason.
    if (
      /wick longer than/i.test(r) &&
      !/\(\d+(\.\d+)?%\)\s*$/.test(r) &&
      card.wick_body_ratio != null
    ) {
      const pct = formatWickPct(card.wick_body_ratio);
      if (pct) r = r + ' (' + pct + ')';
    }
    return r;
  }

  function phaseBPanelValue(card) {
    if (card.active || card.phase_b_ok) return 'Phase B OK · Active';
    const r = phaseBReason(card);
    if (isWaitList(card)) {
      return r ? 'Wait List · ' + r : 'Wait List (wick 35–90%)';
    }
    return r || 'Non-Active';
  }

  function logCellSortValue(row, key) {
    switch (key) {
      case 'symbol':
        return String(row.symbol || '').toUpperCase();
      case 'side':
        return String(row.side || '').toUpperCase();
      case 'state':
        return stateLabel(row);
      case 'reason':
        return phaseBReason(row).toLowerCase();
      case 'contract':
        return String(row.contract || '').toUpperCase();
      case 'entry': {
        const e = entryValue(row);
        return e == null ? Number.NEGATIVE_INFINITY : e;
      }
      case 'sl': {
        const s = slValue(row);
        return s == null ? Number.NEGATIVE_INFINITY : s;
      }
      case 't1':
        return row.t1 == null || !Number.isFinite(Number(row.t1))
          ? Number.NEGATIVE_INFINITY
          : Number(row.t1);
      case 't2':
        return row.t2 == null || !Number.isFinite(Number(row.t2))
          ? Number.NEGATIVE_INFINITY
          : Number(row.t2);
      case 'olh_badge':
        return String(row.olh_badge || '').toUpperCase();
      case 'tag':
        return String(row.sector_badge_display || row.sector_badge || '').toUpperCase();
      default:
        return '';
    }
  }

  function sortLogRows(rows, sortState) {
    const list = (rows || []).slice();
    if (!sortState || !sortState.key) return sortCards(list);
    const key = sortState.key;
    const dir = sortState.dir === 'desc' ? -1 : 1;
    list.sort(function (a, b) {
      const av = logCellSortValue(a, key);
      const bv = logCellSortValue(b, key);
      let cmp = 0;
      if (typeof av === 'number' && typeof bv === 'number') {
        cmp = av === bv ? 0 : av < bv ? -1 : 1;
      } else {
        cmp = String(av).localeCompare(String(bv), undefined, { numeric: true });
      }
      if (cmp !== 0) return cmp * dir;
      return String(a.symbol || '').localeCompare(String(b.symbol || ''));
    });
    return list;
  }

  function logHeaderHtml(sortState) {
    return LOG_COLS.map(function (col) {
      const on = sortState && sortState.key === col.key;
      const ind = on ? (sortState.dir === 'asc' ? ' ▲' : ' ▼') : '';
      const aria = on ? ' aria-sort="' + (sortState.dir === 'asc' ? 'ascending' : 'descending') + '"' : '';
      return (
        '<th class="olh-th-sort' +
        (on ? ' is-sorted' : '') +
        '" data-olh-sort="' +
        escapeHtml(col.key) +
        '" role="columnheader" tabindex="0"' +
        aria +
        ' title="Sort by ' +
        escapeHtml(col.label) +
        '">' +
        escapeHtml(col.label) +
        '<span class="olh-sort-ind" aria-hidden="true">' +
        ind +
        '</span></th>'
      );
    }).join('');
  }

  function splitBands(list) {
    const active = [];
    const wait = [];
    const grey = [];
    (list || []).forEach(function (c) {
      if (c.active) active.push(c);
      else if (isWaitList(c)) wait.push(c);
      else grey.push(c);
    });
    return { active: active, wait: wait, grey: grey };
  }

  function columnHtml(list) {
    const parts = splitBands(sortCards(list));
    const chunks = [];
    function pushDivider() {
      chunks.push('<div class="olh-row-divider" aria-hidden="true"></div>');
    }
    if (parts.active.length) {
      chunks.push('<div class="olh-grid olh-grid-active">' + parts.active.map(cardHtml).join('') + '</div>');
    }
    if (parts.wait.length) {
      if (chunks.length) pushDivider();
      chunks.push(
        '<div class="olh-band-label olh-band-wait">Wait List</div>' +
          '<div class="olh-grid olh-grid-wait">' +
          parts.wait.map(cardHtml).join('') +
          '</div>'
      );
    }
    if (parts.grey.length) {
      if (chunks.length) pushDivider();
      chunks.push(
        '<div class="olh-band-label olh-band-grey">Non-Active</div>' +
          '<div class="olh-grid olh-grid-grey">' +
          parts.grey.map(cardHtml).join('') +
          '</div>'
      );
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
    const wait = isWaitList(card);
    const key = cardKey(card);
    const entry = entryValue(card);
    const sl = slValue(card);
    const stateCls = active ? 'is-active' : wait ? 'is-wait' : 'is-grey';
    const stateChip = active
      ? '<span class="olh-active-chip">ACTIVE</span>'
      : wait
        ? '<span class="olh-wait-chip">WAIT</span>'
        : '<span class="olh-grey-chip">NON-ACTIVE</span>';
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
          ${stateChip}
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
    // Only reset Screen index — never clear logByKey (polls would break Trade Log chart clicks).
    screenByKey = {};
    (bull || []).concat(bear || []).forEach(function (c) {
      screenByKey[cardKey(c)] = c;
    });
  }

  function cardForChartKey(key) {
    if (!key) return null;
    return logByKey[key] || screenByKey[key] || null;
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
    const tradeDate = String(data.trade_date || '').slice(0, 10);
    const stamp = function (c) {
      if (!c) return c;
      if (!c.trade_date && tradeDate) c.trade_date = tradeDate;
      return c;
    };
    const bull = sortCards((data.bullish || []).map(stamp));
    const bear = sortCards((data.bearish || []).map(stamp));
    indexCards(bull, bear);

    const bullGrid = document.getElementById('olhBullGrid');
    const bearGrid = document.getElementById('olhBearGrid');
    const bullEmpty = document.getElementById('olhBullEmpty');
    const bearEmpty = document.getElementById('olhBearEmpty');
    function bandCounts(list) {
      let a = 0;
      let w = 0;
      let g = 0;
      (list || []).forEach(function (c) {
        if (c.active) a += 1;
        else if (isWaitList(c)) w += 1;
        else g += 1;
      });
      return { a: a, w: w, g: g };
    }
    function countText(list) {
      if (!list.length) return '0';
      const c = bandCounts(list);
      return (
        list.length +
        ' (' +
        c.a +
        ' active' +
        (c.w ? ', ' + c.w + ' wait' : '') +
        (c.g ? ', ' + c.g + ' non-active' : '') +
        ')'
      );
    }
    document.getElementById('olhBullCount').textContent = countText(bull);
    document.getElementById('olhBearCount').textContent = countText(bear);

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
    Object.keys(screenByKey).forEach(function (key) {
      const card = screenByKey[key];
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
            insight:
              stateLabel(card).toUpperCase() +
              ' · ' +
              side +
              (active ? '' : phaseBReason(card) ? ' · ' + phaseBReason(card) : ''),
            sections: [
              {
                title: 'OPEN LOW HIGH',
                metrics: [
                  { key: 'entry', label: 'Entry', value: fmt(entryValue(card)) },
                  { key: 'sl', label: 'SL', value: fmt(slValue(card)) },
                  { key: 't1', label: 'T1', value: fmt(card.t1) },
                  { key: 't2', label: 'T2', value: fmt(card.t2) },
                  { key: 'state', label: 'State', value: stateLabel(card) },
                  { key: 'phase_b', label: 'Phase B', value: phaseBPanelValue(card) },
                ],
              },
            ],
          },
          sessionSeparators: true,
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
    if (data) logDataCache = data;
    const src = data || logDataCache;
    if (!src) return;
    const sections = src.sections || [];
    if (!sections.length) {
      host.innerHTML = '';
      if (empty) empty.hidden = false;
      return;
    }
    if (empty) empty.hidden = true;

    // Index log rows for chart clicks (separate from Screen so /today polls cannot wipe them).
    logByKey = {};
    sections.forEach(function (sec) {
      (sec.rows || []).forEach(function (r) {
        const row = Object.assign({}, r, {
          trade_date: String(r.trade_date || sec.date || '').slice(0, 10),
        });
        logByKey[cardKey(row)] = row;
      });
    });

    const head = logHeaderHtml(logSort);
    host.innerHTML = sections
      .map(function (sec) {
        const rows = sortLogRows(sec.rows || [], logSort)
          .map(function (r) {
            const row = Object.assign({}, r, {
              trade_date: String(r.trade_date || sec.date || '').slice(0, 10),
            });
            const key = cardKey(row);
            const active = !!r.active;
            const wait = isWaitList(r);
            const rowCls = active ? 'olh-log-active' : wait ? 'olh-log-wait' : 'olh-log-grey';
            return `<tr class="${rowCls}">
            <td><button type="button" class="olh-sym-btn olh-sym-btn--table" data-olh-chart="${escapeHtml(key)}">${escapeHtml(r.symbol || '')}</button></td>
            <td>${escapeHtml(r.side || '')}</td>
            <td>${escapeHtml(stateLabel(r))}</td>
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
            <thead><tr>${head}</tr></thead>
            <tbody>${rows}</tbody>
          </table></div>`;
      })
      .join('');
  }

  function onLogSortClick(ev) {
    const th = ev.target.closest('[data-olh-sort]');
    if (!th || !document.getElementById('olhLogHost') || !document.getElementById('olhLogHost').contains(th)) {
      return;
    }
    ev.preventDefault();
    const key = th.getAttribute('data-olh-sort');
    if (!key) return;
    if (logSort && logSort.key === key) {
      logSort = { key: key, dir: logSort.dir === 'asc' ? 'desc' : 'asc' };
    } else {
      logSort = { key: key, dir: 'asc' };
    }
    renderLog(null);
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
    const card = cardForChartKey(key);
    if (!card) {
      window.alert('Chart data unavailable for this row');
      return;
    }
    openOlhChart(card);
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
    if (logHost) {
      logHost.addEventListener('click', onChartClick);
      logHost.addEventListener('click', onLogSortClick);
      logHost.addEventListener('keydown', function (ev) {
        if (ev.key !== 'Enter' && ev.key !== ' ') return;
        const th = ev.target.closest('[data-olh-sort]');
        if (!th) return;
        ev.preventDefault();
        th.click();
      });
    }
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
