/**
 * Open Low High — live screener UI (polls /api/open-low-high/today).
 */
(function () {
  'use strict';

  const POLL_MS = 20000;
  let pollTimer = null;
  let logLoaded = false;

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

  function badgeHtml(card) {
    const raw = String(card.sector_badge_display || card.sector_badge || '').trim();
    if (!raw) return '';
    const base = raw.replace(/\*$/, '');
    const isStrong = /^S[123]$/i.test(base);
    const isWeak = /^W[123]$/i.test(base);
    const cls = isStrong ? 'is-strong' : isWeak ? 'is-weak' : '';
    return `<span class="olh-sector-badge ${cls}" title="${card.nifty_sector_label || ''}">${raw}</span>`;
  }

  function cardHtml(card) {
    const side = String(card.side || '').toUpperCase();
    const bull = side === 'BULLISH';
    return `
      <article class="olh-card ${bull ? 'is-bullish' : 'is-bearish'}">
        <div class="olh-card-top">
          <span class="olh-sym">${escapeHtml(card.symbol || '')}</span>
          <span class="olh-side-chip ${bull ? 'bull' : 'bear'}">${bull ? 'BULL' : 'BEAR'}</span>
        </div>
        <div class="olh-contract">${escapeHtml(card.contract || card.instrument_key || '')}</div>
        <dl class="olh-stats">
          <div><dt>Prev close</dt><dd>${fmt(card.prev_close)}</dd></div>
          <div><dt>VWAP 09:15</dt><dd>${fmt(card.vwap_0915)}</dd></div>
          <div><dt>09:15 close</dt><dd>${fmt(card.first_close)}</dd></div>
          <div><dt>09:20 close</dt><dd>${fmt(card.second_close)}</dd></div>
          <div><dt>Prev high</dt><dd>${fmt(card.prev_high)}</dd></div>
          <div><dt>Prev low</dt><dd>${fmt(card.prev_low)}</dd></div>
        </dl>
        <div class="olh-badges">${badgeHtml(card)}</div>
      </article>`;
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
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

  function renderScreen(data) {
    const bull = data.bullish || [];
    const bear = data.bearish || [];
    const bullGrid = document.getElementById('olhBullGrid');
    const bearGrid = document.getElementById('olhBearGrid');
    const bullEmpty = document.getElementById('olhBullEmpty');
    const bearEmpty = document.getElementById('olhBearEmpty');
    document.getElementById('olhBullCount').textContent = String(bull.length);
    document.getElementById('olhBearCount').textContent = String(bear.length);

    if (bullGrid) {
      bullGrid.innerHTML = bull.map(cardHtml).join('');
      bullGrid.hidden = bull.length === 0;
    }
    if (bearGrid) {
      bearGrid.innerHTML = bear.map(cardHtml).join('');
      bearGrid.hidden = bear.length === 0;
    }
    if (bullEmpty) bullEmpty.hidden = bull.length > 0;
    if (bearEmpty) bearEmpty.hidden = bear.length > 0;

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

  async function loadToday() {
    try {
      const data = await api('/api/open-low-high/today');
      renderScreen(data);
    } catch (err) {
      setBanner('Failed to load: ' + (err && err.message ? err.message : err), true);
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
      setTimeout(loadToday, 3000);
      setTimeout(loadToday, 12000);
    } catch (err) {
      setBanner('Force run failed: ' + (err && err.message ? err.message : err), true);
    }
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
    host.innerHTML = sections
      .map((sec) => {
        const rows = (sec.rows || [])
          .map(
            (r) => `<tr>
            <td>${escapeHtml(r.symbol || '')}</td>
            <td>${escapeHtml(r.side || '')}</td>
            <td>${escapeHtml(r.contract || '')}</td>
            <td>${fmt(r.prev_close)}</td>
            <td>${fmt(r.first_close)}</td>
            <td>${fmt(r.second_close)}</td>
            <td>${fmt(r.vwap_0915)}</td>
            <td>${escapeHtml(r.sector_badge_display || r.sector_badge || '')}</td>
          </tr>`
          )
          .join('');
        return `<h3 class="olh-log-date">${escapeHtml(sec.date)}</h3>
          <div class="olh-table-wrap"><table class="olh-table">
            <thead><tr>
              <th>Symbol</th><th>Side</th><th>Contract</th>
              <th>Prev close</th><th>09:15</th><th>09:20</th><th>VWAP</th><th>Tag</th>
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
    document.querySelectorAll('.olh-tab').forEach((btn) => {
      const on = btn.getAttribute('data-olh-tab') === name;
      btn.classList.toggle('active', on);
      btn.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    document.querySelectorAll('[data-olh-panel]').forEach((panel) => {
      panel.hidden = panel.getAttribute('data-olh-panel') !== name;
    });
    if (name === 'log' && !logLoaded) loadLog();
  }

  function bind() {
    document.querySelectorAll('.olh-tab').forEach((btn) => {
      btn.addEventListener('click', () => switchTab(btn.getAttribute('data-olh-tab')));
    });
    const refresh = document.getElementById('olhRefreshBtn');
    if (refresh) refresh.addEventListener('click', () => loadToday());
    const force = document.getElementById('olhForceBtn');
    if (force) force.addEventListener('click', () => forceRun());
    const logReload = document.getElementById('olhLogReload');
    if (logReload) logReload.addEventListener('click', () => loadLog());
    const logSide = document.getElementById('olhLogSide');
    if (logSide) logSide.addEventListener('change', () => loadLog());
  }

  function startPoll() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(loadToday, POLL_MS);
  }

  document.addEventListener('DOMContentLoaded', () => {
    bind();
    loadToday();
    startPoll();
  });
})();
