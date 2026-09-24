/* Kosmic Tarang — Screener / Trade / Report */
(function () {
  const token = () => localStorage.getItem('trademanthan_token') || '';
  let selectedProfile = null;
  let lastSimple = null;
  let lastCandidate = null;
  let pollTimer = null;
  let labels = { gates: {} };

  async function api(path, opts = {}) {
    const res = await fetch(path, {
      ...opts,
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${token()}`,
        'Content-Type': 'application/json',
        ...(opts.headers || {}),
      },
    });
    if (res.status === 401) {
      window.location.href = 'index.html';
      throw new Error('unauthorized');
    }
    if (res.status === 403) throw new Error('Administrator only');
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.detail || data.message || res.statusText);
    return data;
  }

  function fmtInr(n) {
    if (n == null || Number.isNaN(Number(n))) return '—';
    const v = Math.round(Number(n));
    const sign = v < 0 ? '-' : '';
    const s = String(Math.abs(v));
    let body;
    if (s.length <= 3) body = s;
    else {
      const last3 = s.slice(-3);
      let rest = s.slice(0, -3);
      const parts = [];
      while (rest.length > 2) {
        parts.unshift(rest.slice(-2));
        rest = rest.slice(0, -2);
      }
      if (rest) parts.unshift(rest);
      body = parts.join(',') + ',' + last3;
    }
    return `${sign}₹${body}`;
  }
  function fmtPx(n) {
    if (n == null || Number.isNaN(Number(n))) return '—';
    return Number(n).toFixed(2);
  }
  function fmtUsd(n) {
    if (n == null || Number.isNaN(Number(n))) return '—';
    return `$${Number(n).toFixed(2)}`;
  }
  function setBadges(mode) {
    const modeText = String(mode || 'Forward test');
    ['modeBadge', 'modeBadgeMobile'].forEach((id) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = modeText;
    });
    const autoEl = document.getElementById('autoBadge');
    if (autoEl) autoEl.textContent = 'Auto orders: locked';
  }

  function switchTab(name) {
    document.querySelectorAll('.tg-tab').forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.tab === name);
    });
    document.querySelectorAll('.tg-tab-panel').forEach((panel) => {
      panel.hidden = panel.id !== `tab-${name}`;
    });
    stopPoll();
    if (name === 'screener') loadScreener();
    if (name === 'trade') {
      loadTrade();
      startPoll();
    }
    if (name === 'report') loadReport();
  }
  document.querySelectorAll('.tg-tab').forEach((btn) => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  function startPoll() {
    stopPoll();
    pollTimer = setInterval(() => {
      if (document.hidden) return;
      const tradeTab = document.getElementById('tab-trade');
      if (tradeTab && !tradeTab.hidden) refreshActiveQuiet();
    }, 5000);
  }
  function stopPoll() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) return;
    const tradeTab = document.getElementById('tab-trade');
    if (tradeTab && !tradeTab.hidden) refreshActiveQuiet();
  });

  function escHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function whyIcon(outcome) {
    if (outcome === 'pass') {
      return '<span class="tg-why-icon tg-why-pass" title="Passed" aria-label="Passed">✓</span>';
    }
    if (outcome === 'neutral') {
      return '<span class="tg-why-icon tg-why-neutral" title="Not evaluated" aria-label="Not evaluated">–</span>';
    }
    return '<span class="tg-why-icon tg-why-fail" title="Failed" aria-label="Failed">✕</span>';
  }

  function whyPop(why) {
    const items = why || [];
    if (!items.length) return '';
    const lis = items.map((w) => {
      if (typeof w === 'string') {
        return `<li class="tg-why-item"><span class="tg-why-icon tg-why-fail" aria-hidden="true">✕</span><span class="tg-why-body">${escHtml(w)}</span></li>`;
      }
      const outcome = w.outcome || (w.passed ? 'pass' : 'fail');
      const label = w.label || w.text || '';
      const obs = w.observed != null && w.observed !== '' ? w.observed : '—';
      const acc = w.accepted != null && w.accepted !== '' ? w.accepted : '—';
      return `<li class="tg-why-item tg-why-${outcome}">
        ${whyIcon(outcome)}
        <span class="tg-why-body">
          <span class="tg-why-label">${escHtml(label)}</span>
          <span class="tg-why-metrics">observed <strong>${escHtml(obs)}</strong> · need <strong>${escHtml(acc)}</strong></span>
        </span>
      </li>`;
    }).join('');
    return `<details class="tg-why"><summary>Why?</summary><ul class="tg-why-list">${lis}</ul></details>`;
  }

  function renderScreener(data) {
    lastSimple = data;
    setBadges(data.display_mode);
    const host = document.getElementById('screenerList');
    const empty = document.getElementById('screenerEmpty');
    const rows = data.rows || [];
    const anyQ = rows.some((r) => r.qualified);
    if (!anyQ) {
      empty.hidden = false;
      empty.textContent = `Nothing qualifies right now. Last checked ${data.last_checked_ist || '—'}.`;
    } else {
      empty.hidden = true;
    }
    if (!rows.length) {
      host.innerHTML = '<p class="tg-muted">No symbols to show.</p>';
      return;
    }
    host.innerHTML = rows.map((r) => `
      <article class="tg-row-card">
        <div class="tg-row-main">
          <strong>${r.display_name}</strong>
          <span class="tg-status-chip tg-status-${r.status}">${r.status === 'QUALIFIED' ? 'Qualified' : 'Watching'}</span>
        </div>
        <p class="tg-one-line">${r.headline || ''}</p>
        <div class="tg-row-actions">
          ${whyPop(r.why)}
          ${r.qualified ? `<button type="button" class="tg-btn tg-btn-primary tg-view" data-id="${r.candidate_id}" data-profile="${r.profile_id}">View</button>` : ''}
        </div>
      </article>
    `).join('');
    host.querySelectorAll('.tg-view').forEach((btn) => {
      btn.addEventListener('click', () => {
        selectedProfile = btn.dataset.profile;
        lastCandidate = { id: Number(btn.dataset.id), profile_id: selectedProfile };
        switchTab('trade');
      });
    });
  }

  async function loadScreener() {
    const status = document.getElementById('statusLine');
    try {
      labels = await api('/api/tarang/labels').catch(() => labels);
      const data = await api('/api/tarang/screener/simple');
      renderScreener(data);
      status.textContent = `Last checked ${data.last_checked_ist || ''}`;
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  }

  function progressBar(frac, label) {
    const f = frac == null ? 1 : Number(frac);
    const pct = Math.round((1 - Math.min(1, Math.max(0, f))) * 100);
    return `<div class="tg-progress"><div class="tg-progress-fill" style="width:${pct}%"></div><span class="tg-progress-label">${label} ${pct}%</span></div>`;
  }

  function renderActive(views) {
    const host = document.getElementById('activeList');
    if (!views.length) {
      host.innerHTML = '<p class="tg-muted">No active trades.</p>';
      return;
    }
    host.innerHTML = views.map((v) => {
      const t = v.trade || {};
      const mtm = v.mtm || {};
      const ev = v.exit_eval || {};
      const stale = v.quotes_status && v.quotes_status !== 'live';
      const legs = (mtm.per_leg || []).map((l) => `
        <tr>
          <td>${l.side_open || l.side || ''}</td>
          <td>${l.symbol || l.instrument_key || ''}</td>
          <td>${fmtPx(l.entry_price)}</td>
          <td>${fmtPx(l.ltp != null ? l.ltp : l.mark)}${l.mark_source === 'mid_fallback' ? ' <span class="tg-muted">mid</span>' : ''}</td>
          <td>${fmtInr(l.pnl_inr)}</td>
        </tr>`).join('');
      const usd = mtm.usd_note ? ` · ${mtm.usd_note}` : '';
      const secs = ev.seconds_to_hard_exit;
      const cd = secs != null ? `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m` : '—';
      const bars = (ev.triggers || []).slice(0, 3).map((tr) => progressBar(tr.distance_frac, tr.reason === 'PROFIT_TARGET' ? 'Target' : (tr.reason || '').includes('HARD') || (tr.reason || '').includes('TIME') ? 'Time stop' : 'Stop')).join('');
      const age = t.entry_at ? (Date.now() - Date.parse(t.entry_at)) / 1000 : 9999;
      const canVoid = age <= 300;
      return `<article class="tg-intrade-card">
        <header class="tg-intrade-head">
          <div>
            <strong>${t.display_symbol || t.profile_id}</strong>
            ${t.display_structure || t.structure || ''}
            <span class="tg-muted">${t.entry_at_ist || ''}</span>
            ${v.unconfirmed ? '<span class="tg-inline-badge tg-badge-warn">Unconfirmed fills</span>' : ''}
            ${stale ? '<span class="tg-inline-badge tg-badge-bad">Data stale</span>' : ''}
          </div>
          <div class="tg-pnl ${Number(v.pnl_inr) >= 0 ? 'tg-pnl-pos' : 'tg-pnl-neg'}">${fmtInr(v.pnl_inr)}${usd}</div>
        </header>
        ${v.unconfirmed ? '<p class="tg-banner">Confirm your actual fills.</p>' : ''}
        <p class="tg-note">${v.pct_of_max_profit != null ? Math.round(v.pct_of_max_profit) + '% of max profit' : ''}
          ${v.pct_of_max_loss != null ? ' · ' + Math.round(v.pct_of_max_loss) + '% of max loss' : ''}
          · hard exit ${cd}</p>
        <div class="tg-triggers">${bars}</div>
        <div class="tg-table-wrap"><table class="tg-table">
          <thead><tr><th>Side</th><th>Contract</th><th>Entry</th><th>LTP</th><th>P&amp;L</th></tr></thead>
          <tbody>${legs || '<tr><td colspan="5">—</td></tr>'}</tbody>
        </table></div>
        <div class="tg-ticket-actions" style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
          <button type="button" class="tg-btn tg-edit" data-id="${t.id}">Edit</button>
          <button type="button" class="tg-btn tg-btn-danger tg-exit" data-id="${t.id}">Exit</button>
          ${canVoid ? `<button type="button" class="tg-btn tg-void" data-id="${t.id}">Cancel (started by mistake)</button>` : ''}
        </div>
      </article>`;
    }).join('');
    host.querySelectorAll('.tg-edit').forEach((b) => b.addEventListener('click', () => openEdit(Number(b.dataset.id))));
    host.querySelectorAll('.tg-exit').forEach((b) => b.addEventListener('click', () => openExit(Number(b.dataset.id), views)));
    host.querySelectorAll('.tg-void').forEach((b) => b.addEventListener('click', async () => {
      const reason = window.prompt('Why cancel? (required)') || '';
      if (!reason.trim()) return;
      await api(`/api/tarang/trades/${b.dataset.id}/void`, { method: 'POST', body: JSON.stringify({ reason }) });
      await loadTrade();
      await loadScreener();
    }));
  }

  function renderCandidate(t, profile) {
    const host = document.getElementById('candidatePanel');
    if (!t || !t.ok) {
      host.innerHTML = `<p class="tg-note">${(t && t.error) || 'No setup for this symbol.'}</p>`;
      return;
    }
    const q = String(t.status || '').toUpperCase().replace(/\s+/g, '_') === 'QUALIFIED';
    const legs = (t.legs || []).map((l) => `
      <tr>
        <td>${l.side}</td>
        <td>${l.strike} ${l.right || ''}</td>
        <td>${l.symbol || l.instrument_key || ''}</td>
        <td>${fmtPx(l.mid != null ? l.mid : l.limit_conservative)}</td>
        <td><button type="button" class="tg-btn tg-copy" data-copy="${l.symbol || l.instrument_key || ''}">Copy</button></td>
      </tr>`).join('');
    const exits = t.exit_levels || {};
    const exitWords = [
      exits.profit_target ? 'Take profit near the target credit' : null,
      exits.stop || exits.credit_stop ? 'Stop if the credit blows through' : null,
      'Time stop at the session hard-exit',
    ].filter(Boolean).join('. ');
    host.innerHTML = `
      <p><strong>${(lastSimple && lastSimple.rows || []).find((r) => r.profile_id === profile)?.display_name || profile}</strong>
        · ${t.structure || ''} · expiry ${t.expiry || '—'} · DTE ${t.dte || '—'}</p>
      <p class="tg-note">Net credit ${fmtPx(t.net_credit)} · max profit ${fmtInr(t.max_profit_approx_inr)} · max loss ${fmtInr(t.candidate_risk_inr || t.max_loss_per_unit_inr)} · lots ${t.lots_or_contracts ?? '—'}</p>
      <p class="tg-note">${exitWords}</p>
      <div class="tg-table-wrap"><table class="tg-table">
        <thead><tr><th>Side</th><th>Strike / type</th><th>Contract</th><th>Suggested</th><th></th></tr></thead>
        <tbody>${legs || '<tr><td colspan="5">—</td></tr>'}</tbody>
      </table></div>
      <div class="tg-ticket-actions" style="margin-top:10px;">
        <button type="button" class="tg-btn tg-btn-primary" id="btnTrade" ${q ? '' : 'disabled'}>Trade</button>
        ${q ? '' : `<span class="tg-muted">Not qualified${t.failed_gates && t.failed_gates[0] ? ' — ' + (labels.gates[t.failed_gates[0]] || t.failed_gates[0]) : ''}</span>`}
      </div>`;
    host.querySelectorAll('.tg-copy').forEach((btn) => {
      btn.addEventListener('click', () => navigator.clipboard.writeText(btn.dataset.copy || '').catch(() => {}));
    });
    const tradeBtn = document.getElementById('btnTrade');
    if (tradeBtn) {
      tradeBtn.addEventListener('click', async () => {
        if (!window.confirm(`Start ${profile} at suggested prices?`)) return;
        const out = await api(`/api/tarang/ticket/${t.candidate_id}/start`, {
          method: 'POST',
          body: JSON.stringify({ note: '', holding_mode: 'INTRADAY' }),
        });
        document.getElementById('statusLine').textContent = out.message || `Trade #${out.trade_id}`;
        await loadTrade();
        await loadScreener();
      });
    }
  }

  async function loadTrade() {
    const status = document.getElementById('statusLine');
    try {
      const [simple, active] = await Promise.all([
        api('/api/tarang/screener/simple'),
        api('/api/tarang/trades/active'),
      ]);
      lastSimple = simple;
      setBadges(simple.display_mode);
      renderActive(active.trades || []);
      const chips = document.getElementById('symbolChips');
      const free = (simple.rows || []);
      chips.innerHTML = free.map((r) =>
        `<button type="button" class="tg-chip ${selectedProfile === r.profile_id ? 'active' : ''}" data-p="${r.profile_id}" data-id="${r.candidate_id || ''}">${r.display_name}</button>`
      ).join('') || '<span class="tg-muted">No setup — an active trade is hiding this symbol.</span>';
      chips.querySelectorAll('.tg-chip').forEach((btn) => {
        btn.addEventListener('click', async () => {
          selectedProfile = btn.dataset.p;
          chips.querySelectorAll('.tg-chip').forEach((c) => c.classList.toggle('active', c === btn));
          if (btn.dataset.id) {
            const t = await api(`/api/tarang/ticket/${btn.dataset.id}`);
            lastCandidate = t;
            renderCandidate(t, selectedProfile);
          }
        });
      });
      if (selectedProfile) {
        const row = free.find((r) => r.profile_id === selectedProfile);
        if (row && row.candidate_id) {
          const t = await api(`/api/tarang/ticket/${row.candidate_id}`);
          renderCandidate(t, selectedProfile);
        } else {
          document.getElementById('candidatePanel').innerHTML = '<p class="tg-muted">No candidate.</p>';
        }
      } else {
        document.getElementById('candidatePanel').innerHTML = '<p class="tg-muted">Pick a symbol.</p>';
      }
      status.textContent = 'Trade';
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  }

  async function refreshActiveQuiet() {
    try {
      const active = await api('/api/tarang/trades/active');
      renderActive(active.trades || []);
    } catch (e) { /* ignore */ }
  }

  async function openEdit(id) {
    const v = await api(`/api/tarang/quotes?trade_id=${id}`);
    const t = v.trade || {};
    const fills = ((v.mtm && v.mtm.per_leg) || t.meta && t.meta.entry_fills) || [];
    const rows = (fills.length ? fills : (t.legs || [])).map((l, i) =>
      `Leg ${i + 1} ${l.side_open || l.side || ''} ${l.symbol || ''} price <input data-i="${i}" class="tg-input tg-edit-px" value="${l.entry_price != null ? l.entry_price : (l.price || '')}" />`
    ).join('<br/>');
    const root = document.getElementById('modalRoot');
    root.innerHTML = `<div class="tg-modal"><div class="tg-modal-card">
      <h3>Edit fills</h3>
      <p class="tg-note">Original signal stays saved. This is an override.</p>
      ${rows}
      <p>Lots <input id="editLots" class="tg-input" value="${t.lots_or_contracts || 1}" /></p>
      <p>Fees ₹ <input id="editFees" class="tg-input" value="${t.fees_total || 0}" /></p>
      <p>Notes <input id="editNotes" class="tg-input" value="${t.notes || ''}" /></p>
      <label><input type="checkbox" id="editConfirm" checked /> these are my actual fills</label>
      <div class="tg-ticket-actions" style="margin-top:10px;">
        <button type="button" class="tg-btn tg-btn-primary" id="editSave">Save</button>
        <button type="button" class="tg-btn" id="editClose">Close</button>
      </div>
    </div></div>`;
    document.getElementById('editClose').onclick = () => { root.innerHTML = ''; };
    document.getElementById('editSave').onclick = async () => {
      const px = [...document.querySelectorAll('.tg-edit-px')];
      const bodyFills = px.map((inp, i) => {
        const src = fills[i] || {};
        return {
          leg_index: i,
          side: src.side_open || src.side,
          symbol: src.symbol,
          qty: src.qty || t.lots_or_contracts,
          price: Number(inp.value),
        };
      });
      await api(`/api/tarang/trades/${id}/fills`, {
        method: 'PATCH',
        body: JSON.stringify({
          fills: bodyFills,
          lots: Number(document.getElementById('editLots').value),
          fees: Number(document.getElementById('editFees').value),
          notes: document.getElementById('editNotes').value,
          confirm: document.getElementById('editConfirm').checked,
        }),
      });
      root.innerHTML = '';
      await loadTrade();
    };
  }

  async function openExit(id, views) {
    const v = (views || []).find((x) => x.trade && x.trade.id === id) || await api(`/api/tarang/quotes?trade_id=${id}`);
    const legs = (v.mtm && v.mtm.per_leg) || [];
    const root = document.getElementById('modalRoot');
    root.innerHTML = `<div class="tg-modal"><div class="tg-modal-card">
      <h3>Exit</h3>
      ${legs.map((l, i) => `Leg ${i + 1} ${l.side_open || ''} <input class="tg-input tg-ex-px" data-i="${i}" value="${fmtPx(l.ltp != null ? l.ltp : l.mark)}" />`).join('<br/>')}
      <p>Reason
        <select id="exReason" class="tg-input">
          <option value="PROFIT_TARGET">Target</option>
          <option value="CREDIT_STOP">Stop</option>
          <option value="TIME_STOP">Time stop</option>
          <option value="MANUAL" selected>Manual</option>
          <option value="OTHER">Other</option>
        </select>
      </p>
      <p>Notes <input id="exNotes" class="tg-input" /></p>
      <div class="tg-ticket-actions">
        <button type="button" class="tg-btn tg-btn-danger" id="exGo">Close trade</button>
        <button type="button" class="tg-btn" id="exNo">Back</button>
      </div>
    </div></div>`;
    document.getElementById('exNo').onclick = () => { root.innerHTML = ''; };
    document.getElementById('exGo').onclick = async () => {
      const fills = [...document.querySelectorAll('.tg-ex-px')].map((inp, i) => {
        const src = legs[i] || {};
        return { leg_index: i, side: src.side_open, symbol: src.symbol, qty: src.qty, price: Number(inp.value) };
      });
      const out = await api(`/api/tarang/trades/${id}/exit`, {
        method: 'POST',
        body: JSON.stringify({
          reason: document.getElementById('exReason').value,
          note: document.getElementById('exNotes').value,
          fills,
        }),
      });
      root.innerHTML = '';
      document.getElementById('statusLine').textContent = `Closed · net ${fmtInr(out.net_pnl)}`;
      switchTab('report');
    };
  }

  function renderReport(rep) {
    const m = (rep.book === 'LIVE' ? (rep.metrics_live || rep.metrics) : (rep.metrics_forward_test || rep.metrics)) || {};
    const box = document.getElementById('reportMetrics');
    const stats = m.stats_ready
      ? `<div class="tg-metric"><span>Win rate</span><strong>${m.win_rate != null ? (m.win_rate * 100).toFixed(0) + '%' : '—'}</strong></div>
         <div class="tg-metric"><span>Profit factor</span><strong>${m.profit_factor != null ? Number(m.profit_factor).toFixed(2) : '—'}</strong></div>
         <div class="tg-metric"><span>Expectancy</span><strong>${fmtInr(m.expectancy)}</strong></div>`
      : `<div class="tg-metric"><span>Stats</span><strong>${m.stats_note || 'Too few trades to be meaningful'}</strong></div>`;
    box.innerHTML = `
      <div class="tg-metric"><span>Trades</span><strong>${m.count || 0}</strong></div>
      <div class="tg-metric"><span>Net P&amp;L</span><strong>${fmtInr(m.net_total)}</strong></div>
      ${stats}`;
    const body = document.getElementById('reportBody');
    const trades = (rep.trades || []).filter((t) => !t.voided);
    if (!trades.length) {
      body.innerHTML = '<tr><td colspan="7" class="tg-muted">No closed trades</td></tr>';
      return;
    }
    body.innerHTML = trades.map((t) => `<tr>
      <td>${t.exit_at_ist || t.entry_at_ist || ''}</td>
      <td>${t.display_symbol || t.profile_id}</td>
      <td>${t.display_structure || t.structure || ''}</td>
      <td>${t.entry_at_ist || ''}</td>
      <td>${t.exit_at_ist || ''}</td>
      <td class="${Number(t.net_pnl) >= 0 ? 'tg-pnl-pos' : 'tg-pnl-neg'}">${fmtInr(t.net_pnl)}</td>
      <td>${t.exit_reason || '—'}</td>
    </tr>`).join('');
  }

  async function loadReport() {
    const book = (document.querySelector('input[name="book"]:checked') || {}).value || 'FORWARD_TEST';
    document.getElementById('btnCsv').href = `/api/tarang/report.csv?book=${book}`;
    const rep = await api(`/api/tarang/report?book=${book}&limit=100`);
    renderReport(rep);
  }

  document.querySelectorAll('input[name="book"]').forEach((el) => {
    el.addEventListener('change', loadReport);
  });
  document.getElementById('btnRefresh').addEventListener('click', () => {
    const active = document.querySelector('.tg-tab.active');
    switchTab((active && active.dataset.tab) || 'screener');
  });
  document.getElementById('btnCsv').addEventListener('click', async (e) => {
    e.preventDefault();
    const book = (document.querySelector('input[name="book"]:checked') || {}).value || 'FORWARD_TEST';
    const res = await fetch(`/api/tarang/report.csv?book=${book}`, {
      headers: { Authorization: `Bearer ${token()}` },
    });
    const blob = await res.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `tarang_report_${book}.csv`;
    a.click();
  });

  loadScreener();
})();
