/* Kosmic Tarang Phase 2 — screener + paper ticket + data health */
(function () {
  const token = () => localStorage.getItem('trademanthan_token') || '';
  let currentTicketId = null;

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
    if (res.status === 403) {
      throw new Error('Administrator only');
    }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.detail || data.message || res.statusText);
    return data;
  }

  function setModeBadges(mode, auto) {
    const modeText = mode || 'PAPER';
    const autoText = auto ? 'AUTO ON' : 'AUTO OFF';
    ['modeBadge', 'modeBadgeMobile'].forEach((id) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = modeText;
      el.classList.toggle('tg-badge-paper', String(modeText).toUpperCase() === 'PAPER');
    });
    const autoEl = document.getElementById('autoBadge');
    if (autoEl) {
      autoEl.textContent = autoText;
      autoEl.classList.toggle('tg-badge-warn', !!auto);
    }
  }

  function fmtLoss(row) {
    if (row.currency === 'INR') return `₹${Number(row.max_loss_zero_credit).toLocaleString('en-IN')}`;
    return `$${Number(row.max_loss_zero_credit_usd).toFixed(2)}`;
  }

  function switchTab(name) {
    document.querySelectorAll('.tg-tab').forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.tab === name);
    });
    document.querySelectorAll('.tg-tab-panel').forEach((panel) => {
      panel.hidden = panel.id !== `tab-${name}`;
    });
  }

  document.querySelectorAll('.tg-tab').forEach((btn) => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  function renderFeeds(feeds) {
    const host = document.getElementById('feedGrid');
    host.innerHTML = '';
    for (const [name, f] of Object.entries(feeds || {})) {
      const ok = !!f.ok || !!f.public_ok;
      const card = document.createElement('div');
      card.className = 'tg-feed-card';
      const auth = f.auth || {};
      card.innerHTML = `
        <h3>${name}</h3>
        <div><span class="tg-inline-badge ${ok ? 'tg-badge-ok' : 'tg-badge-bad'}">${f.status || (ok ? 'ok' : 'fail')}</span></div>
        <pre class="tg-pre" style="margin-top:8px;">${JSON.stringify({ ...f, auth: auth }, null, 2)}</pre>
      `;
      host.appendChild(card);
    }
  }

  function renderLoss(table) {
    const body = document.getElementById('lossBody');
    const rows = (table && table.rows) || [];
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="5" class="tg-muted">No rows</td></tr>';
      return;
    }
    body.innerHTML = rows.map((r) => {
      const fit = r.currency === 'INR' ? r.fits_energy_budget : r.fits_crypto_budget;
      return `<tr>
        <td>${r.underlying} <span class="tg-muted">(${r.profile_id})</span></td>
        <td>${r.width_label}</td>
        <td>${fmtLoss(r)}</td>
        <td>₹${Number(r.budget_ref_inr).toLocaleString('en-IN')}</td>
        <td class="${fit ? 'tg-fit-yes' : 'tg-fit-no'}">${fit ? 'Yes' : 'No'}</td>
      </tr>`;
    }).join('');
  }

  function statusChip(st) {
    const s = String(st || '—').toUpperCase().replace(/\s+/g, '_');
    return `<span class="tg-status-chip tg-status-${s}">${st || '—'}</span>`;
  }

  function renderScreener(byProfile, results) {
    const body = document.getElementById('screenBody');
    const rows = [];
    if (results && results.length) {
      results.forEach((r) => rows.push(r));
    } else {
      Object.values(byProfile || {}).forEach((c) => {
        const p = c.payload || {};
        rows.push({
          profile_id: c.profile_id,
          candidate_id: c.id,
          status: p.status || c.screen_status || c.status,
          structure: c.structure || p.structure,
          net_credit: p.net_credit,
          width: p.width,
          lots_or_contracts: p.lots_or_contracts,
          failed_gates: p.failed_gates,
        });
      });
    }
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="7" class="tg-muted">No candidates yet — run screener</td></tr>';
      return;
    }
    body.innerHTML = rows.map((r) => {
      const cid = r.candidate_id || r.id || '';
      const failed = (r.failed_gates || []).join(', ') || '—';
      const credit = r.net_credit != null ? Number(r.net_credit).toFixed(2) : '—';
      const width = r.width != null ? Number(r.width).toFixed(2) : '—';
      return `<tr>
        <td>${r.profile_id}</td>
        <td>${statusChip(r.status)}</td>
        <td>${r.structure || '—'}</td>
        <td>${credit} / ${width}</td>
        <td>${r.lots_or_contracts != null ? r.lots_or_contracts : '—'}</td>
        <td class="tg-muted">${failed}</td>
        <td>${cid ? `<button type="button" class="tg-btn tg-open-ticket" data-id="${cid}">Ticket</button>` : ''}</td>
      </tr>`;
    }).join('');
    body.querySelectorAll('.tg-open-ticket').forEach((btn) => {
      btn.addEventListener('click', () => {
        document.getElementById('ticketId').value = btn.dataset.id;
        switchTab('ticket');
        loadTicket(Number(btn.dataset.id));
      });
    });
  }

  function renderRejections(list) {
    const body = document.getElementById('rejBody');
    if (!list || !list.length) {
      body.innerHTML = '<tr><td colspan="4" class="tg-muted">None</td></tr>';
      return;
    }
    body.innerHTML = list.slice(0, 30).map((r) => `<tr>
      <td class="tg-muted">${(r.created_at || '').replace('T', ' ').slice(0, 19)}</td>
      <td>${r.profile_id}</td>
      <td>${r.gate_name}</td>
      <td>${r.detail || ''}</td>
    </tr>`).join('');
  }

  function renderTicket(t) {
    const panel = document.getElementById('ticketPanel');
    const actions = document.getElementById('ticketActions');
    if (!t || !t.ok) {
      panel.innerHTML = `<p class="tg-note">${(t && t.error) || 'Not found'}</p>`;
      actions.hidden = true;
      return;
    }
    currentTicketId = t.candidate_id;
    const legs = (t.legs || []).map((l) =>
      `<tr>
        <td>${l.side}</td>
        <td>${l.right} ${l.strike}</td>
        <td>${l.symbol || ''}</td>
        <td>${l.mid != null ? Number(l.mid).toFixed(2) : '—'}</td>
        <td>${l.limit_conservative != null ? Number(l.limit_conservative).toFixed(2) : '—'}</td>
        <td><button type="button" class="tg-btn tg-copy" data-copy="${l.symbol || l.instrument_key || ''}">Copy</button></td>
      </tr>`
    ).join('');
    const gates = (t.gates || []).map((g) =>
      `<li><span class="${g.passed ? 'tg-gate-pass' : 'tg-gate-fail'}">${g.passed ? 'PASS' : 'FAIL'}</span>
        <strong>${g.name}</strong> — ${g.detail || ''}</li>`
    ).join('');
    panel.innerHTML = `
      <p>${statusChip(t.status)} <strong>${t.profile_id}</strong> · ${t.structure || ''} · ${t.underlying || ''} ${t.expiry || ''}</p>
      <p class="tg-note">Net credit ${t.net_credit != null ? Number(t.net_credit).toFixed(3) : '—'} · width ${t.width != null ? Number(t.width).toFixed(2) : '—'} ·
        lots ${t.lots_or_contracts ?? '—'} · max loss/unit ₹${t.max_loss_per_unit_inr != null ? Number(t.max_loss_per_unit_inr).toLocaleString('en-IN') : '—'} ·
        risk ₹${t.candidate_risk_inr != null ? Number(t.candidate_risk_inr).toLocaleString('en-IN') : '—'}</p>
      <p class="tg-note">${t.note || ''}</p>
      <div class="tg-table-wrap"><table class="tg-table">
        <thead><tr><th>Side</th><th>Strike</th><th>Symbol</th><th>Mid</th><th>Conserv.</th><th></th></tr></thead>
        <tbody>${legs || '<tr><td colspan="6">No legs</td></tr>'}</tbody>
      </table></div>
      <h3 class="tg-section-title" style="margin-top:14px;">Gates</h3>
      <ul class="tg-gate-list">${gates || '<li class="tg-muted">—</li>'}</ul>
      <pre class="tg-pre" style="margin-top:12px;">${JSON.stringify(t.exit_levels || {}, null, 2)}</pre>
    `;
    actions.hidden = false;
    panel.querySelectorAll('.tg-copy').forEach((btn) => {
      btn.addEventListener('click', () => {
        navigator.clipboard.writeText(btn.dataset.copy || '').catch(() => {});
      });
    });
  }

  async function loadTicket(id) {
    const status = document.getElementById('statusLine');
    status.textContent = `Loading ticket ${id}…`;
    try {
      const t = await api(`/api/tarang/ticket/${id}`);
      renderTicket(t);
      status.textContent = `Ticket ${id} loaded`;
    } catch (err) {
      renderTicket({ ok: false, error: String(err.message || err) });
      status.textContent = String(err.message || err);
    }
  }

  async function loadHealth() {
    const h = await api('/api/tarang/health');
    setModeBadges(h.mode, h.auto);
    const banner = document.getElementById('tokenBanner');
    if (h.token_expired) {
      banner.hidden = false;
      banner.textContent = 'Upstox token missing/expired or MCX master incomplete — refresh OAuth (see runbook).';
    } else {
      banner.hidden = true;
    }
    renderFeeds(h.feeds);
    const e = (h.risk && h.risk.buckets && h.risk.buckets.ENERGY) || {};
    const c = (h.risk && h.risk.buckets && h.risk.buckets.CRYPTO) || {};
    document.getElementById('budgetBox').textContent = JSON.stringify({ ENERGY: e, CRYPTO: c }, null, 2);
    renderLoss(h.min_max_loss);
    document.getElementById('ivCounts').textContent = JSON.stringify(h.iv_snapshot_counts || {}, null, 2);
    document.getElementById('wsBox').textContent = JSON.stringify(h.upstox_ws || {}, null, 2);
    return h;
  }

  async function loadScreenerView() {
    const [scr, rej] = await Promise.all([
      api('/api/tarang/screener'),
      api('/api/tarang/rejections?limit=40'),
    ]);
    setModeBadges(scr.mode, scr.auto);
    renderScreener(scr.by_profile, null);
    renderRejections(rej.rejections || []);
  }

  async function load() {
    const status = document.getElementById('statusLine');
    status.textContent = 'Loading…';
    try {
      await loadHealth();
      await loadScreenerView();
      status.textContent = 'Ready';
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  }

  document.getElementById('btnRefresh').addEventListener('click', load);
  document.getElementById('btnScreen').addEventListener('click', async () => {
    const status = document.getElementById('statusLine');
    status.textContent = 'Running screener (live chains)…';
    switchTab('screener');
    try {
      const out = await api('/api/tarang/screener/run', { method: 'POST' });
      renderScreener(null, out.results || []);
      const rej = await api('/api/tarang/rejections?limit=40');
      renderRejections(rej.rejections || []);
      status.textContent = `Screener done at ${out.run_at || ''}`;
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });
  document.getElementById('btnIvSnap').addEventListener('click', async () => {
    const status = document.getElementById('statusLine');
    status.textContent = 'Running IV snapshot…';
    try {
      const out = await api('/api/tarang/iv-snapshots/run', { method: 'POST' });
      status.textContent = `IV snapshot done: ${JSON.stringify(out.results || out)}`;
      await loadHealth();
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });
  document.getElementById('btnLoadTicket').addEventListener('click', () => {
    const id = Number(document.getElementById('ticketId').value);
    if (id) loadTicket(id);
  });
  document.getElementById('btnTake').addEventListener('click', async () => {
    if (!currentTicketId) return;
    if (!window.confirm(`Take PAPER trade for candidate ${currentTicketId}? No live orders will be placed.`)) return;
    const status = document.getElementById('statusLine');
    try {
      const out = await api(`/api/tarang/ticket/${currentTicketId}/take`, {
        method: 'POST',
        body: JSON.stringify({ note: '' }),
      });
      status.textContent = `PAPER trade #${out.trade_id} recorded`;
      await loadTicket(currentTicketId);
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });
  document.getElementById('btnDismiss').addEventListener('click', async () => {
    if (!currentTicketId) return;
    const status = document.getElementById('statusLine');
    try {
      await api(`/api/tarang/ticket/${currentTicketId}/dismiss`, { method: 'POST', body: '{}' });
      status.textContent = `Candidate ${currentTicketId} dismissed`;
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });
  document.getElementById('btnManual').addEventListener('click', async () => {
    if (!currentTicketId) return;
    const note = window.prompt('Optional note for manual PAPER mark:') || '';
    const status = document.getElementById('statusLine');
    try {
      const out = await api(`/api/tarang/ticket/${currentTicketId}/mark-manual`, {
        method: 'POST',
        body: JSON.stringify({ note, fills: {} }),
      });
      status.textContent = `Manual PAPER trade #${out.trade_id} recorded`;
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });

  load();
})();
