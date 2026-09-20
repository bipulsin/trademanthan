/* Kosmic Tarang Phase 3 — screener, ticket, In-Trade, Trade Report, data health */
(function () {
  const token = () => localStorage.getItem('trademanthan_token') || '';
  let currentTicketId = null;
  let intradeTimer = null;

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

  function displayMode(mode, recordType) {
    const rt = String(recordType || '').toUpperCase();
    if (rt === 'LIVE' || String(mode || '').toUpperCase() === 'LIVE') return 'Live';
    return 'Forward test';
  }

  function setModeBadges(mode, auto) {
    const modeText = displayMode(mode);
    const autoText = 'Auto orders: locked';
    ['modeBadge', 'modeBadgeMobile'].forEach((id) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = modeText;
      el.classList.toggle('tg-badge-paper', modeText === 'Forward test');
    });
    const autoEl = document.getElementById('autoBadge');
    if (autoEl) {
      autoEl.textContent = autoText;
    }
    const autoBtn = document.getElementById('btnAutoToggle');
    if (autoBtn) {
      autoBtn.textContent = autoText;
      autoBtn.dataset.on = '0';
    }
  }

  function fmtInr(n) {
    if (n == null || Number.isNaN(Number(n))) return '—';
    const v = Number(n);
    const sign = v < 0 ? '−' : '';
    return `${sign}₹${Math.abs(v).toLocaleString('en-IN', { maximumFractionDigits: 0 })}`;
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
    if (name === 'intrade') loadInTrade();
    if (name === 'report') loadReport();
    if (name === 'backtest') loadBacktest();
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
      body.innerHTML = '<tr><td colspan="7" class="tg-muted">No rows</td></tr>';
      return;
    }
    body.innerHTML = rows.map((r) => {
      const fit = r.currency === 'INR' ? r.fits_energy_budget : r.fits_crypto_budget;
      const hard = r.currency === 'INR' ? r.fits_hard_cap : true;
      return `<tr>
        <td>${r.underlying} <span class="tg-muted">(${r.profile_id})</span></td>
        <td>${r.contract_family || '—'}</td>
        <td>${r.width_label}</td>
        <td>${fmtLoss(r)}</td>
        <td>₹${Number(r.budget_ref_inr).toLocaleString('en-IN')}</td>
        <td class="${fit ? 'tg-fit-yes' : 'tg-fit-no'}">${fit ? 'Yes' : 'No'}</td>
        <td class="${hard ? 'tg-fit-yes' : 'tg-fit-no'}">${r.currency === 'INR' ? (hard ? 'Yes' : 'No') : '—'}</td>
      </tr>`;
    }).join('');
  }

  function renderCoverage(cov) {
    const body = document.getElementById('coverageBody');
    if (!body) return;
    const rows = (cov && cov.underlyings) || [];
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="8" class="tg-muted">No full-chain snapshots yet — collection starts now.</td></tr>';
      return;
    }
    body.innerHTML = rows.map((r) => `<tr>
      <td>${r.underlying}</td>
      <td>${r.days}</td>
      <td>${r.days_wide != null ? r.days_wide : '—'}</td>
      <td>${r.n_narrow != null ? r.n_narrow : '—'}</td>
      <td class="tg-muted">${(r.earliest || '').replace('T', ' ').slice(0, 16)}</td>
      <td>${r.earliest_backtest_start || '—'}</td>
      <td>${r.six_month_backtest_ready_on || '—'}</td>
      <td class="${r.ready_for_6m_backtest ? 'tg-fit-yes' : 'tg-fit-no'}">${r.ready_for_6m_backtest ? 'Yes' : 'No'}</td>
    </tr>`).join('');
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
      <p class="tg-note">Round-trip fees ${t.fees_frac_of_credit != null ? (Number(t.fees_frac_of_credit) * 100).toFixed(1) + '% of gross credit' : '—'}
        (${t.round_trip_fees_inr != null ? '₹' + Number(t.round_trip_fees_inr).toFixed(0) : '—'} / credit ${t.gross_credit_inr != null ? '₹' + Number(t.gross_credit_inr).toFixed(0) : '—'})
        · net/contract ${t.net_credit_per_contract_inr != null ? '₹' + Number(t.net_credit_per_contract_inr).toFixed(2) : '—'}
        · max contracts order/trade ${t.max_contracts_per_order ?? '—'} / ${t.max_contracts_per_trade ?? '—'}</p>
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

  function progressBar(frac, label) {
    const pct = Math.round((1 - Math.min(1, Math.max(0, frac == null ? 1 : frac))) * 100);
    return `<div class="tg-progress" title="${label || ''}">
      <div class="tg-progress-fill" style="width:${pct}%"></div>
      <span class="tg-progress-label">${label || ''} ${pct}%</span>
    </div>`;
  }

  function renderInTrade(data) {
    const host = document.getElementById('intradeList');
    const trades = data.trades || [];
    if (!trades.length) {
      host.innerHTML = '<p class="tg-muted">No open trades. QUALIFIED rows record as Forward test automatically.</p>';
    } else {
      host.innerHTML = trades.map((v) => {
        const t = v.trade || {};
        const ev = v.exit_eval || {};
        const skip = v.skip_exits ? `<p class="tg-note tg-fit-no">Exits paused: ${v.quotes_status || 'stale quotes'}${v.gap_at_open ? ' · gap at open logged' : ''}</p>` : '';
        const triggers = (ev.triggers || []).map((tr) =>
          `<div class="tg-trigger ${tr.hit ? 'tg-trigger-hit' : ''}">
            <strong>${tr.reason}</strong> ${progressBar(tr.distance_frac, tr.hit ? 'HIT' : 'prox')}
            <span class="tg-muted">${tr.detail || ''}</span>
          </div>`
        ).join('');
        const legs = ((v.mtm && v.mtm.per_leg) || []).map((l) =>
          `<tr>
            <td>${l.side_open}</td><td>${l.right || ''} ${l.strike != null ? l.strike : ''}</td>
            <td>${l.mid != null ? Number(l.mid).toFixed(2) : '—'}</td>
            <td>${l.delta != null ? Number(l.delta).toFixed(3) : '—'}</td>
            <td>${l.entry_price != null ? Number(l.entry_price).toFixed(2) : '—'}</td>
          </tr>`
        ).join('');
        const g = v.net_greeks || {};
        const secs = ev.seconds_to_hard_exit;
        const cd = secs != null ? `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m` : '—';
        const usdNote = (v.mtm && v.mtm.usd_note) ? `<span class="tg-muted"> · ${v.mtm.usd_note}</span>` : '';
        return `<article class="tg-intrade-card" data-tid="${t.id}">
          <header class="tg-intrade-head">
            <div>
              <strong>#${t.id}</strong> ${statusChip(t.status)} <span class="tg-muted">${t.holding_mode || 'INTRADAY'}</span>
              ${t.display_mode || displayMode(t.mode, t.record_type)} ${t.display_verified ? '<span class="tg-inline-badge">Verified</span>' : ''}
              ${t.origin || t.auto_managed ? `<span class="tg-inline-badge">${t.origin || (t.auto_managed ? 'AUTO' : 'USER')}</span>` : ''}
              ${t.profile_id || ''} · ${t.structure || ''}
              <span class="tg-muted">${t.venue || ''}</span>
            </div>
            <div class="tg-pnl ${Number(v.pnl_inr) >= 0 ? 'tg-pnl-pos' : 'tg-pnl-neg'}">${fmtInr(v.pnl_inr)}${usdNote}</div>
          </header>
          <p class="tg-note">Max profit ${v.pct_of_max_profit != null ? Number(v.pct_of_max_profit).toFixed(0) + '%' : '—'} ·
            max loss used ${v.pct_of_max_loss != null ? Number(v.pct_of_max_loss).toFixed(0) + '%' : '—'} ·
            hard exit ${ev.hard_exit_at_ist || '—'} IST · countdown ${cd}</p>
          <p class="tg-note"><strong>Exit preview:</strong> ${v.exit_reason_preview || (ev.closest && ev.closest.reason) || '—'}</p>
          ${skip}
          <p class="tg-note">Greeks δ ${g.delta != null ? Number(g.delta).toFixed(3) : '—'} ·
            θ ${g.theta != null ? Number(g.theta).toFixed(3) : '—'} ·
            ν ${g.vega != null ? Number(g.vega).toFixed(3) : '—'}</p>
          <div class="tg-triggers">${triggers}</div>
          <div class="tg-table-wrap"><table class="tg-table">
            <thead><tr><th>Side</th><th>Strike</th><th>Mark</th><th>δ</th><th>Entry</th></tr></thead>
            <tbody>${legs || '<tr><td colspan="5">—</td></tr>'}</tbody>
          </table></div>
          <div class="tg-ticket-actions" style="display:flex;gap:8px;margin-top:10px;">
            <button type="button" class="tg-btn tg-btn-danger tg-exit-one" data-id="${t.id}">Exit</button>
            <button type="button" class="tg-btn tg-note-btn" data-id="${t.id}">Add note</button>
          </div>
        </article>`;
      }).join('');
      host.querySelectorAll('.tg-exit-one').forEach((btn) => {
        btn.addEventListener('click', async () => {
          if (!window.confirm(`Exit forward-test / recorded trade #${btn.dataset.id}?`)) return;
          const status = document.getElementById('statusLine');
          try {
            const out = await api(`/api/tarang/trades/${btn.dataset.id}/exit`, {
              method: 'POST',
              body: JSON.stringify({ reason: 'MANUAL', note: '' }),
            });
            status.textContent = `Closed #${out.trade_id} net ${fmtInr(out.net_pnl)}`;
            await loadInTrade();
          } catch (err) {
            status.textContent = String(err.message || err);
          }
        });
      });
      host.querySelectorAll('.tg-note-btn').forEach((btn) => {
        btn.addEventListener('click', async () => {
          const note = window.prompt('Note:') || '';
          if (!note) return;
          await api(`/api/tarang/trades/${btn.dataset.id}/note`, {
            method: 'POST',
            body: JSON.stringify({ note }),
          });
          await loadInTrade();
        });
      });
    }
    const alertBody = document.getElementById('alertBody');
    const alerts = data.alerts || [];
    if (!alerts.length) {
      alertBody.innerHTML = '<tr><td colspan="4" class="tg-muted">No alerts</td></tr>';
    } else {
      alertBody.innerHTML = alerts.slice(0, 25).map((a) => `<tr>
        <td class="tg-muted">${(a.created_at || '').replace('T', ' ').slice(0, 19)}</td>
        <td>${a.level}</td>
        <td>${a.message}</td>
        <td>${a.trade_id || '—'}</td>
      </tr>`).join('');
    }
  }

  function renderReport(rep) {
    const box = document.getElementById('reportMetrics');
    if (rep.book === 'ALL' && rep.metrics && rep.metrics.paired) {
      box.innerHTML = (rep.metrics.paired || []).map((p) => {
        const ft = p.forward_test;
        const lv = p.live;
        const fmt = (v) => (v == null ? '—' : (typeof v === 'number' && Math.abs(v) < 10 && p.metric !== 'count' ? Number(v).toFixed(2) : v));
        return `<div class="tg-metric"><span>${p.metric}</span><strong>FT ${fmt(ft)} · Live ${fmt(lv)}</strong></div>`;
      }).join('');
    } else {
      const m = (rep.book === 'LIVE' ? (rep.metrics_live || rep.metrics) : (rep.metrics_forward_test || rep.metrics)) || {};
      const wr = m.win_rate != null ? `${(m.win_rate * 100).toFixed(0)}%` : '—';
      const pf = m.profit_factor_infinite ? '∞' : (m.profit_factor != null ? Number(m.profit_factor).toFixed(2) : '—');
      box.innerHTML = `
        <div class="tg-metric"><span>Trades</span><strong>${m.count || 0}</strong></div>
        <div class="tg-metric"><span>Independent cycles</span><strong>${m.independent_cycles || 0}</strong></div>
        <div class="tg-metric"><span>Win rate</span><strong>${wr}</strong></div>
        <div class="tg-metric"><span>Profit factor</span><strong>${pf}</strong></div>
        <div class="tg-metric"><span>Expectancy</span><strong>${fmtInr(m.expectancy)}</strong></div>
        <div class="tg-metric"><span>Max DD</span><strong>${fmtInr(m.max_drawdown)}</strong></div>
        <div class="tg-metric"><span>Worst</span><strong>${fmtInr(m.worst_trade)}</strong></div>
        <div class="tg-metric"><span>Net</span><strong>${fmtInr(m.net_total)}</strong></div>
      `;
    }
    const body = document.getElementById('reportBody');
    const trades = rep.trades || [];
    if (!trades.length) {
      body.innerHTML = `<tr><td colspan="10" class="tg-muted">${rep.note || 'No closed trades'}</td></tr>`;
      return;
    }
    body.innerHTML = trades.map((t) => `<tr>
      <td>${t.id}</td>
      <td>${t.display_record_type || displayMode(t.mode, t.record_type)}</td>
      <td>${t.origin || (t.auto_managed ? 'AUTO' : 'USER')}</td>
      <td>${t.profile_id || ''}</td>
      <td>${t.holding_mode || 'INTRADAY'}</td>
      <td>${t.structure || ''}</td>
      <td>${t.exit_reason || '—'}</td>
      <td>${fmtInr(t.gross_pnl)}</td>
      <td class="${Number(t.net_pnl) >= 0 ? 'tg-pnl-pos' : 'tg-pnl-neg'}">${fmtInr(t.net_pnl)}</td>
      <td>${fmtInr(t.fees_total)}</td>
      <td class="tg-muted">${(t.exit_at || t.updated_at || '').replace('T', ' ').slice(0, 19)}</td>
    </tr>`).join('');
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

  async function loadInTrade() {
    const status = document.getElementById('statusLine');
    try {
      const data = await api('/api/tarang/in-trade');
      renderInTrade(data);
      status.textContent = `In-Trade: ${(data.trades || []).length} open`;
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  }

  async function loadReport() {
    const mode = document.getElementById('reportMode').value || 'FORWARD_TEST';
    document.getElementById('btnCsv').href = `/api/tarang/report.csv?book=${mode}`;
    const status = document.getElementById('statusLine');
    try {
      const rep = await api(`/api/tarang/report?book=${mode}&limit=100`);
      renderReport(rep);
      status.textContent = `Report ${mode}: ${(rep.metrics && rep.metrics.count) || 0} trades`;
    } catch (err) {
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
    renderCoverage(h.chain_coverage);
    document.getElementById('ivCounts').textContent = JSON.stringify(h.iv_snapshot_counts || {}, null, 2);
    document.getElementById('wsBox').textContent = JSON.stringify({
      ws: h.upstox_ws || {},
      analytics: h.upstox_analytics_token || {},
      profile_validation: h.profile_validation || [],
    }, null, 2);
    const p4 = document.getElementById('phase4Box');
    if (p4) p4.textContent = JSON.stringify(h.phase4 || {}, null, 2);
    const eliq = document.getElementById('eodLiqHealth');
    if (eliq) {
      const liq = h.eod_liquidity || {};
      const sample = (liq.rows || []).slice(0, 12);
      eliq.textContent = JSON.stringify({ dates: liq.dates, sample, note: liq.note || liq.error }, null, 2);
    }
    renderEligibility(h.expiry_eligibility);
    const s24 = document.getElementById('screener24Box');
    if (s24) {
      const s = h.screener_24h || {};
      const slim = { ...s };
      delete slim.iv_rv_panel;
      s24.textContent = JSON.stringify(slim, null, 2);
    }
    const ivBody = document.getElementById('ivRvBody');
    if (ivBody) {
      const panel = (h.screener_24h && h.screener_24h.iv_rv_panel) || [];
      ivBody.innerHTML = panel.length ? panel.map((r) => `<tr>
        <td>${r.underlying || r.profile_id || ''}</td>
        <td>${r.atm_iv != null ? Number(r.atm_iv).toFixed(4) : '—'}</td>
        <td>${r.realized_vol_20d != null ? Number(r.realized_vol_20d).toFixed(4) : '—'}</td>
        <td>${r.iv_minus_rv_over_rv != null ? (Number(r.iv_minus_rv_over_rv) * 100).toFixed(1) + '%' : '—'}</td>
        <td>${r.threshold != null ? (Number(r.threshold) * 100).toFixed(0) + '%' : '—'}</td>
        <td class="${r.gate === 'pass' ? 'tg-fit-yes' : 'tg-fit-no'}">${r.gate || '—'}</td>
      </tr>`).join('') : '<tr><td colspan="6" class="tg-muted">No evaluations yet</td></tr>';
    }
    const pipe = (h.pipeline && h.pipeline.jobs) || [];
    const pbody = document.getElementById('pipelineBody');
    if (pbody) {
      pbody.innerHTML = pipe.length ? pipe.map((j) => `<tr>
        <td>${j.label || j.id}</td>
        <td class="tg-muted">${(j.last_success || '').replace('T',' ').slice(0,19) || '—'}</td>
        <td class="${j.last_ok ? 'tg-fit-yes' : 'tg-fit-no'}">${j.last_ok == null ? '—' : (j.last_ok ? 'yes' : 'no')}</td>
        <td>${j.fail_count || 0}</td>
        <td class="tg-muted">${j.next_run || ''}</td>
        <td>${j.open_data_gaps || 0}</td>
        <td class="tg-muted">${j.last_error || ''}</td>
      </tr>`).join('') : '<tr><td colspan="7" class="tg-muted">No job runs yet</td></tr>';
    }
    const dr = document.getElementById('deltaReadyBox');
    if (dr) dr.textContent = JSON.stringify(h.delta_readiness || {}, null, 2);
    const tg = h.telegram || {};
    const a = document.getElementById('tgStartLink');
    if (a && tg.start_link) a.href = tg.start_link;
    const how = document.getElementById('tgHowTo');
    if (how && tg.note) how.textContent = tg.note;
    return h;
  }

  function renderEligibility(elig) {
    const body = document.getElementById('eligBody');
    if (!body) return;
    const rows = (elig && elig.rows) || [];
    if (!rows.length) {
      body.innerHTML = `<tr><td colspan="8" class="tg-muted">${(elig && elig.error) || 'No listed expiries'}</td></tr>`;
      return;
    }
    body.innerHTML = rows.map((r) => `<tr>
      <td>${r.underlying || r.profile_id || ''}</td>
      <td>${r.expiry || '—'}</td>
      <td>${r.dte != null ? r.dte : '—'}</td>
      <td>${r.min_dte != null ? r.min_dte : '—'}</td>
      <td>${r.max_dte != null ? r.max_dte : '—'}</td>
      <td>${r.time_stop_dte != null ? r.time_stop_dte : '—'}</td>
      <td class="${r.tradable ? 'tg-fit-yes' : 'tg-fit-no'}">${r.tradable ? 'Yes' : 'No'}</td>
      <td class="tg-muted">${r.note || (r.reasons || []).join(', ') || ''}</td>
    </tr>`).join('');
  }

  function renderBacktest(bt) {
    const msg = document.getElementById('backtestMsg');
    const metrics = document.getElementById('backtestMetrics');
    const body = document.getElementById('backtestTimeline');
    if (!msg) return;
    const label = bt.label || (bt.source === 'mcx_eod' ? 'MCX EOD-reconstructed, modelled fills, estimated underlying' : '');
    const nTrades = (bt.metrics && bt.metrics.count) || (bt.trades || []).length || 0;
    const nPess = (bt.metrics && bt.metrics.count_pessimistic);
    const cycles = (bt.metrics && bt.metrics.independent_cycles) || (bt.independent_cycles || []).length || bt.expiry_cycles || 0;
    const openN = (bt.metrics && bt.metrics.open_cycles_excluded) || (bt.open_cycles_excluded || []).length || 0;
    const ev = bt.rejection_evaluability || {};
    if (bt.insufficient_data) {
      msg.textContent = bt.message || 'Insufficient data.';
      msg.classList.add('tg-fit-no');
    } else {
      msg.textContent = `${label ? label + '. ' : ''}Independent (expired) cycles: ${cycles}. Open excluded: ${openN}. Trades (base): ${nTrades}${nPess != null ? `; pessimistic: ${nPess}` : ''}. NOT_EVALUABLE days: ${ev.not_evaluable || 0}; FAILED days: ${ev.failed || 0}. ${bt.intraday_note || ''} ${bt.note || ''}`;
      msg.classList.remove('tg-fit-no');
    }
    const gateHidden = bt.show_go_live_gate === false || nTrades < (bt.go_live_gate_hidden_until_trades || 40);
    const st = bt.stats || {};
    metrics.innerHTML = `
      <div class="tg-metric"><span>Label</span><strong>${label || 'Snapshot replay'}</strong></div>
      <div class="tg-metric"><span>Independent cycles</span><strong>${cycles}</strong></div>
      <div class="tg-metric"><span>Open excluded</span><strong>${openN}</strong></div>
      <div class="tg-metric"><span>Trades (base)</span><strong>${nTrades}</strong></div>
      <div class="tg-metric"><span>Trades (pessimistic)</span><strong>${nPess != null ? nPess : '—'}</strong></div>
      <div class="tg-metric"><span>Win rate</span><strong>${st.win_rate != null ? (100 * st.win_rate).toFixed(1) + '%' : '—'}</strong></div>
      <div class="tg-metric"><span>Profit factor</span><strong>${st.profit_factor != null ? Number(st.profit_factor).toFixed(2) : '—'}</strong></div>
      <div class="tg-metric"><span>Expectancy</span><strong>${st.expectancy != null ? fmtInr(st.expectancy) : '—'}</strong></div>
      <div class="tg-metric"><span>Max DD</span><strong>${st.max_drawdown != null ? fmtInr(st.max_drawdown) : '—'}</strong></div>
      <div class="tg-metric"><span>NOT_EVALUABLE days</span><strong>${ev.not_evaluable || 0}</strong></div>
      <div class="tg-metric"><span>FAILED days</span><strong>${ev.failed || 0}</strong></div>
      ${gateHidden ? '' : `<div class="tg-metric"><span>Go-live gate</span><strong>${bt.go_live_gate || 'n/a'}</strong></div>`}
    `;
    const statsEl = document.getElementById('backtestStats');
    if (statsEl) statsEl.textContent = JSON.stringify({
      avg_win: st.avg_win, avg_loss: st.avg_loss, worst_trade: st.worst_trade,
      vs_budget_caps_inr: st.vs_budget_caps_inr, credit_grid_plateaus: (bt.credit_grid || {}).plateaus,
      delta_band_grid: bt.delta_band_grid,
    }, null, 2);
    const tBody = document.getElementById('backtestTrades');
    if (tBody) {
      const ts = bt.trades || [];
      tBody.innerHTML = ts.length ? ts.map((t) => `<tr>
        <td>${t.entry_date || ''}</td>
        <td>${t.expiry || ''}</td>
        <td>${(t.exit_reason || '') + (t.exit_date ? ' ' + t.exit_date : '')}</td>
        <td>${t.net_pnl != null ? fmtInr(t.net_pnl) : '—'}</td>
        <td class="tg-muted">${t.estimated_underlying ? 'estimated underlying' : (t.underlying_source || '')}</td>
      </tr>`).join('') : '<tr><td colspan="5" class="tg-muted">No trades at default 20% min credit</td></tr>';
    }
    const byEl = document.getElementById('backtestByCycle');
    if (byEl) byEl.textContent = JSON.stringify({ by_cycle: st.by_cycle, by_month: st.by_month }, null, 2);
    const liqC = document.getElementById('backtestLiqCycle');
    if (liqC) {
      const rows = (bt.independent_cycles || []).map((c) => ({ expiry: c.expiry, ...(c.traded_strike_liquidity || {}) }));
      liqC.textContent = JSON.stringify(rows, null, 2);
    }
    const cycBody = document.getElementById('backtestCycles');
    if (cycBody) {
      const cyc = [...(bt.independent_cycles || []).map((c) => ({...c, inResults: 'Yes'})), ...(bt.open_cycles_excluded || []).map((c) => ({...c, inResults: 'No'}))];
      cycBody.innerHTML = cyc.length ? cyc.map((c) => `<tr>
        <td>${c.symbol || ''}</td>
        <td>${c.expiry || ''}</td>
        <td>${c.status || ''}</td>
        <td class="${c.inResults === 'Yes' ? 'tg-fit-yes' : 'tg-fit-no'}">${c.inResults}</td>
      </tr>`).join('') : '<tr><td colspan="4" class="tg-muted">No cycles</td></tr>';
    }
    const sumBody = document.getElementById('backtestRejSummary');
    if (sumBody) {
      const sum = bt.rejection_summary || {};
      const keys = Object.keys(sum);
      sumBody.innerHTML = keys.length ? keys.map((k) => `<tr><td>${k}</td><td>${sum[k]}</td></tr>`).join('') +
        `<tr><td>NOT_EVALUABLE (total days)</td><td>${(bt.rejection_evaluability || {}).not_evaluable || 0}</td></tr>` +
        `<tr><td>FAILED (total days)</td><td>${(bt.rejection_evaluability || {}).failed || 0}</td></tr>`
        : '<tr><td colspan="2" class="tg-muted">No rejections</td></tr>';
    }
    const subBody = document.getElementById('backtestDataSub');
    if (subBody) {
      const cycles = bt.independent_cycles || [];
      subBody.innerHTML = cycles.length ? cycles.map((c) => {
        const d = c.data_subreasons || {};
        const wq = (c.would_qualify_if_underlying || []).length;
        return `<tr>
          <td>${c.expiry || ''}</td>
          <td>${d.no_futures || 0}</td>
          <td>${d.parity_failed || 0}</td>
          <td>${d.short_not_traded || 0}</td>
          <td>${d.insufficient_traded_strikes || 0}</td>
          <td>${wq}</td>
        </tr>`;
      }).join('') : '<tr><td colspan="6" class="tg-muted">—</td></tr>';
    }
    const mcBody = document.getElementById('backtestMinCredit');
    if (mcBody) {
      const days = bt.min_credit_days || [];
      mcBody.innerHTML = days.length ? days.map((r) => `<tr>
        <td>${r.trade_date || ''}</td>
        <td>${r.expiry || ''}</td>
        <td>${r.credit_pct_of_width != null ? Number(r.credit_pct_of_width).toFixed(1) : '—'}</td>
        <td class="tg-muted">${(r.short_deltas || []).map((x) => x == null ? '—' : Number(x).toFixed(3)).join(', ')}</td>
      </tr>`).join('') : '<tr><td colspan="4" class="tg-muted">None</td></tr>';
    }
    const gridEl = document.getElementById('backtestCreditGrid');
    if (gridEl) {
      const g = bt.credit_grid || {};
      gridEl.textContent = JSON.stringify({ ...g, plateaus: g.plateaus || [], delta_band: bt.delta_band_grid }, null, 2);
    }
    const dwEl = document.getElementById('backtestDatewise');
    if (dwEl) dwEl.textContent = JSON.stringify(bt.datewise_checksum || { note: 'not run' }, null, 2);
    const logBody = document.getElementById('backtestRejLog');
    if (logBody) {
      const log = (bt.rejection_log || []).slice(0, 250);
      logBody.innerHTML = log.length ? log.map((r) => `<tr>
        <td>${r.trade_date || ''}</td>
        <td>${r.expiry || ''}</td>
        <td>${r.gate || ''}${r.evaluability ? ' (' + r.evaluability + ')' : ''}</td>
        <td>${r.dte != null ? r.dte : ''}</td>
        <td class="tg-muted">${r.detail || ''}</td>
      </tr>`).join('') : '<tr><td colspan="5" class="tg-muted">—</td></tr>';
    }
    const liq = document.getElementById('eodLiqBox');
    if (liq && bt.intraday_note) {
      liq.textContent = bt.intraday_note;
    }
    const rows = bt.timeline || (bt.coverage && bt.coverage.underlyings) || [];
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="6" class="tg-muted">No coverage yet</td></tr>';
      return;
    }
    body.innerHTML = rows.map((r) => `<tr>
      <td>${r.underlying}</td>
      <td>${r.days}</td>
      <td>${r.days_wide != null ? r.days_wide : '—'}</td>
      <td>${r.n_narrow != null ? r.n_narrow : '—'}</td>
      <td class="tg-muted">${(r.earliest || '').replace('T', ' ').slice(0, 16)}</td>
      <td>${r.six_month_backtest_ready_on || '—'}</td>
    </tr>`).join('');
  }

  async function loadBacktest() {
    const status = document.getElementById('statusLine');
    try {
      const bt = await api('/api/tarang/backtest?source=eod');
      renderBacktest(bt);
      status.textContent = bt.insufficient_data ? 'Backtest: insufficient data' : `Backtest: ${(bt.metrics && bt.metrics.count) || 0} trades`;
    } catch (err) {
      status.textContent = String(err.message || err);
    }
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
  const autoBtn = document.getElementById('btnAutoToggle');
  if (autoBtn) {
    autoBtn.addEventListener('click', async () => {
      const on = autoBtn.dataset.on === '1';
      const status = document.getElementById('statusLine');
      try {
        const out = await api('/api/tarang/settings/auto', {
          method: 'POST',
          body: JSON.stringify({ enabled: !on }),
        });
        setModeBadges(out.display_mode || out.mode, false);
        status.textContent = 'Auto orders: locked';
      } catch (err) {
        status.textContent = String(err.message || err);
      }
    });
  }
  document.getElementById('btnScreen').addEventListener('click', async () => {
    const status = document.getElementById('statusLine');
    status.textContent = 'Running screener (live chains)…';
    switchTab('screener');
    try {
      const out = await api('/api/tarang/screener/run', { method: 'POST' });
      renderScreener(null, out.results || []);
      const rej = await api('/api/tarang/rejections?limit=40');
      renderRejections(rej.rejections || []);
      status.textContent = `Screener done at ${out.run_at || ''} · forward tests ${(out.forward_tests && (out.forward_tests.recorded || []).length) || 0}`;
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });
  document.getElementById('btnIvSnap').addEventListener('click', async () => {
    const status = document.getElementById('statusLine');
    status.textContent = 'Running IV snapshot…';
    try {
      const out = await api('/api/tarang/chain-snapshots/run', { method: 'POST' });
      status.textContent = `Full-chain snapshot done: ${JSON.stringify(out.results || out)}`;
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
    if (!window.confirm(`Record forward test for candidate ${currentTicketId}? Simulates fills — no live orders.`)) return;
    const status = document.getElementById('statusLine');
    try {
      const out = await api(`/api/tarang/ticket/${currentTicketId}/take`, {
        method: 'POST',
        body: JSON.stringify({
          note: '',
          holding_mode: (document.getElementById('holdingMode') || {}).value || 'INTRADAY',
        }),
      });
      status.textContent = `Forward test #${out.trade_id} → ${out.status}`;
      switchTab('intrade');
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
    const note = window.prompt('I placed this at my broker. Optional note:') || '';
    const status = document.getElementById('statusLine');
    try {
      const out = await api(`/api/tarang/ticket/${currentTicketId}/record-live`, {
        method: 'POST',
        body: JSON.stringify({ note, fills: [], holding_mode: (document.getElementById('holdingMode') || {}).value || 'INTRADAY' }),
      });
      status.textContent = `Live record #${out.trade_id}`;
      switchTab('intrade');
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });

  document.getElementById('btnRefreshIntrade').addEventListener('click', loadInTrade);
  document.getElementById('btnRunExitEngine').addEventListener('click', async () => {
    const status = document.getElementById('statusLine');
    status.textContent = 'Running ExitEngine…';
    try {
      const out = await api('/api/tarang/exit-engine/run', { method: 'POST' });
      status.textContent = `ExitEngine checked ${out.checked}`;
      await loadInTrade();
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });
  document.getElementById('btnExitAll').addEventListener('click', async () => {
    if (!window.confirm('Exit ALL open Tarang trades (forward-test simulated exits)?')) return;
    const status = document.getElementById('statusLine');
    try {
      await api('/api/tarang/trades/exit-all', { method: 'POST', body: JSON.stringify({ reason: 'MANUAL' }) });
      status.textContent = 'Exit all done';
      await loadInTrade();
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });
  document.getElementById('btnLoadReport').addEventListener('click', loadReport);
  const btnBt = document.getElementById('btnRunBacktest');
  if (btnBt) {
    btnBt.addEventListener('click', async () => {
      const status = document.getElementById('statusLine');
      status.textContent = 'Running replay…';
      try {
        const bt = await api('/api/tarang/backtest/run?source=eod', { method: 'POST' });
        renderBacktest(bt);
        status.textContent = bt.insufficient_data ? 'Insufficient data' : `Replay ${((bt.metrics && bt.metrics.count) || 0)} trades`;
      } catch (err) {
        status.textContent = String(err.message || err);
      }
    });
  }
  const btnUp = document.getElementById('btnBhavcopyUpload');
  if (btnUp) {
    btnUp.addEventListener('click', async () => {
      const f = document.getElementById('bhavcopyFile');
      if (!f || !f.files || !f.files[0]) return;
      const fd = new FormData();
      fd.append('file', f.files[0]);
      const status = document.getElementById('statusLine');
      try {
        const res = await fetch('/api/tarang/bhavcopy/import', {
          method: 'POST',
          headers: { Authorization: `Bearer ${token()}` },
          body: fd,
        });
        const out = await res.json();
        status.textContent = res.ok ? `Imported ${out.inserted || 0} rows` : JSON.stringify(out);
      } catch (err) {
        status.textContent = String(err.message || err);
      }
    });
  }
  const btnTgTest = document.getElementById('btnTgTest');
  if (btnTgTest) {
    btnTgTest.addEventListener('click', async () => {
      const status = document.getElementById('statusLine');
      try {
        const out = await api('/api/tarang/telegram/test', { method: 'POST', body: '{}' });
        document.getElementById('tgBox').textContent = JSON.stringify(out, null, 2);
        status.textContent = out.sent || (out.telegram && out.telegram.sent)
          ? 'Test alert sent'
          : (out.error || 'Test alert not sent (chat may be unlinked)');
      } catch (err) {
        status.textContent = String(err.message || err);
      }
    });
  }
  const btnPoll = document.getElementById('btnTgPoll');
  if (btnPoll) {
    btnPoll.addEventListener('click', async () => {
      const status = document.getElementById('statusLine');
      try {
        const out = await api('/api/tarang/telegram/poll-link', { method: 'POST', body: '{}' });
        document.getElementById('tgBox').textContent = JSON.stringify(out, null, 2);
        status.textContent = out.linked && out.linked.length ? `Linked ${out.linked.join(',')}` : 'No /start seen yet';
      } catch (err) {
        status.textContent = String(err.message || err);
      }
    });
  }
  const btnTgSave = document.getElementById('btnTgSave');
  if (btnTgSave) {
    btnTgSave.addEventListener('click', async () => {
      const on = document.getElementById('tgPublicSignals').checked;
      const out = await api('/api/tarang/telegram/settings', {
        method: 'POST',
        body: JSON.stringify({ telegram_public_signals: on }),
      });
      document.getElementById('statusLine').textContent = out.ok ? 'Telegram setting saved' : JSON.stringify(out);
    });
  }
  document.getElementById('reportMode').addEventListener('change', () => {
    document.getElementById('btnCsv').href =
      `/api/tarang/report.csv?book=${document.getElementById('reportMode').value}`;
  });

  document.getElementById('btnCsv').addEventListener('click', async (e) => {
    e.preventDefault();
    const mode = document.getElementById('reportMode').value || 'FORWARD_TEST';
    try {
      const res = await fetch(`/api/tarang/report.csv?book=${mode}`, {
        headers: { Authorization: `Bearer ${token()}` },
      });
      if (!res.ok) throw new Error('CSV download failed');
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `tarang_report_${mode}.csv`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      document.getElementById('statusLine').textContent = String(err.message || err);
    }
  });

  // Auto-refresh In-Trade when tab visible
  document.querySelectorAll('.tg-tab').forEach((btn) => {
    btn.addEventListener('click', () => {
      if (intradeTimer) {
        clearInterval(intradeTimer);
        intradeTimer = null;
      }
      if (btn.dataset.tab === 'intrade') {
        intradeTimer = setInterval(loadInTrade, 30000);
      }
    });
  });

  load();
})();
