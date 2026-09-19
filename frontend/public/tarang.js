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
    const autoBtn = document.getElementById('btnAutoToggle');
    if (autoBtn) {
      autoBtn.textContent = auto ? 'AUTO PAPER: on' : 'AUTO PAPER: off';
      autoBtn.dataset.on = auto ? '1' : '0';
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
      host.innerHTML = '<p class="tg-muted">No open PAPER trades. Take a QUALIFIED candidate from Trade Ticket.</p>';
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
          if (!window.confirm(`Exit PAPER trade #${btn.dataset.id}?`)) return;
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
    const m = rep.metrics || {};
    const box = document.getElementById('reportMetrics');
    const wr = m.win_rate != null ? `${(m.win_rate * 100).toFixed(0)}%` : '—';
    const pf = m.profit_factor_infinite ? '∞' : (m.profit_factor != null ? Number(m.profit_factor).toFixed(2) : '—');
    box.innerHTML = `
      <div class="tg-metric"><span>Trades</span><strong>${m.count || 0}</strong></div>
      <div class="tg-metric"><span>Win rate</span><strong>${wr}</strong></div>
      <div class="tg-metric"><span>Avg win</span><strong>${fmtInr(m.avg_win)}</strong></div>
      <div class="tg-metric"><span>Avg loss</span><strong>${fmtInr(m.avg_loss)}</strong></div>
      <div class="tg-metric"><span>Profit factor</span><strong>${pf}</strong></div>
      <div class="tg-metric"><span>Expectancy</span><strong>${fmtInr(m.expectancy)}</strong></div>
      <div class="tg-metric"><span>Net total</span><strong>${fmtInr(m.net_total)}</strong></div>
      <div class="tg-metric"><span>Max DD</span><strong>${fmtInr(m.max_drawdown)}</strong></div>
      <div class="tg-metric"><span>INTRADAY</span><strong>${(m.by_holding_mode && m.by_holding_mode.INTRADAY && m.by_holding_mode.INTRADAY.count) || 0}</strong></div>
      <div class="tg-metric"><span>POSITIONAL</span><strong>${(m.by_holding_mode && m.by_holding_mode.POSITIONAL && m.by_holding_mode.POSITIONAL.count) || 0}</strong></div>
      <div class="tg-metric"><span>AUTO</span><strong>${(m.by_origin && m.by_origin.AUTO && m.by_origin.AUTO.count) || 0}</strong></div>
      <div class="tg-metric"><span>USER</span><strong>${(m.by_origin && m.by_origin.USER && m.by_origin.USER.count) || 0}</strong></div>
    `;
    const body = document.getElementById('reportBody');
    const trades = rep.trades || [];
    if (!trades.length) {
      body.innerHTML = `<tr><td colspan="10" class="tg-muted">${rep.note || 'No closed trades'}</td></tr>`;
      return;
    }
    body.innerHTML = trades.map((t) => `<tr>
      <td>${t.id}</td>
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
    const mode = document.getElementById('reportMode').value || 'PAPER';
    document.getElementById('btnCsv').href = `/api/tarang/report.csv?mode=${mode}`;
    const status = document.getElementById('statusLine');
    try {
      const rep = await api(`/api/tarang/report?mode=${mode}&limit=100`);
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
    const label = bt.label || (bt.source === 'mcx_eod' ? 'MCX EOD-reconstructed, modelled fills' : '');
    const nTrades = (bt.metrics && bt.metrics.count) || (bt.trades || []).length || 0;
    const cycles = (bt.metrics && bt.metrics.expiry_cycles) || bt.expiry_cycles || 0;
    if (bt.insufficient_data) {
      msg.textContent = bt.message || 'Insufficient data.';
      msg.classList.add('tg-fit-no');
    } else {
      msg.textContent = `${label ? label + '. ' : ''}Expiry cycles: ${cycles}. Trades: ${nTrades}. ${bt.intraday_note || ''} ${bt.note || ''}`;
      msg.classList.remove('tg-fit-no');
    }
    const m = bt.metrics || {};
    const gateHidden = bt.show_go_live_gate === false || nTrades < (bt.go_live_gate_hidden_until_trades || 40);
    metrics.innerHTML = `
      <div class="tg-metric"><span>Label</span><strong>${label || 'Snapshot replay'}</strong></div>
      <div class="tg-metric"><span>Expiry cycles</span><strong>${cycles}</strong></div>
      <div class="tg-metric"><span>Trades</span><strong>${nTrades}</strong></div>
      <div class="tg-metric"><span>Fill mode</span><strong>${bt.fill_mode || '—'}</strong></div>
      ${gateHidden ? '' : `<div class="tg-metric"><span>Go-live gate</span><strong>${bt.go_live_gate || 'n/a'}</strong></div>`}
    `;
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
        setModeBadges(out.mode || 'PAPER', out.auto);
        status.textContent = out.auto ? 'AUTO PAPER on — LIVE stays admin-gated' : 'AUTO PAPER off';
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
      status.textContent = `Screener done at ${out.run_at || ''}`;
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
    if (!window.confirm(`Take PAPER trade for candidate ${currentTicketId}? Simulates fills — no live orders.`)) return;
    const status = document.getElementById('statusLine');
    try {
      const out = await api(`/api/tarang/ticket/${currentTicketId}/take`, {
        method: 'POST',
        body: JSON.stringify({
          note: '',
          holding_mode: (document.getElementById('holdingMode') || {}).value || 'INTRADAY',
        }),
      });
      status.textContent = `PAPER trade #${out.trade_id} → ${out.status}`;
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
    const note = window.prompt('Optional note for manual PAPER mark:') || '';
    const status = document.getElementById('statusLine');
    try {
      const out = await api(`/api/tarang/ticket/${currentTicketId}/mark-manual`, {
        method: 'POST',
        body: JSON.stringify({ note, fills: {} }),
      });
      status.textContent = `Manual PAPER trade #${out.trade_id}`;
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
    if (!window.confirm('Exit ALL open PAPER Tarang trades?')) return;
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
  document.getElementById('reportMode').addEventListener('change', () => {
    document.getElementById('btnCsv').href =
      `/api/tarang/report.csv?mode=${document.getElementById('reportMode').value}`;
  });

  document.getElementById('btnCsv').addEventListener('click', async (e) => {
    e.preventDefault();
    const mode = document.getElementById('reportMode').value || 'PAPER';
    try {
      const res = await fetch(`/api/tarang/report.csv?mode=${mode}`, {
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
