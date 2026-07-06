(function () {
  'use strict';

  const API = ['/api/btst-backtest', '/btst-backtest'];
  const state = { rows: [], summary: {}, run: null };

  function apiBase(path) {
    return API.map((p) => p + path);
  }

  async function fetchJson(path, opts) {
    let lastErr;
    for (const url of apiBase(path)) {
      try {
        const r = await fetch(url, opts);
        if (!r.ok) {
          const t = await r.text();
          throw new Error(t || r.statusText);
        }
        return await r.json();
      } catch (e) {
        lastErr = e;
      }
    }
    throw lastErr;
  }

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function isNil(v) {
    return v === null || v === undefined || v === '';
  }

  function fmtNum(v, d) {
    if (isNil(v)) return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return n.toFixed(d == null ? 2 : d);
  }

  function fmtPct(v) {
    if (isNil(v)) return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return (n > 0 ? '+' : '') + n.toFixed(2) + '%';
  }

  function fmtRs(v) {
    if (isNil(v)) return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    const sign = n > 0 ? '+' : n < 0 ? '-' : '';
    return sign + '₹' + Math.abs(n).toLocaleString('en-IN', { maximumFractionDigits: 0 });
  }

  function pnlCls(v) {
    if (isNil(v)) return '';
    const n = Number(v);
    if (!Number.isFinite(n)) return '';
    return n > 0 ? 'pnl-pos' : n < 0 ? 'pnl-neg' : '';
  }

  function gateCell(pass) {
    if (pass === true) return '<span class="gate-pass">✓</span>';
    if (pass === false) return '<span class="gate-fail">✗</span>';
    return '—';
  }

  function clearErr() {
    const b = document.getElementById('errBanner');
    if (b) {
      b.style.display = 'none';
      b.textContent = '';
    }
  }

  function showErr(msg) {
    const b = document.getElementById('errBanner');
    if (!b) return;
    b.style.display = 'block';
    b.textContent = msg;
  }

  function renderSummary(s) {
    const el = document.getElementById('summaryGrid');
    if (!el) return;
    const items = [
      ['CE · Scenario A', s.ce_scenario_a_total],
      ['CE · Scenario B', s.ce_scenario_b_total],
      ['PE · Scenario A', s.pe_scenario_a_total],
      ['PE · Scenario B', s.pe_scenario_b_total],
      ['Final · Scenario A', s.final_scenario_a_total],
      ['Final · Scenario B', s.final_scenario_b_total],
      ['Manual fill progress', (s.manual_fill_total - s.manual_fill_needs_data) + ' / ' + s.manual_fill_total + ' complete'],
      ['Rows needing data', s.manual_fill_needs_data + ' of ' + (s.row_count || 0)],
      ['API fetch failed (retryable)', String(s.api_fetch_failed_count || 0)],
    ];
    el.innerHTML = items.map(function (pair) {
      const v = pair[1];
      const isPnl = typeof v === 'number';
      return '<div class="metric"><div class="k">' + esc(pair[0]) + '</div><div class="v ' +
        (isPnl ? pnlCls(v) : '') + '">' + esc(isPnl ? fmtRs(v) : v) + '</div></div>';
    }).join('');
  }

  function premiumCell(row, field) {
    if (row.data_mode === 'manual_fill') {
      const val = row[field] != null ? row[field] : '';
      return '<input type="number" step="0.05" class="manual-in" data-id="' + row.id +
        '" data-field="' + field + '" value="' + esc(val) + '" />';
    }
    return '<span class="num">' + fmtNum(row[field]) + '</span>';
  }

  function sidePill(side) {
    if (side === 'gainer') return '<span class="pill pill-ce">gainer</span>';
    if (side === 'loser') return '<span class="pill pill-pe">loser</span>';
    return '—';
  }

  function renderTable() {
    const head = document.getElementById('tableHead');
    const body = document.getElementById('tableBody');
    if (!head || !body) return;
    const cols = [
      'Date', 'Side', 'Stock', 'Dir', 'ATM', 'Option', 'Chg%', 'CPR', 'RSI', 'Liq',
      'ST', 'Hull', 'Mode', 'Entry', 'Buy cost', 'Exit A', 'PnL A', 'Exit B', 'PnL B', 'Eligible', 'Reason'
    ];
    head.innerHTML = '<tr>' + cols.map(function (c) { return '<th>' + esc(c) + '</th>'; }).join('') + '</tr>';
    body.innerHTML = state.rows.map(function (r) {
      const dir = r.direction === 'bullish' ? '<span class="pill pill-ce">CE</span>' :
        r.direction === 'bearish' ? '<span class="pill pill-pe">PE</span>' : '—';
      const mode = r.data_mode === 'manual_fill' ? '<span class="pill pill-manual">manual</span>' :
        r.data_mode === 'full' ? '<span class="pill pill-full">full</span>' : '—';
      const failApi = r.no_eligible_reason === 'api_fetch_failed';
      const rowCls = r.eligible_final ? '' : (failApi ? ' class="api-failed"' : ' class="ineligible"');
      const rsiTxt = isNil(r.rsi_14_5min) ? '' : fmtNum(r.rsi_14_5min, 1);
      return '<tr' + rowCls + ' data-row-id="' + r.id + '">' +
        '<td>' + esc(r.trade_date) + '</td>' +
        '<td>' + sidePill(r.side) + '</td>' +
        '<td>' + esc(r.stock_symbol || '—') + '</td>' +
        '<td>' + dir + '</td>' +
        '<td class="num">' + fmtNum(r.atm_strike, 0) + '</td>' +
        '<td title="' + esc(r.option_symbol) + '">' + esc((r.option_symbol || '—').slice(0, 18)) + '</td>' +
        '<td class="num">' + fmtPct(r.change_pct_at_1445) + '</td>' +
        '<td>' + gateCell(r.cpr_gate_pass) + '</td>' +
        '<td>' + gateCell(r.rsi_gate_pass) + (rsiTxt ? ' ' + rsiTxt : '') + '</td>' +
        '<td>' + gateCell(r.liquidity_gate_pass) + '</td>' +
        '<td>' + gateCell(r.supertrend_pass) + '</td>' +
        '<td>' + gateCell(r.hull_pass) + '</td>' +
        '<td>' + mode + '</td>' +
        '<td class="num">' + premiumCell(r, 'entry_premium') + '</td>' +
        '<td class="num">' + fmtRs(r.buy_cost) + '</td>' +
        '<td class="num">' + premiumCell(r, 'exit_a_premium') + '</td>' +
        '<td class="num ' + pnlCls(r.exit_a_pnl) + '">' + fmtRs(r.exit_a_pnl) + '</td>' +
        '<td class="num">' + premiumCell(r, 'exit_b_premium') + '</td>' +
        '<td class="num ' + pnlCls(r.exit_b_pnl) + '">' + fmtRs(r.exit_b_pnl) + '</td>' +
        '<td>' + (r.eligible_final ? '✓' : '—') + '</td>' +
        '<td>' + esc(r.no_eligible_reason || '') + '</td>' +
        '</tr>';
    }).join('');
    bindManualInputs();
  }

  function updateRowInState(updated) {
    const idx = state.rows.findIndex(function (r) { return r.id === updated.id; });
    if (idx >= 0) state.rows[idx] = Object.assign({}, state.rows[idx], updated);
  }

  function bindManualInputs() {
    document.querySelectorAll('input.manual-in').forEach(function (inp) {
      inp.addEventListener('blur', async function () {
        const id = inp.dataset.id;
        const field = inp.dataset.field;
        const val = inp.value === '' ? null : Number(inp.value);
        if (val !== null && !Number.isFinite(val)) return;
        const body = {};
        body[field] = val;
        try {
          const res = await fetchJson('/results/' + id, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
          });
          updateRowInState(res.row);
          state.summary = res.summary;
          renderSummary(state.summary);
        } catch (e) {
          showErr('Save failed: ' + e.message);
        }
      });
    });
  }

  function updateToolbar(st) {
    const earlier = document.getElementById('btnEarlier');
    const retry = document.getElementById('btnRetry');
    if (earlier) earlier.style.display = st.earliest_trade_date ? 'inline-block' : 'none';
    if (retry) retry.style.display = (st.failed_row_count > 0) ? 'inline-block' : 'none';
  }

  async function loadLatest() {
    try {
      const doc = await fetchJson('/latest');
      state.rows = doc.rows || [];
      state.summary = doc.summary || {};
      state.run = doc.run;
      clearErr();
      renderSummary(state.summary);
      renderTable();
    } catch (e) {
      if (!String(e.message).includes('404')) showErr('No results yet. Click Run backtest.');
    }
  }

  function formatRunStatus(st) {
    if (!st.running) {
      return st.error ? 'Error: ' + st.error : (st.run_id ? 'Done run #' + st.run_id : '');
    }
    const p = st.progress || {};
    const parts = [];
    if (p.message) parts.push(p.message);
    if (p.phase === 'prefetch' && p.prefetch_total) {
      parts.push('prefetch ' + (p.prefetch_done || 0) + '/' + p.prefetch_total);
    }
    if (p.phase === 'screening' && p.days_total) {
      parts.push('days ' + (p.days_done || 0) + '/' + p.days_total);
    }
    if (st.rows_written_this_run != null) {
      parts.push('rows written: ' + st.rows_written_this_run);
    }
    if (st.elapsed_sec != null) {
      parts.push('elapsed ' + Math.floor(st.elapsed_sec / 60) + 'm');
    }
    if (st.stale_warning) parts.push('⚠ ' + st.stale_warning);
    return parts.length ? parts.join(' · ') : 'Running…';
  }

  async function pollStatus() {
    const st = await fetchJson('/status');
    const el = document.getElementById('runStatus');
    const btn = document.getElementById('btnRun');
    const btnEarlier = document.getElementById('btnEarlier');
    const btnRetry = document.getElementById('btnRetry');
    updateToolbar(st);
    if (st.running) {
      if (el) el.textContent = formatRunStatus(st);
      if (btn) btn.disabled = true;
      if (btnEarlier) btnEarlier.disabled = true;
      if (btnRetry) btnRetry.disabled = true;
      if (st.progress && st.progress.phase === 'screening' && st.rows_written_this_run > 0) {
        loadLatest();
      }
      setTimeout(pollStatus, 5000);
    } else {
      if (btn) btn.disabled = false;
      if (btnEarlier) btnEarlier.disabled = false;
      if (btnRetry) btnRetry.disabled = false;
      if (el) el.textContent = st.error ? 'Error: ' + st.error : (st.run_id ? 'Done run #' + st.run_id : '');
      if (st.run_id && !st.error) loadLatest();
      else updateToolbar(st);
    }
  }

  function daysVal() {
    const inp = document.getElementById('daysInput');
    return Math.max(1, Math.min(60, parseInt(inp && inp.value, 10) || 15));
  }

  document.getElementById('btnRun').addEventListener('click', async function () {
    try {
      clearErr();
      await fetchJson('/run?days=' + daysVal(), { method: 'POST' });
      pollStatus();
    } catch (e) {
      showErr(e.message);
    }
  });

  document.getElementById('btnEarlier').addEventListener('click', async function () {
    try {
      clearErr();
      await fetchJson('/run-earlier?days=' + daysVal(), { method: 'POST' });
      pollStatus();
    } catch (e) {
      showErr(e.message);
    }
  });

  document.getElementById('btnRetry').addEventListener('click', async function () {
    try {
      clearErr();
      await fetchJson('/retry-failed', { method: 'POST' });
      pollStatus();
    } catch (e) {
      showErr(e.message);
    }
  });

  loadLatest().then(function () { pollStatus(); });
})();
