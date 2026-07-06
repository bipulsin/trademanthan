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

  function fmtNum(v, d) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return n.toFixed(d == null ? 2 : d);
  }

  function fmtPct(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return (n > 0 ? '+' : '') + n.toFixed(2) + '%';
  }

  function fmtRs(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    const sign = n > 0 ? '+' : n < 0 ? '-' : '';
    return sign + '₹' + Math.abs(n).toLocaleString('en-IN', { maximumFractionDigits: 0 });
  }

  function pnlCls(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '';
    return n > 0 ? 'pnl-pos' : n < 0 ? 'pnl-neg' : '';
  }

  function gateCell(pass) {
    if (pass === true) return '<span class="gate-pass">✓</span>';
    if (pass === false) return '<span class="gate-fail">✗</span>';
    return '—';
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

  function renderTable() {
    const head = document.getElementById('tableHead');
    const body = document.getElementById('tableBody');
    if (!head || !body) return;
    const cols = [
      'Date', 'NIFTY %', 'Side', 'Stock', 'Dir', 'ATM', 'Option', 'Chg%', 'CPR', 'RSI', 'Liq',
      'ST', 'Hull', 'Mode', 'Entry', 'Buy cost', 'Exit A', 'PnL A', 'Exit B', 'PnL B', 'Eligible', 'Reason'
    ];
    head.innerHTML = '<tr>' + cols.map(function (c) { return '<th>' + esc(c) + '</th>'; }).join('') + '</tr>';
    body.innerHTML = state.rows.map(function (r) {
      const dir = r.direction === 'bullish' ? '<span class="pill pill-ce">CE</span>' :
        r.direction === 'bearish' ? '<span class="pill pill-pe">PE</span>' : '—';
      const mode = r.data_mode === 'manual_fill' ? '<span class="pill pill-manual">manual</span>' :
        r.data_mode === 'full' ? '<span class="pill pill-full">full</span>' : '—';
      const rowCls = r.eligible_final ? '' : ' class="ineligible"';
      return '<tr' + rowCls + ' data-row-id="' + r.id + '">' +
        '<td>' + esc(r.trade_date) + '</td>' +
        '<td class="num">' + fmtPct(r.nifty_change_pct) + '</td>' +
        '<td>' + esc(r.scan_side || '—') + '</td>' +
        '<td>' + esc(r.stock_symbol || '—') + '</td>' +
        '<td>' + dir + '</td>' +
        '<td class="num">' + fmtNum(r.atm_strike, 0) + '</td>' +
        '<td title="' + esc(r.option_symbol) + '">' + esc((r.option_symbol || '—').slice(0, 18)) + '</td>' +
        '<td class="num">' + fmtPct(r.change_pct_at_1445) + '</td>' +
        '<td>' + gateCell(r.cpr_gate_pass) + '</td>' +
        '<td>' + gateCell(r.rsi_gate_pass) + ' ' + fmtNum(r.rsi_14_5min, 1) + '</td>' +
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
          const tr = document.querySelector('tr[data-row-id="' + id + '"]');
          if (tr) {
            const cells = tr.querySelectorAll('td');
            if (cells[15]) cells[15].innerHTML = '<span class="num">' + fmtRs(res.row.buy_cost) + '</span>';
            if (cells[17]) cells[17].className = 'num ' + pnlCls(res.row.exit_a_pnl);
            if (cells[17]) cells[17].textContent = fmtRs(res.row.exit_a_pnl);
            if (cells[19]) cells[19].className = 'num ' + pnlCls(res.row.exit_b_pnl);
            if (cells[19]) cells[19].textContent = fmtRs(res.row.exit_b_pnl);
          }
        } catch (e) {
          showErr('Save failed: ' + e.message);
        }
      });
    });
  }

  function showErr(msg) {
    const b = document.getElementById('errBanner');
    if (!b) return;
    b.style.display = 'block';
    b.textContent = msg;
  }

  async function loadLatest() {
    try {
      const doc = await fetchJson('/latest');
      state.rows = doc.rows || [];
      state.summary = doc.summary || {};
      state.run = doc.run;
      renderSummary(state.summary);
      renderTable();
    } catch (e) {
      if (!String(e.message).includes('404')) showErr('No results yet. Click Run backtest.');
    }
  }

  async function pollStatus() {
    const st = await fetchJson('/status');
    const el = document.getElementById('runStatus');
    const btn = document.getElementById('btnRun');
    if (st.running) {
      if (el) el.textContent = 'Running… (this may take a while)';
      if (btn) btn.disabled = true;
      setTimeout(pollStatus, 5000);
    } else {
      if (btn) btn.disabled = false;
      if (el) el.textContent = st.error ? 'Error: ' + st.error : (st.run_id ? 'Done run #' + st.run_id : '');
      if (st.run_id && !st.error) loadLatest();
    }
  }

  document.getElementById('btnRun').addEventListener('click', async function () {
    try {
      await fetchJson('/run?days=30', { method: 'POST' });
      pollStatus();
    } catch (e) {
      showErr(e.message);
    }
  });

  loadLatest();
  pollStatus();
})();
