(function () {
  'use strict';

  const API = ['/api/btst-backtest', '/btst-backtest'];
  const state = { rows: [], summary: {}, run: null, pending: null };

  function apiBase(path) {
    return API.map(function (p) { return p + path; });
  }

  async function fetchJson(path, opts) {
    var lastErr;
    for (var i = 0; i < apiBase(path).length; i++) {
      var url = apiBase(path)[i];
      try {
        var r = await fetch(url, opts);
        if (!r.ok) {
          var t = await r.text();
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
    var n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return n.toFixed(d == null ? 2 : d);
  }

  function fmtPct(v) {
    if (isNil(v)) return '—';
    var n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return (n > 0 ? '+' : '') + n.toFixed(2) + '%';
  }

  function fmtRs(v) {
    if (isNil(v)) return '—';
    var n = Number(v);
    if (!Number.isFinite(n)) return '—';
    var sign = n > 0 ? '+' : n < 0 ? '-' : '';
    return sign + '₹' + Math.abs(n).toLocaleString('en-IN', { maximumFractionDigits: 0 });
  }

  function pnlCls(v) {
    if (isNil(v)) return '';
    var n = Number(v);
    if (!Number.isFinite(n)) return '';
    return n > 0 ? 'pnl-pos' : n < 0 ? 'pnl-neg' : '';
  }

  function gateCell(pass) {
    if (pass === true) return '<span class="gate-pass">✓</span>';
    if (pass === false) return '<span class="gate-fail">✗</span>';
    return '—';
  }

  function rowReason(r) {
    return r.no_data_reason || r.no_eligible_reason || '';
  }

  function isHiddenRow(r) {
    return rowReason(r) === 'no_data_holiday_or_gap';
  }

  function visibleRows(rows) {
    return (rows || []).filter(function (r) { return !isHiddenRow(r); });
  }

  function computeSummaryFromRows(rows) {
    function hasPnl(r) { return r.entry_premium != null; }
    function sumPnl(dir) {
      var a = 0;
      var b = 0;
      rows.forEach(function (r) {
        if (r.direction !== dir || !hasPnl(r)) return;
        if (r.exit_a_pnl != null) a += Number(r.exit_a_pnl);
        if (r.exit_b_pnl != null) b += Number(r.exit_b_pnl);
      });
      return [a, b];
    }
    var ce = sumPnl('CE');
    var pe = sumPnl('PE');
    var manualTotal = 0;
    var manualNeeds = 0;
    rows.forEach(function (r) {
      if (r.data_mode !== 'manual_fill') return;
      manualTotal++;
      if (r.entry_premium == null || r.exit_a_premium == null || r.exit_b_premium == null) {
        manualNeeds++;
      }
    });
    return {
      ce_scenario_a_total: ce[0],
      ce_scenario_b_total: ce[1],
      pe_scenario_a_total: pe[0],
      pe_scenario_b_total: pe[1],
      final_scenario_a_total: ce[0] + pe[0],
      final_scenario_b_total: ce[1] + pe[1],
      manual_fill_needs_data: manualNeeds,
      manual_fill_total: manualTotal,
      api_fetch_failed_count: rows.filter(function (r) {
        return rowReason(r) === 'option_premium_fetch_failed';
      }).length,
      row_count: rows.length,
    };
  }

  function clearErr() {
    var b = document.getElementById('errBanner');
    if (b) { b.style.display = 'none'; b.textContent = ''; }
  }

  function showErr(msg) {
    var b = document.getElementById('errBanner');
    if (!b) return;
    b.style.display = 'block';
    b.textContent = msg;
  }

  function renderSummary(s) {
    var el = document.getElementById('summaryGrid');
    if (!el) return;
    var items = [
      ['CE · Exit A', s.ce_scenario_a_total],
      ['CE · Exit B', s.ce_scenario_b_total],
      ['PE · Exit A', s.pe_scenario_a_total],
      ['PE · Exit B', s.pe_scenario_b_total],
      ['Total · Exit A', s.final_scenario_a_total],
      ['Total · Exit B', s.final_scenario_b_total],
      ['Manual fill progress',
        (s.manual_fill_total - s.manual_fill_needs_data) + ' / ' + s.manual_fill_total + ' complete'],
      ['Rows needing data', s.manual_fill_needs_data + ' of ' + (s.row_count || 0)],
    ];
    el.innerHTML = items.map(function (pair) {
      var v = pair[1];
      var isPnl = typeof v === 'number';
      return '<div class="metric"><div class="k">' + esc(pair[0]) + '</div><div class="v ' +
        (isPnl ? pnlCls(v) : '') + '">' + esc(isPnl ? fmtRs(v) : v) + '</div></div>';
    }).join('');
  }

  function premiumCell(row, field) {
    if (row.data_mode === 'manual_fill') {
      var val = row[field] != null ? row[field] : '';
      return '<input type="number" step="0.05" class="manual-in" data-id="' + row.id +
        '" data-field="' + field + '" value="' + esc(val) + '" />';
    }
    return '<span class="num">' + fmtNum(row[field]) + '</span>';
  }

  function dirPill(dir) {
    if (dir === 'CE') return '<span class="pill pill-ce">CE</span>';
    if (dir === 'PE') return '<span class="pill pill-pe">PE</span>';
    return '—';
  }

  function renderTable() {
    var head = document.getElementById('tableHead');
    var body = document.getElementById('tableBody');
    if (!head || !body) return;
    var cols = [
      'Date', 'Stock', 'Sector', 'Chg%', 'Dir', 'ATM', 'Option', 'Mode',
      'ST', 'Hull', 'Eligible', 'Entry', 'Buy cost', 'Exit A', 'PnL A', 'Exit B', 'PnL B', 'Note'
    ];
    head.innerHTML = '<tr>' + cols.map(function (c) { return '<th>' + esc(c) + '</th>'; }).join('') + '</tr>';
    body.innerHTML = state.rows.map(function (r) {
      var mode = r.data_mode === 'manual_fill' ? '<span class="pill pill-manual">manual</span>' :
        r.data_mode === 'full' ? '<span class="pill pill-full">full</span>' : '—';
      var rowCls = r.eligible_final ? '' : ' class="ineligible"';
      return '<tr' + rowCls + ' data-row-id="' + r.id + '">' +
        '<td>' + esc(r.trade_date) + '</td>' +
        '<td>' + esc(r.stock_symbol || '—') + '</td>' +
        '<td>' + esc(r.sector || '—') + '</td>' +
        '<td class="num">' + fmtPct(r.change_pct) + '</td>' +
        '<td>' + dirPill(r.direction) + '</td>' +
        '<td class="num">' + fmtNum(r.atm_strike, 0) + '</td>' +
        '<td title="' + esc(r.option_symbol) + '">' + esc((r.option_symbol || '—').slice(0, 20)) + '</td>' +
        '<td>' + mode + '</td>' +
        '<td>' + gateCell(r.supertrend_pass) + '</td>' +
        '<td>' + gateCell(r.hull_pass) + '</td>' +
        '<td>' + (r.eligible_final ? '✓' : '—') + '</td>' +
        '<td class="num">' + premiumCell(r, 'entry_premium') + '</td>' +
        '<td class="num">' + fmtRs(r.buy_cost) + '</td>' +
        '<td class="num">' + premiumCell(r, 'exit_a_premium') + '</td>' +
        '<td class="num ' + pnlCls(r.exit_a_pnl) + '">' + fmtRs(r.exit_a_pnl) + '</td>' +
        '<td class="num">' + premiumCell(r, 'exit_b_premium') + '</td>' +
        '<td class="num ' + pnlCls(r.exit_b_pnl) + '">' + fmtRs(r.exit_b_pnl) + '</td>' +
        '<td>' + esc(r.no_data_reason || '') + '</td>' +
        '</tr>';
    }).join('');
    bindManualInputs();
  }

  function updateRowInState(updated) {
    var idx = state.rows.findIndex(function (r) { return r.id === updated.id; });
    if (idx >= 0) state.rows[idx] = Object.assign({}, state.rows[idx], updated);
  }

  function bindManualInputs() {
    document.querySelectorAll('input.manual-in').forEach(function (inp) {
      inp.addEventListener('blur', async function () {
        var id = inp.dataset.id;
        var field = inp.dataset.field;
        var val = inp.value === '' ? null : Number(inp.value);
        if (val !== null && !Number.isFinite(val)) return;
        var body = {};
        body[field] = val;
        try {
          var res = await fetchJson('/results/' + id, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
          });
          updateRowInState(res.row);
          state.summary = computeSummaryFromRows(state.rows);
          renderSummary(state.summary);
          renderTable();
        } catch (e) {
          showErr('Save failed: ' + e.message);
        }
      });
    });
  }

  async function loadLatest() {
    try {
      var doc = await fetchJson('/latest');
      state.rows = visibleRows(doc.rows || []);
      state.summary = computeSummaryFromRows(state.rows);
      state.run = doc.run;
      clearErr();
      renderSummary(state.summary);
      renderTable();
    } catch (e) {
      if (!String(e.message).includes('404')) showErr('No results yet — upload a CSV and run.');
    }
  }

  function formatRunStatus(st) {
    if (!st.running) {
      return st.error ? 'Error: ' + st.error : (st.run_id ? 'Done run #' + st.run_id : '');
    }
    var p = st.progress || {};
    var parts = [];
    if (p.message) parts.push(p.message);
    if (p.rows_total) parts.push((p.rows_done || 0) + '/' + p.rows_total + ' rows');
    if (st.rows_written_this_run != null) parts.push('written: ' + st.rows_written_this_run);
    if (st.elapsed_sec != null) parts.push(Math.floor(st.elapsed_sec / 60) + 'm');
    return parts.length ? parts.join(' · ') : 'Running…';
  }

  async function pollStatus() {
    var st = await fetchJson('/status');
    var el = document.getElementById('runStatus');
    var btn = document.getElementById('btnRun');
    if (st.running) {
      if (el) el.textContent = formatRunStatus(st);
      if (btn) btn.disabled = true;
      if (st.rows_written_this_run > 0) loadLatest();
      setTimeout(pollStatus, 3000);
    } else {
      if (btn) btn.disabled = !(state.pending && state.pending.row_count > 0);
      if (el) el.textContent = st.error ? 'Error: ' + st.error : (st.run_id ? 'Done run #' + st.run_id : '');
      if (st.run_id && !st.error) loadLatest();
    }
  }

  async function refreshPending() {
    try {
      state.pending = await fetchJson('/pending');
      var info = document.getElementById('csvInfo');
      var btn = document.getElementById('btnRun');
      if (state.pending.row_count > 0) {
        if (info) info.textContent = state.pending.filename + ' — ' + state.pending.row_count + ' rows ready';
        if (btn) btn.disabled = false;
      }
    } catch (e) { /* ignore */ }
  }

  document.getElementById('csvInput').addEventListener('change', async function (ev) {
    var file = ev.target.files && ev.target.files[0];
    if (!file) return;
    clearErr();
    var fd = new FormData();
    fd.append('file', file);
    try {
      var res = await fetchJson('/upload', { method: 'POST', body: fd });
      state.pending = { row_count: res.row_count, filename: res.filename };
      var info = document.getElementById('csvInfo');
      if (info) info.textContent = res.filename + ' — ' + res.row_count + ' rows ready';
      document.getElementById('btnRun').disabled = false;
      if (res.warnings && res.warnings.length) {
        showErr('Uploaded with warnings: ' + res.warnings.slice(0, 3).join('; '));
      }
    } catch (e) {
      showErr('Upload failed: ' + e.message);
    }
  });

  document.getElementById('btnRun').addEventListener('click', async function () {
    try {
      clearErr();
      await fetchJson('/run', { method: 'POST' });
      pollStatus();
    } catch (e) {
      showErr(e.message);
    }
  });

  refreshPending().then(function () { loadLatest(); pollStatus(); });
})();
