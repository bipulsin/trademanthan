/**
 * NKS Best-Buy intraday futures backtest renderer.
 *
 * The page's <body data-day="same|next"> attribute selects which artifact to
 * load. Same HTML/CSS/JS is used by both nks-intraday.html (same day) and
 * nks-intraday-next.html (next trading day).
 */
(function () {
  'use strict';

  const DAY_MODE = (document.body && document.body.dataset && document.body.dataset.day) || 'same';
  const API_PATHS = [
    '/api/nks-intraday/data?day=' + encodeURIComponent(DAY_MODE),
    '/nks-intraday/data?day=' + encodeURIComponent(DAY_MODE),
  ];

  const state = {
    all: [],
    sort: { key: 'csv_date', dir: 'desc' },
  };

  function escapeHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }
  function fmtNum(v, d) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return n.toFixed(d == null ? 2 : d);
  }
  function fmtInt(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return Math.round(n).toLocaleString('en-IN');
  }
  function fmtRupees(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    const sign = n > 0 ? '+' : (n < 0 ? '-' : '');
    return sign + '₹' + Math.abs(n).toLocaleString('en-IN', { maximumFractionDigits: 0 });
  }
  function fmtDate(iso) {
    if (!iso) return '—';
    try {
      const d = new Date(iso.length === 10 ? iso + 'T00:00:00+05:30' : iso);
      if (!Number.isFinite(d.getTime())) return escapeHtml(iso);
      return new Intl.DateTimeFormat('en-IN', {
        timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: '2-digit',
      }).format(d);
    } catch (e) { return escapeHtml(iso); }
  }

  async function loadData() {
    let data = null, lastErr = null;
    for (const p of API_PATHS) {
      try {
        const res = await fetch(p, { cache: 'no-store' });
        if (!res.ok) { lastErr = new Error(p + ' -> HTTP ' + res.status); continue; }
        data = await res.json();
        if (data) break;
      } catch (e) { lastErr = e; }
    }
    if (!data) throw lastErr || new Error('Failed to load artifact');
    return data;
  }

  function buildSummaryMetrics(doc) {
    const s = (doc && doc.summary) || {};
    const cards = [
      { k: 'Rows', v: s.total_rows != null ? s.total_rows : '—' },
      { k: 'Rows with prices', v: s.rows_with_prices != null ? s.rows_with_prices : '—' },
      { k: 'FUT source', v: s.rows_fut_source != null ? s.rows_fut_source : '—' },
      { k: 'EQ source', v: s.rows_eq_source != null ? s.rows_eq_source : '—' },
      { k: 'Best @12:30', v: (s.slot_wins && s.slot_wins['12:30']) || 0 },
      { k: 'Best @14:00', v: (s.slot_wins && s.slot_wins['14:00']) || 0 },
      { k: 'Best @15:15', v: (s.slot_wins && s.slot_wins['15:15']) || 0 },
      {
        k: 'Σ PnL pts',
        v: fmtNum(s.sum_pnl_points, 1),
        cls: (Number(s.sum_pnl_points) > 0 ? 'good' : (Number(s.sum_pnl_points) < 0 ? 'bad' : '')),
      },
      {
        k: 'Σ PnL ₹ (×lot)',
        v: fmtRupees(s.sum_pnl_rupees),
        cls: (Number(s.sum_pnl_rupees) > 0 ? 'good' : (Number(s.sum_pnl_rupees) < 0 ? 'bad' : '')),
      },
      {
        k: 'Σ Max DD ₹',
        v: fmtRupees(s.sum_drawdown_rupees),
        cls: (Number(s.sum_drawdown_rupees) < 0 ? 'bad' : ''),
      },
      {
        k: 'Worst DD ₹',
        v: fmtRupees(s.worst_drawdown_rupees),
        cls: (Number(s.worst_drawdown_rupees) < 0 ? 'bad' : ''),
      },
    ];
    const host = document.getElementById('summaryGrid');
    host.innerHTML = cards.map(function (c) {
      return '<div class="metric"><div class="k">' + escapeHtml(c.k) + '</div>' +
        '<div class="v ' + (c.cls || '') + '">' + escapeHtml(String(c.v)) + '</div></div>';
    }).join('');
  }

  function applyFilters(rows) {
    const src = document.getElementById('fltSource').value || '';
    const slot = document.getElementById('fltSlot').value || '';
    const from = document.getElementById('fltFrom').value || '';
    const to = document.getElementById('fltTo').value || '';
    const minAbs = parseFloat(document.getElementById('fltMinAbs').value);
    return rows.filter(function (r) {
      if (src && r.source !== src) return false;
      if (slot && r.best_slot !== slot) return false;
      const sd = String(r.csv_date || r.session_date || '').slice(0, 10);
      if (from && sd < from) return false;
      if (to && sd > to) return false;
      if (Number.isFinite(minAbs)) {
        const a = Number(r.best_abs_diff);
        if (!Number.isFinite(a) || a < minAbs) return false;
      }
      return true;
    });
  }

  function sortRows(rows) {
    const { key, dir } = state.sort;
    const mult = dir === 'asc' ? 1 : -1;
    return rows.slice().sort(function (a, b) {
      const av = a[key], bv = b[key];
      const an = Number(av), bn = Number(bv);
      const bothNum = Number.isFinite(an) && Number.isFinite(bn);
      if (bothNum) return (an - bn) * mult;
      const as = (av == null ? '' : String(av));
      const bs = (bv == null ? '' : String(bv));
      if (as < bs) return -1 * mult;
      if (as > bs) return 1 * mult;
      return 0;
    });
  }

  function renderTable() {
    const filtered = applyFilters(state.all);
    const sorted = sortRows(filtered);
    document.getElementById('rowCount').textContent =
      filtered.length + ' of ' + state.all.length + ' rows';
    const tbody = document.getElementById('tbody');
    const colspan = DAY_MODE === 'next' ? 13 : 12;
    if (!sorted.length) {
      tbody.innerHTML = '<tr><td class="nodata" colspan="' + colspan + '">No rows match the current filters.</td></tr>';
      return;
    }
    tbody.innerHTML = sorted.map(function (r) {
      const srcPill = r.source === 'FUT'
        ? '<span class="pill pill-fut">FUT</span>'
        : (r.source === 'EQ' ? '<span class="pill pill-eq">EQ</span>' : '<span class="pill pill-err">?</span>');
      const slot = r.best_slot || '';
      const slotCls = slot === '12:30' ? 'slot-chip slot-1230'
        : slot === '14:00' ? 'slot-chip slot-1400'
        : slot === '15:15' ? 'slot-chip slot-1515' : 'slot-chip';
      const slotHtml = slot ? '<span class="' + slotCls + '">' + slot + '</span>' : '—';
      const pnl = r.best_diff_points;
      const pnlCls = (typeof pnl === 'number' && pnl > 0) ? 'pnl-pos'
        : (typeof pnl === 'number' && pnl < 0) ? 'pnl-neg' : 'pnl-flat';
      const pnlText = (typeof pnl === 'number')
        ? ((pnl > 0 ? '+' : '') + fmtNum(pnl, 2)) : '—';
      const pnlR = r.pnl_rupees;
      const pnlRCls = (typeof pnlR === 'number' && pnlR > 0) ? 'pnl-pos'
        : (typeof pnlR === 'number' && pnlR < 0) ? 'pnl-neg' : 'pnl-flat';
      const ddP = r.drawdown_points;
      const ddPCls = (typeof ddP === 'number' && ddP < 0) ? 'pnl-neg' : 'pnl-flat';
      const ddPText = (typeof ddP === 'number')
        ? ((ddP > 0 ? '+' : '') + fmtNum(ddP, 2)) : '—';
      const ddR = r.drawdown_rupees;
      const ddRCls = (typeof ddR === 'number' && ddR < 0) ? 'pnl-neg' : 'pnl-flat';
      const ddTooltip = (r.min_price != null)
        ? ('Low ' + fmtNum(r.min_price) + (r.min_price_at ? ' at ' + r.min_price_at : ''))
        : '';
      const errNote = r.error ? '<div class="err-note">' + escapeHtml(r.error) + '</div>' : '';
      const sessionCell = DAY_MODE === 'next'
        ? '<td>' + escapeHtml(fmtDate(r.session_date)) + '</td>'
        : '';
      return '<tr>' +
        '<td>' + escapeHtml(fmtDate(r.csv_date || r.session_date)) + '</td>' +
        sessionCell +
        '<td>' + srcPill + errNote + '</td>' +
        '<td title="' + escapeHtml(r.instrument_key || '') + '">' +
          escapeHtml(r.trading_symbol || r.symbol || '—') + '</td>' +
        '<td class="num">' + fmtInt(r.fut_lot_size) + '</td>' +
        '<td class="num">' + fmtNum(r.price_0945) + '</td>' +
        '<td class="num">' + fmtNum(r.price_1230) + '</td>' +
        '<td class="num">' + fmtNum(r.price_1400) + '</td>' +
        '<td class="num">' + fmtNum(r.price_1515) + '</td>' +
        '<td>' + slotHtml + '</td>' +
        '<td class="num ' + pnlCls + '">' + pnlText + '</td>' +
        '<td class="num ' + pnlRCls + '">' + fmtRupees(pnlR) + '</td>' +
        '<td class="num ' + ddPCls + '" title="' + escapeHtml(ddTooltip) + '">' + ddPText + '</td>' +
        '<td class="num ' + ddRCls + '" title="' + escapeHtml(ddTooltip) + '">' + fmtRupees(ddR) + '</td>' +
        '</tr>';
    }).join('');
  }

  function updateSortHeaders() {
    const ths = document.querySelectorAll('#resultsTable thead th');
    ths.forEach(function (th) {
      th.classList.remove('sort-asc', 'sort-desc');
      if (th.dataset.key === state.sort.key) {
        th.classList.add(state.sort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
      }
    });
  }

  function wireSortHeaders() {
    const ths = document.querySelectorAll('#resultsTable thead th');
    ths.forEach(function (th) {
      th.addEventListener('click', function () {
        const k = th.dataset.key;
        if (!k) return;
        if (state.sort.key === k) {
          state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
        } else {
          state.sort.key = k;
          state.sort.dir = (k === 'csv_date' || k === 'session_date' || k === 'best_abs_diff'
            || k === 'pnl_rupees' || k === 'best_diff_points'
            || k === 'drawdown_points' || k === 'drawdown_rupees') ? 'desc' : 'asc';
        }
        updateSortHeaders();
        renderTable();
      });
    });
  }

  function wireFilters() {
    const ids = ['fltSource', 'fltSlot', 'fltFrom', 'fltTo', 'fltMinAbs'];
    ids.forEach(function (id) {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener('input', renderTable);
      el.addEventListener('change', renderTable);
    });
  }

  function showError(msg) {
    const el = document.getElementById('errBanner');
    el.style.display = '';
    el.textContent = msg;
  }

  async function main() {
    wireSortHeaders();
    wireFilters();
    updateSortHeaders();
    try {
      const doc = await loadData();
      state.all = Array.isArray(doc.rows) ? doc.rows : [];
      buildSummaryMetrics(doc);
      const meta = [];
      if (doc.day_mode) meta.push('Day mode: ' + doc.day_mode);
      if (doc.generated_at) meta.push('Generated: ' + doc.generated_at);
      if (doc.artifact_path) meta.push('Source: ' + doc.artifact_path);
      document.getElementById('footerMeta').textContent = meta.join(' · ');
      renderTable();
    } catch (e) {
      showError('Could not load backtest artifact: ' + (e.message || e) +
        ' — Run `python3 backend/scripts/run_nks_intraday_backtest.py --mode both` on the server to generate it.');
      const colspan = DAY_MODE === 'next' ? 13 : 12;
      document.getElementById('tbody').innerHTML =
        '<tr><td class="nodata" colspan="' + colspan + '">Backtest data not yet available.</td></tr>';
    }
  }

  document.addEventListener('DOMContentLoaded', main);
})();
