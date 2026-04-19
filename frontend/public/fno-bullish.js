/**
 * F&O Bullish Trend scanner backtest renderer.
 *
 * Loads /api/fno-bullish/data (or fallback /fno-bullish/data) and renders the
 * summary cards, filters, and trade table. Two PnL columns (Exit 1 and Exit 2)
 * are shown side by side so disappearance-based exits can be compared against
 * a 15:15 hold-to-close baseline.
 */
(function () {
  'use strict';

  const API_PATHS = ['/api/fno-bullish/data', '/fno-bullish/data'];

  const state = {
    all: [],
    sort: { key: 'trade_date', dir: 'desc' },
  };

  // ---------- formatters ---------------------------------------------------

  function escapeHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
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
  function pnlCls(v) {
    return (typeof v === 'number' && v > 0) ? 'pnl-pos'
      : (typeof v === 'number' && v < 0) ? 'pnl-neg' : 'pnl-flat';
  }
  function signedPts(v) {
    if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
    const sign = v > 0 ? '+' : '';
    return sign + v.toFixed(2);
  }

  // ---------- column spec ---------------------------------------------------

  const COLS = [
    { key: 'trade_date', label: 'Date', sortable: true,
      cell: r => '<td>' + fmtDate(r.trade_date) + '</td>',
      sortVal: r => r.trade_date || '' },
    { key: 'symbol', label: 'Symbol', sortable: true,
      cell: r => {
        const tag = r.is_reentry ? '<span class="reentry-dot" title="Re-entry #' + r.run_index + '"></span>' : '';
        const sym = escapeHtml(r.symbol || '');
        const tsym = r.trading_symbol ? '<div class="cell-note">' + escapeHtml(r.trading_symbol) + '</div>' : '';
        return '<td class="cell-symbol">' + tag + sym + tsym + '</td>';
      },
      sortVal: r => (r.symbol || '') + '_' + (r.run_index || 1) },
    { key: 'source', label: 'Src', sortable: true,
      cell: r => '<td>' + escapeHtml(r.source || '—') + '</td>',
      sortVal: r => r.source || '' },
    { key: 'lot_size', label: 'Lot', sortable: true,
      cell: r => '<td class="num">' + fmtInt(r.fut_lot_size || r.lot_size) + '</td>',
      sortVal: r => Number(r.fut_lot_size || r.lot_size) || 0 },
    { key: 'first_scan_time', label: '1st scan', sortable: true,
      cell: r => '<td>' + escapeHtml(r.first_scan_time || '—') + '</td>',
      sortVal: r => r.first_scan_time || '' },
    { key: 'last_scan_time', label: 'Last scan', sortable: true,
      cell: r => '<td>' + escapeHtml(r.last_scan_time || '—') + '</td>',
      sortVal: r => r.last_scan_time || '' },
    { key: 'scan_count', label: '#Scans', sortable: true,
      cell: r => '<td class="num">' + fmtInt(r.scan_count) + '</td>',
      sortVal: r => Number(r.scan_count) || 0 },
    { key: 'disappear_scan_time', label: 'Disapp.', sortable: true,
      cell: r => {
        if (r.exit1_kind === 'never_disappeared') {
          return '<td class="gate-ok" title="Symbol stayed in the scanner through 15:15">held to EOD</td>';
        }
        return '<td>' + escapeHtml(r.disappear_scan_time || '—') + '</td>';
      },
      sortVal: r => r.disappear_scan_time || '' },
    { key: 'entry_time', label: 'Entry @', sortable: true,
      cell: r => '<td>' + escapeHtml(r.entry_time || '—') + '</td>',
      sortVal: r => r.entry_time || '' },
    { key: 'entry_price', label: 'Entry ₹', sortable: true,
      cell: r => '<td class="num">' + fmtNum(r.entry_price) + '</td>',
      sortVal: r => Number(r.entry_price) || 0 },
    // Exit 1 block
    { key: 'exit1_time', label: 'Exit 1 @', sortable: true,
      cell: r => '<td class="col-exit1">' + escapeHtml(r.exit1_time || '—') + '</td>',
      sortVal: r => r.exit1_time || '' },
    { key: 'exit1_price', label: 'Exit 1 ₹', sortable: true,
      cell: r => '<td class="num">' + fmtNum(r.exit1_price) + '</td>',
      sortVal: r => Number(r.exit1_price) || 0 },
    { key: 'exit1_pnl_points', label: 'PnL₁ pts', sortable: true,
      cell: r => '<td class="num ' + pnlCls(r.exit1_pnl_points) + '">' + signedPts(r.exit1_pnl_points) + '</td>',
      sortVal: r => Number(r.exit1_pnl_points) || 0 },
    { key: 'exit1_pnl_rupees', label: 'PnL₁ ₹', sortable: true,
      cell: r => '<td class="num ' + pnlCls(r.exit1_pnl_rupees) + '">' + fmtRupees(r.exit1_pnl_rupees) + '</td>',
      sortVal: r => Number(r.exit1_pnl_rupees) || 0 },
    // Exit 2 block
    { key: 'exit2_price', label: 'Exit 2 ₹ (15:15)', sortable: true,
      cell: r => '<td class="num col-exit2">' + fmtNum(r.exit2_price) + '</td>',
      sortVal: r => Number(r.exit2_price) || 0 },
    { key: 'exit2_pnl_rupees', label: 'PnL₂ ₹', sortable: true,
      cell: r => '<td class="num ' + pnlCls(r.exit2_pnl_rupees) + '">' + fmtRupees(r.exit2_pnl_rupees) + '</td>',
      sortVal: r => Number(r.exit2_pnl_rupees) || 0 },
  ];

  // ---------- fetch + render ------------------------------------------------

  async function fetchJson(path) {
    const resp = await fetch(path, { headers: { 'Accept': 'application/json' } });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    return resp.json();
  }

  async function load() {
    let doc = null;
    let lastErr = null;
    for (const p of API_PATHS) {
      try {
        doc = await fetchJson(p);
        if (doc) break;
      } catch (e) { lastErr = e; }
    }
    if (!doc) {
      const banner = document.getElementById('errBanner');
      banner.style.display = '';
      banner.style.borderLeftColor = 'var(--bad)';
      banner.textContent = 'Failed to load backtest data: ' + (lastErr ? lastErr.message : 'no data');
      return;
    }
    state.all = Array.isArray(doc.rows) ? doc.rows.slice() : [];
    renderHeader();
    renderSummary(doc.summary || {});
    renderFooter(doc);
    applyFilters();
  }

  function renderHeader() {
    const tr = document.querySelector('#resultsTable thead tr');
    tr.innerHTML = '';
    COLS.forEach((c, i) => {
      const th = document.createElement('th');
      th.textContent = c.label;
      if (c.sortable) {
        th.classList.add('sortable');
        th.dataset.key = c.key;
        th.addEventListener('click', () => {
          if (state.sort.key === c.key) {
            state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
          } else {
            state.sort.key = c.key;
            state.sort.dir = 'asc';
          }
          applyFilters();
        });
      }
      if (state.sort.key === c.key) {
        th.classList.add(state.sort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
      }
      tr.appendChild(th);
    });
  }

  function renderSummary(s) {
    const grid = document.getElementById('summaryGrid');
    if (!grid) return;
    const e1 = s.exit1 || {};
    const e2 = s.exit2 || {};
    const cards = [
      ['Trades', fmtInt(s.total_trades), (s.reentry_trades || 0) + ' re-entries'],
      ['With entry price', fmtInt(s.trades_with_entry), 'Σ FUT=' + fmtInt(s.fut_rows) + ' / EQ=' + fmtInt(s.eq_rows)],
      ['Never disappeared', fmtInt(s.never_disappeared_rows), 'Exit 1 = Exit 2'],
      ['Exit 1 · Σ PnL', fmtRupees(e1.sum_pnl_rupees), (e1.positive_rows || 0) + ' wins / ' + (e1.negative_rows || 0) + ' losses'],
      ['Exit 1 · Best / Worst', fmtRupees(e1.best_pnl_rupees) + ' / ' + fmtRupees(e1.worst_pnl_rupees), 'single-trade extremes'],
      ['Exit 2 · Σ PnL', fmtRupees(e2.sum_pnl_rupees), (e2.positive_rows || 0) + ' wins / ' + (e2.negative_rows || 0) + ' losses'],
      ['Exit 2 · Best / Worst', fmtRupees(e2.best_pnl_rupees) + ' / ' + fmtRupees(e2.worst_pnl_rupees), 'single-trade extremes'],
      ['Edge of Exit 1 over Exit 2',
        fmtRupees((Number(e1.sum_pnl_rupees) || 0) - (Number(e2.sum_pnl_rupees) || 0)),
        (Number(e1.sum_pnl_rupees) || 0) >= (Number(e2.sum_pnl_rupees) || 0)
          ? 'disappearance exit helps' : 'disappearance exit hurts'],
    ];
    grid.innerHTML = cards.map(([title, val, sub]) => (
      '<div class="card">' +
        '<div class="title">' + escapeHtml(title) + '</div>' +
        '<div class="value">' + escapeHtml(val || '—') + '</div>' +
        '<div class="sub">' + escapeHtml(sub || '') + '</div>' +
      '</div>'
    )).join('');
  }

  function renderFooter(doc) {
    const f = document.getElementById('footerMeta');
    if (!f) return;
    const bits = [];
    if (doc.generated_at) bits.push('Generated: ' + escapeHtml(doc.generated_at));
    if (doc.strategy) bits.push(escapeHtml(doc.strategy));
    if (doc.artifact_path) bits.push('<span class="muted">' + escapeHtml(doc.artifact_path) + '</span>');
    f.innerHTML = bits.join(' · ');
  }

  // ---------- filters + sorting --------------------------------------------

  function applyFilters() {
    const from = document.getElementById('fltFrom').value;
    const to = document.getElementById('fltTo').value;
    const src = document.getElementById('fltSource').value;
    const kind = document.getElementById('fltKind').value;
    const symQ = (document.getElementById('fltSymbol').value || '').trim().toUpperCase();

    let rows = state.all.filter(r => {
      if (from && r.trade_date < from) return false;
      if (to && r.trade_date > to) return false;
      if (src && r.source !== src) return false;
      if (kind === 'first' && r.is_reentry) return false;
      if (kind === 'reentry' && !r.is_reentry) return false;
      if (kind === 'never' && r.exit1_kind !== 'never_disappeared') return false;
      if (symQ && !(r.symbol || '').toUpperCase().includes(symQ)) return false;
      return true;
    });

    const col = COLS.find(c => c.key === state.sort.key);
    if (col) {
      const dir = state.sort.dir === 'desc' ? -1 : 1;
      rows.sort((a, b) => {
        const va = col.sortVal(a);
        const vb = col.sortVal(b);
        if (va === vb) return 0;
        if (va == null) return 1;
        if (vb == null) return -1;
        return va < vb ? -1 * dir : 1 * dir;
      });
    }

    document.getElementById('rowCount').textContent =
      rows.length + ' / ' + state.all.length + ' trades';

    const tbody = document.getElementById('tbody');
    if (!rows.length) {
      tbody.innerHTML = '<tr><td class="nodata" colspan="' + COLS.length + '">No rows match the filters.</td></tr>';
      return;
    }
    const html = rows.map(r => {
      const cells = COLS.map(c => c.cell(r)).join('');
      return '<tr>' + cells + '</tr>';
    }).join('');
    tbody.innerHTML = html;

    // repaint sort indicator on the header
    document.querySelectorAll('#resultsTable thead th').forEach(th => {
      th.classList.remove('sort-asc', 'sort-desc');
      if (th.dataset && th.dataset.key === state.sort.key) {
        th.classList.add(state.sort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
      }
    });
  }

  // ---------- wire up -------------------------------------------------------

  function init() {
    ['fltFrom','fltTo','fltSource','fltKind','fltSymbol'].forEach(id => {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener('input', applyFilters);
      el.addEventListener('change', applyFilters);
    });
    load();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
