/**
 * NKS Best-Buy intraday futures backtest renderer.
 *
 * <body data-day="same|next"> selects which artifact is loaded and which
 * column layout is rendered. Same-day layout surfaces the pro-grade entry
 * filters (VWAP entry, risk cap, ORB confirmation); next-day layout keeps
 * the raw 09:45-anchored columns.
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

  // ---------- formatting helpers --------------------------------------------
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
  function signed(v, d) {
    if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
    return (v > 0 ? '+' : '') + fmtNum(v, d);
  }

  // ---------- cell builders -------------------------------------------------
  function cellDate(r) {
    return '<td>' + escapeHtml(fmtDate(r.csv_date || r.session_date)) + '</td>';
  }
  function cellSessionDate(r) {
    return '<td>' + escapeHtml(fmtDate(r.session_date)) + '</td>';
  }
  function cellSrc(r) {
    const pill = r.source === 'FUT'
      ? '<span class="pill pill-fut">FUT</span>'
      : (r.source === 'EQ' ? '<span class="pill pill-eq">EQ</span>' : '<span class="pill pill-err">?</span>');
    const err = r.error ? '<div class="err-note">' + escapeHtml(r.error) + '</div>' : '';
    return '<td>' + pill + err + '</td>';
  }
  function cellContract(r) {
    return '<td title="' + escapeHtml(r.instrument_key || '') + '">' +
      escapeHtml(r.trading_symbol || r.symbol || '—') + '</td>';
  }
  function cellLot(r) { return '<td class="num">' + fmtInt(r.fut_lot_size) + '</td>'; }
  function cellPx(key) { return function (r) { return '<td class="num">' + fmtNum(r[key]) + '</td>'; }; }
  function cellBestSlot(r) {
    const slot = r.best_slot || '';
    const cls = slot === '12:30' ? 'slot-chip slot-1230'
      : slot === '14:00' ? 'slot-chip slot-1400'
      : slot === '15:15' ? 'slot-chip slot-1515' : 'slot-chip';
    return '<td>' + (slot ? '<span class="' + cls + '">' + slot + '</span>' : '—') + '</td>';
  }
  function cellSignedPts(key) {
    return function (r) {
      const v = r[key];
      return '<td class="num ' + pnlCls(v) + '">' + signed(v, 2) + '</td>';
    };
  }
  function cellSignedRupees(key) {
    return function (r) {
      const v = r[key];
      return '<td class="num ' + pnlCls(v) + '">' + fmtRupees(v) + '</td>';
    };
  }
  function cellDdPts(key) {
    return function (r) {
      const v = r[key];
      const cls = (typeof v === 'number' && v < 0) ? 'pnl-neg' : 'pnl-flat';
      const tip = (r.min_price != null)
        ? ('Low ' + fmtNum(r.min_price) + (r.min_price_at ? ' at ' + r.min_price_at : ''))
        : '';
      return '<td class="num ' + cls + '" title="' + escapeHtml(tip) + '">' + signed(v, 2) + '</td>';
    };
  }
  function cellDdRupees(key) {
    return function (r) {
      const v = r[key];
      const cls = (typeof v === 'number' && v < 0) ? 'pnl-neg' : 'pnl-flat';
      const tip = (r.min_price != null)
        ? ('Low ' + fmtNum(r.min_price) + (r.min_price_at ? ' at ' + r.min_price_at : ''))
        : '';
      return '<td class="num ' + cls + '" title="' + escapeHtml(tip) + '">' + fmtRupees(v) + '</td>';
    };
  }
  function cellRiskRupees(r) {
    const v = r.risk_rupees;
    let cls = 'pnl-flat';
    if (typeof v === 'number') cls = (r.risk_pass === false) ? 'pnl-neg' : 'gate-ok';
    const cap = (typeof v === 'number')
      ? '₹' + Math.round(v).toLocaleString('en-IN')
      : '—';
    return '<td class="num ' + cls + '" title="Cap ₹10,000 per trade">' + cap + '</td>';
  }
  function cellOrb(r) {
    if (r.orb_pass === true) {
      const tip = 'ORH ' + fmtNum(r.or_high_0945) + ' | low 09:46-10:00 ' +
        fmtNum(r.orb_confirm_min_low) + ' | px 10:00 ' + fmtNum(r.orb_price_at_1000);
      return '<td class="gate-ok" title="' + escapeHtml(tip) + '">✓</td>';
    }
    if (r.orb_pass === false) {
      const tip = 'ORH ' + fmtNum(r.or_high_0945) + ' | low 09:46-10:00 ' +
        fmtNum(r.orb_confirm_min_low) + ' | px 10:00 ' + fmtNum(r.orb_price_at_1000);
      return '<td class="gate-bad" title="' + escapeHtml(tip) + '">✗</td>';
    }
    return '<td class="gate-na">—</td>';
  }
  function cellStatus(r) {
    if (r.trade_taken === true) {
      return '<td><span class="status-pill status-taken">TAKEN</span></td>';
    }
    const reasons = Array.isArray(r.skip_reasons) ? r.skip_reasons : [];
    const pretty = reasons.map(function (rr) {
      if (rr === 'risk_gt_10k') return 'Risk > ₹10k';
      if (rr === 'orb_failed') return 'ORB failed';
      if (rr === 'no_vwap') return 'VWAP missing';
      return rr;
    }).join(', ');
    return '<td><span class="status-pill status-skip" title="' + escapeHtml(pretty) +
      '">SKIPPED</span></td>';
  }

  // ---------- per-mode column specs -----------------------------------------
  // Each spec: { key, label, align?, cell(r) }
  const COLS_SAME = [
    { key: 'csv_date',             label: 'Date',         cell: cellDate },
    { key: 'source',               label: 'Src',          cell: cellSrc },
    { key: 'trading_symbol',       label: 'Contract',     cell: cellContract },
    { key: 'fut_lot_size',         label: 'Lot',          align: 'num', cell: cellLot },
    { key: 'vwap_0945',            label: '09:45 VWAP',   align: 'num', cell: cellPx('vwap_0945') },
    { key: 'or_high_0945',         label: '09:45 ORH',    align: 'num', cell: cellPx('or_high_0945') },
    { key: 'price_1230',           label: '12:30',        align: 'num', cell: cellPx('price_1230') },
    { key: 'price_1400',           label: '14:00',        align: 'num', cell: cellPx('price_1400') },
    { key: 'price_1515',           label: '15:15',        align: 'num', cell: cellPx('price_1515') },
    { key: 'best_slot',            label: 'Best',         cell: cellBestSlot },
    { key: 'risk_rupees',          label: 'Risk ₹',       align: 'num', cell: cellRiskRupees },
    { key: 'orb_pass',             label: 'ORB',          cell: cellOrb },
    { key: 'trade_taken',          label: 'Status',       cell: cellStatus },
    { key: 'pnl_points_vwap',      label: 'PnL pts',      align: 'num', cell: cellSignedPts('pnl_points_vwap') },
    { key: 'pnl_rupees_vwap',      label: 'PnL ₹',        align: 'num', cell: cellSignedRupees('pnl_rupees_vwap') },
    { key: 'drawdown_points_vwap', label: 'Max DD pts',   align: 'num', cell: cellDdPts('drawdown_points_vwap') },
    { key: 'drawdown_rupees_vwap', label: 'Max DD ₹',     align: 'num', cell: cellDdRupees('drawdown_rupees_vwap') },
  ];
  const COLS_NEXT = [
    { key: 'csv_date',         label: 'Shortlist',  cell: cellDate },
    { key: 'session_date',     label: 'Priced on',  cell: cellSessionDate },
    { key: 'source',           label: 'Src',        cell: cellSrc },
    { key: 'trading_symbol',   label: 'Contract',   cell: cellContract },
    { key: 'fut_lot_size',     label: 'Lot',        align: 'num', cell: cellLot },
    { key: 'price_0945',       label: '09:45',      align: 'num', cell: cellPx('price_0945') },
    { key: 'price_1230',       label: '12:30',      align: 'num', cell: cellPx('price_1230') },
    { key: 'price_1400',       label: '14:00',      align: 'num', cell: cellPx('price_1400') },
    { key: 'price_1515',       label: '15:15',      align: 'num', cell: cellPx('price_1515') },
    { key: 'best_slot',        label: 'Best',       cell: cellBestSlot },
    { key: 'best_diff_points', label: 'PnL pts',    align: 'num', cell: cellSignedPts('best_diff_points') },
    { key: 'pnl_rupees',       label: 'PnL ₹',      align: 'num', cell: cellSignedRupees('pnl_rupees') },
    { key: 'drawdown_points',  label: 'Max DD pts', align: 'num', cell: cellDdPts('drawdown_points') },
    { key: 'drawdown_rupees',  label: 'Max DD ₹',   align: 'num', cell: cellDdRupees('drawdown_rupees') },
  ];
  const COLS = DAY_MODE === 'same' ? COLS_SAME : COLS_NEXT;

  // Keys whose natural sort direction is descending (biggest moves first).
  const DESC_KEYS = new Set([
    'csv_date', 'session_date', 'best_abs_diff',
    'best_diff_points', 'pnl_rupees',
    'pnl_points_vwap', 'pnl_rupees_vwap',
    'drawdown_points', 'drawdown_rupees',
    'drawdown_points_vwap', 'drawdown_rupees_vwap',
    'risk_rupees',
  ]);

  // ---------- network -------------------------------------------------------
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

  // ---------- summary -------------------------------------------------------
  function buildSummaryMetrics(doc) {
    const s = (doc && doc.summary) || {};
    const isSame = DAY_MODE === 'same';
    const cards = [];
    cards.push({ k: 'Rows', v: s.total_rows != null ? s.total_rows : '—' });
    cards.push({ k: 'With prices', v: s.rows_with_prices != null ? s.rows_with_prices : '—' });
    cards.push({ k: 'FUT / EQ',
      v: (s.rows_fut_source != null ? s.rows_fut_source : '—') + ' / ' +
         (s.rows_eq_source  != null ? s.rows_eq_source  : '—') });
    cards.push({ k: 'Best @12:30', v: (s.slot_wins && s.slot_wins['12:30']) || 0 });
    cards.push({ k: 'Best @14:00', v: (s.slot_wins && s.slot_wins['14:00']) || 0 });
    cards.push({ k: 'Best @15:15', v: (s.slot_wins && s.slot_wins['15:15']) || 0 });

    if (isSame) {
      const br = s.skipped_by_reason || {};
      const skipDetail =
        'Risk > ₹10k: ' + (br.risk_gt_10k || 0) +
        ' · ORB failed: ' + (br.orb_failed || 0) +
        (br.no_vwap ? ' · No VWAP: ' + br.no_vwap : '');
      cards.push({ k: 'Taken (pro filters)',
        v: (s.taken_rows != null ? s.taken_rows : '—'),
        cls: 'good' });
      cards.push({ k: 'Skipped',
        v: (s.skipped_rows != null ? s.skipped_rows : '—'),
        cls: 'bad', tip: skipDetail });
      cards.push({ k: 'Σ PnL ₹ — taken',
        v: fmtRupees(s.taken_sum_pnl_rupees),
        cls: (Number(s.taken_sum_pnl_rupees) > 0 ? 'good'
            : Number(s.taken_sum_pnl_rupees) < 0 ? 'bad' : '') });
      cards.push({ k: 'Σ DD ₹ — taken',
        v: fmtRupees(s.taken_sum_drawdown_rupees),
        cls: (Number(s.taken_sum_drawdown_rupees) < 0 ? 'bad' : '') });
      cards.push({ k: 'Worst DD ₹ — taken',
        v: fmtRupees(s.taken_worst_drawdown_rupees),
        cls: (Number(s.taken_worst_drawdown_rupees) < 0 ? 'bad' : '') });
    } else {
      cards.push({ k: 'Σ PnL ₹ (×lot)',
        v: fmtRupees(s.sum_pnl_rupees),
        cls: (Number(s.sum_pnl_rupees) > 0 ? 'good' : Number(s.sum_pnl_rupees) < 0 ? 'bad' : '') });
      cards.push({ k: 'Σ Max DD ₹',
        v: fmtRupees(s.sum_drawdown_rupees),
        cls: (Number(s.sum_drawdown_rupees) < 0 ? 'bad' : '') });
      cards.push({ k: 'Worst DD ₹',
        v: fmtRupees(s.worst_drawdown_rupees),
        cls: (Number(s.worst_drawdown_rupees) < 0 ? 'bad' : '') });
    }

    const host = document.getElementById('summaryGrid');
    host.innerHTML = cards.map(function (c) {
      const tip = c.tip ? ' title="' + escapeHtml(c.tip) + '"' : '';
      return '<div class="metric"' + tip + '><div class="k">' + escapeHtml(c.k) + '</div>' +
        '<div class="v ' + (c.cls || '') + '">' + escapeHtml(String(c.v)) + '</div></div>';
    }).join('');
  }

  // ---------- filters -------------------------------------------------------
  function applyFilters(rows) {
    const src = (document.getElementById('fltSource') || {}).value || '';
    const slot = (document.getElementById('fltSlot') || {}).value || '';
    const from = (document.getElementById('fltFrom') || {}).value || '';
    const to = (document.getElementById('fltTo') || {}).value || '';
    const minAbs = parseFloat((document.getElementById('fltMinAbs') || {}).value);
    const status = (document.getElementById('fltStatus') || {}).value || '';
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
      if (status === 'taken' && r.trade_taken !== true) return false;
      if (status === 'skipped' && r.trade_taken === true) return false;
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

  // ---------- table ---------------------------------------------------------
  function buildHead() {
    const head = document.querySelector('#resultsTable thead tr');
    if (!head) return;
    head.innerHTML = COLS.map(function (col) {
      const cls = col.align === 'num' ? 'num' : '';
      return '<th data-key="' + escapeHtml(col.key) + '" class="' + cls + '">' +
        escapeHtml(col.label) + '</th>';
    }).join('');
  }

  function renderTable() {
    const filtered = applyFilters(state.all);
    const sorted = sortRows(filtered);
    document.getElementById('rowCount').textContent =
      filtered.length + ' of ' + state.all.length + ' rows';
    const tbody = document.getElementById('tbody');
    if (!sorted.length) {
      tbody.innerHTML = '<tr><td class="nodata" colspan="' + COLS.length + '">No rows match the current filters.</td></tr>';
      return;
    }
    tbody.innerHTML = sorted.map(function (r) {
      const rowCls = (DAY_MODE === 'same' && r.trade_taken === false) ? ' class="row-skipped"' : '';
      const cells = COLS.map(function (col) { return col.cell(r); }).join('');
      return '<tr' + rowCls + '>' + cells + '</tr>';
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
          state.sort.dir = DESC_KEYS.has(k) ? 'desc' : 'asc';
        }
        updateSortHeaders();
        renderTable();
      });
    });
  }

  function wireFilters() {
    ['fltSource', 'fltSlot', 'fltFrom', 'fltTo', 'fltMinAbs', 'fltStatus'].forEach(function (id) {
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
    buildHead();
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
      if (DAY_MODE === 'same' && doc.summary && doc.summary.risk_cap_rupees) {
        meta.push('Risk cap: ₹' + doc.summary.risk_cap_rupees.toLocaleString('en-IN'));
      }
      document.getElementById('footerMeta').textContent = meta.join(' · ');
      renderTable();
    } catch (e) {
      showError('Could not load backtest artifact: ' + (e.message || e) +
        ' — Run `python3 backend/scripts/run_nks_intraday_backtest.py --mode both` on the server to generate it.');
      document.getElementById('tbody').innerHTML =
        '<tr><td class="nodata" colspan="' + COLS.length + '">Backtest data not yet available.</td></tr>';
    }
  }

  document.addEventListener('DOMContentLoaded', main);
})();
