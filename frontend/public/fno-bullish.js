/**
 * F&O Bullish Trend scanner backtest renderer.
 *
 * ``<body data-fno-mode="dual">`` — two exits (disappearance + 15:15), full table.
 * ``<body data-fno-mode="eod">`` — fixed exit at 15:15 IST only; summary + columns
 * use exit2 fields only.
 *
 * Loads /api/fno-bullish/data (or fallback /fno-bullish/data).
 */
(function () {
  'use strict';

  const MODE = (document.body && document.body.dataset && document.body.dataset.fnoMode) || 'dual';
  const IS_EOD = MODE === 'eod';

  /** Illustrative NRML-style margin ≈ this fraction of contract value at entry (SPAN varies by broker). */
  const EST_MARGIN_FRAC = 0.125;

  const API_PATHS = ['/api/fno-bullish/data', '/fno-bullish/data'];

  const state = {
    all: [],
    sort: { key: 'trade_date', dir: 'desc' },
  };

  function entryContractValue(r) {
    const p = Number(r.entry_price);
    const lot = Number(r.fut_lot_size || r.lot_size);
    if (!Number.isFinite(p) || !Number.isFinite(lot) || lot <= 0) return null;
    return p * lot;
  }

  /** Rough margin rupees for one position (contract value × EST_MARGIN_FRAC). */
  function estMarginRs(r) {
    const cv = entryContractValue(r);
    if (cv == null) return null;
    return cv * EST_MARGIN_FRAC;
  }

  /** Minutes from midnight for intra-day ordering (entry trigger ordering). */
  function scanTimeToMinutes(hhmm) {
    if (!hhmm || typeof hhmm !== 'string') return 99999;
    const m = hhmm.trim().match(/^(\d{1,2}):(\d{2})$/);
    if (!m) return 99999;
    return parseInt(m[1], 10) * 60 + parseInt(m[2], 10);
  }

  /** Peak-to-trough drawdown on cumulative realised P&amp;L ordered by entry (first_scan). */
  function portfolioMaxDrawdownRupees(pairsSorted) {
    let peak = 0;
    let eq = 0;
    let maxDd = 0;
    for (let i = 0; i < pairsSorted.length; i++) {
      const p = pairsSorted[i].pnl;
      if (typeof p !== 'number' || !Number.isFinite(p)) continue;
      eq += p;
      peak = Math.max(peak, eq);
      maxDd = Math.max(maxDd, peak - eq);
    }
    return maxDd;
  }

  function aggregateDayStats(dayRows) {
    let sumRs = 0;
    let sumPts = 0;
    let wins = 0;
    let losses = 0;
    let flat = 0;
    let worst = null;
    let best = null;
    const withPnl = [];
    for (let i = 0; i < dayRows.length; i++) {
      const r = dayRows[i];
      const rs = r.exit2_pnl_rupees;
      const pt = r.exit2_pnl_points;
      if (typeof rs === 'number' && Number.isFinite(rs)) {
        sumRs += rs;
        withPnl.push({ pnl: rs, min: scanTimeToMinutes(r.first_scan_time), sym: r.symbol || '' });
      }
      if (typeof pt === 'number' && Number.isFinite(pt)) sumPts += pt;
      if (typeof rs === 'number' && Number.isFinite(rs)) {
        if (rs > 0) wins++;
        else if (rs < 0) losses++;
        else flat++;
        if (worst === null || rs < worst) worst = rs;
        if (best === null || rs > best) best = rs;
      }
    }
    withPnl.sort((a, b) => {
      if (a.min !== b.min) return a.min - b.min;
      return String(a.sym).localeCompare(String(b.sym));
    });
    const maxDdRs = portfolioMaxDrawdownRupees(withPnl);
    let sumMarginEst = 0;
    for (let i = 0; i < dayRows.length; i++) {
      const m = estMarginRs(dayRows[i]);
      if (typeof m === 'number' && Number.isFinite(m)) sumMarginEst += m;
    }
    return {
      n: dayRows.length,
      sumRs: sumRs,
      sumPts: sumPts,
      wins: wins,
      losses: losses,
      flat: flat,
      worst: worst,
      best: best,
      maxDdRs: maxDdRs,
      sumMarginEst: sumMarginEst,
    };
  }

  function renderSummaryFromFilteredRows(rows) {
    const grid = document.getElementById('summaryGrid');
    if (!grid || !IS_EOD) return;
    let withEntry = 0;
    let sumMarginAll = 0;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (r.entry_price != null) withEntry++;
      const m = estMarginRs(r);
      if (typeof m === 'number' && Number.isFinite(m)) sumMarginAll += m;
    }
    let sumRs = 0;
    let wins = 0;
    let losses = 0;
    let worst = null;
    let best = null;
    const forDd = [];
    for (let i = 0; i < rows.length; i++) {
      const rs = rows[i].exit2_pnl_rupees;
      if (typeof rs !== 'number' || !Number.isFinite(rs)) continue;
      sumRs += rs;
      if (rs > 0) wins++;
      else if (rs < 0) losses++;
      if (worst === null || rs < worst) worst = rs;
      if (best === null || rs > best) best = rs;
      forDd.push({
        pnl: rs,
        min: scanTimeToMinutes(rows[i].first_scan_time),
        sym: rows[i].symbol || '',
      });
    }
    forDd.sort((a, b) => {
      if (a.min !== b.min) return a.min - b.min;
      return String(a.sym).localeCompare(String(b.sym));
    });
    const portfolioMdd = portfolioMaxDrawdownRupees(forDd);

    const cards = [
      ['Exit rule', '15:15 IST · FUT only · 1× symbol / day', 'First scanner streak · EQ rows excluded'],
      ['Trades (filtered)', fmtInt(rows.length), 'with entry price=' + fmtInt(withEntry)],
      ['Σ est. margin @ entry', fmtRupees(sumMarginAll), Math.round(EST_MARGIN_FRAC * 100) + '% of contract value · illustrative'],
      ['Σ PnL @ 15:15', fmtRupees(sumRs), wins + ' W / ' + losses + ' L'],
      ['Portfolio max DD (₹)', fmtRupees(portfolioMdd), 'prefix path along entry-time order · worst trade ' + fmtRupees(worst)],
      ['Best / Worst trade', fmtRupees(best) + ' / ' + fmtRupees(worst), 'single-symbol extremes'],
    ];
    grid.innerHTML = cards.map(([title, val, sub]) => (
      '<div class="card">' +
        '<div class="title">' + escapeHtml(title) + '</div>' +
        '<div class="value">' + escapeHtml(val || '—') + '</div>' +
        '<div class="sub">' + escapeHtml(sub || '') + '</div>' +
      '</div>'
    )).join('');
  }

  function renderDateQuickSummary(rows) {
    const host = document.getElementById('fnoDateQuickSummary');
    if (!host || !IS_EOD) return;
    if (!rows.length) {
      host.innerHTML = '<p class="lede" style="margin:0 0 10px;">No FUT rows for the current filters.</p>';
      return;
    }
    const byDate = {};
    for (let i = 0; i < rows.length; i++) {
      const d = rows[i].trade_date;
      if (!byDate[d]) byDate[d] = [];
      byDate[d].push(rows[i]);
    }
    const dates = Object.keys(byDate).sort(function (a, b) { return b.localeCompare(a); });
    let totTrades = 0;
    let totPnl = 0;
    let totMargin = 0;
    const body = [];
    for (let i = 0; i < dates.length; i++) {
      const d = dates[i];
      const dayRows = byDate[d];
      const st = aggregateDayStats(dayRows);
      totTrades += st.n;
      totPnl += st.sumRs;
      totMargin += st.sumMarginEst;
      body.push(
        '<tr>' +
        '<td>' + escapeHtml(fmtDate(d)) + '</td>' +
        '<td class="num">' + fmtInt(st.n) + '</td>' +
        '<td class="num ' + pnlCls(st.sumRs) + '">' + fmtRupees(st.sumRs) + '</td>' +
        '<td class="num ' + pnlCls(st.worst) + '">' + fmtRupees(st.worst) + '</td>' +
        '<td class="num">' + fmtRupees(st.sumMarginEst) + '</td>' +
        '</tr>'
      );
    }
    const pnlClsTot = pnlCls(totPnl);
    host.innerHTML =
      '<h2 class="fno-quick-h2">Date-wise summary (FUT · filtered)</h2>' +
      '<div class="table-wrap">' +
      '<table class="data fno-quick-summary" id="fnoQuickSummaryTable">' +
      '<thead><tr>' +
      '<th>Date</th><th class="num">Trades</th><th class="num">Σ PnL</th>' +
      '<th class="num">Worst trade</th><th class="num">Est. total margin</th>' +
      '</tr></thead><tbody>' +
      body.join('') +
      '<tr class="fno-quick-total">' +
      '<td><strong>Total</strong></td>' +
      '<td class="num"><strong>' + fmtInt(totTrades) + '</strong></td>' +
      '<td class="num ' + pnlClsTot + '"><strong>' + fmtRupees(totPnl) + '</strong></td>' +
      '<td class="num">—</td>' +
      '<td class="num"><strong>' + fmtRupees(totMargin) + '</strong></td>' +
      '</tr></tbody></table></div>' +
      '<p class="cell-note" style="margin:8px 0 0; font-size:0.85rem;">' +
      'Est. total margin = sum of (entry price × lot size × ' + (EST_MARGIN_FRAC * 100) +
      '%) for each trade that day. Use as a scale figure; actual SPAN + exposure is set by the exchange and your broker.</p>';
  }

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
    if (v === null || v === undefined) return '—';
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

  // ---------- column specs ------------------------------------------------

  const COLS_EOD = [
    { key: 'trade_date', label: 'Date', sortable: true,
      cell: r => '<td>' + fmtDate(r.trade_date) + '</td>',
      sortVal: r => r.trade_date || '' },
    { key: 'symbol', label: 'Symbol', sortable: true,
      cell: r => {
        const sym = escapeHtml(r.symbol || '');
        const tsym = r.trading_symbol ? '<div class="cell-note">' + escapeHtml(r.trading_symbol) + '</div>' : '';
        return '<td class="cell-symbol">' + sym + tsym + '</td>';
      },
      sortVal: r => r.symbol || '' },
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
    { key: 'entry_time', label: 'Entry @', sortable: true,
      cell: r => '<td>' + escapeHtml(r.entry_time || '—') + '</td>',
      sortVal: r => r.entry_time || '' },
    { key: 'entry_price', label: 'Entry ₹', sortable: true,
      cell: r => '<td class="num">' + fmtNum(r.entry_price) + '</td>',
      sortVal: r => Number(r.entry_price) || 0 },
    { key: 'exit2_time', label: 'Exit @', sortable: true,
      cell: r => '<td title="Fixed session exit">' + escapeHtml(r.exit2_time || '15:15') + '</td>',
      sortVal: r => r.exit2_time || '' },
    { key: 'exit2_price', label: 'Exit ₹', sortable: true,
      cell: r => '<td class="num col-eod-exit">' + fmtNum(r.exit2_price) + '</td>',
      sortVal: r => Number(r.exit2_price) || 0 },
    { key: 'exit2_pnl_points', label: 'PnL pts', sortable: true,
      cell: r => '<td class="num ' + pnlCls(r.exit2_pnl_points) + '">' + signedPts(r.exit2_pnl_points) + '</td>',
      sortVal: r => Number(r.exit2_pnl_points) || 0 },
    { key: 'exit2_pnl_rupees', label: 'PnL ₹', sortable: true,
      cell: r => '<td class="num ' + pnlCls(r.exit2_pnl_rupees) + '">' + fmtRupees(r.exit2_pnl_rupees) + '</td>',
      sortVal: r => Number(r.exit2_pnl_rupees) || 0 },
  ];

  const COLS_DUAL = [
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
    { key: 'exit2_price', label: 'Exit 2 ₹ (15:15)', sortable: true,
      cell: r => '<td class="num col-exit2">' + fmtNum(r.exit2_price) + '</td>',
      sortVal: r => Number(r.exit2_price) || 0 },
    { key: 'exit2_pnl_rupees', label: 'PnL₂ ₹', sortable: true,
      cell: r => '<td class="num ' + pnlCls(r.exit2_pnl_rupees) + '">' + fmtRupees(r.exit2_pnl_rupees) + '</td>',
      sortVal: r => Number(r.exit2_pnl_rupees) || 0 },
  ];

  const COLS = IS_EOD ? COLS_EOD : COLS_DUAL;

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
    const raw = Array.isArray(doc.rows) ? doc.rows.slice() : [];
    state.all = IS_EOD ? raw.filter(function (r) {
      return Number(r.run_index) === 1 && r.source === 'FUT';
    }) : raw;
    renderHeader();
    if (IS_EOD) {
      renderSummaryFromFilteredRows(state.all);
    } else {
      renderSummary(doc.summary || {});
    }
    renderFooter(doc);
    applyFilters();
  }

  function renderHeader() {
    const tr = document.querySelector('#resultsTable thead tr');
    tr.innerHTML = '';
    COLS.forEach((c) => {
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
    if (!grid || IS_EOD) return;

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
    if (IS_EOD) {
      bits.push('<span class="muted">EOD 15:15 · FUT only · first run / symbol / day</span>');
    }
    f.innerHTML = bits.join(' · ');
  }

  // ---------- filters + sorting --------------------------------------------

  function applyFilters() {
    const from = document.getElementById('fltFrom').value;
    const to = document.getElementById('fltTo').value;
    const src = document.getElementById('fltSource').value;
    const kindEl = document.getElementById('fltKind');
    const kind = kindEl ? kindEl.value : '';
    const symQ = (document.getElementById('fltSymbol').value || '').trim().toUpperCase();

    let rows = state.all.filter(function (r) {
      if (from && r.trade_date < from) return false;
      if (to && r.trade_date > to) return false;
      if (!IS_EOD && src && r.source !== src) return false;
      if (!IS_EOD) {
        if (kind === 'first' && r.is_reentry) return false;
        if (kind === 'reentry' && !r.is_reentry) return false;
        if (kind === 'never' && r.exit1_kind !== 'never_disappeared') return false;
      }
      if (symQ && !(r.symbol || '').toUpperCase().includes(symQ)) return false;
      return true;
    });

    const col = COLS.find(function (c) { return c.key === state.sort.key; });

    if (IS_EOD) {
      const byDate = {};
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const d = r.trade_date;
        if (!byDate[d]) byDate[d] = [];
        byDate[d].push(r);
      }
      let dates = Object.keys(byDate);
      if (state.sort.key === 'trade_date') {
        dates.sort(function (a, b) {
          const cmp = a.localeCompare(b);
          return state.sort.dir === 'desc' ? -cmp : cmp;
        });
      } else {
        dates.sort(function (a, b) { return b.localeCompare(a); });
      }
      dates.forEach(function (d) {
        const arr = byDate[d];
        if (col && state.sort.key !== 'trade_date') {
          const dir = state.sort.dir === 'desc' ? -1 : 1;
          arr.sort(function (a, b) {
            const va = col.sortVal(a);
            const vb = col.sortVal(b);
            if (va === vb) return 0;
            if (va == null) return 1;
            if (vb == null) return -1;
            return va < vb ? -1 * dir : 1 * dir;
          });
        } else {
          arr.sort(function (a, b) {
            return String(a.symbol || '').localeCompare(String(b.symbol || ''));
          });
        }
      });

      document.getElementById('rowCount').textContent =
        rows.length + ' / ' + state.all.length + ' FUT trades (1× symbol / day)';
      renderSummaryFromFilteredRows(rows);
      renderDateQuickSummary(rows);

      const tbody = document.getElementById('tbody');
      if (!rows.length) {
        tbody.innerHTML = '<tr><td class="nodata" colspan="' + COLS.length +
          '">No rows match the filters.</td></tr>';
      } else {
        const parts = [];
        const nc = COLS.length;
        const labelCols = nc - 2;
        for (let di = 0; di < dates.length; di++) {
          const d = dates[di];
          const dayRows = byDate[d];
          parts.push(
            '<tr class="fno-date-head"><td colspan="' + nc + '">' +
            '<strong>' + escapeHtml(fmtDate(d)) + '</strong> · ' + dayRows.length + ' symbol(s)' +
            '</td></tr>'
          );
          const st = aggregateDayStats(dayRows);
          for (let j = 0; j < dayRows.length; j++) {
            const r = dayRows[j];
            parts.push('<tr>' + COLS.map(function (c) { return c.cell(r); }).join('') + '</tr>');
          }
          parts.push(
            '<tr class="fno-date-subtotal">' +
            '<td colspan="' + labelCols + '">' +
            '<strong>Subtotal</strong> · ' + escapeHtml(fmtDate(d)) +
            ' · Σ ' + fmtRupees(st.sumRs) +
            ' · ' + st.wins + ' W / ' + st.losses + ' L' +
            (st.flat ? ' · ' + st.flat + ' flat' : '') +
            ' · portfolio MDD ' + fmtRupees(st.maxDdRs) +
            ' · worst trade ' + fmtRupees(st.worst) +
            ' · est. margin ' + fmtRupees(st.sumMarginEst) +
            '</td>' +
            '<td class="num">' + (Number.isFinite(st.sumPts) ? signedPts(st.sumPts) : '—') + '</td>' +
            '<td class="num ' + pnlCls(st.sumRs) + '">' + fmtRupees(st.sumRs) + '</td>' +
            '</tr>'
          );
        }
        tbody.innerHTML = parts.join('');
      }
    } else {
      if (col) {
        const dir = state.sort.dir === 'desc' ? -1 : 1;
        rows.sort(function (a, b) {
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
        tbody.innerHTML = '<tr><td class="nodata" colspan="' + COLS.length +
          '">No rows match the filters.</td></tr>';
      } else {
        tbody.innerHTML = rows.map(function (r) {
          return '<tr>' + COLS.map(function (c) { return c.cell(r); }).join('') + '</tr>';
        }).join('');
      }
    }

    document.querySelectorAll('#resultsTable thead th').forEach(function (th) {
      th.classList.remove('sort-asc', 'sort-desc');
      if (th.dataset && th.dataset.key === state.sort.key) {
        th.classList.add(state.sort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
      }
    });
  }

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
