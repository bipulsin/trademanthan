/**
 * Daily Futures — workspace polling + buy/sell modals (ChartInk-driven picks).
 */
(function () {
  const API_BASE =
    window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
      ? 'http://localhost:8000'
      : window.location.origin;

  function authHeaders() {
    const t = localStorage.getItem('trademanthan_token') || '';
    return {
      Authorization: 'Bearer ' + t,
      'Content-Type': 'application/json',
    };
  }

  function istHmNow() {
    const parts = new Intl.DateTimeFormat('en-GB', {
      timeZone: 'Asia/Kolkata',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    }).formatToParts(new Date());
    const h = parts.find(function (p) { return p.type === 'hour'; });
    const m = parts.find(function (p) { return p.type === 'minute'; });
    return (h ? h.value : '00') + ':' + (m ? m.value : '00');
  }

  function fmtIso(iso) {
    if (!iso) return '—';
    try {
      const d = new Date(iso);
      if (!Number.isNaN(d.getTime())) {
        return d.toLocaleString('en-IN', {
          timeZone: 'Asia/Kolkata',
          dateStyle: 'short',
          timeStyle: 'short',
        });
      }
    } catch (e) {}
    return String(iso);
  }

  /** IST time only (HH:MM, 24h) for ISO timestamps — used in Running order 1st/Last scan. */
  function fmtIsoTimeIst(iso) {
    if (!iso) return '—';
    try {
      const d = new Date(iso);
      if (!Number.isNaN(d.getTime())) {
        return d.toLocaleTimeString('en-GB', {
          timeZone: 'Asia/Kolkata',
          hour: '2-digit',
          minute: '2-digit',
          hour12: false,
        });
      }
    } catch (e) {}
    return String(iso);
  }

  function rowUnrealizedPnlRupees(r) {
    const ltp = Number(r.ltp);
    const ep = Number(r.entry_price);
    const qty = Number(r.lot_size);
    if (!Number.isFinite(ltp) || !Number.isFinite(ep) || !Number.isFinite(qty)) return null;
    return (ltp - ep) * qty;
  }

  function unrealizedPnlCell(r) {
    const pnl = rowUnrealizedPnlRupees(r);
    if (pnl == null) return '<td class="num">—</td>';
    const cls = pnl > 0 ? 'df-pnl-pos' : pnl < 0 ? 'df-pnl-neg' : '';
    return '<td class="num ' + cls + '">' + fmtMoney(pnl) + '</td>';
  }

  function sumRunningUnrealized(rows) {
    var sum = 0;
    var n = 0;
    rows.forEach(function (r) {
      var v = rowUnrealizedPnlRupees(r);
      if (v != null) {
        sum += v;
        n += 1;
      }
    });
    return { sum: sum, n: n };
  }

  function fmtNum(v, d) {
    if (v == null || v === '') return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return n.toFixed(d != null ? d : 2);
  }

  function fmtMoney(v) {
    if (v == null || v === '') return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return '₹' + n.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  const state = {
    workspace: null,
    pickScreeningId: null,
    sellTradeId: null,
  };

  async function fetchWorkspace() {
    const paths = ['/api/daily-futures/workspace', '/daily-futures/workspace'];
    let lastErr = null;
    for (let i = 0; i < paths.length; i++) {
      try {
        const res = await fetch(API_BASE + paths[i], { headers: authHeaders(), cache: 'no-store' });
        if (res.status === 401) {
          window.location.replace('index.html');
          return null;
        }
        if (!res.ok) {
          const t = await res.text();
          lastErr = new Error(t.slice(0, 200) || res.status);
          continue;
        }
        return await res.json();
      } catch (e) {
        lastErr = e;
      }
    }
    throw lastErr || new Error('workspace');
  }

  function renderPicks(picks) {
    const el = document.getElementById('dfPicksTable');
    if (!el) return;
    if (!picks || !picks.length) {
      el.innerHTML = '<p class="df-meta">No picks yet.</p>';
      return;
    }
    const th =
      '<thead><tr><th>Future</th><th>Qty (1 lot)</th><th>Scan #</th><th>1st scan</th><th>Last scan</th><th>Conviction</th><th class="num">LTP</th><th></th></tr></thead>';
    const body = picks
      .map(function (r) {
        return (
          '<tr><td><strong>' +
          esc(r.future_symbol || r.underlying) +
          '</strong><div style="font-size:0.75rem;color:var(--theme-muted);">' +
          esc(r.underlying) +
          '</div></td><td class="num">' +
          esc(r.lot_size) +
          '</td><td class="num">' +
          esc(r.scan_count) +
          '</td><td>' +
          fmtIso(r.first_hit_at) +
          '</td><td>' +
          fmtIso(r.last_hit_at) +
          '</td><td class="num">' +
          fmtNum(r.conviction_score, 1) +
          '</td><td class="num">' +
          fmtNum(r.ltp, 2) +
          '</td><td><button type="button" class="df-btn df-btn-order" data-sid="' +
          r.screening_id +
          '">Order</button></td></tr>'
        );
      })
      .join('');
    el.innerHTML = '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
    el.querySelectorAll('button[data-sid]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        const sid = parseInt(btn.getAttribute('data-sid'), 10);
        const row = picks.find(function (p) { return p.screening_id === sid; });
        openBuyModal(sid, row);
      });
    });
  }

  function renderRunning(rows) {
    const el = document.getElementById('dfRunningTable');
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<p style="color:var(--theme-muted);">No open positions.</p>';
      return;
    }
    const th =
      '<thead><tr><th>Future</th><th>Qty</th><th>Scan #</th><th>1st scan</th><th>Last scan</th><th>Conviction</th><th class="num">LTP</th><th>Entry time</th><th class="num">Entry ₹</th><th class="num">Unrealized PnL</th><th></th></tr></thead>';
    const tot = sumRunningUnrealized(rows);
    const totalLine =
      '<p class="df-meta" style="margin:0 0 10px;font-size:0.9rem;">' +
      '<strong>Total unrealized PnL:</strong> ' +
      (tot.n > 0
        ? '<span class="' +
          (tot.sum > 0 ? 'df-pnl-pos' : tot.sum < 0 ? 'df-pnl-neg' : '') +
          '">' +
          fmtMoney(tot.sum) +
          '</span>'
        : '—') +
      '</p>';
    const body = rows
      .map(function (r) {
        const warn = r.warn_two_misses
          ? '<span class="df-blink" title="Not seen in the last two consecutive webhooks">↓</span>'
          : '';
        return (
          '<tr><td><strong>' +
          esc(r.future_symbol || r.underlying) +
          '</strong></td><td class="num">' +
          esc(r.lot_size) +
          '</td><td class="num">' +
          esc(r.scan_count) +
          warn +
          '</td><td>' +
          fmtIsoTimeIst(r.first_hit_at) +
          '</td><td>' +
          fmtIsoTimeIst(r.last_hit_at) +
          '</td><td class="num">' +
          fmtNum(r.conviction_score, 1) +
          '</td><td class="num">' +
          fmtNum(r.ltp, 2) +
          '</td><td>' +
          esc(r.entry_time) +
          '</td><td class="num">' +
          fmtNum(r.entry_price, 2) +
          '</td>' +
          unrealizedPnlCell(r) +
          '<td><button type="button" class="df-btn df-btn-sell" data-tid="' +
          r.trade_id +
          '">Sell</button></td></tr>'
        );
      })
      .join('');
    el.innerHTML = totalLine + '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
    el.querySelectorAll('button[data-tid]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        const tid = parseInt(btn.getAttribute('data-tid'), 10);
        const row = rows.find(function (x) { return x.trade_id === tid; });
        openSellModal(tid, row);
      });
    });
  }

  function renderClosed(rows, summary) {
    const sumEl = document.getElementById('dfClosedSummary');
    const el = document.getElementById('dfClosedTable');
    if (sumEl) {
      const s = summary || {};
      sumEl.innerHTML =
        '<span><strong>Cumulative PnL:</strong> ' +
        fmtMoney(s.cumulative_pnl_rupees) +
        '</span><span><strong>Wins / Losses:</strong> ' +
        esc(s.wins) +
        ' / ' +
        esc(s.losses) +
        '</span><span><strong>Win rate:</strong> ' +
        (s.win_rate_pct != null ? esc(s.win_rate_pct) + '%' : '—') +
        '</span>';
    }
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<p style="color:var(--theme-muted);">No closed trades yet.</p>';
      return;
    }
    const th =
      '<thead><tr><th>Future</th><th>Qty</th><th>Entry</th><th class="num">Entry ₹</th><th>Exit</th><th class="num">Exit ₹</th><th class="num">PnL ₹</th><th>Win/Loss</th></tr></thead>';
    const body = rows
      .map(function (r) {
        const wl = r.win_loss || '—';
        const wlCls =
          wl === 'Win' ? 'df-wl-win' : wl === 'Loss' ? 'df-wl-loss' : '';
        const pnl = r.pnl_rupees;
        const pnlCls =
          typeof pnl === 'number'
            ? pnl > 0
              ? 'df-pnl-pos'
              : pnl < 0
                ? 'df-pnl-neg'
                : ''
            : '';
        return (
          '<tr><td><strong>' +
          esc(r.future_symbol || r.underlying) +
          '</strong></td><td class="num">' +
          esc(r.lot_size) +
          '</td><td>' +
          esc(r.entry_time) +
          '</td><td class="num">' +
          fmtNum(r.entry_price, 2) +
          '</td><td>' +
          esc(r.exit_time) +
          '</td><td class="num">' +
          fmtNum(r.exit_price, 2) +
          '</td><td class="num ' +
          pnlCls +
          '">' +
          fmtMoney(pnl) +
          '</td><td><span class="' +
          wlCls +
          '">' +
          esc(wl) +
          '</span></td></tr>'
        );
      })
      .join('');
    el.innerHTML = '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
  }

  function rowProjectedPnlRupees(r) {
    const ltp = Number(r.ltp);
    const ep = Number(r.entry_price);
    const qty = Number(r.lot_size);
    if (!Number.isFinite(ltp) || !Number.isFinite(ep) || !Number.isFinite(qty)) return null;
    return (ltp - ep) * qty;
  }

  function renderWhatIfContinuing(rows) {
    const sumEl = document.getElementById('dfWhatIfSummary');
    const el = document.getElementById('dfWhatIfTable');
    if (!el) return;
    const sold = (rows || []).filter(function (r) {
      return r && r.screening_id != null;
    });
    var sumProj = 0;
    var wins = 0;
    var losses = 0;
    var n = 0;
    sold.forEach(function (r) {
      var p = rowProjectedPnlRupees(r);
      if (p == null) return;
      sumProj += p;
      n += 1;
      if (p > 0) wins += 1;
      else if (p < 0) losses += 1;
    });
    var denom = wins + losses;
    var wr = denom ? (100.0 * wins / denom).toFixed(1) : null;
    if (sumEl) {
      sumEl.innerHTML =
        '<span><strong>Cumulative PnL:</strong> ' +
        (n ? fmtMoney(sumProj) : '—') +
        '</span><span><strong>Wins / Losses:</strong> ' +
        wins +
        ' / ' +
        losses +
        '</span><span><strong>Win rate:</strong> ' +
        (wr != null ? wr + '%' : '—') +
        '</span>';
    }
    if (!sold.length) {
      el.innerHTML = '<p style="color:var(--theme-muted);">No sold trades yet.</p>';
      return;
    }
    const th =
      '<thead><tr><th>Future</th><th class="num">Qty</th><th>Entry time</th><th class="num">Entry ₹</th><th class="num">Current LTP</th><th class="num">Projected PnL</th></tr></thead>';
    const body = sold
      .map(function (r) {
        const pnl = rowProjectedPnlRupees(r);
        const pnlCls = pnl > 0 ? 'df-pnl-pos' : pnl < 0 ? 'df-pnl-neg' : '';
        return (
          '<tr><td><strong>' +
          esc(r.future_symbol || r.underlying) +
          '</strong></td><td class="num">' +
          esc(r.lot_size) +
          '</td><td>' +
          esc(r.entry_time) +
          '</td><td class="num">' +
          fmtNum(r.entry_price, 2) +
          '</td><td class="num">' +
          fmtNum(r.ltp, 2) +
          '</td><td class="num ' +
          pnlCls +
          '">' +
          fmtMoney(pnl) +
          '</td></tr>'
        );
      })
      .join('');
    el.innerHTML = '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
  }

  function renderTradeIfCouldHaveDone(rows) {
    const el = document.getElementById('dfTradeIfCouldTable');
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<p style="color:var(--theme-muted);">No eligible picks.</p>';
      return;
    }
    const th =
      '<thead><tr><th>Future</th><th class="num">Qty</th><th>1st scan</th><th>Entry time (+5m)</th><th class="num">Entry LTP</th><th>Exit 12:45</th><th class="num">LTP 12:45</th><th class="num">PnL 12:45</th><th>Exit 15:15</th><th class="num">LTP 15:15</th><th class="num">PnL 15:15</th></tr></thead>';
    const body = rows
      .map(function (r) {
        const p1245 = Number(r.pnl_1245_rupees);
        const p1515 = Number(r.pnl_1515_rupees);
        const c1245 = Number.isFinite(p1245) ? (p1245 > 0 ? 'df-pnl-pos' : p1245 < 0 ? 'df-pnl-neg' : '') : '';
        const c1515 = Number.isFinite(p1515) ? (p1515 > 0 ? 'df-pnl-pos' : p1515 < 0 ? 'df-pnl-neg' : '') : '';
        const skip1245 = !!r.after_1230_only_1515;
        return (
          '<tr><td><strong>' +
          esc(r.future_symbol || r.underlying) +
          '</strong></td><td class="num">' +
          esc(r.qty) +
          '</td><td>' +
          esc(r.first_scan_time) +
          '</td><td>' +
          esc(r.entry_time) +
          '</td><td class="num">' +
          fmtNum(r.entry_ltp, 2) +
          '</td><td>' +
          (skip1245 ? '—' : esc(r.exit_1245_time)) +
          '</td><td class="num">' +
          (skip1245 ? '—' : fmtNum(r.exit_1245_ltp, 2)) +
          '</td><td class="num ' +
          c1245 +
          '">' +
          (skip1245 ? '—' : fmtMoney(r.pnl_1245_rupees)) +
          '</td><td>' +
          esc(r.exit_1515_time) +
          '</td><td class="num">' +
          fmtNum(r.exit_1515_ltp, 2) +
          '</td><td class="num ' +
          c1515 +
          '">' +
          fmtMoney(r.pnl_1515_rupees) +
          '</td></tr>'
        );
      })
      .join('');
    el.innerHTML = '<div class="df-table-wrap"><table class="df-table">' + th + '<tbody>' + body + '</tbody></table></div>';
  }

  function esc(s) {
    if (s == null) return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function openBuyModal(screeningId, row) {
    state.pickScreeningId = screeningId;
    const m = document.getElementById('dfBuyModal');
    document.getElementById('dfBuySym').textContent = row
      ? row.future_symbol + ' · ' + row.underlying
      : '';
    document.getElementById('dfBuyTime').value = istHmNow();
    document.getElementById('dfBuyPrice').value =
      row && row.ltp != null ? String(row.ltp) : '';
    document.getElementById('dfBuyErr').textContent = '';
    m.setAttribute('aria-hidden', 'false');
  }

  function closeBuyModal() {
    document.getElementById('dfBuyModal').setAttribute('aria-hidden', 'true');
    state.pickScreeningId = null;
  }

  function openSellModal(tradeId, row) {
    state.sellTradeId = tradeId;
    document.getElementById('dfSellSym').textContent = row
      ? row.future_symbol + ' · ' + row.underlying
      : '';
    document.getElementById('dfSellTime').value = istHmNow();
    document.getElementById('dfSellPrice').value =
      row && row.ltp != null ? String(row.ltp) : '';
    document.getElementById('dfSellErr').textContent = '';
    document.getElementById('dfSellModal').setAttribute('aria-hidden', 'false');
  }

  function closeSellModal() {
    document.getElementById('dfSellModal').setAttribute('aria-hidden', 'true');
    state.sellTradeId = null;
  }

  async function submitBuy() {
    const sid = state.pickScreeningId;
    const et = document.getElementById('dfBuyTime').value.trim();
    const ep = parseFloat(String(document.getElementById('dfBuyPrice').value).replace(/,/g, ''));
    const err = document.getElementById('dfBuyErr');
    err.textContent = '';
    if (!sid || !et || !Number.isFinite(ep)) {
      err.textContent = 'Enter valid time and price.';
      return;
    }
    const paths = ['/api/daily-futures/order/buy', '/daily-futures/order/buy'];
    let lastErr = null;
    for (let i = 0; i < paths.length; i++) {
      try {
        const res = await fetch(API_BASE + paths[i], {
          method: 'POST',
          headers: authHeaders(),
          body: JSON.stringify({
            screening_id: sid,
            entry_time: et,
            entry_price: ep,
          }),
        });
        const raw = await res.text();
        if (res.ok) {
          closeBuyModal();
          await refresh();
          return;
        }
        try {
          const j = JSON.parse(raw);
          err.textContent = j.detail || raw.slice(0, 120);
        } catch (e2) {
          err.textContent = raw.slice(0, 120);
        }
        return;
      } catch (e) {
        lastErr = e;
      }
    }
    err.textContent = lastErr ? lastErr.message : 'Request failed';
  }

  async function submitSell() {
    const tid = state.sellTradeId;
    const xt = document.getElementById('dfSellTime').value.trim();
    const xp = parseFloat(String(document.getElementById('dfSellPrice').value).replace(/,/g, ''));
    const err = document.getElementById('dfSellErr');
    err.textContent = '';
    if (!tid || !xt || !Number.isFinite(xp)) {
      err.textContent = 'Enter valid time and price.';
      return;
    }
    const paths = ['/api/daily-futures/order/sell', '/daily-futures/order/sell'];
    for (let i = 0; i < paths.length; i++) {
      try {
        const res = await fetch(API_BASE + paths[i], {
          method: 'POST',
          headers: authHeaders(),
          body: JSON.stringify({
            trade_id: tid,
            exit_time: xt,
            exit_price: xp,
          }),
        });
        const raw = await res.text();
        if (res.ok) {
          closeSellModal();
          await refresh();
          return;
        }
        try {
          const j = JSON.parse(raw);
          err.textContent = j.detail || raw.slice(0, 120);
        } catch (e2) {
          err.textContent = raw.slice(0, 120);
        }
        return;
      } catch (e) {
        /* try next path */
      }
    }
    err.textContent = 'Request failed';
  }

  async function refresh() {
    const b = document.getElementById('dfBanner');
    try {
      const data = await fetchWorkspace();
      state.workspace = data;
      if (b) {
        b.textContent =
          'Session date (IST): ' +
          (data.trade_date || '—') +
          ' · Auto-refresh ~12s';
      }
      renderPicks(data.picks || []);
      renderRunning(data.running || []);
      renderClosed(data.closed || [], data.summary);
      renderWhatIfContinuing(data.closed || []);
      renderTradeIfCouldHaveDone(data.trade_if_could_have_done || []);
    } catch (e) {
      if (b) b.textContent = 'Could not load workspace: ' + (e && e.message ? e.message : e);
    }
  }

  function bindModals() {
    document.getElementById('dfBuyBackdrop').addEventListener('click', closeBuyModal);
    document.getElementById('dfBuyCancel').addEventListener('click', closeBuyModal);
    document.getElementById('dfBuyOk').addEventListener('click', submitBuy);
    document.getElementById('dfSellBackdrop').addEventListener('click', closeSellModal);
    document.getElementById('dfSellCancel').addEventListener('click', closeSellModal);
    document.getElementById('dfSellOk').addEventListener('click', submitSell);
  }

  document.addEventListener('DOMContentLoaded', function () {
    bindModals();
    refresh();
    setInterval(refresh, 12000);
  });
})();
