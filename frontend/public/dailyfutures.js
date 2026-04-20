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

  function unrealizedPnlCell(r) {
    const ltp = Number(r.ltp);
    const ep = Number(r.entry_price);
    const qty = Number(r.lot_size);
    if (!Number.isFinite(ltp) || !Number.isFinite(ep) || !Number.isFinite(qty)) {
      return '<td class="num">—</td>';
    }
    const pnl = (ltp - ep) * qty;
    const cls = pnl > 0 ? 'df-pnl-pos' : pnl < 0 ? 'df-pnl-neg' : '';
    return '<td class="num ' + cls + '">' + fmtMoney(pnl) + '</td>';
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
    el.innerHTML = '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
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
