/**
 * Premium Futures — workspace polling + buy/sell modals (ChartInk-driven picks).
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
      Accept: 'application/json',
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

  function istDateTodayStr() {
    try {
      const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Kolkata',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
      }).formatToParts(new Date());
      const y = (parts.find(function (p) { return p.type === 'year'; }) || {}).value || '0000';
      const m = (parts.find(function (p) { return p.type === 'month'; }) || {}).value || '00';
      const d = (parts.find(function (p) { return p.type === 'day'; }) || {}).value || '00';
      return y + '-' + m + '-' + d;
    } catch (e) {
      return '';
    }
  }

  function updateSnapshotChip(workspace) {
    const chip = document.getElementById('dfSnapshotChip');
    if (!chip) return;
    const td = String((workspace && workspace.trade_date) || '').trim();
    const today = istDateTodayStr();
    const show = !!(td && today && td !== today);
    chip.hidden = !show;
    if (show) {
      chip.textContent = 'Snapshot';
      chip.title = 'Showing previous trading session data';
    }
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
    if (!r || r.ltp == null || r.lot_size == null || r.lot_size === '') {
      return null;
    }
    const ltp = Number(r.ltp);
    const qty = Number(r.lot_size);
    if (!Number.isFinite(ltp) || !Number.isFinite(qty)) return null;
    const isShort = String(r.direction_type || '').toUpperCase() === 'SHORT';
    if (isShort) {
      if (r.sell_price == null || r.sell_price === '') return null;
      const sp = Number(r.sell_price);
      if (!Number.isFinite(sp)) return null;
      return (sp - ltp) * qty;
    }
    if (r.entry_price == null || r.entry_price === '') {
      return null;
    }
    const ep = Number(r.entry_price);
    if (!Number.isFinite(ep)) return null;
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

  /**
   * Relative strength: day % (future) vs Nifty 50, plus FUT−Nify spread.
   * Backend sets stock_change_pct, nifty_change_pct, relative_strength_vs_nifty on each workspace load.
   */
  /**
   * Same pattern as Today's pick: Entry (second scan) and Live conviction from screening.
   */
  function formatConvictionEntryLive(r) {
    let convTxt =
      r.conviction_score == null
        ? '—'
        : '<span class="df-score-live">' + fmtNum(r.conviction_score, 1) + ' (L)</span>';
    const secondConv = r.second_scan_conviction_score == null ? null : Number(r.second_scan_conviction_score);
    const liveConv = r.conviction_score == null ? null : Number(r.conviction_score);
    if (Number.isFinite(secondConv)) {
      if (Number.isFinite(liveConv)) {
        convTxt =
          '<span class="df-score-entry">' +
          fmtNum(secondConv, 1) +
          ' (E)</span> | <span class="df-score-live">' +
          fmtNum(liveConv, 1) +
          ' (L)</span>';
      } else {
        convTxt = '<span class="df-score-entry">' + fmtNum(secondConv, 1) + ' (E)</span>';
      }
    }
    return convTxt;
  }

  function fmtRelStrength(r) {
    const s = r.stock_change_pct;
    const n = r.nifty_change_pct;
    if (s == null || n == null) {
      return '<span class="df-rs">—</span>';
    }
    const fs = Number(s);
    const fn = Number(n);
    if (!Number.isFinite(fs) || !Number.isFinite(fn)) {
      return '<span class="df-rs">—</span>';
    }
    const spread = r.relative_strength_vs_nifty != null && r.relative_strength_vs_nifty !== ''
      ? Number(r.relative_strength_vs_nifty)
      : fs - fn;
    if (!Number.isFinite(spread)) {
      return '<span class="df-rs">—</span>';
    }
    const strong = fs >= fn;
    const sp = (spread >= 0 ? '+' : '') + spread.toFixed(2) + '%';
    return (
      '<span class="df-rs ' + (strong ? 'df-rs-strong' : 'df-rs-weak') + '" ' +
      'title="Relative strength: future day % minus Nifty 50 day % = ' +
      esc(sp) +
      '. Green when FUT ≥ Nifty.">' +
      '<span class="df-rs-spread" style="font-weight:600;">' +
      esc(sp) +
      '</span></span>'
    );
  }

  const state = {
    workspace: null,
    pickScreeningId: null,
    sellTradeId: null,
    /** @type {Record<number, number>} trade_id -> bit mask of active exit alerts (1=nifty,2=trail,4=momo) */
    prevRunAlertBits: {},
    /** @type {Record<number, string>} trade_id -> last 15m strip decision (lock_profit, dual_exit, watch, hold) */
    prevStripDecisionByTid: {},
    refreshSeq: 0,
  };

  /** @param {string} kind  'lock_profit' | 'dual_exit' */
  function playStrip15mBeep(kind) {
    try {
      const Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      const ctx = new Ctx();
      const o = ctx.createOscillator();
      const g = ctx.createGain();
      o.connect(g);
      g.connect(ctx.destination);
      o.frequency.value = kind === 'dual_exit' ? 520 : 1100;
      o.type = 'sine';
      g.gain.setValueAtTime(0.12, ctx.currentTime);
      g.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.18);
      o.start();
      o.stop(ctx.currentTime + 0.18);
    } catch (e) {
      /* ignore */
    }
  }

  function updateStrip15mDecisionAudio(rows) {
    if (!rows || !rows.length) return;
    rows.forEach(function (r) {
      var tid = r.trade_id;
      if (tid == null) return;
      var s = (r.alert_strip && r.alert_strip.decision) || 'hold';
      var prev = state.prevStripDecisionByTid[tid];
      if (
        prev !== undefined &&
        prev !== s &&
        (s === 'lock_profit' || s === 'dual_exit')
      ) {
        playStrip15mBeep(s);
      }
      state.prevStripDecisionByTid[tid] = s;
    });
  }

  function stripL1Cell(st) {
    var l1 = (st && st.l1) || 'nifty_no_higher_high';
    if (l1 === 'nifty_higher_high') {
      return (
        '<span class="df-s-cell df-s-ok" title="Strong: latest completed 15m close is above previous 15m close by more than configured threshold.">Strong</span>'
      );
    }
    if (l1 === 'nifty_lower_low') {
      return (
        '<span class="df-s-cell df-s-neg" title="Weak: latest completed 15m close is below previous 15m close by more than configured threshold.">Weak</span>'
      );
    }
    if (l1 === 'nifty_no_lower_low') {
      return (
        '<span class="df-s-cell df-s-muted" title="Neutral: close is not lower than previous 15m close beyond configured threshold.">Neutral</span>'
      );
    }
    return (
      '<span class="df-s-cell df-s-muted" title="Neutral: close is not higher than previous 15m close beyond configured threshold.">Neutral</span>'
    );
  }

  function stripL2Cell(st) {
    var k = (st && st.l2) || 'building';
    if (k === 'hit') {
      return (
        '<span class="df-s-cell df-s-neg" title="Fell to the profit-trail line (lock / exit review), not a trend label.">Trail stop</span>'
      );
    }
    if (k === 'active') {
      return (
        '<span class="df-s-cell df-s-teal" title="Price was at least +1.5× 15m ATR above entry; trail is on.">Trail armed</span>'
      );
    }
    return (
      '<span class="df-s-cell df-s-muted" title="Not yet +1.5× ATR profit from entry, so the trail is not armed. Says nothing about price up vs down.">Trail not armed</span>'
    );
  }

  function stripL3Cell(st) {
    var l3 = (st && st.l3) || '';
    if (l3 === 'fading') {
      return (
        '<span class="df-s-cell df-s-neg" title="Weak: stock shows fading momentum (latest 15m bar has weaker body and weak close).">Weak</span>'
      );
    }
    if (l3 === 'strong') {
      return (
        '<span class="df-s-cell df-s-ok" title="Strong: stock is not showing the 15m fading pattern right now.">Strong</span>'
      );
    }
    return (
      '<span class="df-s-cell df-s-muted" title="Neutral: no clear stock momentum signal for this strip rule.">Neutral</span>'
    );
  }

  function stripDecisionCell(st) {
    var d = (st && st.decision) || 'hold';
    if (d === 'lock_profit') {
      return (
        '<span class="df-s-cell df-s-neg df-s-decis" title="Trail stop is hit; lock gains and exit.">LOCK PROFIT — EXIT</span>'
      );
    }
    if (d === 'dual_exit') {
      return (
        '<span class="df-s-cell df-s-neg df-s-decis" title="Both momentum warnings are active together: Nifty lower-low plus stock 15m fade.">REVIEW EXIT — dual</span>'
      );
    }
    if (d === 'watch') {
      return (
        '<span class="df-s-cell df-s-amb df-s-decis" title="One momentum warning is active (Nifty or stock), but not both.">WATCH</span>'
      );
    }
    return (
      '<span class="df-s-cell df-s-ok df-s-decis" title="No active exit signal from this strip: no trail-stop hit and no L1/L3 momentum warning combination.">No exit signal</span>'
    );
  }

  function render15mAlertStrip(rows) {
    const el = document.getElementById('dfAlertStrip15m');
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<p class="df-s-empty">No open positions.</p>';
      return;
    }
    const th =
      '<thead><tr><th>Position</th><th class="df-s-c" title="Nifty momentum signal from the last two completed 15m bars.">Nifty Momentum</th>' +
      '<th class="df-s-c" title="Profit trail state (arm at +1.5× 15m ATR).">L2 Trail</th>' +
      '<th class="df-s-c" title="Stock momentum signal from the latest completed 15m bars.">Stock Momentum</th>' +
      '<th class="df-s-c" title="Final decision combining trail + momentum signals.">Decision</th></tr></thead>';
    const body = rows
      .map(function (r) {
        const st = r.alert_strip || {};
        return (
          '<tr class="df-s-tr"><td class="df-s-sym"><strong>' +
          symbolWithDirectionHtml(r) +
          '</strong></td><td class="df-s-c">' +
          stripL1Cell(st) +
          '</td><td class="df-s-c">' +
          stripL2Cell(st) +
          '</td><td class="df-s-c">' +
          stripL3Cell(st) +
          '</td><td class="df-s-c">' +
          stripDecisionCell(st) +
          '</td></tr>'
        );
      })
      .join('');
    el.innerHTML = '<table class="df-s-table" role="presentation">' + th + '<tbody>' + body + '</tbody></table>';
    updateStrip15mDecisionAudio(rows);
  }

  function playExitAlertBeep() {
    try {
      const Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      const ctx = new Ctx();
      const o = ctx.createOscillator();
      const g = ctx.createGain();
      o.connect(g);
      g.connect(ctx.destination);
      o.frequency.value = 880;
      o.type = 'sine';
      g.gain.setValueAtTime(0.1, ctx.currentTime);
      g.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.22);
      o.start();
      o.stop(ctx.currentTime + 0.22);
    } catch (e) {
      /* ignore */
    }
  }

  function updateRunningExitAlertAudio(rows) {
    if (!rows || !rows.length) return;
    var anyNew = false;
    rows.forEach(function (r) {
      var tid = r.trade_id;
      if (tid == null) return;
      var bits =
        (r.nifty_structure_weakening ? 1 : 0) +
        (r.trail_stop_hit ? 2 : 0) +
        (r.momentum_exhausting ? 4 : 0) +
        (r.drawdown_15atr_breach ? 8 : 0);
      var prev = state.prevRunAlertBits[tid] != null ? state.prevRunAlertBits[tid] : 0;
      if (bits > prev) anyNew = true;
      state.prevRunAlertBits[tid] = bits;
    });
    if (anyNew) playExitAlertBeep();
  }

  function runningExitBadges(r) {
    var isShort = String(r.direction_type || '').toUpperCase() === 'SHORT';
    var parts = [];
    if (r.drawdown_15atr_breach) {
      parts.push(
        isShort
          ? '<span class="df-exit-badge df-exit-dd" title="Short: LTP is above your sell (entry) by at least 1.5× 15m ATR; profit trail was never in play.">⛔ Drawdown (≥1.5×ATR)</span>'
          : '<span class="df-exit-badge df-exit-dd" title="LTP is below entry by at least 1.5× 15m ATR; profit trail was never in play for this.">⛔ Drawdown (≥1.5×ATR)</span>',
      );
    }
    if (r.nifty_structure_weakening) {
      parts.push(
        isShort
          ? '<span class="df-exit-badge df-exit-nifty" title="Short: Nifty 15m close-to-close moved against the short (e.g. strong up leg); position &gt; 45 min">⚠ Index vs short</span>'
          : '<span class="df-exit-badge df-exit-nifty" title="Nifty 15m: current bar low &lt; previous bar low; position &gt; 45 min">⚠ Index Weakening</span>',
      );
    }
    if (r.trail_stop_hit) {
      parts.push(
        isShort
          ? '<span class="df-exit-badge df-exit-trail" title="Short: price rose to within 0.8×ATR of your sell after trail was armed">🔴 Lock Profit</span>'
          : '<span class="df-exit-badge df-exit-trail" title="Price fell below entry + 0.8×ATR after profit trail was armed">🔴 Lock Profit</span>',
      );
    }
    if (r.momentum_exhausting) {
      parts.push(
        isShort
          ? '<span class="df-exit-badge df-exit-momo" title="Short: 15m bounce / fade pattern in stock">📈 Momentum vs short</span>'
          : '<span class="df-exit-badge df-exit-momo" title="Weaker body and weak close in latest 15m">📉 Momentum Fading</span>',
      );
    }
    if (!parts.length) return '—';
    return '<div class="df-exit-badges">' + parts.join(' ') + '</div>';
  }

  async function fetchWorkspace(opts) {
    opts = opts || {};
    const timeoutMs = Number(opts.timeoutMs) > 0 ? Number(opts.timeoutMs) : 20000;
    const lite = opts.lite === true;
    const qs = lite ? '?lite=1' : '';
    const paths = ['/api/daily-futures/workspace', '/daily-futures/workspace'];
    let lastErr = null;
    for (let i = 0; i < paths.length; i++) {
      const ac = typeof AbortController !== 'undefined' ? new AbortController() : null;
      const timer = ac ? window.setTimeout(function () { ac.abort(); }, timeoutMs) : null;
      try {
        const res = await fetch(API_BASE + paths[i] + qs, {
          headers: authHeaders(),
          cache: 'no-store',
          signal: ac ? ac.signal : undefined,
        });
        const raw = await res.text();
        const ct = (res.headers.get('content-type') || '').toLowerCase();
        const looksJson =
          ct.includes('application/json') || /^\s*[\[{]/.test(raw.slice(0, 30));
        if (res.status === 401) {
          window.location.replace('index.html');
          return null;
        }
        if (!res.ok) {
          lastErr = new Error(raw.slice(0, 200) || res.status);
          continue;
        }
        if (!looksJson) {
          lastErr = new Error(
            'Server returned a web page instead of JSON. The API may be mis-routed. Try signing in again or use /api path.'
          );
          continue;
        }
        try {
          return JSON.parse(raw);
        } catch (pe) {
          lastErr = new Error('Invalid JSON from workspace: ' + (pe.message || pe));
          continue;
        }
      } catch (e) {
        if (e && e.name === 'AbortError') {
          lastErr = new Error('Workspace request timed out');
          continue;
        }
        lastErr = e;
      } finally {
        if (timer) window.clearTimeout(timer);
      }
    }
    throw lastErr || new Error('workspace');
  }

  async function fetchWorkspaceSection(paths, timeoutMs) {
    let lastErr = null;
    for (let i = 0; i < paths.length; i++) {
      const ac = typeof AbortController !== 'undefined' ? new AbortController() : null;
      const timer = ac ? window.setTimeout(function () { ac.abort(); }, timeoutMs) : null;
      try {
        const res = await fetch(API_BASE + paths[i], {
          headers: authHeaders(),
          cache: 'no-store',
          signal: ac ? ac.signal : undefined,
        });
        const raw = await res.text();
        if (res.status === 401) {
          window.location.replace('index.html');
          return null;
        }
        if (!res.ok) {
          lastErr = new Error(raw.slice(0, 200) || res.status);
          continue;
        }
        return JSON.parse(raw);
      } catch (e) {
        if (e && e.name === 'AbortError') {
          lastErr = new Error('Section request timed out');
          continue;
        }
        lastErr = e;
      } finally {
        if (timer) window.clearTimeout(timer);
      }
    }
    throw lastErr || new Error('section');
  }

  function buildPicksReadonlyTableHtml(rows) {
    if (!rows || !rows.length) {
      return '<p class="df-meta" style="margin:0">No rows to show.</p>';
    }
    const th =
      '<thead><tr><th>Future</th><th>Qty (1 lot)</th><th>Scan #</th><th>1st scan</th><th>Last scan</th><th class="num">Conviction</th>' +
      '<th class="df-th-rs" title="(FUT day % − Nifty day %); line shows spread + S and N %">Rel. str.</th>' +
      '<th class="num">LTP</th></tr></thead>';
    const body = rows
      .map(function (r) {
        const convTxt = formatConvictionEntryLive(r);
        return (
          '<tr><td><strong>' +
          symbolWithDirectionHtml(r) +
          '</strong><div style="font-size:0.75rem;color:var(--theme-muted);">' +
          esc(r.underlying) +
          '</div></td><td class="num">' +
          esc(r.lot_size) +
          '</td><td class="num">' +
          esc(r.scan_count) +
          '</td><td>' +
          fmtIsoTimeIst(r.first_hit_at) +
          '</td><td>' +
          fmtIsoTimeIst(r.last_hit_at) +
          '</td><td class="num">' +
          convTxt +
          '</td><td class="df-rs-cell">' +
          fmtRelStrength(r) +
          '</td><td class="num">' +
          fmtNum(r.ltp, 2) +
          '</td></tr>'
        );
      })
      .join('');
    return '<table class="df-table df-table-picks-more">' + th + '<tbody>' + body + '</tbody></table>';
  }

  function openPicksMoreModal(kind) {
    const d = state.workspace || {};
    const rows =
      kind === 'bear' ? d.picks_low_conv_bear || [] : d.picks_low_conv_bull || [];
    const titleEl = document.getElementById('dfPicksMoreTitle');
    const wrap = document.getElementById('dfPicksMoreTableWrap');
    const m = document.getElementById('dfPicksMoreModal');
    if (titleEl) {
      titleEl.textContent =
        kind === 'bear'
          ? "Today's pick — Bearish · conviction below 50"
          : "Today's pick — Bullish · conviction below 50";
    }
    if (wrap) {
      wrap.innerHTML = buildPicksReadonlyTableHtml(rows);
    }
    if (m) {
      m.setAttribute('aria-hidden', 'false');
    }
  }

  function closePicksMoreModal() {
    const m = document.getElementById('dfPicksMoreModal');
    if (m) m.setAttribute('aria-hidden', 'true');
  }

  function updatePicksMoreControls(data) {
    const bull = (data && data.picks_low_conv_bull) || [];
    const bear = (data && data.picks_low_conv_bear) || [];
    const bullBtn = document.getElementById('dfBullPicksMoreBtn');
    const bearBtn = document.getElementById('dfBearPicksMoreBtn');
    const open = data && !data.session_before_open;
    if (bullBtn) bullBtn.hidden = !(open && bull.length > 0);
    if (bearBtn) bearBtn.hidden = !(open && bear.length > 0);
  }

  function renderAll(data) {
    state.workspace = data;
    renderPicksBullish(data);
    renderPicksBearish(data);
    updatePicksMoreControls(data);
    render15mAlertStrip(data.running || []);
    renderRunning(data.running || []);
    renderClosed(data.closed || [], data.summary);
    renderWhatIfContinuing(data.closed || []);
    renderTradeIfCouldHaveDone(data.trade_if_could_have_done || []);
  }

  function renderPicksBullish(data) {
    const el = document.getElementById('dfPicksTable');
    if (!el) return;
    const picks = (data && data.picks) || [];
    const d = (data && data.picks_diagnostics) || {};
    const scn = d.screening_count;
    const hb = d.hidden_because_bought;
    const hcl = d.hidden_because_sold_today;
    if (!picks || !picks.length) {
      if (data && data.session_before_open) {
        el.innerHTML =
          '<p class="df-meta">Session starts at <strong>09:00 IST</strong> — today&rsquo;s picks and scans appear after the market day opens. (ChartInk runs on its schedule; rows land in the DB with today&rsquo;s trade date in IST.)</p>';
        return;
      }
      if (scn === 0) {
        el.innerHTML =
          '<p class="df-meta">No symbols in <strong>today&rsquo;s</strong> Premium Futures screening yet. ' +
            'If ChartInk already fired, check that webhooks are reaching the server and the screening row&rsquo;s <code>trade_date</code> is today (IST). After 9:00 IST, refresh in a few minutes.</p>';
        return;
      }
      if (hb > 0 || hcl > 0) {
        const parts = [];
        if (hb) parts.push('you have an <strong>open (bought)</strong> order for ' + hb + ' symbol' + (hb === 1 ? '' : 's'));
        if (hcl) {
          parts.push(
            hcl + ' symbol' + (hcl === 1 ? ' is' : 's are') + ' in <strong>Today&rsquo;s trade</strong> (sold) and are hidden here',
          );
        }
        el.innerHTML =
          '<p class="df-meta">Scanner has <strong>' +
          scn +
          '</strong> symbol' +
          (scn === 1 ? '' : 's') +
          ' for today, but <strong>Today&rsquo;s pick (Bullish)</strong> is empty: ' +
          parts.join(' and ') +
          '.</p>';
        return;
      }
      el.innerHTML = '<p class="df-meta">No picks yet.</p>';
      return;
    }
    const th =
      '<thead><tr><th>Future</th><th>Qty (1 lot)</th><th>Scan #</th><th>1st scan</th><th>Last scan</th><th class="num">Conviction</th>' +
      '<th class="df-th-rs" title="(FUT day % − Nifty day %); line shows spread + S and N %">Rel. str.</th>' +
      '<th class="num">LTP</th><th></th></tr></thead>';
    const body = picks
      .map(function (r) {
        const eligible = r.order_eligible === true;
        const reason = r.order_block_reason || 'Not eligible to enter';
        const convTxt = formatConvictionEntryLive(r);
        return (
          '<tr><td><strong>' +
          symbolWithDirectionHtml(r) +
          '</strong><div style="font-size:0.75rem;color:var(--theme-muted);">' +
          esc(r.underlying) +
          '</div></td><td class="num">' +
          esc(r.lot_size) +
          '</td><td class="num">' +
          esc(r.scan_count) +
          '</td><td>' +
          fmtIsoTimeIst(r.first_hit_at) +
          '</td><td>' +
          fmtIsoTimeIst(r.last_hit_at) +
          '</td><td class="num">' +
          convTxt +
          '</td><td class="df-rs-cell">' +
          fmtRelStrength(r) +
          '</td><td class="num">' +
          fmtNum(r.ltp, 2) +
          '</td><td><button type="button" class="df-btn df-btn-order" data-sid="' +
          r.screening_id +
          '"' + (eligible ? '' : ' disabled title="' + esc(reason) + '"') + '>Enter</button></td></tr>'
        );
      })
      .join('');
    el.innerHTML = '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
    el.querySelectorAll('button[data-sid]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        if (btn.disabled) return;
        const sid = parseInt(btn.getAttribute('data-sid'), 10);
        const row = picks.find(function (p) { return p.screening_id === sid; });
        openBuyModal(sid, row, false);
      });
    });
  }

  function renderPicksBearish(data) {
    const el = document.getElementById('dfPicksBearTable');
    const hint = document.getElementById('dfBearishGateLine');
    if (!el) return;
    const gate = (data && data.index_bearish_gate) || {};
    if (hint) {
      if (gate && gate.ok === false) {
        hint.innerHTML =
          'Bearish list is shown only when <strong>NIFTY</strong> and <strong>BANKNIFTY</strong> are both below the day open. ' +
          'Nifty: ' +
          (gate.nifty_ok === true ? 'OK' : gate.nifty_ok === false ? 'not below open' : '—') +
          ' · Bank: ' +
          (gate.banknifty_ok === true ? 'OK' : gate.banknifty_ok === false ? 'not below open' : '—') +
          '.';
      } else {
        hint.textContent = '';
      }
    }
    const picks = (data && data.picks_bearish) || [];
    if (!picks || !picks.length) {
      if (data && data.session_before_open) {
        el.innerHTML = '<p class="df-meta">Session starts at <strong>09:00 IST</strong>.</p>';
        return;
      }
      el.innerHTML =
        '<p class="df-meta">No bearish candidates.</p>';
      return;
    }
    const th =
      '<thead><tr><th>Future</th><th>Qty (1 lot)</th><th>Scan #</th><th>1st scan</th><th>Last scan</th><th class="num">Conviction</th>' +
      '<th class="df-th-rs" title="(FUT day % − Nifty day %); line shows spread + S and N %">Rel. str.</th>' +
      '<th class="num">LTP</th><th></th></tr></thead>';
    const body = picks
      .map(function (r) {
        const eligible = r.order_eligible === true;
        const reason = r.order_block_reason || 'Not eligible to enter';
        const convTxt = formatConvictionEntryLive(r);
        return (
          '<tr><td><strong>' +
          symbolWithDirectionHtml(r) +
          '</strong><div style="font-size:0.75rem;color:var(--theme-muted);">' +
          esc(r.underlying) +
          '</div></td><td class="num">' +
          esc(r.lot_size) +
          '</td><td class="num">' +
          esc(r.scan_count) +
          '</td><td>' +
          fmtIsoTimeIst(r.first_hit_at) +
          '</td><td>' +
          fmtIsoTimeIst(r.last_hit_at) +
          '</td><td class="num">' +
          convTxt +
          '</td><td class="df-rs-cell">' +
          fmtRelStrength(r) +
          '</td><td class="num">' +
          fmtNum(r.ltp, 2) +
          '</td><td><button type="button" class="df-btn df-btn-order" data-bear="1" data-sid="' +
          r.screening_id +
          '"' + (eligible ? '' : ' disabled title="' + esc(reason) + '"') + '>Enter</button></td></tr>'
        );
      })
      .join('');
    el.innerHTML = '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
    el.querySelectorAll('button[data-sid]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        if (btn.disabled) return;
        const sid = parseInt(btn.getAttribute('data-sid'), 10);
        const row = picks.find(function (p) { return p.screening_id === sid; });
        openBuyModal(sid, row, true);
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
      '<thead><tr><th>Future</th><th>Qty</th><th>Scan #</th><th>1st scan</th><th>Last scan</th><th class="num">Conviction</th><th class="df-th-rs" title="(FUT day % − Nifty %); S and N in parentheses">Rel. str.</th><th class="num">LTP</th><th>Entry time</th><th class="num" title="Long: buy; Short: sell">Entry/Sell ₹</th><th class="num">Unrealized PnL</th><th>Alerts</th><th>Action</th></tr></thead>';
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
        const review =
          r.exit_review === true
            ? '<button type="button" class="df-btn df-btn-review" data-tid="' + r.trade_id + '">REVIEW EXIT</button>'
            : '';
        const refPx =
          String(r.direction_type || "").toUpperCase() === "SHORT" ? r.sell_price : r.entry_price;
        return (
          '<tr><td><strong>' +
          symbolWithDirectionHtml(r) +
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
          formatConvictionEntryLive(r) +
          '</td><td class="df-rs-cell">' +
          fmtRelStrength(r) +
          '</td><td class="num">' +
          fmtNum(r.ltp, 2) +
          '</td><td>' +
          esc(r.entry_time) +
          '</td><td class="num">' +
          fmtNum(refPx, 2) +
          '</td>' +
          unrealizedPnlCell(r) +
          '<td class="df-alerts-cell">' +
          runningExitBadges(r) +
          '</td><td class="df-run-actions"><div class="df-run-action-btns">' +
          review +
          '<button type="button" class="df-btn df-btn-sell" data-tid="' +
          r.trade_id +
          '">Exit</button></div></td></tr>'
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
    updateRunningExitAlertAudio(rows);
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
      '<thead><tr><th>Future</th><th>Qty</th><th>1st scan</th><th title="Long: buy time; Short: sell time">Entry</th><th class="num" title="Long: buy ₹; Short: sell ₹">Entry ₹</th><th title="Long: sell time; Short: cover time">Exit</th><th class="num" title="Long: sell ₹; Short: cover ₹">Exit ₹</th><th class="num">PnL ₹</th><th>Win/Loss</th></tr></thead>';
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
          symbolWithDirectionHtml(r) +
          '</strong></td><td class="num">' +
          esc(r.lot_size) +
          '</td><td>' +
          fmtIsoTimeIst(r.first_scan_time) +
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
    if (!r || r.ltp == null || r.lot_size == null || r.lot_size === '') {
      return null;
    }
    const ltp = Number(r.ltp);
    const qty = Number(r.lot_size);
    if (!Number.isFinite(ltp) || !Number.isFinite(qty)) return null;
    const isShort = String(r.direction_type || '').toUpperCase() === 'SHORT';
    if (isShort) {
      if (r.entry_price == null || r.entry_price === '') return null;
      const sp = Number(r.entry_price);
      if (!Number.isFinite(sp)) return null;
      return (sp - ltp) * qty;
    }
    if (r.entry_price == null || r.entry_price === '') {
      return null;
    }
    const ep = Number(r.entry_price);
    if (!Number.isFinite(ep)) return null;
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
          symbolWithDirectionHtml(r) +
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
      '<thead><tr><th>Future</th><th class="num">Qty</th><th>1st scan</th><th>2nd scan</th><th>Entry (2nd+5m)</th><th class="num">Entry LTP</th><th>Exit (time of scan)</th><th class="num">Current LTP</th><th class="num">PnL Currnt</th><th>Exit 15:15</th><th class="num">LTP 15:15</th><th class="num">PnL 15:15</th></tr></thead>';
    const body = rows
      .map(function (r) {
        const pScan = Number(r.pnl_scan_rupees);
        const p1515 = Number(r.pnl_1515_rupees);
        const cScan = Number.isFinite(pScan) ? (pScan > 0 ? 'df-pnl-pos' : pScan < 0 ? 'df-pnl-neg' : '') : '';
        const c1515 = Number.isFinite(p1515) ? (p1515 > 0 ? 'df-pnl-pos' : p1515 < 0 ? 'df-pnl-neg' : '') : '';
        return (
          '<tr><td><strong>' +
          symbolWithDirectionHtml(r) +
          '</strong></td><td class="num">' +
          esc(r.qty) +
          '</td><td>' +
          esc(r.first_scan_time) +
          '</td><td>' +
          (r.second_scan_hm != null && r.second_scan_hm !== '' ? esc(r.second_scan_hm) : '—') +
          '</td><td>' +
          esc(r.entry_time) +
          '</td><td class="num">' +
          fmtNum(r.entry_ltp, 2) +
          '</td><td>' +
          esc(r.exit_scan_time) +
          '</td><td class="num">' +
          fmtNum(r.current_ltp != null ? r.current_ltp : r.exit_scan_ltp, 2) +
          '</td><td class="num ' +
          cScan +
          '">' +
          fmtMoney(r.pnl_scan_rupees) +
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

  function symbolWithDirection(r) {
    var base = (r && (r.future_symbol || r.underlying)) ? String(r.future_symbol || r.underlying) : '';
    var dir = r && r.direction_type ? String(r.direction_type).trim().toUpperCase() : '';
    if (!base) return '';
    return dir ? (base + ' (' + dir + ')') : base;
  }

  function symbolWithDirectionHtml(r) {
    var base = esc((r && (r.future_symbol || r.underlying)) ? String(r.future_symbol || r.underlying) : '');
    var dir = r && r.direction_type ? String(r.direction_type).trim().toUpperCase() : '';
    if (!base) return '';
    if (!dir) return base;
    var cls = dir === 'LONG' ? 'df-dir-long' : (dir === 'SHORT' ? 'df-dir-short' : 'df-dir-neutral');
    return base + ' <span class="df-dir-pill ' + cls + '">' + esc(dir) + '</span>';
  }

  function openBuyModal(screeningId, row, isBearish) {
    state.pickScreeningId = screeningId;
    const m = document.getElementById('dfBuyModal');
    const isShort = isBearish === true || (row && String(row.direction_type || '').toUpperCase() === 'SHORT');
    const tEl = document.getElementById('dfBuyTitle');
    const tl = document.getElementById('dfBuyTimeLabel');
    const pl = document.getElementById('dfBuyPriceLabel');
    if (tEl) tEl.textContent = isShort ? 'Confirm short (sell to open)' : 'Confirm buy';
    if (tl) tl.textContent = isShort ? 'Sell time (IST, HH:MM)' : 'Entry time (IST, HH:MM)';
    if (pl) pl.textContent = isShort ? 'Sell price (₹)' : 'Entry price (₹)';
    document.getElementById('dfBuySym').innerHTML = row
      ? symbolWithDirectionHtml(row) + ' · ' + esc(row.underlying)
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
    document.getElementById('dfSellSym').innerHTML = row
      ? symbolWithDirectionHtml(row) + ' · ' + esc(row.underlying)
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
    const okBtn = document.getElementById('dfSellOk');
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
    const originalBtnText = okBtn ? okBtn.textContent : '';
    function restoreSellBtn() {
      if (!okBtn) return;
      okBtn.disabled = false;
      okBtn.textContent = originalBtnText || 'Confirm sell';
    }
    if (okBtn) {
      okBtn.disabled = true;
      okBtn.textContent = 'Submitting...';
    }
    let lastErr = null;
    for (let i = 0; i < paths.length; i++) {
      const ac = typeof AbortController !== 'undefined' ? new AbortController() : null;
      const timer = ac ? window.setTimeout(function () { ac.abort(); }, 12000) : null;
      try {
        const res = await fetch(API_BASE + paths[i], {
          method: 'POST',
          headers: authHeaders(),
          signal: ac ? ac.signal : undefined,
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
        restoreSellBtn();
        return;
      } catch (e) {
        if (e && e.name === 'AbortError') {
          lastErr = new Error('Sell request timed out. Please try again.');
        } else {
          lastErr = e;
        }
      } finally {
        if (timer) window.clearTimeout(timer);
      }
    }
    err.textContent = lastErr && lastErr.message ? lastErr.message : 'Request failed';
    restoreSellBtn();
  }

  async function refresh() {
    const b = document.getElementById('dfBanner');
    state.refreshSeq += 1;
    const seq = state.refreshSeq;
    try {
      const liteData = await fetchWorkspace({ lite: true, timeoutMs: 9000 });
      if (seq !== state.refreshSeq) return;
      updateSnapshotChip(liteData);
      if (b) {
        if (liteData.session_before_open) {
          b.textContent =
            (liteData.session_message ||
              'Premium Futures shows the current IST session from 09:00 onward.') +
            ' Session date: ' +
            (liteData.trade_date || '—') +
            ' · Auto-refresh every 120 s';
        } else {
          b.textContent =
            'Session date (IST): ' +
            (liteData.trade_date || '—') +
            ' · Data for this IST session only · Loading advanced sections…';
        }
      }
      renderAll(liteData);
      if (!liteData.session_before_open) {
        fetchWorkspaceSection(
          ['/api/daily-futures/workspace/running-enriched', '/daily-futures/workspace/running-enriched'],
          12000,
        )
          .then(function (runData) {
            if (seq !== state.refreshSeq || !runData) return;
            render15mAlertStrip(runData.running || []);
            renderRunning(runData.running || []);
          })
          .catch(function () {
            /* keep lite running view */
          });

        fetchWorkspaceSection(
          ['/api/daily-futures/workspace/trade-if-could', '/daily-futures/workspace/trade-if-could'],
          18000,
        )
          .then(function (ticData) {
            if (seq !== state.refreshSeq || !ticData) return;
            renderTradeIfCouldHaveDone(ticData.trade_if_could_have_done || []);
            if (b) {
              b.textContent =
                'Session date (IST): ' +
                (liteData.trade_date || '—') +
                ' · Data for this IST session only · Auto-refresh every 120 s';
            }
          })
          .catch(function () {
            if (b) {
              b.textContent =
                'Session date (IST): ' +
                (liteData.trade_date || '—') +
                ' · Core sections loaded; heavy sections delayed (will retry).';
            }
          });
      }
    } catch (e) {
      updateSnapshotChip(null);
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
    const pmB = document.getElementById('dfPicksMoreBackdrop');
    const pmC = document.getElementById('dfPicksMoreClose');
    if (pmB) pmB.addEventListener('click', closePicksMoreModal);
    if (pmC) pmC.addEventListener('click', closePicksMoreModal);
    const bullM = document.getElementById('dfBullPicksMoreBtn');
    const bearM = document.getElementById('dfBearPicksMoreBtn');
    if (bullM) bullM.addEventListener('click', function () { openPicksMoreModal('bull'); });
    if (bearM) bearM.addEventListener('click', function () { openPicksMoreModal('bear'); });
  }

  document.addEventListener('DOMContentLoaded', function () {
    bindModals();
    refresh();
    setInterval(refresh, 120 * 1000);
  });
})();
