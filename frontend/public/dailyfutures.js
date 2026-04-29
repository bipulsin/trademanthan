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
   * Scores are always clickable (when screening_id is present) to open OI / VWAP / reason modal.
   */
  function formatConvictionEntryLive(r) {
    const sid = r && r.screening_id != null ? Number(r.screening_id) : null;
    function livePart() {
      const eff = r.effective_conviction == null ? null : Number(r.effective_conviction);
      const raw = r.conviction_score == null ? null : Number(r.conviction_score);
      if (!Number.isFinite(eff) && !Number.isFinite(raw)) return '—';
      const shown = Number.isFinite(eff) ? eff : raw;
      const txt = fmtNum(shown, 1) + ' (L)';
      const tip = Number.isFinite(raw)
        ? ('Effective conviction (live): ' + fmtNum(shown, 1) + ' | Raw conviction: ' + fmtNum(raw, 1))
        : ('Effective conviction (live): ' + fmtNum(shown, 1));
      if (sid == null) return '<span class="df-score-live">' + txt + '</span>';
      return (
        '<button type="button" class="df-conv-link df-score-live" data-csid="' +
        sid +
        '" data-cmode="live" title="' + esc(tip) + '. Click to view/edit OI-VWAP details.">' +
        txt +
        '</button>'
      );
    }
    function entryPart() {
      if (r.second_scan_conviction_score == null) return '';
      const txt = fmtNum(r.second_scan_conviction_score, 1) + ' (E)';
      if (sid == null) return '<span class="df-score-entry">' + txt + '</span>';
      return (
        '<button type="button" class="df-conv-link df-score-entry" data-csid="' +
        sid +
        '" data-cmode="entry" title="Entry OI / VWAP legs — click to view or edit">' +
        txt +
        '</button>'
      );
    }
    let convTxt = livePart();
    const secondConv = r.second_scan_conviction_score == null ? null : Number(r.second_scan_conviction_score);
    const liveConv = r.conviction_score == null ? null : Number(r.conviction_score);
    if (Number.isFinite(secondConv)) {
      if (Number.isFinite(liveConv)) {
        convTxt =
          entryPart() + ' | ' + livePart();
      } else {
        convTxt = entryPart();
      }
    }
    return convTxt;
  }

  function bindConvictionLinks(scopeEl) {
    if (!scopeEl) return;
    scopeEl.querySelectorAll('button.df-conv-link[data-csid][data-cmode]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        const sid = parseInt(btn.getAttribute('data-csid'), 10);
        const mode = String(btn.getAttribute('data-cmode') || '').toLowerCase();
        if (!Number.isFinite(sid) || (mode !== 'live' && mode !== 'entry')) return;
        openConvictionModal(sid, mode);
      });
    });
  }

  function fmtRelStrength(r) {
    // Prefer live rel-strength fields; fall back to second-scan snapshot so
    // Today's pick doesn't render blank when live enrichment is temporarily missing.
    const s = r.stock_change_pct != null ? r.stock_change_pct : r.second_scan_stock_change_pct;
    const n = r.nifty_change_pct != null ? r.nifty_change_pct : r.second_scan_nifty_change_pct;
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
    convictionEditScreeningId: null,
    convictionEditMode: null,
    /** @type {Record<number, number>} trade_id -> bit mask of active exit alerts (1=nifty,2=trail,4=momo) */
    prevRunAlertBits: {},
    /** @type {Record<string, boolean>} dedupe key: trade|candle|decision */
    prevIndicatorAlertKeys: {},
    refreshSeq: 0,
  };

  /** @param {'amber' | 'hard'} kind */
  function playStrip15mBeep(kind) {
    try {
      const Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      const ctx = new Ctx();
      const tones = kind === 'hard' ? [1240, 980] : [760];
      tones.forEach(function (freq, idx) {
        const o = ctx.createOscillator();
        const g = ctx.createGain();
        o.connect(g);
        g.connect(ctx.destination);
        o.frequency.value = freq;
        o.type = 'square';
        const t0 = ctx.currentTime + idx * 0.14;
        g.gain.setValueAtTime(0.0001, t0);
        g.gain.exponentialRampToValueAtTime(0.14, t0 + 0.01);
        g.gain.exponentialRampToValueAtTime(0.0001, t0 + 0.11);
        o.start(t0);
        o.stop(t0 + 0.12);
      });
    } catch (e) {
      /* ignore */
    }
  }

  function fireDecisionNotification(title, body) {
    try {
      if (!('Notification' in window)) return;
      if (Notification.permission === 'granted') {
        new Notification(title, { body: body || '' });
      } else if (Notification.permission !== 'denied') {
        Notification.requestPermission().then(function (perm) {
          if (perm === 'granted') new Notification(title, { body: body || '' });
        });
      }
    } catch (e) {
      /* ignore */
    }
  }

  function updateStrip15mDecisionAudio(rows) {
    if (!rows || !rows.length) return;
    rows.forEach(function (r) {
      var tid = r.trade_id;
      if (tid == null) return;
      var st = r.alert_strip || {};
      var count = Number(st.indicator_count || 0);
      var decision = String(st.decision || 'hold');
      var candleTs = String(st.indicator_latest_candle_ts || '');
      var dedupeKey = String(tid) + '|' + candleTs + '|' + decision;
      if ((decision === 'exit_now' || decision === 'hard_exit') && candleTs && !state.prevIndicatorAlertKeys[dedupeKey]) {
        var symbol = String(r.future_symbol || r.underlying || 'Position');
        if (decision === 'hard_exit') {
          playStrip15mBeep('hard');
          fireDecisionNotification('HARD EXIT: ' + symbol, (st.indicator_conditions_text || []).join(' · '));
        } else {
          playStrip15mBeep('amber');
          fireDecisionNotification('Exit Warning: ' + symbol, (st.indicator_conditions_text || []).join(' · '));
        }
        state.prevIndicatorAlertKeys[dedupeKey] = true;
      }
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

  function _trailStopPrice(r) {
    if (!r) return null;
    var atr = Number(r.position_atr);
    if (!Number.isFinite(atr) || atr <= 0) return null;
    var isShort = String(r.direction_type || '').toUpperCase() === 'SHORT';
    var ref = isShort ? Number(r.sell_price) : Number(r.entry_price);
    if (!Number.isFinite(ref)) return null;
    // Backend hit condition uses:
    // long  -> ltp < entry + 0.8*ATR
    // short -> ltp > sell  - 0.8*ATR
    var sl = isShort ? (ref - 0.8 * atr) : (ref + 0.8 * atr);
    return Number.isFinite(sl) ? sl : null;
  }

  function stripL2Cell(st, r) {
    var k = (st && st.l2) || 'building';
    var sl = _trailStopPrice(r);
    var slTxt = sl != null ? (' @ ₹' + fmtNum(sl, 2)) : '';
    if (k === 'hit') {
      return (
        '<span class="df-s-cell df-s-neg" title="Fell to the profit-trail line (lock / exit review), not a trend label.">Trail stop' + slTxt + '</span>'
      );
    }
    if (k === 'active') {
      return (
        '<span class="df-s-cell df-s-teal" title="Price was at least +1.5× 15m ATR in favor; trail is on. Place stop near this level.">Trail armed' + slTxt + '</span>'
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
    var b = Number((st && st.indicator_bullish_count) || 0);
    var s = Number((st && st.indicator_bearish_count) || 0);
    var c = Number((st && st.indicator_count) || 0);
    var en = Number((st && st.indicator_exit_now_threshold) || 3);
    var hx = Number((st && st.indicator_hard_exit_threshold) || 4);
    var conds = (st && st.indicator_conditions_text && st.indicator_conditions_text.length)
      ? st.indicator_conditions_text.join(' · ')
      : 'No flipped indicator conditions on latest closed 15m candle.';
    if (d === 'hard_exit') {
      return (
        '<span class="df-s-cell df-s-neg df-s-decis df-s-pulse" style="font-size:14px;font-weight:800;background:#b91c1c !important;color:#ffffff !important;border:1px solid #7f1d1d;" title="' + esc(conds) + '. Count=' + esc(c) + ' (Bull=' + esc(b) + ', Bear=' + esc(s) + '), HARD at ≥' + esc(hx) + '.">HARD EXIT</span>'
      );
    }
    if (d === 'exit_now') {
      return (
        '<span class="df-s-cell df-s-amb df-s-decis" style="font-weight:700;background:#f59e0b !important;color:#111827 !important;border:1px solid #d97706;" title="' + esc(conds) + '. Count=' + esc(c) + ' (Bull=' + esc(b) + ', Bear=' + esc(s) + '), EXIT NOW at ≥' + esc(en) + '.">EXIT NOW</span>'
      );
    }
    return (
      '<span class="df-s-cell df-s-ok df-s-decis" title="No exit signal">No exit signal</span>'
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
        const carry = isCarryForwardTrade(r) ? ' <span class="df-carry-badge">Carry-forward</span>' : '';
        return (
          '<tr class="df-s-tr"><td class="df-s-sym"><strong>' +
          symbolWithDirectionHtml(r) +
          carry +
          '</strong><div style="font-size:0.74rem;color:var(--theme-muted);">Trade date: ' +
          esc(r.trade_date || '—') +
          '</div></td><td class="df-s-c">' +
          stripL1Cell(st) +
          '</td><td class="df-s-c">' +
          stripL2Cell(st, r) +
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
        (r.drawdown_15atr_breach ? 8 : 0) +
        (r.profit_giveback_breach ? 16 : 0);
      var prev = state.prevRunAlertBits[tid] != null ? state.prevRunAlertBits[tid] : 0;
      if (bits > prev) anyNew = true;
      state.prevRunAlertBits[tid] = bits;
    });
    if (anyNew) playExitAlertBeep();
  }

  function runningExitBadges(r) {
    // Exit/caution signals are intentionally displayed only in the 15m strip section.
    return '—';
  }

  function runningActionPlaybook(r) {
    var decision = String(((r && r.alert_strip) ? r.alert_strip.decision : '') || '').toLowerCase();
    var hard = Boolean(r && (
      r.trail_stop_hit ||
      r.drawdown_15atr_breach ||
      r.profit_giveback_breach ||
      decision === 'lock_profit' ||
      decision === 'giveback_exit'
    ));
    if (hard) {
      return '<span class="df-playbook df-playbook-hard" title="Strict action: exit now (lock profit hit or deep ATR drawdown).">Hard Exit</span>';
    }
    if (decision === 'dual_exit') {
      return '<span class="df-playbook df-playbook-confirm" title="Two caution signals are active together. Tighten risk and confirm quickly.">Confirm Exit</span>';
    }
    if (decision === 'watch') {
      return '<span class="df-playbook df-playbook-monitor" title="Single caution signal. Keep monitoring next 15m updates.">Monitor</span>';
    }
    return '';
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

  function sectorMoverBadgeHtml(r, bullOrBear) {
    var n;
    var letter;
    var cls;
    if (bullOrBear === 'bear') {
      n = r && r.sector_in_top_losers_rank;
      letter = 'L';
      cls = 'df-sector-badge df-sector-badge--bear';
    } else {
      n = r && r.sector_in_top_gainers_rank;
      letter = 'S';
      cls = 'df-sector-badge df-sector-badge--bull';
    }
    if (n !== 1 && n !== 2 && n !== 3) return '';
    var title = r && r.nifty_sector_label ? 'Nifty sector: ' + esc(r.nifty_sector_label) : '';
    return (
      ' <span class="' +
      cls +
      '"' +
      (title ? ' title="' + title + '"' : '') +
      '><span class="df-sector-badge-inner">' +
      letter +
      String(n) +
      '</span></span>'
    );
  }

  function buildPicksReadonlyTableHtml(rows, modalKind) {
    if (!rows || !rows.length) {
      return '<p class="df-meta" style="margin:0">No rows to show.</p>';
    }
    var badgeKind = modalKind === 'bear' ? 'bear' : 'bull';
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
          sectorMoverBadgeHtml(r, badgeKind) +
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
      wrap.innerHTML = buildPicksReadonlyTableHtml(rows, kind);
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
          '<p class="df-meta df-wait-premium-msg" style="font-size:0.78rem;">Waiting for Premium Futures in this section, Algo is in work!</p>';
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
        const enterBtn =
          '<button type="button" class="df-btn df-btn-order" data-sid="' +
          r.screening_id +
          '"' + (eligible ? '' : ' disabled') + '>Enter</button>';
        const enterCell = eligible
          ? enterBtn
          : ('<span class="df-disabled-tip" title="' + esc(reason) + '">' + enterBtn + '</span>');
        return (
          '<tr><td><strong>' +
          symbolWithDirectionHtml(r) +
          sectorMoverBadgeHtml(r, 'bull') +
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
          '</td><td>' + enterCell + '</td></tr>'
        );
      })
      .join('');
    el.innerHTML = '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
    bindConvictionLinks(el);
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
      if (gate && gate.index_gate_disabled === true) {
        hint.innerHTML =
          '<span class="df-meta">NIFTY day-open filter for bearish is <strong>off</strong> (server setting).</span>';
      } else if (gate && gate.nifty_bullish === true) {
        hint.innerHTML =
          '<span class="df-meta">NIFTY is Bullish, so no trade will be displayed in this section.</span>';
      } else if (gate && (gate.nifty_quote_incomplete || gate.error)) {
        hint.innerHTML =
          '<span class="df-meta">NIFTY 5m data unavailable; bearish list is hidden until index candles load.</span>';
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
      if (gate && !gate.index_gate_disabled && gate.nifty_bullish === true) {
        el.innerHTML =
          '<p class="df-meta">NIFTY is Bullish, so no trade will be displayed in this section.</p>';
        return;
      }
      if (gate && !gate.index_gate_disabled && (gate.nifty_quote_incomplete || gate.error)) {
        el.innerHTML =
          '<p class="df-meta">NIFTY 5m data unavailable. Bearish picks will show once index candles are available.</p>';
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
        const enterBtn =
          '<button type="button" class="df-btn df-btn-order" data-bear="1" data-sid="' +
          r.screening_id +
          '"' + (eligible ? '' : ' disabled') + '>Enter</button>';
        const enterCell = eligible
          ? enterBtn
          : ('<span class="df-disabled-tip" title="' + esc(reason) + '">' + enterBtn + '</span>');
        return (
          '<tr><td><strong>' +
          symbolWithDirectionHtml(r) +
          sectorMoverBadgeHtml(r, 'bear') +
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
          '</td><td>' + enterCell + '</td></tr>'
        );
      })
      .join('');
    el.innerHTML = '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
    bindConvictionLinks(el);
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
      '<thead><tr><th>Future</th><th>Trade date</th><th>Qty</th><th class="num">Conviction</th><th class="num">LTP</th><th>Entry time</th><th class="num" title="Long: buy; Short: sell">Entry/Sell ₹</th><th class="num">SL ₹</th><th class="num">Unrealized PnL</th><th>Alerts</th><th>Action</th></tr></thead>';
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
        const refPx =
          String(r.direction_type || "").toUpperCase() === "SHORT" ? r.sell_price : r.entry_price;
        const carry = isCarryForwardTrade(r) ? ' <span class="df-carry-badge">Carry-forward</span>' : '';
        const slTxt = r.running_sl_price != null ? fmtNum(r.running_sl_price, 2) : '—';
        const slTitle = r.running_sl_source ? (' title="' + esc(r.running_sl_source) + '"') : '';
        return (
          '<tr><td><strong>' +
          symbolWithDirectionHtml(r) +
          carry +
          '</strong></td><td>' +
          esc(r.trade_date || '—') +
          '</td><td class="num">' +
          esc(r.lot_size) +
          '</td><td class="num">' +
          formatConvictionEntryLive(r) +
          '</td><td class="num">' +
          fmtNum(r.ltp, 2) +
          '</td><td>' +
          esc(r.entry_time) +
          '</td><td class="num">' +
          fmtNum(refPx, 2) +
          '</td><td class="num"' + slTitle + '>' +
          slTxt +
          '</td>' +
          unrealizedPnlCell(r) +
          '<td class="df-alerts-cell">' +
          runningExitBadges(r) +
          '</td><td class="df-run-actions"><div class="df-run-action-btns">' +
          '<button type="button" class="df-btn df-btn-sell" data-tid="' +
          r.trade_id +
          '">Exit</button></div></td></tr>'
        );
      })
      .join('');
    el.innerHTML = totalLine + '<table class="df-table">' + th + '<tbody>' + body + '</tbody></table>';
    bindConvictionLinks(el);
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

  function isCarryForwardTrade(r) {
    if (!r || !r.trade_date) return false;
    var today = state && state.workspace ? state.workspace.trade_date : null;
    if (!today) return false;
    return String(r.trade_date) < String(today);
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
      ? symbolWithDirectionHtml(row) +
          sectorMoverBadgeHtml(row, isShort ? 'bear' : 'bull') +
          ' · ' +
          esc(row.underlying)
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
    const okBtn = document.getElementById('dfSellOk');
    if (okBtn) {
      okBtn.disabled = false;
      okBtn.textContent = 'Confirm sell';
    }
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
    const okBtn = document.getElementById('dfSellOk');
    if (okBtn) {
      okBtn.disabled = false;
      okBtn.textContent = 'Confirm sell';
    }
    state.sellTradeId = null;
  }

  function _parseBreakdownJson(r) {
    const b = r && r.conviction_breakdown_json;
    if (b && typeof b === 'object' && !Array.isArray(b)) {
      return b;
    }
    if (typeof b === 'string') {
      try {
        return JSON.parse(b);
      } catch (_e) {
        return {};
      }
    }
    return {};
  }

  function _findScreeningRow(screeningId) {
    const w = state.workspace;
    if (!w || screeningId == null) return null;
    const id = Number(screeningId);
    function first(arr) {
      if (!arr || !arr.length) return null;
      for (let i = 0; i < arr.length; i++) {
        if (Number(arr[i].screening_id) === id) return arr[i];
      }
      return null;
    }
    return (
      first(w.picks_mixed) ||
      first(w.picks) ||
      first(w.picks_bearish) ||
      first(w.picks_low_conv_bull) ||
      first(w.picks_low_conv_bear) ||
      first(w.running)
    );
  }

  function openConvictionModal(screeningId, mode) {
    state.convictionEditScreeningId = screeningId;
    state.convictionEditMode = mode;
    const m = document.getElementById('dfConvModal');
    const tEl = document.getElementById('dfConvTitle');
    const subEl = document.getElementById('dfConvSub');
    const oiEl = document.getElementById('dfConvOi');
    const iEl = document.getElementById('dfConvVwap');
    const reasonEl = document.getElementById('dfConvReason');
    const eEl = document.getElementById('dfConvErr');
    const r = _findScreeningRow(screeningId);
    const br = _parseBreakdownJson(r || {});

    if (tEl) {
      tEl.textContent = mode === 'entry' ? 'Entry conviction (E)' : 'Live conviction (L)';
    }
    const und = r && r.underlying ? r.underlying : '—';
    const fs = r && r.future_symbol ? r.future_symbol : '';
    if (subEl) {
      subEl.textContent = fs && und && fs !== und ? fs + ' · ' + und : fs || und;
    }

    if (mode === 'entry') {
      const oiE =
        r && r.second_scan_oi_leg != null
          ? r.second_scan_oi_leg
          : r && r.conviction_oi_leg != null
            ? r.conviction_oi_leg
            : null;
      if (oiEl) {
        oiEl.value =
          oiE != null && Number.isFinite(Number(oiE)) ? Number(oiE).toFixed(1) : '—';
      }
      const vwE = br.entry_manual_session_vwap;
      if (iEl) {
        iEl.value =
          vwE != null && Number.isFinite(Number(vwE)) ? String(vwE) : '';
      }
      if (reasonEl) {
        reasonEl.value = br.entry_vwap_leg_reason != null ? String(br.entry_vwap_leg_reason) : '';
      }
    } else {
      const oiL = r && r.conviction_oi_leg;
      if (oiEl) {
        oiEl.value =
          oiL != null && Number.isFinite(Number(oiL)) ? Number(oiL).toFixed(1) : '—';
      }
      const sv =
        r && r.session_vwap != null ? r.session_vwap : br.session_vwap != null ? br.session_vwap : null;
      if (iEl) {
        iEl.value = sv != null && Number.isFinite(Number(sv)) ? String(sv) : '';
      }
      if (reasonEl) {
        reasonEl.value = br.vwap_leg_reason != null ? String(br.vwap_leg_reason) : '';
      }
    }
    if (eEl) eEl.textContent = '';
    if (m) m.setAttribute('aria-hidden', 'false');
  }

  function closeConvictionModal() {
    const m = document.getElementById('dfConvModal');
    if (m) m.setAttribute('aria-hidden', 'true');
    state.convictionEditScreeningId = null;
    state.convictionEditMode = null;
  }

  async function submitConvictionVwap() {
    const sid = state.convictionEditScreeningId;
    const mode = state.convictionEditMode;
    const inp = document.getElementById('dfConvVwap');
    const reasonInp = document.getElementById('dfConvReason');
    const err = document.getElementById('dfConvErr');
    const okBtn = document.getElementById('dfConvOk');
    if (!sid || (mode !== 'live' && mode !== 'entry')) return;
    const vwap = parseFloat(String((inp && inp.value) || '').replace(/,/g, ''));
    if (!Number.isFinite(vwap) || vwap <= 0) {
      if (err) err.textContent = 'Enter valid VWAP greater than 0.';
      return;
    }
    const reasonTrim = reasonInp && String(reasonInp.value).trim() ? String(reasonInp.value).trim() : '';
    if (err) err.textContent = '';
    const original = okBtn ? okBtn.textContent : 'Save & refresh';
    if (okBtn) {
      okBtn.disabled = true;
      okBtn.textContent = 'Saving...';
    }
    const paths = ['/api/daily-futures/conviction/manual-vwap', '/daily-futures/conviction/manual-vwap'];
    let lastErr = null;
    for (let i = 0; i < paths.length; i++) {
      try {
        const res = await fetch(API_BASE + paths[i], {
          method: 'POST',
          headers: authHeaders(),
          body: JSON.stringify({
            screening_id: sid,
            mode: mode,
            session_vwap: vwap,
            vwap_leg_reason: reasonTrim.length ? reasonTrim : null,
          }),
        });
        const raw = await res.text();
        if (res.ok) {
          closeConvictionModal();
          await refresh();
          return;
        }
        try {
          const j = JSON.parse(raw);
          lastErr = new Error(j.detail || raw.slice(0, 140));
        } catch (_e) {
          lastErr = new Error(raw.slice(0, 140));
        }
      } catch (e) {
        lastErr = e;
      }
    }
    if (err) err.textContent = lastErr && lastErr.message ? lastErr.message : 'Request failed';
    if (okBtn) {
      okBtn.disabled = false;
      okBtn.textContent = original || 'Save & refresh';
    }
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
          // Do not block the modal flow on a full refresh; this can be slow.
          refresh().catch(function () { /* non-blocking */ });
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
      const liteData = await fetchWorkspace({ lite: true, timeoutMs: 90000 });
      if (seq !== state.refreshSeq) return;
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
            if (state.workspace && runData.running) {
              state.workspace.running = runData.running;
            }
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
    const cvB = document.getElementById('dfConvBackdrop');
    const cvC = document.getElementById('dfConvCancel');
    const cvO = document.getElementById('dfConvOk');
    if (cvB) cvB.addEventListener('click', closeConvictionModal);
    if (cvC) cvC.addEventListener('click', closeConvictionModal);
    if (cvO) cvO.addEventListener('click', submitConvictionVwap);
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
