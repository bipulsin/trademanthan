/* divtest frontend — vanilla JS, no build step */
(() => {
  const state = {
    jobId: null,
    es: null,
    pollTimer: null,
    page: 1,
    pageSize: 50,
    sortKey: 'entry_datetime',
    sortDir: 'desc',
    instrument: 'All',
    timeframe: 'All',
    tradesCache: [],
  };

  const $ = (id) => document.getElementById(id);
  const consoleEl = $('consoleLog');

  function logLine(msg, level = 'info') {
    const line = document.createElement('div');
    if (level === 'error') line.className = 'err';
    if (level === 'warn') line.className = 'warn';
    const ts = new Date().toLocaleTimeString();
    line.textContent = `[${ts}] ${msg}`;
    consoleEl.appendChild(line);
    consoleEl.scrollTop = consoleEl.scrollHeight;
  }

  async function api(path, opts = {}) {
    const res = await fetch(path, {
      headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
      ...opts,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || res.statusText);
    return data;
  }

  function collectSettings() {
    const out = {};
    document.querySelectorAll('#settingsGrid [data-key]').forEach((el) => {
      const key = el.getAttribute('data-key');
      if (el.type === 'checkbox') out[key] = el.checked;
      else if (el.type === 'number') out[key] = Number(el.value);
      else out[key] = el.value;
    });
    return out;
  }

  function fillSettings(s) {
    document.querySelectorAll('#settingsGrid [data-key]').forEach((el) => {
      const key = el.getAttribute('data-key');
      if (!(key in s)) return;
      if (el.type === 'checkbox') el.checked = Boolean(s[key]);
      else el.value = s[key];
    });
  }

  let equityChart = null;
  function renderChart(curve) {
    const ctx = $('equityChart').getContext('2d');
    const labels = (curve || []).map((p) => String(p.t).slice(0, 16));
    const data = (curve || []).map((p) => p.equity);
    if (equityChart) equityChart.destroy();
    equityChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'Cumulative PnL (₹)',
            data,
            borderColor: '#3dba7a',
            backgroundColor: 'rgba(61,186,122,0.12)',
            fill: true,
            tension: 0.2,
            pointRadius: 0,
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: {
            ticks: { color: '#8fa398', maxTicksLimit: 8 },
            grid: { color: 'rgba(180,210,190,0.08)' },
          },
          y: {
            ticks: { color: '#8fa398' },
            grid: { color: 'rgba(180,210,190,0.08)' },
          },
        },
      },
    });
  }

  function fmt(n) {
    if (n == null || Number.isNaN(n)) return '—';
    if (!Number.isFinite(n)) return '∞';
    return Number(n).toLocaleString('en-IN', { maximumFractionDigits: 2 });
  }

  function pnlClass(n) {
    if (n > 0) return 'pos';
    if (n < 0) return 'neg';
    return '';
  }

  function renderCards(a) {
    const items = [
      ['Trades', a.total_trades],
      ['Win rate', `${fmt(a.win_rate_pct)}%`],
      ['Total PnL', fmt(a.total_pnl), true],
      ['Avg PnL', fmt(a.avg_pnl), true],
      ['Avg win', fmt(a.avg_win), true],
      ['Avg loss', fmt(a.avg_loss), true],
      ['Max DD ₹', fmt(a.max_drawdown_inr)],
      ['Max DD %', fmt(a.max_drawdown_pct)],
      ['Profit factor', fmt(a.profit_factor)],
      ['Largest win', fmt(a.largest_win), true],
      ['Largest loss', fmt(a.largest_loss), true],
      ['Avg hold (bars)', fmt(a.avg_holding_bars)],
    ];
    $('summaryCards').innerHTML = items
      .map(([k, v, isPnl]) => {
        const cls = isPnl ? pnlClass(Number(String(v).replace(/,/g, ''))) : '';
        return `<div class="card"><div class="k">${k}</div><div class="v ${cls}">${v}</div></div>`;
      })
      .join('');
  }

  function renderTfBreakdown(by) {
    if (!by) {
      $('tfBreakdown').innerHTML = '';
      return;
    }
    $('tfBreakdown').innerHTML = ['10min', '15min', '1hr']
      .map((tf) => {
        const a = by[tf] || {};
        return `<div class="tf-box"><h3>${tf}</h3>
          <div>${a.total_trades || 0} trades · WR ${fmt(a.win_rate_pct)}%</div>
          <div class="${pnlClass(a.total_pnl)}">PnL ₹ ${fmt(a.total_pnl)} · PF ${fmt(a.profit_factor)}</div>
        </div>`;
      })
      .join('');
  }

  function renderTable(trades) {
    const sorted = [...trades].sort((a, b) => {
      const av = a[state.sortKey];
      const bv = b[state.sortKey];
      if (av == null && bv == null) return 0;
      if (typeof av === 'number' && typeof bv === 'number') {
        return state.sortDir === 'asc' ? av - bv : bv - av;
      }
      const cmp = String(av).localeCompare(String(bv));
      return state.sortDir === 'asc' ? cmp : -cmp;
    });
    $('tradesBody').innerHTML = sorted
      .map((t) => {
        const pnl = Number(t.pnl_inr) || 0;
        return `<tr>
          <td>${t.instrument || ''}</td>
          <td>${t.timeframe || ''}</td>
          <td>${String(t.entry_datetime || '').replace('T', ' ').slice(0, 19)}</td>
          <td>${fmt(t.entry_price)}</td>
          <td class="${t.side === 'LONG' ? 'side-long' : 'side-short'}">${t.side}</td>
          <td>${String(t.exit_datetime || '').replace('T', ' ').slice(0, 19)}</td>
          <td>${fmt(t.exit_price)}</td>
          <td>${t.qty_per_lot}</td>
          <td class="${pnlClass(pnl)}">${fmt(pnl)}</td>
          <td>${t.divergence_type || ''}</td>
          <td>${t.exit_reason || ''}</td>
        </tr>`;
      })
      .join('');
  }

  function updateInstrumentFilter(list) {
    const sel = $('filterInstrument');
    const cur = state.instrument;
    sel.innerHTML =
      `<option value="All">All Instruments</option>` +
      (list || []).map((i) => `<option value="${i}">${i}</option>`).join('');
    sel.value = list.includes(cur) ? cur : 'All';
    state.instrument = sel.value;
  }

  async function loadResults() {
    const q = new URLSearchParams({
      instrument: state.instrument,
      timeframe: state.timeframe,
      page: String(state.page),
      pageSize: String(state.pageSize),
    });
    const data = await api(`/api/results?${q}`);
    updateInstrumentFilter(data.instruments || []);
    renderCards(data.analysis || {});
    renderChart((data.analysis && data.analysis.equity_curve) || []);
    renderTfBreakdown(data.by_timeframe);
    state.tradesCache = data.trades || [];
    renderTable(state.tradesCache);
    const p = data.pagination || {};
    $('pageInfo').textContent = `Page ${p.page || 1} / ${p.pages || 1} (${p.total || 0} trades)`;
  }

  function setProgress(pct, text) {
    $('progressWrap').classList.remove('hidden');
    $('progressFill').style.width = `${Math.max(0, Math.min(100, pct))}%`;
    $('progressText').textContent = text || '';
  }

  function subscribeJob(jobId) {
    if (state.es) {
      state.es.close();
      state.es = null;
    }
    if (state.pollTimer) {
      clearInterval(state.pollTimer);
      state.pollTimer = null;
    }
    state.jobId = jobId;
    const es = new EventSource(`/api/jobs/${jobId}/events`);
    state.es = es;

    const finish = async (msg) => {
      if (state.pollTimer) {
        clearInterval(state.pollTimer);
        state.pollTimer = null;
      }
      if (state.es) {
        try {
          state.es.close();
        } catch (_) {
          /* ignore */
        }
        state.es = null;
      }
      if (msg) logLine(msg);
      setProgress(100, 'Completed');
      $('btnRun').disabled = false;
      await loadResults();
    };

    es.addEventListener('progress', async (ev) => {
      const p = JSON.parse(ev.data);
      const pct = p.total ? (p.done / p.total) * 100 : 0;
      setProgress(pct, p.message || `${p.done}/${p.total}`);
      try {
        await loadResults();
      } catch (_) {
        /* ignore mid-run */
      }
    });
    es.addEventListener('log', (ev) => {
      const entry = JSON.parse(ev.data);
      logLine(entry.message, entry.level || 'info');
    });
    es.addEventListener('done', async () => {
      await finish('Backtest completed — results appended to history');
    });
    es.addEventListener('error', async (ev) => {
      try {
        if (ev.data) {
          const data = JSON.parse(ev.data);
          logLine(data.message || 'Job error', 'error');
          $('btnRun').disabled = false;
        }
      } catch {
        /* SSE reconnect noise */
      }
    });

    // Fallback poll in case SSE misses the terminal event on fast jobs
    state.pollTimer = setInterval(async () => {
      try {
        const job = await api(`/api/jobs/${jobId}`);
        const p = job.progress || {};
        const pct = p.total ? (p.done / p.total) * 100 : 0;
        setProgress(pct, p.message || `${p.done}/${p.total}`);
        if (job.status === 'completed') {
          await finish('Backtest completed — results appended to history');
        } else if (job.status === 'failed') {
          clearInterval(state.pollTimer);
          state.pollTimer = null;
          logLine(job.error || 'Job failed', 'error');
          $('btnRun').disabled = false;
        }
      } catch (_) {
        /* ignore */
      }
    }, 1000);
  }

  $('runForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const instruments = $('instruments').value.trim();
    if (!instruments) return;
    $('btnRun').disabled = true;
    setProgress(0, 'Starting…');
    logLine(`Starting backtest for ${instruments}`);
    try {
      const body = {
        instruments,
        periodMonths: Number($('period').value),
        forceRefresh: $('forceRefresh').checked,
        settings: collectSettings(),
      };
      const res = await api('/api/run-backtest', {
        method: 'POST',
        body: JSON.stringify(body),
      });
      logLine(`Job ${res.jobId} accepted (${res.months.join(', ')})`);
      subscribeJob(res.jobId);
    } catch (err) {
      logLine(err.message, 'error');
      $('btnRun').disabled = false;
    }
  });

  $('btnSaveSettings').addEventListener('click', async () => {
    try {
      await api('/api/settings', {
        method: 'POST',
        body: JSON.stringify(collectSettings()),
      });
      $('settingsStatus').textContent = 'Saved';
      setTimeout(() => {
        $('settingsStatus').textContent = '';
      }, 2000);
    } catch (err) {
      $('settingsStatus').textContent = err.message;
    }
  });

  $('filterInstrument').addEventListener('change', async (e) => {
    state.instrument = e.target.value;
    state.page = 1;
    await loadResults();
  });

  document.getElementById('tfTabs').addEventListener('click', async (ev) => {
    const btn = ev.target.closest('.tab');
    if (!btn) return;
    document.querySelectorAll('#tfTabs .tab').forEach((b) => b.classList.remove('active'));
    btn.classList.add('active');
    state.timeframe = btn.getAttribute('data-tf');
    state.page = 1;
    await loadResults();
  });

  document.querySelectorAll('#tradesTable th[data-sort]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = th.getAttribute('data-sort');
      if (state.sortKey === key) state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
      else {
        state.sortKey = key;
        state.sortDir = 'asc';
      }
      renderTable(state.tradesCache);
    });
  });

  $('prevPage').addEventListener('click', async () => {
    if (state.page > 1) {
      state.page -= 1;
      await loadResults();
    }
  });
  $('nextPage').addEventListener('click', async () => {
    state.page += 1;
    await loadResults();
  });

  async function init() {
    try {
      const settings = await api('/api/settings');
      fillSettings(settings);
      const status = await api('/api/upstox/status');
      $('tokenStatus').textContent = status.demoMode
        ? 'Mode: DEMO (synthetic data)'
        : status.configured
          ? `Upstox token: ${status.preview}`
          : 'Upstox token: not configured';
      await loadResults();
      logLine('Ready. Results append across runs; cached months are skipped unless Force Refresh.');
    } catch (err) {
      logLine(`Init error: ${err.message}`, 'error');
    }
  }

  init();
})();
