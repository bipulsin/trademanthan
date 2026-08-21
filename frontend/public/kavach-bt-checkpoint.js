(function () {
  const token = () => localStorage.getItem('trademanthan_token') || '';
  const statusEl = () => document.getElementById('kbtStatus');

  function setStatus(msg) {
    const el = statusEl();
    if (el) el.textContent = msg || '';
  }

  async function api(path) {
    const res = await fetch(path, {
      headers: { Authorization: 'Bearer ' + token() },
    });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(res.status + ' ' + t.slice(0, 200));
    }
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('text/csv')) return res.text();
    return res.json();
  }

  function qs(extra) {
    const p = new URLSearchParams();
    const sym = document.getElementById('fSymbol').value.trim();
    const from = document.getElementById('fFrom').value;
    const to = document.getElementById('fTo').value;
    const g = document.getElementById('fGaruda').value;
    const res = document.getElementById('fRes').value;
    const pb = document.getElementById('fPb').value;
    if (sym) p.set('symbol', sym);
    if (from) p.set('from', from);
    if (to) p.set('to', to);
    if (g) p.set('garuda', g);
    if (res !== '') p.set('res_confluence', res);
    if (pb !== '') p.set('pb_hard_blocked', pb);
    if (extra) Object.entries(extra).forEach(([k, v]) => p.set(k, v));
    const s = p.toString();
    return s ? '?' + s : '';
  }

  function badgeBlock(on) {
    return on
      ? '<span class="badge badge-block">HARD BLOCK</span>'
      : '<span class="badge badge-ok">ok</span>';
  }
  function badgeRes(on) {
    return on
      ? '<span class="badge badge-warn">WARN</span>'
      : '<span class="badge badge-na">—</span>';
  }
  function badgeGaruda(v) {
    if (v === 'MATCH') return '<span class="badge badge-ok">MATCH</span>';
    if (v === 'NO_MATCH') return '<span class="badge badge-warn">NO_MATCH</span>';
    return '<span class="badge badge-na">N/A</span>';
  }
  function fmt(v) {
    if (v == null || v === '') return '—';
    if (typeof v === 'number') return Number.isInteger(v) ? String(v) : v.toFixed(2);
    return String(v);
  }

  function renderCards(cohorts) {
    const overall = (cohorts || []).find((c) => c.cohort_type === 'overall');
    const exits = (cohorts || []).filter((c) => c.cohort_type === 'exit_method');
    const el = document.getElementById('kbtCards');
    const cards = [];
    if (overall) {
      cards.push(['Trades', fmt(overall.n)]);
      cards.push(['Win %', fmt(overall.win_rate)]);
      cards.push(['Avg R', fmt(overall.avg_r)]);
      cards.push(['Total PnL', fmt(overall.total_pnl)]);
    }
    exits.forEach((e) => cards.push(['Exit ' + e.cohort_key + ' avg R', fmt(e.avg_r)]));
    el.innerHTML = cards
      .map(
        ([h, v]) =>
          `<div class="kbt-card"><h3>${h}</h3><div class="val">${v}</div></div>`
      )
      .join('');
  }

  function renderRecs(recs) {
    const ul = document.getElementById('kbtRecs');
    ul.innerHTML = (recs || [])
      .map((r) => `<li><strong>${r.cohort_key}:</strong> ${r.recommendation_text || ''}</li>`)
      .join('') || '<li>No recommendations yet — run the checkpoint script.</li>';
  }

  function renderCohorts(cohorts) {
    const tb = document.querySelector('#tblCohorts tbody');
    tb.innerHTML = (cohorts || [])
      .map(
        (c) => `<tr>
        <td>${fmt(c.cohort_type)}</td><td>${fmt(c.cohort_key)}</td><td>${fmt(c.n)}</td>
        <td>${fmt(c.win_rate)}</td><td>${fmt(c.avg_r)}</td><td>${fmt(c.total_pnl)}</td>
        <td>${fmt(c.avg_mfe)}</td><td>${fmt(c.avg_mae)}</td>
      </tr>`
      )
      .join('');
  }

  function renderTrades(trades) {
    const tb = document.querySelector('#tblTrades tbody');
    tb.innerHTML = (trades || [])
      .map((t) => {
        const d = (t.session_date || '').toString().slice(0, 10);
        return `<tr>
          <td>${d}</td><td>${fmt(t.symbol)}</td><td>${fmt(t.direction)}</td>
          <td>${fmt(t.pb_legacy)}</td><td>${fmt(t.pb_v2)}</td>
          <td>${badgeBlock(!!t.pb_hard_blocked)}</td>
          <td>${badgeRes(!!t.res_confluence)}</td>
          <td>${fmt(t.nearest_pivot)}</td>
          <td>${fmt(t.exit_a_r)}</td><td>${fmt(t.exit_b_r)}</td><td>${fmt(t.exit_c_r)}</td>
          <td><strong>${fmt(t.best_exit_method)}</strong></td>
          <td>${badgeGaruda(t.garuda_confluence)}</td>
          <td>${fmt(t.garuda_rank)}</td>
          <td>${fmt(t.r_realized)}</td><td>${fmt(t.mfe_r)}</td>
        </tr>`;
      })
      .join('');
  }

  async function loadAll() {
    setStatus('Loading…');
    try {
      const sum = await api('/api/kavach-bt-checkpoint/summary');
      const tr = await api('/api/kavach-bt-checkpoint/trades' + qs());
      renderCards(sum.cohorts);
      renderRecs(sum.recommendations);
      renderCohorts(sum.cohorts);
      renderTrades(tr.trades);
      setStatus(
        sum.run_id
          ? `run_id=${sum.run_id} · trades=${tr.count}`
          : 'No checkpoint run in DB yet. Execute scripts/run_kavach_bt_checkpoint.py'
      );
    } catch (e) {
      setStatus('Error: ' + e.message);
    }
  }

  async function downloadCsv(kind) {
    try {
      const body = await api('/api/kavach-bt-checkpoint/export.csv' + qs({ kind }));
      const blob = new Blob([body], { type: 'text/csv' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = `kavach_bt_${kind}.csv`;
      a.click();
      URL.revokeObjectURL(a.href);
    } catch (e) {
      setStatus('Export failed: ' + e.message);
    }
  }

  document.getElementById('btnLoad').addEventListener('click', loadAll);
  document.getElementById('btnCsvDetail').addEventListener('click', () => downloadCsv('detail'));
  document.getElementById('btnCsvSummary').addEventListener('click', () => downloadCsv('summary'));
  loadAll();
})();
