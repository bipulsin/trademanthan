/* Kosmic Tarang Phase 1 — data health UI */
(function () {
  const token = () => localStorage.getItem('trademanthan_token') || '';

  async function api(path, opts = {}) {
    const res = await fetch(path, {
      ...opts,
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${token()}`,
        ...(opts.headers || {}),
      },
    });
    if (res.status === 401) {
      window.location.href = 'index.html';
      throw new Error('unauthorized');
    }
    if (res.status === 403) {
      throw new Error('Administrator only');
    }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.detail || data.message || res.statusText);
    return data;
  }

  function fmtLoss(row) {
    if (row.currency === 'INR') return `₹${Number(row.max_loss_zero_credit).toLocaleString('en-IN')}`;
    return `$${Number(row.max_loss_zero_credit_usd).toFixed(2)}`;
  }

  function renderFeeds(feeds) {
    const host = document.getElementById('feedGrid');
    host.innerHTML = '';
    for (const [name, f] of Object.entries(feeds || {})) {
      const ok = !!f.ok || !!f.public_ok;
      const card = document.createElement('div');
      card.className = 'tarang-card';
      const auth = f.auth || {};
      card.innerHTML = `
        <h3>${name}</h3>
        <div><span class="tarang-badge ${ok ? 'tarang-badge-ok' : 'tarang-badge-bad'}">${f.status || (ok ? 'ok' : 'fail')}</span></div>
        <pre class="tarang-pre" style="margin-top:8px;">${JSON.stringify({ ...f, auth: auth }, null, 2)}</pre>
      `;
      host.appendChild(card);
    }
  }

  function renderLoss(table) {
    const body = document.getElementById('lossBody');
    const rows = (table && table.rows) || [];
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="5" class="tarang-muted">No rows</td></tr>';
      return;
    }
    body.innerHTML = rows.map((r) => {
      const fit = r.currency === 'INR' ? r.fits_energy_budget : r.fits_crypto_budget;
      return `<tr>
        <td>${r.underlying} <span class="tarang-muted">(${r.profile_id})</span></td>
        <td>${r.width_label}</td>
        <td>${fmtLoss(r)}</td>
        <td>₹${Number(r.budget_ref_inr).toLocaleString('en-IN')}</td>
        <td class="${fit ? 'tarang-fit-yes' : 'tarang-fit-no'}">${fit ? 'Yes' : 'No'}</td>
      </tr>`;
    }).join('');
  }

  async function load() {
    const status = document.getElementById('statusLine');
    status.textContent = 'Loading health…';
    try {
      const h = await api('/api/tarang/health');
      document.getElementById('modeBadge').textContent = h.mode || 'PAPER';
      document.getElementById('autoBadge').textContent = h.auto ? 'AUTO ON' : 'AUTO OFF';
      const banner = document.getElementById('tokenBanner');
      if (h.token_expired) {
        banner.hidden = false;
        banner.textContent = 'Upstox token missing/expired or MCX master incomplete — refresh OAuth (see runbook).';
      } else {
        banner.hidden = true;
      }
      renderFeeds(h.feeds);
      const e = (h.risk && h.risk.buckets && h.risk.buckets.ENERGY) || {};
      const c = (h.risk && h.risk.buckets && h.risk.buckets.CRYPTO) || {};
      document.getElementById('budgetBox').textContent = JSON.stringify({ ENERGY: e, CRYPTO: c }, null, 2);
      renderLoss(h.min_max_loss);
      document.getElementById('ivCounts').textContent = JSON.stringify(h.iv_snapshot_counts || {}, null, 2);
      document.getElementById('wsBox').textContent = JSON.stringify(h.upstox_ws || {}, null, 2);
      status.textContent = `Updated ${h.checked_at || ''}`;
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  }

  document.getElementById('btnRefresh').addEventListener('click', load);
  document.getElementById('btnIvSnap').addEventListener('click', async () => {
    const status = document.getElementById('statusLine');
    status.textContent = 'Running IV snapshot…';
    try {
      const out = await api('/api/tarang/iv-snapshots/run', { method: 'POST' });
      status.textContent = `IV snapshot done: ${JSON.stringify(out.results || out)}`;
      await load();
    } catch (err) {
      status.textContent = String(err.message || err);
    }
  });

  load();
})();
