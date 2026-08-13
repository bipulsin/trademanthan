(function () {
  const API_BASE =
    window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
      ? 'http://localhost:8000'
      : window.location.origin;
  const ROOT = `${API_BASE}/api/sambhav`;

  function token() {
    return localStorage.getItem('trademanthan_token') || '';
  }

  async function api(path, opts) {
    const res = await fetch(`${ROOT}${path}`, {
      ...opts,
      headers: {
        Authorization: `Bearer ${token()}`,
        'Content-Type': 'application/json',
        ...(opts && opts.headers),
      },
      cache: 'no-store',
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.detail || res.statusText || 'request failed');
    return data;
  }

  function kv(obj) {
    return Object.entries(obj)
      .map(([k, v]) => `<div><span class="k">${k}:</span> <span class="v">${v == null ? '—' : v}</span></div>`)
      .join('');
  }

  function badge(status) {
    const s = String(status || 'RESEARCH').toUpperCase();
    let cls = 'research';
    if (s === 'VALIDATED') cls = 'validated';
    if (s === 'LIVE') cls = 'live';
    if (s.includes('POOR') || s.includes('INSUFFICIENT') || s.includes('NOT VALIDATED')) cls = 'poor';
    return `<span class="sb-badge ${cls}">${s}</span>`;
  }

  async function loadAll() {
    const [status, current, hist, perf, cal, model, tv] = await Promise.all([
      api('/status'),
      api('/current'),
      api('/history?limit=40'),
      api('/performance'),
      api('/calibration'),
      api('/model'),
      api('/tradingview-stub'),
    ]);

    document.getElementById('liveBody').innerHTML = current.ok
      ? kv({
          candle: current.candle_start,
          'P(UP) raw': current.p_up_raw,
          'P(UP) cal': current.p_up_calibrated,
          'P(DOWN) cal': current.p_down_calibrated,
          direction: current.predicted_direction,
          status: current.status,
          warning: current.warning || '',
        })
      : kv({ status: current.status || 'INSUFFICIENT DATA', message: current.message });

    const active = model.active;
    document.getElementById('modelBody').innerHTML = active
      ? kv({
          id: active.id,
          name: active.name,
          status: badge(active.status),
          type: active.model_type,
          calibration: active.calibration_method,
          created: active.created_at,
          note: model.lifecycle_note,
        })
      : kv({ status: badge('MODEL NOT VALIDATED'), message: 'No model trained yet' });

    document.getElementById('perfBody').innerHTML = kv({
      'backtest n': perf.backtest && perf.backtest.n,
      'backtest acc': perf.backtest && (perf.backtest.accuracy ?? perf.backtest.status),
      'live n': perf.live && perf.live.n,
      'live acc': perf.live && (perf.live.accuracy ?? perf.live.status),
      verdict: badge(perf.verdict || 'MODEL NOT VALIDATED'),
    });

    const buckets = (cal.calibration_buckets && cal.calibration_buckets.status) || cal.status;
    document.getElementById('calBody').innerHTML = kv({
      status: badge(buckets || 'INSUFFICIENT DATA'),
      ece: cal.calibration_buckets && cal.calibration_buckets.ece,
      n: cal.calibration_buckets && cal.calibration_buckets.n,
      model_id: cal.model_id,
    });

    const tbody = document.querySelector('#histTable tbody');
    tbody.innerHTML = (hist.items || [])
      .map(
        (r) => `<tr>
        <td>${r.candle_start || ''}</td>
        <td>${r.p_up_calibrated == null ? '—' : Number(r.p_up_calibrated).toFixed(3)}</td>
        <td>${r.predicted_direction || ''}</td>
        <td>${r.status || ''}</td>
        <td>${r.actual_direction || '—'}</td>
        <td>${r.future_return == null ? '—' : Number(r.future_return).toFixed(5)}</td>
        <td>${r.source || ''}</td>
      </tr>`
      )
      .join('');

    document.getElementById('tvStub').textContent = tv.message || 'Deferred';
    window.__sambhavStatus = status;
  }

  async function pollJob(jobId) {
    const log = document.getElementById('jobLog');
    for (let i = 0; i < 120; i++) {
      const j = await api(`/jobs/${jobId}`);
      log.textContent = JSON.stringify(j, null, 2);
      if (j.status === 'done' || j.status === 'error') {
        await loadAll();
        return j;
      }
      await new Promise((r) => setTimeout(r, 2500));
    }
    log.textContent += '\n(timeout waiting for job)';
  }

  document.getElementById('btnRefresh').addEventListener('click', () => loadAll().catch(alert));
  document.getElementById('btnPredict').addEventListener('click', async () => {
    try {
      const out = await api('/predict', { method: 'POST', body: '{}' });
      document.getElementById('jobLog').textContent = JSON.stringify(out, null, 2);
      await loadAll();
    } catch (e) {
      alert(e.message);
    }
  });
  document.getElementById('btnImport').addEventListener('click', async () => {
    try {
      const from_date = document.getElementById('importFrom').value;
      const to_date = document.getElementById('importTo').value || null;
      if (!from_date) return alert('Set import from date');
      const body = { from_date, to_date, rebuild_10m: true };
      const { job_id } = await api('/import', { method: 'POST', body: JSON.stringify(body) });
      await pollJob(job_id);
    } catch (e) {
      alert(e.message);
    }
  });
  document.getElementById('btnTrain').addEventListener('click', async () => {
    try {
      const { job_id } = await api('/train', {
        method: 'POST',
        body: JSON.stringify({ model_kind: 'xgboost', calibration: 'isotonic', run_validation: true }),
      });
      await pollJob(job_id);
    } catch (e) {
      alert(e.message);
    }
  });
  document.getElementById('btnBacktest').addEventListener('click', async () => {
    try {
      const { job_id } = await api('/backtest', {
        method: 'POST',
        body: JSON.stringify({ train_bars: 800, test_bars: 200, step_bars: 200 }),
      });
      await pollJob(job_id);
    } catch (e) {
      alert(e.message);
    }
  });

  loadAll().catch((e) => {
    document.getElementById('liveBody').textContent = e.message;
  });
})();
