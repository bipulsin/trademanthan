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

  function qualityBadge(status) {
    const s = String(status || 'NOT_IMPORTED').toUpperCase().replace(/_/g, ' ');
    let cls = 'research';
    if (s === 'PASS') cls = 'validated';
    if (s === 'IMPORTING') cls = 'live';
    if (s === 'WARNING') cls = 'research';
    if (s === 'FAIL' || s === 'NOT IMPORTED' || s.includes('INSUFFICIENT')) cls = 'poor';
    return `<span class="sb-badge ${cls}">${s}</span>`;
  }

  function renderImportProgress(j, jobId) {
    const el = document.getElementById('importProgress');
    if (!el) return;
    if (!j) {
      el.innerHTML = kv({ status: 'No import running.' });
      return;
    }
    const chunk = j.current_chunk != null && j.total_chunks != null
      ? `${j.current_chunk} / ${j.total_chunks}`
      : '—';
    const period = j.chunk_from && j.chunk_to ? `${j.chunk_from} → ${j.chunk_to}` : '—';
    const imported = j.candles_imported != null ? Number(j.candles_imported).toLocaleString() : '—';
    const errN = Array.isArray(j.errors) ? j.errors.length : (j.error ? 1 : 0);
    el.innerHTML = kv({
      'job ID': jobId || j.job_id || '—',
      status: String(j.status || '—').toUpperCase(),
      chunk: chunk,
      period: period,
      'completed chunks': j.completed_chunks != null ? j.completed_chunks : '—',
      'candles imported': imported,
      errors: errN,
      'final status': j.result && j.result.ok === false ? 'ERROR' : (j.status || '—'),
    });
  }

  async function loadAll() {
    const [status, current, hist, perf, cal, model, tv, dataStatus] = await Promise.all([
      api('/status'),
      api('/current'),
      api('/history?limit=40'),
      api('/performance'),
      api('/calibration'),
      api('/model'),
      api('/tradingview-stub'),
      api('/data-status'),
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

    const calN = Number((cal.n != null ? cal.n : (cal.calibration_buckets && cal.calibration_buckets.n)) || 0);
    const calStatus = calN > 0 ? (cal.status || 'OK') : 'INSUFFICIENT DATA';
    const calEce = calN > 0 ? (cal.ece != null ? cal.ece : (cal.calibration_buckets && cal.calibration_buckets.ece)) : '—';
    document.getElementById('calBody').innerHTML = kv({
      status: badge(calStatus),
      ece: calEce == null ? '—' : calEce,
      n: calN,
      model_id: cal.model_id,
    });

    const ds = dataStatus || {};
    const range = ds.start_date && ds.end_date ? `${ds.start_date} → ${ds.end_date}` : '—';
    document.getElementById('dataStatusBody').innerHTML = kv({
      Instrument: ds.instrument || 'NIFTY 50',
      Interval: '10 minutes',
      'Historical range': range,
      '10-minute candles': ds.candle_count != null ? Number(ds.candle_count).toLocaleString() : 0,
      'Trading days': ds.trading_days != null ? ds.trading_days : 0,
      'Missing candles': ds.missing_candles != null ? ds.missing_candles : 0,
      'Data quality': qualityBadge(ds.status || 'NOT_IMPORTED'),
      'Current phase': ds.phase || 'DATA COLLECTION',
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
    for (let i = 0; i < 400; i++) {
      const j = await api(`/jobs/${jobId}`);
      renderImportProgress(j, jobId);
      log.textContent = JSON.stringify(j, null, 2);
      const st = String(j.status || '').toLowerCase();
      if (st === 'done' || st === 'error' || st === 'fail') {
        await loadAll();
        renderImportProgress(j, jobId);
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
      const body = { from_date, to_date, resume: true };
      const { job_id } = await api('/import', { method: 'POST', body: JSON.stringify(body) });
      document.getElementById('jobLog').textContent = `job_id=${job_id}\nIMPORTING…`;
      renderImportProgress({ status: 'IMPORTING', candles_imported: 0, current_chunk: 0, total_chunks: 0, errors: [] }, job_id);
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
