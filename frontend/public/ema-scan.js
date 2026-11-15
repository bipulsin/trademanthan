(function () {
  const HIT_KEYS = ["0.25", "0.5", "0.75", "1", "1.5", "2"];
  let report = null;
  let sortKey = "date";
  let sortDir = 1;

  function apiBase() {
    const h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }

  function fmt(value, digits) {
    if (value === null || value === undefined || value === "") return "—";
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return n.toLocaleString("en-IN", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  }

  function fmtDate(iso) {
    if (!iso) return "—";
    const [y, m, d] = iso.split("-").map(Number);
    const dt = new Date(Date.UTC(y, m - 1, d));
    return dt.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric", timeZone: "UTC" });
  }

  function el(id) { return document.getElementById(id); }

  function card(label, value) {
    return `<div class="es-card"><span>${label}</span><strong>${value}</strong></div>`;
  }

  function breakTable(rows) {
    const body = (rows || []).map((row) => `
      <tr>
        <td>${row.label}</td>
        <td class="num">${row.count}</td>
        <td class="num">${fmt(row.avg_mfe, 2)}</td>
        <td class="num">${fmt(row.median_mfe, 2)}</td>
        <td class="num">${fmt(row.pct_hit_1, 1)}%</td>
      </tr>`).join("");
    return `<table class="es-table">
      <thead><tr><th>Bucket</th><th class="num">Signals</th><th class="num">Avg +ATR</th><th class="num">Median</th><th class="num">Hit 1 ATR</th></tr></thead>
      <tbody>${body || '<tr><td colspan="5" class="es-empty">No signals</td></tr>'}</tbody>
    </table>`;
  }

  function renderChart(bins) {
    const host = el("esChart");
    const rows = bins || [];
    const max = Math.max(1, ...rows.map((b) => b.count || 0));
    if (!rows.length) {
      host.innerHTML = '<p class="es-empty">No signals</p>';
      return;
    }
    host.innerHTML = rows.map((bin) => {
      const h = Math.round(((bin.count || 0) / max) * 120);
      return `<div class="es-bar">
        <div class="es-bar-count">${bin.count}</div>
        <div class="es-bar-fill" style="height:${h}px"></div>
        <div class="es-bar-label">${bin.label}</div>
      </div>`;
    }).join("");
  }

  function renderDays() {
    const symbol = (el("esSymbol").value || "").trim().toUpperCase();
    const date = el("esDate").value;
    const host = el("esDays");
    const days = (report.days || []).filter((day) => !date || day.date === date);
    host.innerHTML = days.map((day) => {
      const rows = (day.signals || []).filter((s) => !symbol || s.symbol.indexOf(symbol) !== -1);
      const open = rows.length > 0;
      const inner = rows.length
        ? `<div class="es-table-wrap"><table class="es-table"><thead><tr>
            <th>Symbol</th><th>Contract</th><th class="num">Candle</th><th>Time</th>
            <th class="num">ADX</th><th class="num">% above EMA</th><th class="num">Max +ATR</th>
          </tr></thead><tbody>${rows.map((s) => `<tr>
            <td>${s.symbol}</td><td>${s.contract || ""}</td>
            <td class="num">${s.candle_number}</td><td>${s.time}</td>
            <td class="num">${fmt(s.adx, 1)}</td>
            <td class="num">${fmt(s.pct_above_ema, 3)}%</td>
            <td class="num">${fmt(s.max_favorable_atr, 2)}</td>
          </tr>`).join("")}</tbody></table></div>`
        : '<p class="es-empty">No signals</p>';
      return `<details class="es-day" ${open ? "open" : ""}>
        <summary><span>${fmtDate(day.date)}</span><span>${rows.length} signal${rows.length === 1 ? "" : "s"}</span></summary>
        ${inner}
      </details>`;
    }).join("");
  }

  function visibleSignals() {
    const symbol = (el("esSymbol").value || "").trim().toUpperCase();
    const date = el("esDate").value;
    return (report.signals || []).filter((s) => {
      if (date && s.date !== date) return false;
      if (symbol && String(s.symbol).indexOf(symbol) === -1) return false;
      return true;
    });
  }

  function renderTable() {
    const rows = visibleSignals().slice().sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (av === bv) return 0;
      if (av === null || av === undefined) return 1;
      if (bv === null || bv === undefined) return -1;
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * sortDir;
      return String(av).localeCompare(String(bv)) * sortDir;
    });
    el("esTableHint").textContent = `${rows.length} shown`;
    const touchClass = (label) => {
      if (label === "+1 ATR") return "es-touch-up";
      if (label === "-1 ATR") return "es-touch-down";
      return "";
    };
    el("esTbody").innerHTML = rows.length
      ? rows.map((s) => `<tr>
          <td>${fmtDate(s.date)}</td>
          <td>${s.symbol}</td>
          <td>${s.contract || ""}</td>
          <td class="num">${s.candle_number}</td>
          <td>${s.time}</td>
          <td class="num">${fmt(s.open, 2)}</td>
          <td class="num">${fmt(s.close, 2)}</td>
          <td class="num">${fmt(s.ema30, 2)}</td>
          <td class="num">${fmt(s.pct_above_ema, 3)}%</td>
          <td class="num">${fmt(s.adx, 1)}</td>
          <td class="num">${fmt(s.plus_di, 1)}</td>
          <td class="num">${fmt(s.minus_di, 1)}</td>
          <td class="num">${fmt(s.atr, 2)}</td>
          <td class="num">${fmt(s.entry, 2)}</td>
          <td class="num">${fmt(s.max_favorable_atr, 2)}</td>
          <td class="num">${fmt(s.max_adverse_atr, 2)}</td>
          <td class="num">${fmt(s.close_at_2h_atr, 2)}</td>
          <td class="num">${s.time_to_max_favorable}</td>
          <td class="${touchClass(s.first_touch)}">${s.first_touch}</td>
        </tr>`).join("")
      : '<tr><td colspan="19" class="es-empty">No signals match the filter</td></tr>';
  }

  function renderSkipped() {
    const symbol = (el("esSkipSymbol").value || "").trim().toUpperCase();
    const rows = (report.skipped || []).filter((s) => !symbol || String(s.symbol).indexOf(symbol) !== -1);
    el("esSkipBody").innerHTML = rows.length
      ? rows.map((s) => `<tr>
          <td>${fmtDate(s.date)}</td><td>${s.symbol}</td><td>${s.contract || ""}</td><td>${s.reason}</td>
        </tr>`).join("")
      : '<tr><td colspan="4" class="es-empty">Nothing skipped</td></tr>';
  }

  function render(doc) {
    report = doc;
    const header = doc.header || {};
    const params = doc.params || {};
    const summary = doc.summary || {};
    el("esRule").textContent = header.contract_rule || el("esRule").textContent;
    el("esMeta").textContent = [
      `${fmtDate(header.date_from)} → ${fmtDate(header.date_to)}`,
      `${header.trading_days || 0} trading days`,
      `${header.signals || 0} signals`,
      `${header.stocks_skipped || 0} stock${header.stocks_skipped === 1 ? "" : "s"} skipped`,
      header.universe ? `${header.universe} in universe` : "",
    ].filter(Boolean).join(" · ");
    const atrLabel = (params.atr_basis || "daily") === "daily"
      ? "daily ATR(14) as of the previous close"
      : "5-minute ATR(14) at the signal candle";
    el("esParams").textContent =
      `EMA ${params.ema_period || 30} on close, carried across sessions on the same contract. ` +
      `ADX(${params.adx_period || 14}) Wilder, ADX > ${params.adx_min || 25}, +DI > −DI. ` +
      `Close within ${((params.max_above_ema || 0.005) * 100).toFixed(1)}% above EMA. ` +
      `First ${params.screen_candles || 6} candles (09:15–09:45). first_only = ${params.first_only !== false}. ` +
      `Forward window ${params.window_minutes || 120} minutes. ATR basis: ${atrLabel}. ` +
      `A contract needs ${params.ema_stable_bars || 90} prior 5-minute bars before the session.`;

    el("esCards").innerHTML = [
      card("Signals", summary.count ?? 0),
      card("Avg max +ATR", fmt(summary.avg_mfe, 2)),
      card("Median max +ATR", fmt(summary.median_mfe, 2)),
      card("25th", fmt(summary.p25_mfe, 2)),
      card("75th", fmt(summary.p75_mfe, 2)),
      card("90th", fmt(summary.p90_mfe, 2)),
      card("Avg max −ATR", fmt(summary.avg_mae, 2)),
    ].join("");

    const hits = summary.hit_pct || {};
    el("esHits").innerHTML = HIT_KEYS.map((key) => `
      <div class="es-hit"><span>Reached ${key} ATR</span><strong>${fmt(hits[key], 1)}%</strong></div>
    `).join("");

    renderChart(doc.distribution);
    const breaks = doc.breakdowns || {};
    el("esByCandle").innerHTML = breakTable(breaks.candle);
    el("esByAdx").innerHTML = breakTable(breaks.adx);
    el("esByDist").innerHTML = breakTable(breaks.distance);

    const dateSel = el("esDate");
    const current = dateSel.value;
    dateSel.innerHTML = '<option value="">All dates</option>' + (doc.days || []).map((d) =>
      `<option value="${d.date}">${fmtDate(d.date)}</option>`
    ).join("");
    dateSel.value = current;

    const skipCount = (doc.skipped || []).length;
    const stocks = header.stocks_skipped || 0;
    el("esSkipped").querySelector("summary").textContent =
      `Skipped — ${stocks} stock${stocks === 1 ? "" : "s"} (${skipCount} stock-day${skipCount === 1 ? "" : "s"})`;
    renderDays();
    renderTable();
    renderSkipped();
  }

  function showStatus(message) {
    const node = el("esStatus");
    node.hidden = !message;
    node.textContent = message || "";
  }

  async function load() {
    showStatus("");
    try {
      const res = await fetch(apiBase() + "/api/ema-scan", { cache: "no-store" });
      if (!res.ok) throw new Error("HTTP " + res.status);
      const doc = await res.json();
      if (!doc || doc.ok === false) {
        showStatus((doc && doc.detail) || "EMA scan result is not on this server yet.");
        return;
      }
      render(doc);
    } catch (err) {
      showStatus("Could not load the EMA scan. " + (err && err.message ? err.message : ""));
    }
  }

  document.querySelectorAll("#esTable th[data-sort]").forEach((th) => {
    th.addEventListener("click", () => {
      const key = th.getAttribute("data-sort");
      if (sortKey === key) sortDir = -sortDir;
      else { sortKey = key; sortDir = key === "symbol" || key === "date" || key === "time" ? 1 : -1; }
      if (report) renderTable();
    });
  });
  el("esSymbol").addEventListener("input", () => { if (report) { renderTable(); renderDays(); } });
  el("esDate").addEventListener("change", () => { if (report) { renderTable(); renderDays(); } });
  el("esSkipSymbol").addEventListener("input", () => { if (report) renderSkipped(); });
  load();
})();
