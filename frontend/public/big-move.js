(function () {
  const FEATURES = [
    ["rvol_30", "RVOL first 30m"],
    ["rvol_15", "RVOL first 15m"],
    ["gap_atr", "Gap (ATR)"],
    ["gap_pct", "Gap %"],
    ["gap_location", "Gap vs prior day"],
    ["gap_held", "Gap held"],
    ["oi_state", "OI state"],
    ["oi_change_pct", "OI change %"],
    ["oi_basis", "OI basis"],
    ["prev_range_atr", "Prev day range / ATR"],
    ["nr7", "NR7"],
    ["range_5_20", "5d range / 20d range"],
    ["atr5_atr50", "ATR(5) / ATR(50)"],
    ["bb_width_pct", "BB bandwidth percentile"],
    ["or15_atr", "First 15m range / ATR"],
    ["or_break", "Beyond opening range"],
    ["close_pos_30", "Close in 30m range"],
    ["move_open_atr", "Move from open"],
    ["rs_nifty", "RS vs Nifty"],
    ["rs_sector", "RS vs sector"],
    ["rs_20d", "20-day RS vs Nifty"],
    ["dist_ema20_atr", "vs EMA20"],
    ["dist_ema50_atr", "vs EMA50"],
    ["ema20_slope_atr", "EMA20 slope"],
    ["adx_daily", "Daily ADX"],
    ["adx_5m", "5m ADX"],
    ["di_spread", "+DI minus −DI"],
    ["ema30_dist_pct", "vs 5m EMA30 %"],
    ["extreme_gap", "Extreme gap"],
    ["extreme_rvol", "Extreme RVOL"],
  ];
  const COLS = [
    ["date", "Date"], ["symbol", "Symbol"], ["contract", "Contract"],
    ["max_up_atr", "Max up"], ["max_down_atr", "Max down"], ["close_move_atr", "Close"],
    ["gap_atr", "Gap ATR"], ["rvol_30", "RVOL 30m"], ["oi_state", "OI state"],
    ["oi_basis", "OI basis"], ["rs_nifty", "RS Nifty"], ["adx_5m", "5m ADX"],
    ["di_spread", "+DI −DI"], ["ema30_dist_pct", "vs EMA30 %"],
    ["prev_range_atr", "Prev range"], ["nr7", "NR7"],
    ["extreme_gap", "X-gap"], ["extreme_rvol", "X-RVOL"],
  ];

  let doc = null;
  let side = "up";
  let threshold = "2";
  let ruleKind = "pairs";
  let chartFeature = null;
  const tables = {
    ev: { offset: 0, sort: "date", dir: "desc", path: "/events" },
    rec: { offset: 0, sort: "date", dir: "desc", path: "/rows" },
    sk: { offset: 0 },
  };

  function apiBase() {
    const h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }
  function el(id) { return document.getElementById(id); }
  function fmt(value, digits) {
    if (value === null || value === undefined || value === "") return "—";
    const n = Number(value);
    if (!Number.isFinite(n)) return String(value).replaceAll("_", " ");
    return n.toLocaleString("en-IN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
  }
  function pct(value) {
    if (value === null || value === undefined || value === "") return "—";
    return (Number(value) * 100).toFixed(1) + "%";
  }
  function lift(value) {
    if (value === null || value === undefined || value === "") return "—";
    return Number(value).toFixed(2) + "×";
  }
  function fmtDate(iso) {
    if (!iso) return "—";
    const [y, m, d] = String(iso).split("-").map(Number);
    if (!y || !m || !d) return iso;
    return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString("en-GB", {
      day: "2-digit", month: "short", year: "numeric", timeZone: "UTC",
    });
  }
  function esc(value) {
    return String(value ?? "").replace(/[&<>"]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]));
  }

  function currentRank() {
    return (((doc || {}).rankings || {})[side] || {})[threshold] || [];
  }
  function currentBuckets() {
    return (((doc || {}).buckets || {})[side] || {})[threshold] || {};
  }
  function currentRules() {
    return ((((doc || {}).rules || {})[side] || {})[threshold] || {})[ruleKind] || [];
  }
  function currentScreen() {
    if (side === "either") return [];
    return (((doc || {}).screens || {})[side] || {})[threshold] || [];
  }

  function render() {
    const header = doc.header || {};
    const params = doc.params || {};
    el("bmRule").textContent = doc.contract_rule || el("bmRule").textContent;
    const skipped = header.skipped || 0;
    el("bmMeta").textContent =
      `${header.stock_days || 0} stock-days · ${header.trading_days || 0} trading days · ` +
      `${header.symbols || 0} symbols · ${skipped} skipped`;
    const trainEnd = params.train_end || header.train_end || "";
    el("bmParams").innerHTML = [
      `${fmtDate(params.date_from)} – ${fmtDate(params.date_to)}`,
      `Reference ${params.reference || "09:45"}`,
      `Thresholds 2, 3 and 4 ATR`,
      `Train through ${fmtDate(trainEnd)} (${header.train_rows || 0} rows)`,
      `Test after that (${header.test_rows || 0} rows)`,
      `Rule needs at least ${params.min_rule_n || 30} stock-days`,
      `Open interest is the 09:45 print when the candle has it, otherwise the previous session, and the row says which`,
      `No historical results calendar, so that flag is omitted`,
    ].map((item) => `<span>${esc(item)}</span>`).join("");
    renderRates();
    renderRank();
    renderCharts();
    renderRules();
    renderScreen();
    fillFeatures();
  }

  function renderRates() {
    const all = (doc.base_rates || {}).all || {};
    const order = [
      ["big_up", "Big up"],
      ["big_down", "Big down"],
      ["clean_up", "Clean up"],
      ["clean_down", "Clean down"],
      ["big_either", "Either"],
    ];
    el("bmRates").innerHTML = ["2", "3", "4"].map((tag) => {
      const block = all[tag] || {};
      const cards = order.map(([key, label]) => {
        const item = block[key] || {};
        const warn = item.low_sample ? `<span class="bm-warn">low sample</span>` : "";
        return `<div class="bm-card"><span>${label} ${tag} ATR${warn}</span><strong>${pct(item.rate)}</strong><span>${item.positives || 0} of ${item.n || 0} · ${pct(item.ci_lo)}–${pct(item.ci_hi)}</span></div>`;
      }).join("");
      const cat = ["up", "down", "either"].map((name) => {
        const share = block["catalyst_share_" + name];
        return `${name} ${share == null ? "—" : pct(share)}`;
      }).join(" · ");
      return `<h3>${tag} ATR</h3><div class="bm-cards">${cards}</div><p class="bm-note">Catalyst share — ${esc(cat)}</p>`;
    }).join("");
  }

  function renderRank() {
    const rows = currentRank();
    el("bmRankBody").innerHTML = rows.length ? rows.map((row) => `
      <tr data-feature="${esc(row.feature)}" class="${chartFeature === row.feature ? "bm-hold" : ""}">
        <td>${esc(row.label)}</td>
        <td>${esc(row.group)}</td>
        <td>${esc(row.top_bucket)}</td>
        <td class="num">${row.top_n ?? "—"}</td>
        <td class="num">${pct(row.top_rate)}</td>
        <td class="num">${lift(row.top_lift)}</td>
        <td class="num">${pct(row.ci_lo)}–${pct(row.ci_hi)}</td>
        <td class="num">${fmt(row.auc, 2)}</td>
        <td class="num">${row.test_n ?? "—"}</td>
        <td class="num">${lift(row.test_lift)}</td>
      </tr>`).join("") : `<tr><td colspan="10" class="bm-empty">No feature had enough stock-days in the training window.</td></tr>`;
  }

  function renderCharts() {
    const rank = currentRank();
    const buckets = currentBuckets();
    const names = [];
    if (chartFeature) names.push(chartFeature);
    rank.forEach((row) => { if (names.length < 6 && !names.includes(row.feature)) names.push(row.feature); });
    const base = ((((doc.base_rates || {}).train || {})[threshold] || {})["big_" + side] || {}).rate || 0;
    el("bmCharts").innerHTML = names.map((name) => {
      const meta = rank.find((row) => row.feature === name) || { label: name };
      const bars = buckets[name] || [];
      const maxRate = Math.max(base, ...bars.map((bar) => Number(bar.rate) || 0), 0.01);
      const body = bars.map((bar) => {
        const width = Math.max(2, (Number(bar.rate) || 0) / maxRate * 100);
        const baseLeft = Math.min(100, base / maxRate * 100);
        return `<div class="bm-bar">
          <span class="bm-bar-name">${esc(bar.bucket)}</span>
          <div class="bm-bar-track" title="Base rate ${pct(base)}">
            <div class="bm-bar-fill" style="width:${width}%"></div>
            <div class="bm-bar-base" style="left:${baseLeft}%"></div>
          </div>
          <span class="bm-bar-meta">${pct(bar.rate)} · ${lift(bar.lift)} · n=${bar.n} · ${pct(bar.ci_lo)}–${pct(bar.ci_hi)}</span>
        </div>`;
      }).join("");
      return `<div class="bm-chart-card"><h3>${esc(meta.label || name)}</h3>${body || "<p class='bm-note'>No buckets.</p>"}</div>`;
    }).join("");
  }

  function renderRules() {
    const rows = currentRules();
    const held = rows.filter((row) => row.holds).length;
    el("bmRuleNote").textContent = held
      ? `${held} of ${rows.length} rules still lift on the later months. Those rows are highlighted.`
      : "None of these rules still lift on the later months. The table is what the training window liked.";
    el("bmRules").innerHTML = rows.length ? rows.map((row) => `
      <tr class="${row.holds ? "bm-hold" : ""}">
        <td>${esc(row.text)}</td>
        <td class="num">${row.train_n}</td>
        <td class="num">${pct(row.train_rate)}</td>
        <td class="num">${lift(row.train_lift)}</td>
        <td class="num">${pct(row.train_clean)}</td>
        <td class="num">${row.test_n}</td>
        <td class="num">${pct(row.test_rate)}</td>
        <td class="num">${lift(row.test_lift)}</td>
        <td class="num">${pct(row.test_clean)}</td>
      </tr>`).join("") : `<tr><td colspan="9" class="bm-empty">No rule cleared 30 training stock-days.</td></tr>`;
  }

  function renderScreen() {
    const rows = currentScreen();
    const dates = [...new Set(rows.map((row) => row.date))].sort();
    const select = el("bmScreenDate");
    const previous = select.value;
    select.innerHTML = dates.map((day) => `<option value="${esc(day)}">${fmtDate(day)}</option>`).join("");
    const pick = dates.includes(previous) ? previous : (dates.filter((day) => day > ((doc.params || {}).train_end || "")).pop() || dates[dates.length - 1] || "");
    select.value = pick;
    const confirmed = rows.length ? rows[0].confirmed : false;
    el("bmScreenNote").textContent = side === "either"
      ? "The daily screen uses the up and down rules. Switch to Up or Down."
      : confirmed
        ? "Stocks passing a rule whose lift still shows up in the later months, with what the session actually did."
        : "No rule held up later, so this list is the training favourites. Treat it as unconfirmed.";
    const shown = rows.filter((row) => row.date === pick);
    el("bmScreen").innerHTML = shown.length ? shown.map((row) => `
      <tr>
        <td>${esc(row.symbol)}</td>
        <td>${esc(row.contract)}</td>
        <td>${esc((row.rules || []).join(" · "))}</td>
        <td class="num">${fmt(row.max_up_atr, 2)}</td>
        <td class="num">${fmt(row.max_down_atr, 2)}</td>
        <td class="num">${fmt(row.close_move_atr, 2)}</td>
      </tr>`).join("") : `<tr><td colspan="6" class="bm-empty">Nothing passed on this date.</td></tr>`;
  }

  function fillFeatures() {
    const html = `<option value="">Any</option>` + FEATURES.map(([key, label]) => `<option value="${key}">${esc(label)}</option>`).join("");
    ["evFeature", "recFeature"].forEach((id) => {
      if (!el(id).options.length) el(id).innerHTML = html;
    });
  }

  function cell(row, key) {
    if (key === "date") return fmtDate(row[key]);
    if (["nr7", "extreme_gap", "extreme_rvol"].includes(key)) {
      if (row[key] === null || row[key] === undefined || row[key] === "") return "—";
      return Number(row[key]) === 1 ? "Yes" : "No";
    }
    if (["symbol", "contract", "oi_state", "oi_basis"].includes(key)) return esc(row[key] || "—").replaceAll("_", " ");
    return fmt(row[key], 2);
  }

  function tableHtml(rows) {
    const head = `<tr>${COLS.map(([key, label]) => `<th data-sort="${key}">${label}</th>`).join("")}</tr>`;
    const body = rows.length
      ? rows.map((row) => `<tr>${COLS.map(([key]) => `<td class="${["date", "symbol", "contract", "oi_state", "oi_basis"].includes(key) ? "" : "num"}">${cell(row, key)}</td>`).join("")}</tr>`).join("")
      : `<tr><td colspan="${COLS.length}" class="bm-empty">No rows.</td></tr>`;
    return `<table class="bm-table"><thead>${head}</thead><tbody>${body}</tbody></table>`;
  }

  async function loadTable(which) {
    const state = tables[which];
    const token = (state.seq = (state.seq || 0) + 1);
    const prefix = which === "ev" ? "ev" : "rec";
    const params = new URLSearchParams({
      offset: String(state.offset),
      limit: "50",
      symbol: el(prefix + "Symbol").value.trim(),
      date: el(prefix + "Date").value.trim(),
      feature: el(prefix + "Feature").value,
      feature_value: el(prefix + "Value").value.trim(),
      sort: state.sort,
      direction: state.dir,
    });
    if (which === "ev") {
      params.set("threshold", threshold);
      params.set("side", side);
    }
    const response = await fetch(`${apiBase()}/api/big-move${state.path}?${params}`);
    const payload = await response.json();
    if (token !== state.seq) return;
    const wrap = el(which === "ev" ? "evWrap" : "recWrap");
    wrap.innerHTML = tableHtml(payload.rows || []);
    const hint = el(which === "ev" ? "evHint" : "recHint");
    const start = payload.total ? state.offset + 1 : 0;
    const end = Math.min(payload.total || 0, state.offset + (payload.rows || []).length);
    hint.textContent = `${payload.total || 0} rows · showing ${start}–${end}`;
    const pager = el(which === "ev" ? "evPager" : "recPager");
    pager.innerHTML = "";
    const prev = document.createElement("button");
    prev.type = "button";
    prev.textContent = "Previous";
    prev.disabled = state.offset <= 0;
    prev.onclick = () => { state.offset = Math.max(0, state.offset - 50); loadTable(which); };
    const next = document.createElement("button");
    next.type = "button";
    next.textContent = "Next";
    next.disabled = state.offset + 50 >= (payload.total || 0);
    next.onclick = () => { state.offset += 50; loadTable(which); };
    pager.append(prev, next);
    wrap.querySelectorAll("th[data-sort]").forEach((th) => {
      th.onclick = () => {
        const key = th.getAttribute("data-sort");
        state.dir = state.sort === key && state.dir === "desc" ? "asc" : "desc";
        state.sort = key;
        state.offset = 0;
        loadTable(which);
      };
    });
  }

  async function loadSkips() {
    const params = new URLSearchParams({ offset: String(tables.sk.offset), limit: "100", symbol: el("skSymbol").value.trim() });
    const response = await fetch(`${apiBase()}/api/big-move/skips?${params}`);
    const payload = await response.json();
    el("bmSkipBody").innerHTML = (payload.rows || []).map((row) =>
      `<tr><td>${fmtDate(row.date)}</td><td>${esc(row.symbol)}</td><td>${esc(row.contract)}</td><td>${esc(row.reason)}</td></tr>`
    ).join("") || `<tr><td colspan="4" class="bm-empty">Nothing skipped.</td></tr>`;
    const reasons = (doc && doc.skip_reasons) || [];
    el("bmSkipReasons").innerHTML = reasons.map((row) => `<p class="bm-note">${esc(row.reason)} — ${row.count}</p>`).join("");
    const totalSkipped = (doc.header || {}).skipped || payload.total || 0;
    el("bmSkipSummary").textContent = `${totalSkipped} stock-day${totalSkipped === 1 ? "" : "s"} skipped`;
    const pager = el("skPager");
    pager.innerHTML = "";
    const prev = document.createElement("button");
    prev.type = "button";
    prev.textContent = "Previous";
    prev.disabled = tables.sk.offset <= 0;
    prev.onclick = () => { tables.sk.offset = Math.max(0, tables.sk.offset - 100); loadSkips(); };
    const next = document.createElement("button");
    next.type = "button";
    next.textContent = "Next";
    next.disabled = tables.sk.offset + 100 >= (payload.total || 0);
    next.onclick = () => { tables.sk.offset += 100; loadSkips(); };
    pager.append(prev, next);
  }

  function wire() {
    el("bmSide").onclick = (event) => {
      const button = event.target.closest("button");
      if (!button) return;
      side = button.dataset.side;
      el("bmSide").querySelectorAll("button").forEach((item) => item.classList.toggle("active", item === button));
      tables.ev.offset = 0;
      render();
      loadTable("ev");
    };
    el("bmThreshold").onclick = (event) => {
      const button = event.target.closest("button");
      if (!button) return;
      threshold = button.dataset.threshold;
      el("bmThreshold").querySelectorAll("button").forEach((item) => item.classList.toggle("active", item === button));
      tables.ev.offset = 0;
      render();
      loadTable("ev");
    };
    el("bmRuleKind").onclick = (event) => {
      const button = event.target.closest("button");
      if (!button) return;
      ruleKind = button.dataset.kind;
      el("bmRuleKind").querySelectorAll("button").forEach((item) => item.classList.toggle("active", item === button));
      renderRules();
    };
    el("bmRank").onclick = (event) => {
      const row = event.target.closest("tr[data-feature]");
      if (!row) return;
      chartFeature = row.dataset.feature;
      renderRank();
      renderCharts();
    };
    el("bmScreenDate").onchange = renderScreen;
    ["ev", "rec"].forEach((which) => {
      const prefix = which === "ev" ? "ev" : "rec";
      ["Symbol", "Date", "Value"].forEach((suffix) => {
        el(prefix + suffix).oninput = () => {
          tables[which].offset = 0;
          clearTimeout(tables[which].timer);
          tables[which].timer = setTimeout(() => loadTable(which), 250);
        };
      });
      el(prefix + "Feature").onchange = () => { tables[which].offset = 0; loadTable(which); };
    });
    el("skSymbol").oninput = () => {
      tables.sk.offset = 0;
      clearTimeout(tables.sk.timer);
      tables.sk.timer = setTimeout(loadSkips, 250);
    };
  }

  async function boot() {
    wire();
    fillFeatures();
    try {
      const response = await fetch(`${apiBase()}/api/big-move`);
      doc = await response.json();
      if (!doc || doc.ok === false) {
        el("bmStatus").hidden = false;
        el("bmStatus").textContent = (doc && doc.detail) || "The study is not available yet.";
        el("bmMeta").textContent = "Not ready";
        return;
      }
      render();
      await loadTable("ev");
      await loadTable("rec");
      await loadSkips();
    } catch (error) {
      el("bmStatus").hidden = false;
      el("bmStatus").textContent = "Could not load the study.";
      el("bmMeta").textContent = "Not ready";
    }
  }

  boot();
})();
