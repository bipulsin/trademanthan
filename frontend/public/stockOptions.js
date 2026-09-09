(function () {
  const API = (typeof trademanthanApiBase === "function")
    ? trademanthanApiBase()
    : window.location.origin;

  function authHeaders() {
    const t = localStorage.getItem("trademanthan_token") || "";
    return t ? { Authorization: "Bearer " + t, "Content-Type": "application/json" } : { "Content-Type": "application/json" };
  }

  function num(v, digits) {
    if (v == null || v === "") return "—";
    const n = Number(v);
    if (!Number.isFinite(n)) return "—";
    return n.toFixed(digits == null ? 2 : digits);
  }

  function sideChip(side) {
    const tips = SIDE_TIPS[side];
    if (!tips) return esc(side || "—");
    const cls = side === "BEAR CALL" ? "so-chip-bear" : "so-chip-bull";
    const items = tips.map((t) => "<li>" + esc(t) + "</li>").join("");
    return (
      '<span class="so-chip ' + cls + ' so-chip-tip" tabindex="0">' +
      esc(side) +
      '<span class="so-side-tip" role="tooltip"><ol class="so-side-tip-list">' +
      items +
      "</ol></span></span>"
    );
  }

  let floatTipChip = null;

  function ensureFloatTip() {
    let el = document.getElementById("soSideTipFloat");
    if (!el) {
      el = document.createElement("div");
      el.id = "soSideTipFloat";
      el.className = "so-side-tip-float";
      el.setAttribute("role", "tooltip");
      el.addEventListener("pointerleave", (ev) => {
        if (ev.relatedTarget && floatTipChip && floatTipChip.contains(ev.relatedTarget)) return;
        hideFloatTip();
      });
      document.body.appendChild(el);
    }
    return el;
  }

  function hideFloatTip() {
    floatTipChip = null;
    const el = document.getElementById("soSideTipFloat");
    if (!el) return;
    el.classList.remove("so-side-tip-visible", "so-side-tip-scroll");
    el.style.top = "";
    el.style.left = "";
    el.style.maxHeight = "";
    el.innerHTML = "";
  }

  function positionFloatTip(chip) {
    const src = chip && chip.querySelector(".so-side-tip");
    if (!src) return;
    const el = ensureFloatTip();
    floatTipChip = chip;
    el.innerHTML = src.innerHTML;
    el.classList.add("so-side-tip-visible");
    el.classList.remove("so-side-tip-scroll");
    el.style.maxHeight = "";
    el.style.top = "0px";
    el.style.left = "0px";

    const margin = 12;
    const gap = 8;
    const chipRect = chip.getBoundingClientRect();
    let tipRect = el.getBoundingClientRect();
    const spaceAbove = chipRect.top - margin;
    const spaceBelow = window.innerHeight - chipRect.bottom - margin;
    const placeBelow = spaceAbove < tipRect.height + gap && spaceBelow > spaceAbove;

    let top = placeBelow
      ? chipRect.bottom + gap
      : chipRect.top - tipRect.height - gap;

    const maxAvail = Math.max(spaceAbove, spaceBelow, window.innerHeight - 2 * margin);
    if (tipRect.height > maxAvail) {
      el.style.maxHeight = Math.floor(maxAvail) + "px";
      el.classList.add("so-side-tip-scroll");
      tipRect = el.getBoundingClientRect();
      top = placeBelow ? chipRect.bottom + gap : Math.max(margin, chipRect.top - tipRect.height - gap);
    }

    top = Math.min(top, window.innerHeight - margin - tipRect.height);
    top = Math.max(margin, top);

    let left = chipRect.left;
    left = Math.min(left, window.innerWidth - tipRect.width - margin);
    left = Math.max(margin, left);

    el.style.top = Math.round(top) + "px";
    el.style.left = Math.round(left) + "px";
  }

  function showFloatTip(chip) {
    if (!chip) return;
    positionFloatTip(chip);
  }

  const SIDE_TIPS = {
    "BEAR CALL": [
      "(when William%R is OverBought Above -5 / -1)",
      "WHEN - EMA9 crosses below both EMA30 & EMA100 (EMA30 can be above or below EMA100)",
      "When the LOW of the candle breaches (10% rule) then create the Bear CALL Spread. PROVIDED the candle size is less than 1.5%",
      "Higher Delta SELL - Lower Delta BUY - example",
      "Sell ~28 Δ , Buy ~18 Δ (OR conservative: Sell 15 Δ, BUY 2 Δ)",
      "ALWAYS - First Buy the Option , then SELL, to get the margin benefit",
      "The Options selected should be of the current monthly expiry (but if the date to expiry is less than 3 days, look for next month expiry provided it has liquidity)",
      "TP - 3% ROI on Margin Requirement.",
      "SL - EMA9 above EMA30 and EMA30 above EMA100",
      "Extreme SL - 3x of the Sell PE price, placed as GTT/Forever order",
    ],
    "BULL PUT": [
      "when William%R is Oversold below -95 / -99",
      "WHEN - EMA9 crosses above both EMA30 & EMA100 (EMA30 can be above or below EMA100)",
      "When the High of the candle breaches (10% rule) then create the BULL PUT Spread. PROVIDED the candle size is less than 1.5%",
      "Higher Delta SELL - Lower Delta BUY - example",
      "Sell ~28 Δ , Buy ~18 Δ (OR conservative: Sell 15 Δ, BUY 2 Δ)",
      "ALWAYS - First Buy the Option , then SELL, to get the margin benefit",
      "The Options selected should be of the current monthly expiry (but if the date to expiry is less than 3 days, look for next month expiry provided it has liquidity)",
      "TP - 3% ROI on Margin Requirement.",
      "SL - EMA 9 below EMA30 and EMA30 below EMA100",
      "Extreme SL - 3x of the Sell PE price, placed as GTT/Forever order",
    ],
  };

  function optionSuffix(side) {
    const s = String(side || "").toUpperCase();
    if (s.indexOf("CALL") >= 0) return "CE";
    if (s.indexOf("PUT") >= 0) return "PE";
    return "";
  }

  function setStrikeLabels(side, buyLblId, sellLblId) {
    const xx = optionSuffix(side) || "";
    const buyLbl = document.getElementById(buyLblId);
    const sellLbl = document.getElementById(sellLblId);
    if (buyLbl) buyLbl.textContent = xx ? "Buy Strike " + xx : "Buy Strike";
    if (sellLbl) sellLbl.textContent = xx ? "Sell Strike " + xx : "Sell Strike";
  }

  function strikeLine(label, strike, side) {
    if (strike == null || strike === "") return label + " —";
    const suf = optionSuffix(side);
    return label + " " + esc(strike) + (suf ? " " + suf : "");
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  let workspace = { radar: [], active: [], executed: [], completed: [] };
  let activeTab = "radar";
  let tradeRow = null;
  let exitRow = null;
  let exitQuoteGen = 0;
  let exitPxDirty = { sell: false, buy: false };
  let sortState = { tab: "", key: "", dir: "asc" };

  function todayIso() {
    const d = new Date();
    const local = new Date(d.getTime() - d.getTimezoneOffset() * 60000);
    return local.toISOString().slice(0, 10);
  }

  function dateOnly(v) {
    if (!v) return "";
    return String(v).slice(0, 10);
  }

  function emptyVal(v) {
    return v == null || v === "";
  }

  function cmpVals(a, b, type) {
    if (emptyVal(a) && emptyVal(b)) return 0;
    if (emptyVal(a)) return 1;
    if (emptyVal(b)) return -1;
    if (type === "num") return Number(a) - Number(b);
    if (type === "bool") return (a ? 1 : 0) - (b ? 1 : 0);
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
  }

  function sortRows(tab, rows) {
    if (sortState.tab !== tab || !sortState.key) return rows.slice();
    const col = (COLUMNS[tab] || []).find((c) => c.key === sortState.key);
    if (!col || !col.sort) return rows.slice();
    const dir = sortState.dir === "desc" ? -1 : 1;
    return rows.slice().sort((ra, rb) => dir * cmpVals(col.sort(ra), col.sort(rb), col.type));
  }

  function toggleSort(tab, key) {
    if (sortState.tab === tab && sortState.key === key) {
      sortState.dir = sortState.dir === "asc" ? "desc" : "asc";
    } else {
      sortState = { tab: tab, key: key, dir: "asc" };
    }
    render();
  }

  function headerHtml(tab) {
    return (COLUMNS[tab] || []).map((c) => {
      if (!c.sort) return "<th>" + c.label + "</th>";
      const on = sortState.tab === tab && sortState.key === c.key;
      const mark = on ? (sortState.dir === "asc" ? " ▲" : " ▼") : "";
      return '<th class="so-sortable" data-sort="' + esc(c.key) + '" data-sort-tab="' + tab + '">' +
        c.label + '<span class="so-sort-mark">' + mark + "</span></th>";
    }).join("");
  }

  function strikeSort(r) {
    const sell = r.user_sell_strike != null ? r.user_sell_strike : r.sell_strike;
    const buy = r.user_buy_strike != null ? r.user_buy_strike : r.buy_strike;
    if (sell == null && buy == null) return null;
    return Number(sell != null ? sell : buy);
  }

  const COLUMNS = {
    radar: [
      { key: "symbol", label: "Symbol", type: "str", sort: (r) => r.symbol },
      { key: "trigger_at", label: "Trigger date-time", type: "date", sort: (r) => r.trigger_at },
      { key: "ema9", label: "EMA9", type: "num", sort: (r) => r.ema9 },
      { key: "ema30", label: "EMA30", type: "num", sort: (r) => r.ema30 },
      { key: "ema100", label: "EMA100", type: "num", sort: (r) => r.ema100 },
      { key: "status", label: "Status", type: "str", sort: (r) => r.status },
      { key: "side", label: "Side", type: "str", sort: (r) => r.side },
    ],
    active: [
      { key: "symbol", label: "Symbol", type: "str", sort: (r) => r.symbol },
      { key: "armed_at", label: "Armed date-time", type: "date", sort: (r) => r.armed_at },
      { key: "ema9", label: "EMA9", type: "num", sort: (r) => r.ema9 },
      { key: "ema30", label: "EMA30", type: "num", sort: (r) => r.ema30 },
      { key: "ema100", label: "EMA100", type: "num", sort: (r) => r.ema100 },
      { key: "status", label: "Status", type: "str", sort: (r) => r.status },
      { key: "side", label: "Side", type: "str", sort: (r) => r.side },
      { key: "contract", label: "Contract", type: "str", sort: (r) => r.contract_mmm_yyyy },
      { key: "spread", label: "Spread", type: "str", sort: (r) => [r.spread_sell, r.spread_buy].filter(Boolean).join(" ") },
      { key: "trade", label: "" },
    ],
    executed: [
      { key: "date", label: "Date traded", type: "date", sort: (r) => r.date_traded || r.armed_at },
      { key: "symbol", label: "Symbol", type: "str", sort: (r) => r.symbol },
      { key: "side", label: "Side", type: "str", sort: (r) => r.side },
      { key: "contract", label: "Contract", type: "str", sort: (r) => r.contract_mmm_yyyy },
      { key: "strikes", label: "Strikes", type: "num", sort: strikeSort },
      { key: "costs", label: "Costs", type: "num", sort: (r) => r.sell_cost },
      { key: "ltp", label: "LTP", type: "num", sort: (r) => r.sell_ltp },
      { key: "pnl", label: "P&amp;L ₹", type: "num", sort: (r) => (r.combined_pnl_inr != null ? r.combined_pnl_inr : r.combined_pnl) },
      { key: "hard_stop", label: "Hard stop", type: "num", sort: (r) => r.hard_stop },
      { key: "exit", label: "" },
    ],
    report: [
      { key: "date", label: "Entry date", type: "date", sort: (r) => r.date_traded || r.armed_at },
      { key: "symbol", label: "Symbol", type: "str", sort: (r) => r.symbol },
      { key: "side", label: "Side", type: "str", sort: (r) => r.side },
      { key: "contract", label: "Contract", type: "str", sort: (r) => r.contract_mmm_yyyy },
      { key: "strikes", label: "Strikes", type: "num", sort: strikeSort },
      { key: "costs", label: "Entry costs", type: "num", sort: (r) => r.sell_cost },
      { key: "exit_px", label: "Exit prices", type: "num", sort: (r) => r.sell_exit_price },
      { key: "exit_date", label: "Exit date", type: "date", sort: (r) => r.exit_date },
      { key: "pnl", label: "P&amp;L ₹", type: "num", sort: (r) => (r.combined_pnl_inr != null ? r.combined_pnl_inr : r.combined_pnl) },
    ],
  };

  function setTab(name) {
    activeTab = name;
    document.querySelectorAll(".bf-tab").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === name);
    });
    const note = document.getElementById("soExecutedNote");
    if (note) note.hidden = name !== "executed";
    const reportNote = document.getElementById("soReportNote");
    if (reportNote) reportNote.hidden = name !== "report";
    render();
  }

  function renderRadar() {
    const rows = sortRows("radar", workspace.radar || []);
    if (!rows.length) return '<p class="so-empty">No Radar symbols.</p>';
    const body = rows.map((r) => `
      <tr>
        <td>${esc(r.symbol)}</td>
        <td>${esc(r.trigger_at || "—")}</td>
        <td>${num(r.ema9)}</td>
        <td>${num(r.ema30)}</td>
        <td>${num(r.ema100)}</td>
        <td>${esc(r.status)}</td>
        <td>${sideChip(r.side)}</td>
      </tr>`).join("");
    return `<div class="so-table-wrap"><table class="so-table">
      <thead><tr>${headerHtml("radar")}</tr></thead><tbody>${body}</tbody></table></div>`;
  }

  function renderActive() {
    const rows = sortRows("active", workspace.active || []);
    if (!rows.length) return '<p class="so-empty">No Active symbols.</p>';
    const body = rows.map((r) => {
      const spread = [r.spread_sell, r.spread_buy].filter(Boolean).map(esc).join("<br>") || "—";
      return `
      <tr>
        <td>${esc(r.symbol)}</td>
        <td>${esc(r.armed_at || "—")}</td>
        <td>${num(r.ema9)}</td>
        <td>${num(r.ema30)}</td>
        <td>${num(r.ema100)}</td>
        <td>${esc(r.status)}</td>
        <td>${sideChip(r.side)}</td>
        <td>${esc(r.contract_mmm_yyyy || "—")}</td>
        <td class="so-spread">${spread}</td>
        <td><button type="button" class="button-41" role="button" data-trade="${r.id}"><span class="text">Trade</span></button></td>
      </tr>`;
    }).join("");
    return `<div class="so-table-wrap"><table class="so-table">
      <thead><tr>${headerHtml("active")}</tr></thead><tbody>${body}</tbody></table></div>`;
  }

  function pnlDisplay(r) {
    const lot = Number(r.lot_size);
    const inr = r.combined_pnl_inr;
    if (inr != null && inr !== "" && Number.isFinite(lot) && lot > 0) {
      const n = Number(inr);
      return {
        cls: Number.isFinite(n) ? (n >= 0 ? "so-pnl-pos" : "so-pnl-neg") : "",
        html: "₹" + num(inr),
        title: "P&L ₹ = premium points × lot " + lot,
      };
    }
    if (r.combined_pnl != null && r.combined_pnl !== "") {
      return {
        cls: "so-pnl-pts",
        html: num(r.combined_pnl) + ' <span class="so-pnl-unit">pts</span>',
        title: "Lot size unknown — premium points, not ₹",
      };
    }
    return { cls: "", html: "—", title: "" };
  }

  function renderExecuted() {
    const rows = sortRows("executed", workspace.executed || []);
    if (!rows.length) return '<p class="so-empty">No Executed symbols.</p>';
    const body = rows.map((r) => {
      const pnl = pnlDisplay(r);
      const hsBlank = r.hard_stop == null || r.hard_stop === "";
      const checked = r.hard_stop_placed ? "checked" : "";
      const hsBox = hsBlank ? "" : `<input type="checkbox" data-hs="${r.id}" ${checked} aria-label="Hard stop placed">`;
      const when = r.date_traded || r.armed_at || "—";
      return `
      <tr>
        <td>${esc(when)}</td>
        <td>${esc(r.symbol)}</td>
        <td>${sideChip(r.side)}</td>
        <td>${esc(r.contract_mmm_yyyy || "—")}</td>
        <td class="so-tight">${strikeLine("Sell", r.user_sell_strike, r.side)}<br>${strikeLine("Buy", r.user_buy_strike, r.side)}</td>
        <td class="so-tight">Sell ${num(r.sell_cost)}<br>Buy ${num(r.buy_cost)}</td>
        <td class="so-tight">Sell ${num(r.sell_ltp)}<br>Buy ${num(r.buy_ltp)}</td>
        <td class="so-tight ${pnl.cls}" title="${esc(pnl.title)}">${pnl.html}</td>
        <td class="so-hs"><span class="so-hs-cell">${num(r.hard_stop)}${hsBox}</span></td>
        <td class="so-exit-cell"><button type="button" class="so-exit-btn" data-exit="${r.id}">Exit</button></td>
      </tr>`;
    }).join("");
    return `<div class="so-table-wrap"><table class="so-table so-table-executed">
      <thead><tr>${headerHtml("executed")}</tr></thead><tbody>${body}</tbody></table></div>`;
  }

  function renderReport() {
    const rows = sortRows("report", workspace.completed || []);
    if (!rows.length) return '<p class="so-empty">No completed trades.</p>';
    const body = rows.map((r) => {
      const pnl = pnlDisplay(r);
      return `
      <tr>
        <td>${esc(r.date_traded || r.armed_at || "—")}</td>
        <td>${esc(r.symbol)}</td>
        <td>${sideChip(r.side)}</td>
        <td>${esc(r.contract_mmm_yyyy || "—")}</td>
        <td class="so-spread">${strikeLine("Sell", r.user_sell_strike, r.side)}<br>${strikeLine("Buy", r.user_buy_strike, r.side)}</td>
        <td class="so-spread">Sell ${num(r.sell_cost)}<br>Buy ${num(r.buy_cost)}</td>
        <td class="so-spread">Sell ${num(r.sell_exit_price)}<br>Buy ${num(r.buy_exit_price)}</td>
        <td>${esc(r.exit_date || "—")}</td>
        <td class="${pnl.cls}" title="${esc(pnl.title)}">${pnl.html}</td>
      </tr>`;
    }).join("");
    return `<div class="so-table-wrap"><table class="so-table">
      <thead><tr>${headerHtml("report")}</tr></thead><tbody>${body}</tbody></table></div>`;
  }

  function render() {
    hideFloatTip();
    const host = document.getElementById("soPanel");
    if (!host) return;
    if (activeTab === "active") host.innerHTML = renderActive();
    else if (activeTab === "executed") host.innerHTML = renderExecuted();
    else if (activeTab === "report") host.innerHTML = renderReport();
    else host.innerHTML = renderRadar();
  }

  async function load() {
    const banner = document.getElementById("soBanner");
    try {
      const res = await fetch(API + "/api/stock-options/workspace", { headers: authHeaders() });
      if (!res.ok) throw new Error("HTTP " + res.status);
      const data = await res.json();
      workspace = data;
      if (banner) banner.textContent = "";
      render();
    } catch (e) {
      if (banner) banner.textContent = "Could not load Stock Options workspace.";
    }
  }

  function openTrade(id) {
    tradeRow = (workspace.active || []).find((r) => Number(r.id) === Number(id));
    if (!tradeRow) return;
    document.getElementById("soTradeSymbol").textContent = tradeRow.symbol + " · " + (tradeRow.side || "");
    setStrikeLabels(tradeRow.side, "soBuyStrikeLbl", "soSellStrikeLbl");
    document.getElementById("soBuyStrike").value = tradeRow.buy_strike != null ? tradeRow.buy_strike : "";
    document.getElementById("soBuyCost").value = "";
    document.getElementById("soSellStrike").value = tradeRow.sell_strike != null ? tradeRow.sell_strike : "";
    document.getElementById("soSellCost").value = "";
    const today = new Date();
    const iso = today.toISOString().slice(0, 10);
    document.getElementById("soDateTraded").value = iso;
    document.getElementById("soTradeErr").textContent = "";
    document.getElementById("soTradeModal").hidden = false;
  }

  function closeTrade() {
    tradeRow = null;
    document.getElementById("soTradeModal").hidden = true;
  }

  async function submitTrade(ev) {
    ev.preventDefault();
    if (!tradeRow) return;
    const err = document.getElementById("soTradeErr");
    const body = {
      date_traded: document.getElementById("soDateTraded").value,
      buy_strike: Number(document.getElementById("soBuyStrike").value),
      buy_cost: Number(document.getElementById("soBuyCost").value),
      sell_strike: Number(document.getElementById("soSellStrike").value),
      sell_cost: Number(document.getElementById("soSellCost").value),
    };
    if (!body.date_traded || !(body.buy_strike > 0) || !(body.sell_strike > 0) || !Number.isFinite(body.buy_cost) || !Number.isFinite(body.sell_cost)) {
      err.textContent = "Enter date, both strikes, and both costs.";
      return;
    }
    try {
      const res = await fetch(API + "/api/stock-options/signals/" + tradeRow.id + "/submit", {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const t = await res.json().catch(() => ({}));
        throw new Error(t.detail || "Submit failed");
      }
      closeTrade();
      setTab("executed");
      await load();
    } catch (e) {
      err.textContent = e.message || "Submit failed";
    }
  }

  function setExitPx(id, value, dirtyKey) {
    if (exitPxDirty[dirtyKey]) return;
    const el = document.getElementById(id);
    if (!el || value == null || value === "") return;
    el.value = value;
  }

  function openExit(id) {
    exitRow = (workspace.executed || []).find((r) => Number(r.id) === Number(id));
    if (!exitRow) return;
    exitPxDirty = { sell: false, buy: false };
    const gen = ++exitQuoteGen;
    document.getElementById("soExitSymbol").textContent = exitRow.symbol + " · " + (exitRow.side || "");
    setStrikeLabels(exitRow.side, "soExitBuyStrikeLbl", "soExitSellStrikeLbl");
    document.getElementById("soExitEntryDate").value = dateOnly(exitRow.date_traded || exitRow.armed_at);
    document.getElementById("soExitBuyStrike").value = exitRow.user_buy_strike != null ? exitRow.user_buy_strike : "";
    document.getElementById("soExitBuyEntry").value = exitRow.buy_cost != null ? exitRow.buy_cost : "";
    document.getElementById("soExitSellStrike").value = exitRow.user_sell_strike != null ? exitRow.user_sell_strike : "";
    document.getElementById("soExitSellEntry").value = exitRow.sell_cost != null ? exitRow.sell_cost : "";
    document.getElementById("soExitDate").value = todayIso();
    document.getElementById("soExitSellPx").value = exitRow.sell_ltp != null ? exitRow.sell_ltp : "";
    document.getElementById("soExitBuyPx").value = exitRow.buy_ltp != null ? exitRow.buy_ltp : "";
    document.getElementById("soExitErr").textContent = "";
    document.getElementById("soExitModal").hidden = false;
    fetch(API + "/api/stock-options/signals/" + exitRow.id + "/exit-quote", { headers: authHeaders() })
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (!data || gen !== exitQuoteGen || !exitRow) return;
        setExitPx("soExitSellPx", data.sell_ltp, "sell");
        setExitPx("soExitBuyPx", data.buy_ltp, "buy");
      })
      .catch(() => {});
  }

  function closeExit() {
    exitRow = null;
    exitQuoteGen += 1;
    document.getElementById("soExitModal").hidden = true;
  }

  async function submitExit(ev) {
    ev.preventDefault();
    if (!exitRow) return;
    const err = document.getElementById("soExitErr");
    const body = {
      date_traded: document.getElementById("soExitEntryDate").value,
      buy_strike: Number(document.getElementById("soExitBuyStrike").value),
      buy_cost: Number(document.getElementById("soExitBuyEntry").value),
      sell_strike: Number(document.getElementById("soExitSellStrike").value),
      sell_cost: Number(document.getElementById("soExitSellEntry").value),
      exit_date: document.getElementById("soExitDate").value,
      sell_exit: Number(document.getElementById("soExitSellPx").value),
      buy_exit: Number(document.getElementById("soExitBuyPx").value),
    };
    if (!body.date_traded || !body.exit_date || !(body.buy_strike > 0) || !(body.sell_strike > 0)
      || !Number.isFinite(body.buy_cost) || !Number.isFinite(body.sell_cost)
      || !Number.isFinite(body.sell_exit) || !Number.isFinite(body.buy_exit)) {
      err.textContent = "Enter entry date, both strikes and entry prices, exit date, and both exit prices.";
      return;
    }
    try {
      const res = await fetch(API + "/api/stock-options/signals/" + exitRow.id + "/exit", {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const t = await res.json().catch(() => ({}));
        throw new Error(t.detail || "Submit failed");
      }
      closeExit();
      setTab("report");
      await load();
    } catch (e) {
      err.textContent = e.message || "Submit failed";
    }
  }

  async function toggleHardStop(id, placed) {
    try {
      await fetch(API + "/api/stock-options/signals/" + id + "/hard-stop", {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ placed: placed }),
      });
    } catch (e) {
      await load();
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll(".bf-tab").forEach((btn) => {
      btn.addEventListener("click", () => setTab(btn.dataset.tab));
    });
    const panel = document.getElementById("soPanel");
    panel.addEventListener("click", (ev) => {
      const th = ev.target.closest("th[data-sort]");
      if (th) {
        toggleSort(th.dataset.sortTab, th.dataset.sort);
        return;
      }
      const btn = ev.target.closest("[data-trade]");
      if (btn) openTrade(btn.dataset.trade);
      const exitBtn = ev.target.closest("[data-exit]");
      if (exitBtn) openExit(exitBtn.dataset.exit);
    });
    panel.addEventListener("change", (ev) => {
      const box = ev.target.closest("[data-hs]");
      if (box) toggleHardStop(box.dataset.hs, box.checked);
    });
    panel.addEventListener("pointerover", (ev) => {
      const chip = ev.target.closest(".so-chip-tip");
      if (!chip || !panel.contains(chip)) return;
      if (ev.relatedTarget && chip.contains(ev.relatedTarget)) return;
      showFloatTip(chip);
    });
    panel.addEventListener("pointerout", (ev) => {
      const chip = ev.target.closest(".so-chip-tip");
      if (!chip) return;
      if (ev.relatedTarget && chip.contains(ev.relatedTarget)) return;
      const floatEl = document.getElementById("soSideTipFloat");
      if (ev.relatedTarget && floatEl && floatEl.contains(ev.relatedTarget)) return;
      if (floatTipChip === chip) hideFloatTip();
    });
    panel.addEventListener("focusin", (ev) => {
      const chip = ev.target.closest(".so-chip-tip");
      if (chip && panel.contains(chip)) showFloatTip(chip);
    });
    panel.addEventListener("focusout", (ev) => {
      const chip = ev.target.closest(".so-chip-tip");
      if (!chip) return;
      if (ev.relatedTarget && chip.contains(ev.relatedTarget)) return;
      if (floatTipChip === chip) hideFloatTip();
    });
    window.addEventListener("scroll", () => {
      if (floatTipChip) positionFloatTip(floatTipChip);
    }, true);
    window.addEventListener("resize", () => {
      if (floatTipChip) positionFloatTip(floatTipChip);
    });
    document.getElementById("soTradeForm").addEventListener("submit", submitTrade);
    document.getElementById("soTradeCancel").addEventListener("click", closeTrade);
    document.getElementById("soExitForm").addEventListener("submit", submitExit);
    document.getElementById("soExitCancel").addEventListener("click", closeExit);
    document.getElementById("soExitSellPx").addEventListener("input", () => { exitPxDirty.sell = true; });
    document.getElementById("soExitBuyPx").addEventListener("input", () => { exitPxDirty.buy = true; });
    load();
    setInterval(load, 60000);
    loadIndiaVix();
    setInterval(loadIndiaVix, VIX_POLL_MS);
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") loadIndiaVix();
    });
  });

  const VIX_API = "/scan/market-sentiment-dials";
  const VIX_POLL_MS = 5 * 60 * 1000;

  function applyVixDisplay(vix) {
    const valEl = document.getElementById("soVixValue");
    const warnEl = document.getElementById("soVixWarn");
    if (!valEl) return;
    valEl.classList.remove("so-vix-green", "so-vix-red", "so-vix-blink");
    if (vix == null || !Number.isFinite(vix)) {
      valEl.textContent = "—";
      if (warnEl) warnEl.hidden = true;
      return;
    }
    valEl.textContent = vix.toFixed(2);
    if (vix < 20.01) {
      valEl.classList.add("so-vix-green");
    } else {
      valEl.classList.add("so-vix-red");
      if (vix > 23) valEl.classList.add("so-vix-blink");
    }
    if (warnEl) warnEl.hidden = !(vix > 22.7);
  }

  async function loadIndiaVix() {
    try {
      const url = VIX_API + "?basis=today&_=" + Date.now();
      const res = await fetch(url, { cache: "no-store", credentials: "same-origin" });
      const data = await res.json();
      let vix = null;
      if (data && data.success && Array.isArray(data.indices)) {
        const row = data.indices.find((r) => String(r.id || "").toLowerCase() === "indiavix");
        if (row) {
          const raw = row.vix_value != null ? row.vix_value : row.last;
          const n = Number(raw);
          if (Number.isFinite(n)) vix = n;
        }
      }
      applyVixDisplay(vix);
    } catch (e) {
      console.warn("stockOptions india vix:", e);
      applyVixDisplay(null);
    }
  }
})();
