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
    if (side === "BEAR CALL") return '<span class="so-chip so-chip-bear">' + esc(side) + "</span>";
    if (side === "BULL PUT") return '<span class="so-chip so-chip-bull">' + esc(side) + "</span>";
    return esc(side || "—");
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  let workspace = { radar: [], active: [], executed: [] };
  let activeTab = "radar";
  let tradeRow = null;
  let sortState = { tab: "", key: "", dir: "asc" };

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
      { key: "spread", label: "Spread", type: "str", sort: (r) => [r.spread_sell, r.spread_buy].filter(Boolean).join(" ") },
      { key: "trade", label: "" },
    ],
    executed: [
      { key: "date", label: "Date traded", type: "date", sort: (r) => r.date_traded || r.armed_at },
      { key: "symbol", label: "Symbol", type: "str", sort: (r) => r.symbol },
      { key: "side", label: "Side", type: "str", sort: (r) => r.side },
      { key: "strikes", label: "Strikes", type: "num", sort: strikeSort },
      { key: "costs", label: "Costs", type: "num", sort: (r) => r.sell_cost },
      { key: "ltp", label: "LTP", type: "num", sort: (r) => r.sell_ltp },
      { key: "pnl", label: "P&amp;L", type: "num", sort: (r) => r.combined_pnl },
      { key: "hard_stop", label: "Hard stop", type: "num", sort: (r) => r.hard_stop },
      { key: "hard_stop_placed", label: "Hard stop placed", type: "bool", sort: (r) => r.hard_stop_placed },
      { key: "remarks", label: "Remarks", type: "str", sort: (r) => r.remarks },
    ],
  };

  function setTab(name) {
    activeTab = name;
    document.querySelectorAll(".bf-tab").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === name);
    });
    const note = document.getElementById("soExecutedNote");
    if (note) note.hidden = name !== "executed";
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
        <td class="so-spread">${spread}</td>
        <td><button type="button" class="button-41" role="button" data-trade="${r.id}"><span class="text">Trade</span></button></td>
      </tr>`;
    }).join("");
    return `<div class="so-table-wrap"><table class="so-table">
      <thead><tr>${headerHtml("active")}</tr></thead><tbody>${body}</tbody></table></div>`;
  }

  function renderExecuted() {
    const rows = sortRows("executed", workspace.executed || []);
    if (!rows.length) return '<p class="so-empty">No Executed symbols.</p>';
    const body = rows.map((r) => {
      const pnl = r.combined_pnl;
      const pnlCls = pnl == null ? "" : (Number(pnl) >= 0 ? "so-pnl-pos" : "so-pnl-neg");
      const checked = r.hard_stop_placed ? "checked" : "";
      const when = r.date_traded || r.armed_at || "—";
      return `
      <tr>
        <td>${esc(when)}</td>
        <td>${esc(r.symbol)}</td>
        <td>${sideChip(r.side)}</td>
        <td class="so-spread">Sell ${esc(r.user_sell_strike == null ? "—" : r.user_sell_strike)}<br>Buy ${esc(r.user_buy_strike == null ? "—" : r.user_buy_strike)}</td>
        <td class="so-spread">Sell ${num(r.sell_cost)}<br>Buy ${num(r.buy_cost)}</td>
        <td class="so-spread">Sell ${num(r.sell_ltp)}<br>Buy ${num(r.buy_ltp)}</td>
        <td class="${pnlCls}">${pnl == null ? "—" : num(pnl)}</td>
        <td>${num(r.hard_stop)}</td>
        <td><input type="checkbox" data-hs="${r.id}" ${checked} aria-label="Hard stop placed"></td>
        <td class="so-remarks">${esc(r.remarks || "")}</td>
      </tr>`;
    }).join("");
    return `<div class="so-table-wrap"><table class="so-table">
      <thead><tr>${headerHtml("executed")}</tr></thead><tbody>${body}</tbody></table></div>`;
  }

  function render() {
    const host = document.getElementById("soPanel");
    if (!host) return;
    if (activeTab === "active") host.innerHTML = renderActive();
    else if (activeTab === "executed") host.innerHTML = renderExecuted();
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
    document.getElementById("soPanel").addEventListener("click", (ev) => {
      const th = ev.target.closest("th[data-sort]");
      if (th) {
        toggleSort(th.dataset.sortTab, th.dataset.sort);
        return;
      }
      const btn = ev.target.closest("[data-trade]");
      if (btn) openTrade(btn.dataset.trade);
    });
    document.getElementById("soPanel").addEventListener("change", (ev) => {
      const box = ev.target.closest("[data-hs]");
      if (box) toggleHardStop(box.dataset.hs, box.checked);
    });
    document.getElementById("soTradeForm").addEventListener("submit", submitTrade);
    document.getElementById("soTradeCancel").addEventListener("click", closeTrade);
    load();
    setInterval(load, 60000);
  });
})();
