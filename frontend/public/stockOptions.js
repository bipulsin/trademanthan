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

  function sideClass(side) {
    if (side === "BEAR CALL") return "so-side-bear";
    if (side === "BULL PUT") return "so-side-bull";
    return "";
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

  function setTab(name) {
    activeTab = name;
    document.querySelectorAll(".so-tab").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === name);
    });
    const note = document.getElementById("soExecutedNote");
    if (note) note.hidden = name !== "executed";
    render();
  }

  function renderRadar() {
    const rows = workspace.radar || [];
    if (!rows.length) return '<p class="so-empty">No Radar symbols.</p>';
    const body = rows.map((r) => `
      <tr>
        <td>${esc(r.symbol)}</td>
        <td>${esc(r.trigger_at || "—")}</td>
        <td>${num(r.ema9)}</td>
        <td>${num(r.ema30)}</td>
        <td>${num(r.ema100)}</td>
        <td>${esc(r.status)}</td>
        <td class="${sideClass(r.side)}">${esc(r.side || "—")}</td>
      </tr>`).join("");
    return `<div class="so-table-wrap"><table class="so-table">
      <thead><tr>
        <th>Symbol</th><th>Trigger date-time</th><th>EMA9</th><th>EMA30</th><th>EMA100</th><th>Status</th><th>Side</th>
      </tr></thead><tbody>${body}</tbody></table></div>`;
  }

  function renderActive() {
    const rows = workspace.active || [];
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
        <td class="${sideClass(r.side)}">${esc(r.side || "—")}</td>
        <td class="so-spread">${spread}</td>
        <td><button type="button" class="so-btn" data-trade="${r.id}">Trade</button></td>
      </tr>`;
    }).join("");
    return `<div class="so-table-wrap"><table class="so-table">
      <thead><tr>
        <th>Symbol</th><th>Armed date-time</th><th>EMA9</th><th>EMA30</th><th>EMA100</th><th>Status</th><th>Side</th><th>Spread</th><th></th>
      </tr></thead><tbody>${body}</tbody></table></div>`;
  }

  function renderExecuted() {
    const rows = workspace.executed || [];
    if (!rows.length) return '<p class="so-empty">No Executed symbols.</p>';
    const body = rows.map((r) => {
      const pnl = r.combined_pnl;
      const pnlCls = pnl == null ? "" : (Number(pnl) >= 0 ? "so-pnl-pos" : "so-pnl-neg");
      const checked = r.hard_stop_placed ? "checked" : "";
      return `
      <tr>
        <td>${esc(r.date_traded || "—")}</td>
        <td>${esc(r.symbol)}</td>
        <td class="so-spread">Sell ${esc(r.user_sell_strike == null ? "—" : r.user_sell_strike)}<br>Buy ${esc(r.user_buy_strike == null ? "—" : r.user_buy_strike)}</td>
        <td class="so-spread">Sell ${num(r.sell_cost)}<br>Buy ${num(r.buy_cost)}</td>
        <td class="so-spread">Sell ${num(r.sell_ltp)}<br>Buy ${num(r.buy_ltp)}</td>
        <td class="${pnlCls}">${pnl == null ? "—" : num(pnl)}</td>
        <td>${num(r.hard_stop)}</td>
        <td><input type="checkbox" data-hs="${r.id}" ${checked} aria-label="Hard stop placed"></td>
        <td>${esc(r.remarks || "")}</td>
      </tr>`;
    }).join("");
    return `<div class="so-table-wrap"><table class="so-table">
      <thead><tr>
        <th>Date traded</th><th>Symbol</th><th>Strikes</th><th>Costs</th>
        <th>LTP</th><th>P&amp;L</th><th>Hard stop</th><th>Hard stop placed</th><th>Remarks</th>
      </tr></thead><tbody>${body}</tbody></table></div>`;
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
    document.querySelectorAll(".so-tab").forEach((btn) => {
      btn.addEventListener("click", () => setTab(btn.dataset.tab));
    });
    document.getElementById("soPanel").addEventListener("click", (ev) => {
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
