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

  function emaCell(r, key) {
    if (r && (r.ema_stale === true || r.ema_fetch_ok === false)) {
      return '<span class="so-ema-warn" title="EMA not updated this cycle">⚠</span>';
    }
    return num(r ? r[key] : null);
  }

  const ACTIVATED_SOUND = "option_activated.mp3";
  const ACTIVATED_MUTE_KEY = "so_option_activated_mute";
  const ACTIVATED_SEEN_KEY = "so_option_activated_seen";
  let optionActivatedAudio = null;
  let optionActivatedUnlocked = false;

  function isActivatedMuted() {
    try {
      return localStorage.getItem(ACTIVATED_MUTE_KEY) === "1";
    } catch (e) {
      return false;
    }
  }

  function setActivatedMuted(muted) {
    try {
      localStorage.setItem(ACTIVATED_MUTE_KEY, muted ? "1" : "0");
    } catch (e) { /* ignore */ }
    const cb = document.getElementById("soActivatedMute");
    if (cb) cb.checked = !!muted;
  }

  function loadSeenActivated() {
    try {
      const raw = sessionStorage.getItem(ACTIVATED_SEEN_KEY);
      const arr = raw ? JSON.parse(raw) : [];
      return new Set(Array.isArray(arr) ? arr : []);
    } catch (e) {
      return new Set();
    }
  }

  function saveSeenActivated(set) {
    try {
      sessionStorage.setItem(ACTIVATED_SEEN_KEY, JSON.stringify(Array.from(set)));
    } catch (e) { /* ignore */ }
  }

  function activatedKey(row) {
    return String(row.id) + "|" + String(row.armed_at || "");
  }

  function ensureActivatedAudio() {
    if (!optionActivatedAudio) {
      optionActivatedAudio = new Audio(ACTIVATED_SOUND);
      optionActivatedAudio.preload = "auto";
    }
    return optionActivatedAudio;
  }

  function setActivatedAckBanner(visible) {
    const ban = document.getElementById("soActivatedAck");
    if (!ban) return;
    ban.hidden = !visible;
  }

  function unlockActivatedAudio() {
    if (optionActivatedUnlocked) return Promise.resolve(true);
    try {
      const a = ensureActivatedAudio();
      a.muted = true;
      const p = a.play();
      if (p && typeof p.then === "function") {
        return p.then(function () {
          a.pause();
          a.currentTime = 0;
          a.muted = false;
          optionActivatedUnlocked = true;
          setActivatedAckBanner(false);
          return true;
        }).catch(function () {
          a.muted = false;
          setActivatedAckBanner(true);
          return false;
        });
      }
      a.muted = false;
      optionActivatedUnlocked = true;
      setActivatedAckBanner(false);
      return Promise.resolve(true);
    } catch (e) {
      setActivatedAckBanner(true);
      return Promise.resolve(false);
    }
  }

  function playActivatedSound() {
    if (isActivatedMuted()) return;
    unlockActivatedAudio().then(function (ok) {
      if (!ok || isActivatedMuted()) return;
      try {
        const a = ensureActivatedAudio();
        a.currentTime = 0;
        const p = a.play();
        if (p && typeof p.catch === "function") {
          p.catch(function () { setActivatedAckBanner(true); });
        }
      } catch (e) {
        setActivatedAckBanner(true);
      }
    });
  }

  function maybePlayActivatedSounds(data) {
    const rows = (data && data.active) || [];
    const seen = loadSeenActivated();
    let played = false;
    rows.forEach(function (r) {
      if (!r || r.id == null) return;
      const key = activatedKey(r);
      if (seen.has(key)) return;
      seen.add(key);
      if (!played) {
        playActivatedSound();
        played = true;
      }
    });
    // Seed seen set on first poll so pre-existing Active rows don't all chime.
    saveSeenActivated(seen);
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
  let exitModalMode = "exit"; // "exit" | "edit"
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

  const INDEX_PIN = { NIFTY: 0, BANKNIFTY: 1 };

  function isIndexRow(r) {
    if (r && (r.is_index || r.index_fut)) return true;
    const s = String((r && r.symbol) || "").trim().toUpperCase();
    return Object.prototype.hasOwnProperty.call(INDEX_PIN, s);
  }

  function pinIndexRows(rows) {
    const pinned = [];
    const rest = [];
    (rows || []).forEach((r) => {
      if (isIndexRow(r)) pinned.push(r);
      else rest.push(r);
    });
    pinned.sort((a, b) => {
      const ia = INDEX_PIN[String(a.symbol || "").toUpperCase()];
      const ib = INDEX_PIN[String(b.symbol || "").toUpperCase()];
      return (ia == null ? 99 : ia) - (ib == null ? 99 : ib);
    });
    return pinned.concat(rest);
  }

  function symbolCell(r) {
    const name = esc(r.symbol || "—");
    if (!isIndexRow(r)) return name;
    return (
      '<span class="so-index-sym" title="Index FUT">' +
      '<span class="so-index-arrow" aria-hidden="true"></span>' +
      name +
      "</span>"
    );
  }

  function sortRows(tab, rows) {
    let out;
    if (sortState.tab !== tab || !sortState.key) {
      out = rows.slice();
    } else {
      const col = (COLUMNS[tab] || []).find((c) => c.key === sortState.key);
      if (!col || !col.sort) out = rows.slice();
      else {
        const dir = sortState.dir === "desc" ? -1 : 1;
        out = rows.slice().sort((ra, rb) => dir * cmpVals(col.sort(ra), col.sort(rb), col.type));
      }
    }
    return pinIndexRows(out);
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
      { key: "edit", label: "" },
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
      <tr${isIndexRow(r) ? ' class="so-index-row"' : ""}>
        <td>${symbolCell(r)}</td>
        <td>${esc(r.trigger_at || "—")}</td>
        <td>${emaCell(r, "ema9")}</td>
        <td>${emaCell(r, "ema30")}</td>
        <td>${emaCell(r, "ema100")}</td>
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
      <tr${isIndexRow(r) ? ' class="so-index-row"' : ""}>
        <td>${symbolCell(r)}</td>
        <td>${esc(r.armed_at || "—")}</td>
        <td>${emaCell(r, "ema9")}</td>
        <td>${emaCell(r, "ema30")}</td>
        <td>${emaCell(r, "ema100")}</td>
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
      <tr${isIndexRow(r) ? ' class="so-index-row"' : ""}>
        <td>${esc(when)}</td>
        <td>${symbolCell(r)}</td>
        <td>${sideChip(r.side)}</td>
        <td>${esc(r.contract_mmm_yyyy || "—")}</td>
        <td class="so-tight">${strikeLine("Sell", r.user_sell_strike, r.side)}<br>${strikeLine("Buy", r.user_buy_strike, r.side)}</td>
        <td class="so-tight">Sell ${num(r.sell_cost)}<br>Buy ${num(r.buy_cost)}</td>
        <td class="so-tight">Sell ${num(r.sell_ltp)}<br>Buy ${num(r.buy_ltp)}</td>
        <td class="so-tight ${pnl.cls}" title="${esc(pnl.title)}">${pnl.html}</td>
        <td class="so-hs"><span class="so-hs-cell">${num(r.hard_stop)}${hsBox}</span></td>
        <td class="so-exit-cell"><span class="so-row-actions">
          <button type="button" class="so-edit-btn" data-edit="${r.id}" title="Edit trade" aria-label="Edit trade"><i class="fas fa-pencil-alt" aria-hidden="true"></i></button>
          <button type="button" class="so-exit-btn" data-exit="${r.id}">Exit</button>
        </span></td>
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
      <tr${isIndexRow(r) ? ' class="so-index-row"' : ""}>
        <td>${esc(r.date_traded || r.armed_at || "—")}</td>
        <td>${symbolCell(r)}</td>
        <td>${sideChip(r.side)}</td>
        <td>${esc(r.contract_mmm_yyyy || "—")}</td>
        <td class="so-tight">${strikeLine("Sell", r.user_sell_strike, r.side)}<br>${strikeLine("Buy", r.user_buy_strike, r.side)}</td>
        <td class="so-tight">Sell ${num(r.sell_cost)}<br>Buy ${num(r.buy_cost)}</td>
        <td class="so-exit-px">Sell ${num(r.sell_exit_price)}<br>Buy ${num(r.buy_exit_price)}</td>
        <td class="so-exit-date">${esc(r.exit_date || "—")}</td>
        <td class="so-tight ${pnl.cls}" title="${esc(pnl.title)}">${pnl.html}</td>
        <td class="so-exit-cell"><button type="button" class="so-edit-btn" data-edit="${r.id}" title="Edit trade" aria-label="Edit trade"><i class="fas fa-pencil-alt" aria-hidden="true"></i></button></td>
      </tr>`;
    }).join("");
    return `<div class="so-table-wrap"><table class="so-table so-table-report">
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

  let workspaceBootstrapped = false;

  async function load() {
    const banner = document.getElementById("soBanner");
    try {
      const res = await fetch(API + "/api/stock-options/workspace", { headers: authHeaders() });
      if (!res.ok) throw new Error("HTTP " + res.status);
      const data = await res.json();
      workspace = data;
      if (banner) banner.textContent = "";
      if (!workspaceBootstrapped) {
        // First poll: remember current Active arms without playing sound.
        const seen = loadSeenActivated();
        ((data && data.active) || []).forEach(function (r) {
          if (r && r.id != null) seen.add(activatedKey(r));
        });
        saveSeenActivated(seen);
        workspaceBootstrapped = true;
      } else {
        maybePlayActivatedSounds(data);
      }
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

  function findTradeRow(id) {
    const nid = Number(id);
    return (workspace.executed || []).find((r) => Number(r.id) === nid)
      || (workspace.completed || []).find((r) => Number(r.id) === nid)
      || null;
  }

  function setExitFieldRequired(required) {
    ["soExitDate", "soExitSellPx", "soExitBuyPx"].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.required = !!required;
    });
  }

  function setEntryFieldRequired(required) {
    ["soExitEntryDate", "soExitBuyStrike", "soExitBuyEntry", "soExitSellStrike", "soExitSellEntry"].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.required = !!required;
    });
  }

  function applyExitModalChrome(mode) {
    exitModalMode = mode;
    const title = document.getElementById("soExitTitle");
    const hint = document.getElementById("soExitHint");
    const submitBtn = document.getElementById("soExitSubmit");
    if (mode === "edit") {
      if (title) title.textContent = "Edit trade";
      if (hint) hint.textContent = "Partial updates OK — fill only the fields you want to change, then Save.";
      if (submitBtn) submitBtn.textContent = "Save";
      setEntryFieldRequired(false);
      setExitFieldRequired(false);
    } else {
      if (title) title.textContent = "Exit";
      if (hint) hint.textContent = "First Exit SELL leg then the BUY leg";
      if (submitBtn) submitBtn.textContent = "Submit";
      setEntryFieldRequired(true);
      setExitFieldRequired(true);
    }
  }

  function fillExitForm(row, opts) {
    const preferStoredExit = !!(opts && opts.preferStoredExit);
    const fetchQuote = !!(opts && opts.fetchQuote);
    const forEdit = !!(opts && opts.forEdit);
    exitPxDirty = { sell: false, buy: false };
    const gen = ++exitQuoteGen;
    document.getElementById("soExitSymbol").textContent = row.symbol + " · " + (row.side || "");
    setStrikeLabels(row.side, "soExitBuyStrikeLbl", "soExitSellStrikeLbl");
    document.getElementById("soExitEntryDate").value = dateOnly(row.date_traded || row.armed_at);
    document.getElementById("soExitBuyStrike").value = row.user_buy_strike != null ? row.user_buy_strike : "";
    document.getElementById("soExitBuyEntry").value = row.buy_cost != null ? row.buy_cost : "";
    document.getElementById("soExitSellStrike").value = row.user_sell_strike != null ? row.user_sell_strike : "";
    document.getElementById("soExitSellEntry").value = row.sell_cost != null ? row.sell_cost : "";

    const hasExitDate = !!dateOnly(row.exit_date);
    const hasSellExit = row.sell_exit_price != null && row.sell_exit_price !== "";
    const hasBuyExit = row.buy_exit_price != null && row.buy_exit_price !== "";

    if (forEdit) {
      // Edit: only show stored values — blank means "leave unchanged" on Save.
      document.getElementById("soExitDate").value = hasExitDate ? dateOnly(row.exit_date) : "";
      document.getElementById("soExitSellPx").value = hasSellExit ? row.sell_exit_price : "";
      document.getElementById("soExitBuyPx").value = hasBuyExit ? row.buy_exit_price : "";
    } else if (preferStoredExit && (hasExitDate || hasSellExit || hasBuyExit)) {
      document.getElementById("soExitDate").value = hasExitDate ? dateOnly(row.exit_date) : todayIso();
      document.getElementById("soExitSellPx").value = hasSellExit ? row.sell_exit_price : (row.sell_ltp != null ? row.sell_ltp : "");
      document.getElementById("soExitBuyPx").value = hasBuyExit ? row.buy_exit_price : (row.buy_ltp != null ? row.buy_ltp : "");
    } else {
      document.getElementById("soExitDate").value = hasExitDate ? dateOnly(row.exit_date) : todayIso();
      document.getElementById("soExitSellPx").value = hasSellExit
        ? row.sell_exit_price
        : (row.sell_ltp != null ? row.sell_ltp : "");
      document.getElementById("soExitBuyPx").value = hasBuyExit
        ? row.buy_exit_price
        : (row.buy_ltp != null ? row.buy_ltp : "");
    }

    document.getElementById("soExitErr").textContent = "";
    document.getElementById("soExitModal").hidden = false;

    const needQuote = fetchQuote
      && String((row.status || "")).toLowerCase() === "executed"
      && !(hasSellExit && hasBuyExit);
    if (!needQuote) return;
    fetch(API + "/api/stock-options/signals/" + row.id + "/exit-quote", { headers: authHeaders() })
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (!data || gen !== exitQuoteGen || !exitRow) return;
        setExitPx("soExitSellPx", data.sell_ltp, "sell");
        setExitPx("soExitBuyPx", data.buy_ltp, "buy");
      })
      .catch(() => {});
  }

  function openExit(id) {
    exitRow = (workspace.executed || []).find((r) => Number(r.id) === Number(id));
    if (!exitRow) return;
    applyExitModalChrome("exit");
    fillExitForm(exitRow, { preferStoredExit: true, fetchQuote: true });
  }

  function openEdit(id) {
    exitRow = findTradeRow(id);
    if (!exitRow) return;
    applyExitModalChrome("edit");
    fillExitForm(exitRow, { preferStoredExit: true, fetchQuote: false, forEdit: true });
  }

  function closeExit() {
    exitRow = null;
    exitModalMode = "exit";
    exitQuoteGen += 1;
    document.getElementById("soExitModal").hidden = true;
  }

  function readNumField(id) {
    const raw = document.getElementById(id).value;
    if (raw === "" || raw == null) return null;
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  }

  function readExitFormBody() {
    const exitDate = document.getElementById("soExitDate").value;
    const sellExitRaw = document.getElementById("soExitSellPx").value;
    const buyExitRaw = document.getElementById("soExitBuyPx").value;
    const body = {
      date_traded: document.getElementById("soExitEntryDate").value,
      buy_strike: Number(document.getElementById("soExitBuyStrike").value),
      buy_cost: Number(document.getElementById("soExitBuyEntry").value),
      sell_strike: Number(document.getElementById("soExitSellStrike").value),
      sell_cost: Number(document.getElementById("soExitSellEntry").value),
    };
    const sellExit = sellExitRaw === "" ? null : Number(sellExitRaw);
    const buyExit = buyExitRaw === "" ? null : Number(buyExitRaw);
    const anyExit = !!(exitDate || sellExitRaw !== "" || buyExitRaw !== "");
    return { body, exitDate, sellExit, buyExit, anyExit };
  }

  function readPartialUpdateBody() {
    const body = {};
    const dateTraded = document.getElementById("soExitEntryDate").value;
    if (dateTraded) body.date_traded = dateTraded;
    const buyStrike = readNumField("soExitBuyStrike");
    if (buyStrike != null) body.buy_strike = buyStrike;
    const buyCost = readNumField("soExitBuyEntry");
    if (buyCost != null) body.buy_cost = buyCost;
    const sellStrike = readNumField("soExitSellStrike");
    if (sellStrike != null) body.sell_strike = sellStrike;
    const sellCost = readNumField("soExitSellEntry");
    if (sellCost != null) body.sell_cost = sellCost;
    const exitDate = document.getElementById("soExitDate").value;
    if (exitDate) body.exit_date = exitDate;
    const sellExit = readNumField("soExitSellPx");
    if (sellExit != null) body.sell_exit = sellExit;
    const buyExit = readNumField("soExitBuyPx");
    if (buyExit != null) body.buy_exit = buyExit;
    return body;
  }

  async function submitExit(ev) {
    ev.preventDefault();
    if (!exitRow) return;
    const err = document.getElementById("soExitErr");

    if (exitModalMode === "edit") {
      const body = readPartialUpdateBody();
      if (!Object.keys(body).length) {
        err.textContent = "Fill at least one field to save.";
        return;
      }
      if (body.buy_strike != null && !(body.buy_strike > 0)) {
        err.textContent = "Buy strike must be > 0.";
        return;
      }
      if (body.sell_strike != null && !(body.sell_strike > 0)) {
        err.textContent = "Sell strike must be > 0.";
        return;
      }
      if (body.buy_cost != null && body.buy_cost < 0) {
        err.textContent = "Buy entry price must be >= 0.";
        return;
      }
      if (body.sell_cost != null && body.sell_cost < 0) {
        err.textContent = "Sell entry price must be >= 0.";
        return;
      }
      if (body.sell_exit != null && body.sell_exit < 0) {
        err.textContent = "Sell exit price must be >= 0.";
        return;
      }
      if (body.buy_exit != null && body.buy_exit < 0) {
        err.textContent = "Buy exit price must be >= 0.";
        return;
      }
      try {
        const res = await fetch(API + "/api/stock-options/signals/" + exitRow.id + "/update", {
          method: "POST",
          headers: authHeaders(),
          body: JSON.stringify(body),
        });
        if (!res.ok) {
          const t = await res.json().catch(() => ({}));
          throw new Error(t.detail || "Save failed");
        }
        const completed = String(exitRow.status || "").toLowerCase() === "completed";
        const stayTab = completed ? "report" : "executed";
        closeExit();
        setTab(stayTab);
        await load();
      } catch (e) {
        err.textContent = e.message || "Save failed";
      }
      return;
    }

    const parsed = readExitFormBody();
    const body = parsed.body;

    if (!body.date_traded || !(body.buy_strike > 0) || !(body.sell_strike > 0)
      || !Number.isFinite(body.buy_cost) || !Number.isFinite(body.sell_cost)) {
      err.textContent = "Enter entry date, both strikes, and both entry prices.";
      return;
    }

    if (!parsed.exitDate || !Number.isFinite(parsed.sellExit) || !Number.isFinite(parsed.buyExit)) {
      err.textContent = "Enter entry date, both strikes and entry prices, exit date, and both exit prices.";
      return;
    }
    body.exit_date = parsed.exitDate;
    body.sell_exit = parsed.sellExit;
    body.buy_exit = parsed.buyExit;
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
      const editBtn = ev.target.closest("[data-edit]");
      if (editBtn) openEdit(editBtn.dataset.edit);
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
    const muteCb = document.getElementById("soActivatedMute");
    if (muteCb) {
      muteCb.checked = isActivatedMuted();
      muteCb.addEventListener("change", () => {
        setActivatedMuted(!!muteCb.checked);
        if (!muteCb.checked) unlockActivatedAudio();
      });
    }
    const unlockOnce = () => {
      unlockActivatedAudio();
      document.removeEventListener("pointerdown", unlockOnce, true);
      document.removeEventListener("keydown", unlockOnce, true);
    };
    document.addEventListener("pointerdown", unlockOnce, true);
    document.addEventListener("keydown", unlockOnce, true);
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
