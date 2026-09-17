/* Commodities Div frontend */
(function () {
  "use strict";

  function apiBase() {
    if (typeof trademanthanApiBase === "function") return trademanthanApiBase();
    var h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }

  function token() {
    try {
      return localStorage.getItem("trademanthan_token") || "";
    } catch (e) {
      return "";
    }
  }

  function authHeaders() {
    var t = token();
    var h = { "Content-Type": "application/json", Accept: "application/json" };
    if (t) h.Authorization = "Bearer " + t;
    return h;
  }

  async function api(path, opts) {
    var bases = [apiBase() + "/api/commodities-div", apiBase() + "/commodities-div"];
    var lastErr = null;
    for (var i = 0; i < bases.length; i++) {
      try {
        var res = await fetch(bases[i] + path, Object.assign({ headers: authHeaders() }, opts || {}));
        var data = await res.json().catch(function () {
          return {};
        });
        if (!res.ok) {
          var detail = data.detail || data.message || res.statusText;
          throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
        }
        return data;
      } catch (e) {
        lastErr = e;
      }
    }
    throw lastErr || new Error("API failed");
  }

  var tradeAudio = null;
  var exitAudio = null;
  var lastExitAudioForId = null;
  var pollTimer = null;
  var takeSignalId = null;
  var editSignalId = null;
  var editMode = "edit"; // "edit" | "exit"
  var currentTab = "active";
  var activeById = {};
  var inTradeById = {};
  var historyById = {};
  var _cdChartEngineLoadPromise = null;

  function $(id) {
    return document.getElementById(id);
  }

  function ensureChartEngine() {
    if (window.SecurityChartEngine) return Promise.resolve(window.SecurityChartEngine);
    if (_cdChartEngineLoadPromise) return _cdChartEngineLoadPromise;
    _cdChartEngineLoadPromise = new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      s.src = "security-chart/security-chart-engine.js?v=12";
      s.async = true;
      s.onload = function () {
        if (window.SecurityChartEngine) resolve(window.SecurityChartEngine);
        else reject(new Error("Chart module failed to initialize"));
      };
      s.onerror = function () {
        reject(new Error("Chart module failed to load"));
      };
      document.head.appendChild(s);
    });
    return _cdChartEngineLoadPromise;
  }

  function findCdRowById(id) {
    var nid = Number(id);
    if (!Number.isFinite(nid)) return null;
    return activeById[nid] || inTradeById[nid] || historyById[nid] || null;
  }

  function displaySymbolFor(row) {
    if (!row) return "—";
    var label =
      row.display_symbol ||
      row.mcx_symbol ||
      row.trading_symbol ||
      row.contract ||
      row.symbol_mapped ||
      row.symbol_raw ||
      "";
    var s = String(label).trim();
    return s || "—";
  }

  function openCommDivChart(row) {
    if (!row) return;
    var mapped = String(row.symbol_mapped || "").trim();
    var raw = String(row.symbol_raw || "").trim();
    var symbol = mapped || raw;
    if (!symbol || symbol === "—") return;
    var instrumentKey = String(row.instrument_key || "").trim();
    var displaySymbol = displaySymbolFor(row);
    if (displaySymbol === "—") displaySymbol = raw || mapped || symbol;
    ensureChartEngine()
      .then(function (eng) {
        if (!eng || typeof eng.openSecurityChart !== "function") {
          throw new Error("Chart module unavailable");
        }
        return eng.openSecurityChart({
          symbol: symbol,
          instrumentType: "FUT",
          instrumentKey: instrumentKey,
          displaySymbol: displaySymbol,
          exchange: "MCX",
          timeframe: "1hr",
          direction: row.direction || "",
          screenerData: {
            direction: row.direction,
            status: row.status,
            entryPrice: row.entry_price,
            ltp: row.ltp,
            pnl: row.pnl,
          },
          metadata: { algo: "commodities_div" },
          noIndicators: true,
          macdDivergenceEnabled: true,
        });
      })
      .catch(function (err) {
        if (window.console && window.console.warn) window.console.warn("Commodities Div chart:", err);
      });
  }

  function symbolChartButtonHtml(row) {
    var displaySym = displaySymbolFor(row);
    var mapped = String(row.symbol_mapped || "").trim();
    var raw = String(row.symbol_raw || "").trim();
    var symbol = mapped || raw;
    if (!symbol || displaySym === "—") return esc(fmt(displaySym));
    return (
      '<span role="button" tabindex="0" class="cd-security-link" title="Open chart" ' +
      'data-cd-chart-id="' +
      esc(String(row.id != null ? row.id : "")) +
      '" data-chart-symbol="' +
      esc(symbol) +
      '" data-chart-instrument-key="' +
      esc(String(row.instrument_key || "")) +
      '" data-chart-label="' +
      esc(String(displaySym)) +
      '" data-chart-direction="' +
      esc(String(row.direction || "")) +
      '">' +
      esc(fmt(displaySym)) +
      "</span>"
    );
  }

  function showBanner(msg, isErr) {
    var el = $("cdBanner");
    if (!el) return;
    if (!msg) {
      el.hidden = true;
      el.textContent = "";
      return;
    }
    el.hidden = false;
    el.textContent = msg;
    el.setAttribute("data-error", isErr ? "1" : "0");
  }

  function playTrade() {
    try {
      if (!tradeAudio) tradeAudio = new Audio("audio/trade_now.mp3");
      tradeAudio.currentTime = 0;
      tradeAudio.play().catch(function () {});
    } catch (e) {}
  }

  function playExit(signalId) {
    if (signalId != null && lastExitAudioForId === signalId) return;
    lastExitAudioForId = signalId;
    try {
      if (!exitAudio) exitAudio = new Audio("audio/exit_now.mp3");
      exitAudio.currentTime = 0;
      exitAudio.play().catch(function () {});
    } catch (e) {}
  }

  function fmt(v) {
    return v == null || v === "" ? "—" : String(v);
  }

  /** Date on first line, time below in smaller font (all datetime columns). */
  function dtStackHtml(dt) {
    var s = String(dt || "").trim().replace("T", " ");
    if (!s || s === "—") return '<span class="cd-dt">—</span>';
    var datePart = s.length >= 10 ? s.slice(0, 10) : s;
    var timePart = "";
    if (s.length >= 16) {
      timePart = s.slice(11, 19);
      if (timePart.length === 5) timePart += ":00";
    }
    if (!timePart) {
      return '<span class="cd-dt"><span class="cd-dt-date">' + esc(datePart) + "</span></span>";
    }
    return (
      '<span class="cd-dt">' +
      '<span class="cd-dt-date">' +
      esc(datePart) +
      "</span>" +
      '<span class="cd-dt-time">' +
      esc(timePart) +
      "</span></span>"
    );
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  /** HH if :15:00 cadence, else HH:MM */
  function cardTimeLabel(dt) {
    var m = String(dt || "").match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
    if (!m) return "—";
    var h = Number(m[1]);
    var min = Number(m[2]);
    var sec = m[3] != null ? Number(m[3]) : 0;
    if (min === 15 && sec === 0) return h + "h";
    return (h < 10 ? "0" : "") + h + ":" + (min < 10 ? "0" : "") + min;
  }

  function dateKey(dt) {
    var s = String(dt || "");
    return s.length >= 10 ? s.slice(0, 10) : "—";
  }

  function formatDateLabel(iso) {
    if (!iso || iso === "—") return "Date unknown";
    var parts = String(iso).slice(0, 10).split("-");
    if (parts.length !== 3) return iso;
    var months = [
      "January", "February", "March", "April", "May", "June",
      "July", "August", "September", "October", "November", "December",
    ];
    var m = Number(parts[1]);
    var d = Number(parts[2]);
    if (!m || m < 1 || m > 12) return iso;
    return months[m - 1] + " " + d + ", " + parts[0];
  }

  /** BULL = blue chip, BEAR = red chip (all tabs) */
  function dirChip(direction) {
    var dir = String(direction || "").toUpperCase();
    var cls = dir === "BEAR" ? "cd-chip cd-chip-bear" : "cd-chip cd-chip-bull";
    return '<span class="' + cls + '">' + esc(dir || "—") + "</span>";
  }

  function pnlHtml(pnl) {
    if (pnl == null || pnl === "") return '<span class="cd-pnl cd-pnl-na">—</span>';
    var n = Number(pnl);
    if (isNaN(n)) return '<span class="cd-pnl cd-pnl-na">—</span>';
    var cls = n > 0 ? "cd-pnl cd-pnl-pos" : n < 0 ? "cd-pnl cd-pnl-neg" : "cd-pnl";
    var sign = n > 0 ? "+" : "";
    return '<span class="' + cls + '">' + sign + n.toFixed(2) + "</span>";
  }

  function fieldHtml(label, html) {
    return (
      '<div class="cd-mcard-field"><span class="cd-mcard-field-label">' +
      label +
      '</span><span class="cd-mcard-field-val">' +
      html +
      "</span></div>"
    );
  }

  function splitIst(dt) {
    var s = String(dt || "").trim().replace("T", " ");
    if (s.length >= 16) {
      return { date: s.slice(0, 10), time: s.slice(11, 19) };
    }
    return { date: "", time: "" };
  }

  function combineIst(dateEl, timeEl) {
    var d = (dateEl && dateEl.value) || "";
    var t = (timeEl && timeEl.value) || "";
    if (!d) return "";
    if (!t) t = "00:00:00";
    if (t.length === 5) t += ":00";
    return d + " " + t.slice(0, 8);
  }

  function toDatetimeLocalValue(serverIst) {
    if (!serverIst || serverIst.length < 16) {
      var d = new Date();
      var pad = function (n) {
        return n < 10 ? "0" + n : "" + n;
      };
      return {
        date:
          d.getFullYear() +
          "-" +
          pad(d.getMonth() + 1) +
          "-" +
          pad(d.getDate()),
        time: pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":00",
      };
    }
    return splitIst(serverIst);
  }

  function setTab(name) {
    currentTab = name;
    document.querySelectorAll(".cd-tab").forEach(function (btn) {
      var on = btn.getAttribute("data-cd-tab") === name;
      btn.classList.toggle("active", on);
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });
    document.querySelectorAll(".cd-tab-panel").forEach(function (panel) {
      var on = panel.getAttribute("data-cd-panel") === name;
      panel.hidden = !on;
    });
  }

  function activeActionHtml(row, idSuffix) {
    var suffix = idSuffix || "";
    if (row.status === "Activated") {
      return (
        '<button type="button" class="cd-btn cd-btn-primary" data-cd-action="take" data-cd-id="' +
        row.id +
        '" id="cdTakeBtn' +
        suffix +
        '">Take Trade</button>'
      );
    }
    if (row.status === "Exit Trade") {
      return (
        '<button type="button" class="cd-btn cd-btn-danger" data-cd-action="exit-active" data-cd-id="' +
        row.id +
        '" id="cdExitBtn' +
        suffix +
        '">Exit</button>'
      );
    }
    return (
      '<button type="button" class="cd-btn" disabled>Waiting…</button>'
    );
  }

  function bindRowActions(container, rowsById) {
    if (!container) return;
    container.querySelectorAll("[data-cd-action]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var id = Number(btn.getAttribute("data-cd-id"));
        var row = rowsById[id] || inTradeById[id] || historyById[id];
        if (!row) return;
        var action = btn.getAttribute("data-cd-action");
        if (action === "take") openTake(row);
        else if (action === "exit-active" || action === "exit") openEdit(row, "exit");
        else if (action === "edit") openEdit(row, "edit");
        else if (action === "delete") confirmDelete(row);
      });
    });
  }

  function normalizeTradeMode(mode) {
    return String(mode || "PAPER").trim().toUpperCase() === "LIVE" ? "LIVE" : "PAPER";
  }

  function historyActionsHtml(row) {
    return (
      '<div class="cd-row-actions">' +
      '<button type="button" class="cd-icon-btn" data-cd-action="edit" data-cd-id="' +
      row.id +
      '" title="Edit"><i class="fas fa-pencil-alt"></i></button>' +
      '<button type="button" class="cd-icon-btn cd-icon-danger" data-cd-action="delete" data-cd-id="' +
      row.id +
      '" title="Delete"><i class="fas fa-trash"></i></button>' +
      "</div>"
    );
  }

  function historyRowHtml(r) {
    return (
      "<tr>" +
      "<td>" +
      symbolChartButtonHtml(r) +
      "</td><td>" +
      dirChip(r.direction) +
      "</td><td>" +
      dtStackHtml(r.div_received_at) +
      "</td><td>" +
      dtStackHtml(r.go_received_at) +
      "</td><td>" +
      esc(fmt(r.entry_price)) +
      "</td><td>" +
      dtStackHtml(r.trade_taken_at) +
      "</td><td>" +
      esc(fmt(r.exit_price)) +
      "</td><td>" +
      dtStackHtml(r.exit_at) +
      "</td><td>" +
      pnlHtml(r.pnl) +
      "</td><td>" +
      historyActionsHtml(r) +
      "</td></tr>"
    );
  }

  function historyCardHtml(r) {
    var tSrc = r.exit_at || r.trade_taken_at || r.go_received_at || "";
    return (
      '<article class="cd-mcard">' +
      '<button type="button" class="cd-mcard-summary" aria-expanded="false">' +
      '<span class="cd-mcard-time">' +
      esc(cardTimeLabel(tSrc)) +
      "</span>" +
      '<span class="cd-mcard-sym">' +
      symbolChartButtonHtml(r) +
      "</span>" +
      '<span class="cd-mcard-side">' +
      dirChip(r.direction) +
      "</span>" +
      '<span class="cd-mcard-pnl">' +
      pnlHtml(r.pnl) +
      "</span>" +
      '<i class="fas fa-chevron-down cd-mcard-chev" aria-hidden="true"></i>' +
      "</button>" +
      '<div class="cd-mcard-body" hidden>' +
      fieldHtml("DIV", dtStackHtml(r.div_received_at)) +
      fieldHtml("GO", dtStackHtml(r.go_received_at)) +
      fieldHtml("Entry", esc(fmt(r.entry_price))) +
      fieldHtml("Entry time", dtStackHtml(r.trade_taken_at)) +
      fieldHtml("Exit", esc(fmt(r.exit_price))) +
      fieldHtml("Exit time", dtStackHtml(r.exit_at)) +
      fieldHtml("PnL", pnlHtml(r.pnl)) +
      '<div class="cd-mcard-actions">' +
      historyActionsHtml(r) +
      "</div>" +
      "</div></article>"
    );
  }

  function historyMobileGroupsHtml(rows) {
    var groups = {};
    var order = [];
    rows.forEach(function (r) {
      var k = dateKey(r.exit_at || r.trade_taken_at || r.go_received_at);
      if (!groups[k]) {
        groups[k] = [];
        order.push(k);
      }
      groups[k].push(r);
    });
    order.sort(function (a, b) {
      if (a === "—") return 1;
      if (b === "—") return -1;
      return String(b).localeCompare(String(a));
    });
    return order
      .map(function (k) {
        var list = groups[k].map(historyCardHtml).join("");
        return (
          '<section class="cd-mgroup">' +
          '<header class="cd-mgroup-head">' +
          '<span class="cd-mgroup-title">' +
          esc(formatDateLabel(k)) +
          "</span>" +
          '<span class="cd-mgroup-meta">' +
          groups[k].length +
          (groups[k].length === 1 ? " trade" : " trades") +
          "</span></header>" +
          '<div class="cd-mgroup-cards">' +
          list +
          "</div></section>"
        );
      })
      .join("");
  }

  function renderHistoryModeBlock(mode, title, rows) {
    var head =
      '<header class="cd-history-mode-head">' +
      '<h3 class="cd-history-mode-title">' +
      esc(title) +
      "</h3>" +
      '<span class="cd-history-mode-meta">' +
      rows.length +
      (rows.length === 1 ? " trade" : " trades") +
      "</span></header>";
    if (!rows.length) {
      return (
        '<section class="cd-history-mode" data-mode="' +
        mode +
        '">' +
        head +
        '<p class="cd-empty cd-empty-mode">No ' +
        esc(title.toLowerCase()) +
        "s yet.</p></section>"
      );
    }
    var table =
      '<div class="cd-desktop-table"><div class="cd-table-wrap">' +
      '<table class="cd-table">' +
      "<thead><tr>" +
      "<th>Symbol</th><th>Dir</th><th>DIV</th><th>GO</th>" +
      "<th>Entry</th><th>Entry time</th><th>Exit</th><th>Exit time</th>" +
      "<th>PnL</th><th>Actions</th>" +
      "</tr></thead><tbody>" +
      rows.map(historyRowHtml).join("") +
      "</tbody></table></div></div>";
    var mobile =
      '<div class="cd-mobile-cards">' + historyMobileGroupsHtml(rows) + "</div>";
    return (
      '<section class="cd-history-mode" data-mode="' +
      mode +
      '">' +
      head +
      table +
      mobile +
      "</section>"
    );
  }

  function renderHistory(rows) {
    var empty = $("cdHistoryEmpty");
    var host = $("cdHistoryHost");
    var list = Array.isArray(rows) ? rows : [];
    historyById = {};
    list.forEach(function (r) {
      historyById[r.id] = r;
    });

    if (!list.length) {
      empty.hidden = false;
      if (host) {
        host.hidden = true;
        host.innerHTML = "";
      }
      return;
    }
    empty.hidden = true;
    if (!host) return;
    host.hidden = false;

    var live = list.filter(function (r) {
      return normalizeTradeMode(r.trade_mode) === "LIVE";
    });
    var paper = list.filter(function (r) {
      return normalizeTradeMode(r.trade_mode) !== "LIVE";
    });
    host.innerHTML =
      '<div class="cd-history-split">' +
      renderHistoryModeBlock("LIVE", "Live Trade", live) +
      renderHistoryModeBlock("PAPER", "Paper Trade", paper) +
      "</div>";
    bindRowActions(host, historyById);
  }

  function renderActive(rows) {
    var empty = $("cdActiveEmpty");
    var panel = $("cdActiveRow");
    var mobile = $("cdActiveMobile");
    var list = Array.isArray(rows) ? rows : rows ? [rows] : [];
    if (!list.length) {
      empty.hidden = false;
      panel.hidden = true;
      panel.innerHTML = "";
      if (mobile) {
        mobile.hidden = true;
        mobile.innerHTML = "";
      }
      return;
    }
    empty.hidden = true;
    panel.hidden = false;
    if (mobile) mobile.hidden = false;

    var rowsById = {};
    activeById = {};
    list.forEach(function (r) {
      rowsById[r.id] = r;
      activeById[r.id] = r;
    });

    var bodyHtml = list
      .map(function (row) {
        var statusClass = "cd-status";
        if (row.status === "Exit Trade") statusClass += " cd-status-exit cd-status-blink";
        return (
          "<tr>" +
          '<td class="cd-field-val">' +
          symbolChartButtonHtml(row) +
          "</td>" +
          "<td>" +
          dirChip(row.direction) +
          "</td>" +
          "<td><span class=\"" +
          statusClass +
          '">' +
          esc(fmt(row.status)) +
          "</span></td>" +
          '<td class="cd-field-val">' +
          dtStackHtml(row.div_received_at) +
          "</td>" +
          '<td class="cd-field-val">' +
          dtStackHtml(row.go_received_at) +
          "</td>" +
          '<td class="cd-field-val">' +
          esc(fmt(row.entry_price)) +
          (row.trade_taken_at
            ? '<div class="cd-dt-with-price">' + dtStackHtml(row.trade_taken_at) + "</div>"
            : "") +
          "</td>" +
          '<td class="cd-field-val">' +
          esc(fmt(row.ltp)) +
          (row.ltp_updated_at
            ? '<div class="cd-dt-with-price">' + dtStackHtml(row.ltp_updated_at) + "</div>"
            : "") +
          "</td>" +
          "<td>" +
          activeActionHtml(row, String(row.id)) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");

    panel.innerHTML =
      '<div class="cd-table-wrap cd-active-table-wrap">' +
      '<table class="cd-table cd-active-table">' +
      "<thead><tr>" +
      "<th>Symbol</th>" +
      "<th>Direction</th>" +
      "<th>Status</th>" +
      "<th>DIV received</th>" +
      "<th>GO received</th>" +
      "<th>Entry</th>" +
      "<th>LTP</th>" +
      "<th>Action</th>" +
      "</tr></thead>" +
      "<tbody>" +
      bodyHtml +
      "</tbody></table></div>";

    if (mobile) {
      mobile.innerHTML = list
        .map(function (row) {
          var statusClass = "cd-status";
          if (row.status === "Exit Trade") statusClass += " cd-status-exit cd-status-blink";
          var timeSrc = row.go_received_at || row.div_received_at || row.trade_taken_at || "";
          return (
            '<article class="cd-mcard">' +
            '<button type="button" class="cd-mcard-summary" aria-expanded="false">' +
            '<span class="cd-mcard-time">' +
            esc(cardTimeLabel(timeSrc)) +
            "</span>" +
            '<span class="cd-mcard-sym">' +
            symbolChartButtonHtml(row) +
            "</span>" +
            '<span class="cd-mcard-side">' +
            dirChip(row.direction) +
            "</span>" +
            '<i class="fas fa-chevron-down cd-mcard-chev" aria-hidden="true"></i>' +
            "</button>" +
            '<div class="cd-mcard-body" hidden>' +
            fieldHtml("Status", '<span class="' + statusClass + '">' + esc(row.status) + "</span>") +
            fieldHtml("DIV", dtStackHtml(row.div_received_at)) +
            fieldHtml("GO", dtStackHtml(row.go_received_at)) +
            fieldHtml(
              "Entry",
              esc(fmt(row.entry_price)) +
                (row.trade_taken_at
                  ? '<div class="cd-dt-with-price">' + dtStackHtml(row.trade_taken_at) + "</div>"
                  : "")
            ) +
            fieldHtml(
              "LTP",
              esc(fmt(row.ltp)) +
                (row.ltp_updated_at
                  ? '<div class="cd-dt-with-price">' + dtStackHtml(row.ltp_updated_at) + "</div>"
                  : "")
            ) +
            '<div class="cd-mcard-actions">' +
            activeActionHtml(row, "M" + row.id) +
            "</div>" +
            "</div></article>"
          );
        })
        .join("");
    }

    bindRowActions(panel, rowsById);
    bindRowActions(mobile, rowsById);
    list.forEach(function (row) {
      if (row.status === "Exit Trade") playExit(row.id);
    });
  }

  function inTradeActionsHtml(row) {
    return (
      '<div class="cd-row-actions">' +
      '<button type="button" class="cd-icon-btn" data-cd-action="edit" data-cd-id="' +
      row.id +
      '" title="Edit"><i class="fas fa-pencil-alt"></i></button>' +
      '<button type="button" class="cd-icon-btn cd-icon-danger" data-cd-action="delete" data-cd-id="' +
      row.id +
      '" title="Delete"><i class="fas fa-trash"></i></button>' +
      '<button type="button" class="cd-btn cd-btn-danger cd-btn-sm" data-cd-action="exit" data-cd-id="' +
      row.id +
      '">Exit</button>' +
      "</div>"
    );
  }

  function renderInTrade(rows) {
    var empty = $("cdInTradeEmpty");
    var desktop = $("cdInTradeDesktop");
    var body = $("cdInTradeBody");
    var mobile = $("cdInTradeMobile");
    var list = Array.isArray(rows) ? rows : [];
    inTradeById = {};
    list.forEach(function (r) {
      inTradeById[r.id] = r;
    });

    if (!list.length) {
      empty.hidden = false;
      if (desktop) desktop.hidden = true;
      if (body) body.innerHTML = "";
      if (mobile) {
        mobile.hidden = true;
        mobile.innerHTML = "";
      }
      return;
    }
    empty.hidden = true;
    if (desktop) desktop.hidden = false;
    if (mobile) mobile.hidden = false;

    if (body) {
      body.innerHTML = list
        .map(function (row) {
          return (
            "<tr>" +
            '<td class="cd-field-val">' +
            symbolChartButtonHtml(row) +
            "</td>" +
            "<td>" +
            dirChip(row.direction) +
            "</td>" +
            '<td class="cd-field-val">' +
            dtStackHtml(row.trade_taken_at) +
            "</td>" +
            '<td class="cd-field-val">' +
            esc(fmt(row.entry_price)) +
            "</td>" +
            '<td class="cd-field-val">' +
            esc(fmt(row.ltp)) +
            (row.ltp_updated_at
              ? '<div class="cd-dt-with-price">' + dtStackHtml(row.ltp_updated_at) + "</div>"
              : "") +
            "</td>" +
            "<td>" +
            pnlHtml(row.pnl) +
            "</td>" +
            "<td>" +
            inTradeActionsHtml(row) +
            "</td>" +
            "</tr>"
          );
        })
        .join("");
    }

    if (mobile) {
      mobile.innerHTML = list
        .map(function (row) {
          var timeSrc = row.trade_taken_at || "";
          return (
            '<article class="cd-mcard">' +
            '<button type="button" class="cd-mcard-summary" aria-expanded="false">' +
            '<span class="cd-mcard-time">' +
            esc(cardTimeLabel(timeSrc)) +
            "</span>" +
            '<span class="cd-mcard-sym">' +
            symbolChartButtonHtml(row) +
            "</span>" +
            '<span class="cd-mcard-side">' +
            dirChip(row.direction) +
            "</span>" +
            '<span class="cd-mcard-pnl">' +
            pnlHtml(row.pnl) +
            "</span>" +
            '<i class="fas fa-chevron-down cd-mcard-chev" aria-hidden="true"></i>' +
            "</button>" +
            '<div class="cd-mcard-body" hidden>' +
            fieldHtml("Entry time", dtStackHtml(row.trade_taken_at)) +
            fieldHtml("Entry price", esc(fmt(row.entry_price))) +
            fieldHtml(
              "LTP",
              esc(fmt(row.ltp)) +
                (row.ltp_updated_at
                  ? '<div class="cd-dt-with-price">' + dtStackHtml(row.ltp_updated_at) + "</div>"
                  : "")
            ) +
            fieldHtml("PnL", pnlHtml(row.pnl)) +
            fieldHtml("Mode", esc(fmt(row.trade_mode || "PAPER"))) +
            '<div class="cd-mcard-actions">' +
            inTradeActionsHtml(row) +
            "</div>" +
            "</div></article>"
          );
        })
        .join("");
    }

    bindRowActions(desktop, inTradeById);
    bindRowActions(mobile, inTradeById);
  }

  function openTake(row) {
    takeSignalId = row.id;
    $("cdTakeMeta").textContent =
      displaySymbolFor(row) + " " + row.direction;
    $("cdEntryPrice").value = "";
    if ($("cdTakeMode")) $("cdTakeMode").value = "PAPER";
    $("cdTakeModal").hidden = false;
  }

  function closeTake() {
    $("cdTakeModal").hidden = true;
    takeSignalId = null;
  }

  function openEdit(row, mode) {
    editSignalId = row.id;
    editMode = mode || "edit";
    var isHistory = String(row.status || "") === "History";
    var title = editMode === "exit" ? "Exit Trade" : "Edit Trade";
    $("cdEditTitle").textContent = title;
    $("cdEditMeta").textContent =
      displaySymbolFor(row) + " · " + (row.status || "");

    var entry = splitIst(row.trade_taken_at);
    $("cdEditEntryDate").value = entry.date || "";
    $("cdEditEntryTime").value = entry.time ? entry.time.slice(0, 8) : "";
    $("cdEditEntryPrice").value = row.entry_price != null ? row.entry_price : "";
    $("cdEditDirection").value = String(row.direction || "BULL").toUpperCase() === "BEAR" ? "BEAR" : "BULL";
    $("cdEditMode").value = normalizeTradeMode(row.trade_mode);

    var exitParts = splitIst(row.exit_at);
    if (editMode === "exit" && !exitParts.date) {
      exitParts = toDatetimeLocalValue($("cdServerTime").textContent);
    }
    $("cdEditExitDate").value = exitParts.date || "";
    $("cdEditExitTime").value = exitParts.time ? exitParts.time.slice(0, 8) : "";
    var exitPx = row.exit_price != null ? row.exit_price : row.ltp != null ? row.ltp : "";
    $("cdEditExitPrice").value = exitPx;

    var saveBtn = $("cdEditSaveBtn");
    var exitBtn = $("cdEditExitSubmitBtn");
    if (editMode === "exit") {
      saveBtn.hidden = true;
      exitBtn.hidden = false;
      $("cdEditExitPrice").required = true;
      $("cdEditExitDate").required = true;
      $("cdEditExitTime").required = true;
      setTimeout(function () {
        $("cdEditExitPrice").focus();
      }, 50);
    } else {
      saveBtn.hidden = false;
      // History stays History — no "move to History" action.
      exitBtn.hidden = isHistory;
      $("cdEditExitPrice").required = isHistory;
      $("cdEditExitDate").required = isHistory;
      $("cdEditExitTime").required = isHistory;
    }

    $("cdEditModal").hidden = false;
  }

  function closeEdit() {
    $("cdEditModal").hidden = true;
    editSignalId = null;
    editMode = "edit";
  }

  async function confirmDelete(row) {
    var sym = displaySymbolFor(row);
    if (sym === "—") sym = row.id;
    var isHistory = String(row.status || "") === "History";
    var msg = isHistory
      ? "Delete History row for " + sym + "?\nSignal is removed; trade_log (if any) is kept."
      : "Delete In-Trade row for " + sym + "?\nWebhook raw log is kept.";
    if (!window.confirm(msg)) return;
    try {
      await api("/signal/delete", {
        method: "POST",
        body: JSON.stringify({ signal_id: row.id }),
      });
      showBanner("Deleted " + sym, false);
      await load();
    } catch (e) {
      showBanner(String(e.message || e), true);
    }
  }

  function collectEditPayload() {
    var entryAt = combineIst($("cdEditEntryDate"), $("cdEditEntryTime"));
    var exitAt = combineIst($("cdEditExitDate"), $("cdEditExitTime"));
    var entryPrice = parseFloat($("cdEditEntryPrice").value);
    var exitPriceRaw = $("cdEditExitPrice").value;
    var exitPrice = exitPriceRaw !== "" ? parseFloat(exitPriceRaw) : null;
    return {
      signal_id: editSignalId,
      entry_price: entryPrice,
      trade_taken_at: entryAt,
      direction: $("cdEditDirection").value,
      trade_mode: $("cdEditMode").value,
      exit_price: exitPrice,
      exit_at: exitAt || null,
    };
  }

  async function load() {
    try {
      var data = await api("/workspace");
      if (data.server_time_ist) $("cdServerTime").textContent = data.server_time_ist;
      renderActive(data.actives || data.active || []);
      renderInTrade(data.in_trade || []);
      renderHistory(data.history || []);
      showBanner("");
    } catch (e) {
      showBanner(String(e.message || e), true);
    }
  }

  function bind() {
    $("cdReloadBtn").addEventListener("click", load);
    $("cdTakeCancel").addEventListener("click", closeTake);
    $("cdEditCancel").addEventListener("click", closeEdit);

    document.querySelectorAll(".cd-tab").forEach(function (btn) {
      btn.addEventListener("click", function () {
        setTab(btn.getAttribute("data-cd-tab"));
      });
    });

    document.addEventListener("click", function (ev) {
      var chartBtn = ev.target.closest(".cd-security-link");
      if (chartBtn) {
        ev.preventDefault();
        ev.stopPropagation();
        var id = chartBtn.getAttribute("data-cd-chart-id");
        var row = findCdRowById(id) || {
          id: id,
          symbol_mapped: chartBtn.getAttribute("data-chart-symbol") || "",
          symbol_raw: chartBtn.getAttribute("data-chart-symbol") || "",
          display_symbol: chartBtn.getAttribute("data-chart-label") || "",
          instrument_key: chartBtn.getAttribute("data-chart-instrument-key") || "",
          direction: chartBtn.getAttribute("data-chart-direction") || "",
        };
        openCommDivChart(row);
        return;
      }
      var summary = ev.target.closest(".cd-mcard-summary");
      if (!summary) return;
      var card = summary.closest(".cd-mcard");
      if (!card) return;
      var body = card.querySelector(".cd-mcard-body");
      var open = summary.getAttribute("aria-expanded") === "true";
      summary.setAttribute("aria-expanded", open ? "false" : "true");
      card.classList.toggle("cd-mcard-open", !open);
      if (body) body.hidden = open;
    });

    document.addEventListener("keydown", function (ev) {
      if (ev.key !== "Enter" && ev.key !== " ") return;
      var chartBtn = ev.target.closest(".cd-security-link");
      if (!chartBtn) return;
      ev.preventDefault();
      ev.stopPropagation();
      var id = chartBtn.getAttribute("data-cd-chart-id");
      var row = findCdRowById(id) || {
        id: id,
        symbol_mapped: chartBtn.getAttribute("data-chart-symbol") || "",
        symbol_raw: chartBtn.getAttribute("data-chart-symbol") || "",
        display_symbol: chartBtn.getAttribute("data-chart-label") || "",
        instrument_key: chartBtn.getAttribute("data-chart-instrument-key") || "",
        direction: chartBtn.getAttribute("data-chart-direction") || "",
      };
      openCommDivChart(row);
    });

    $("cdTakeForm").addEventListener("submit", async function (ev) {
      ev.preventDefault();
      if (takeSignalId == null) return;
      var price = parseFloat($("cdEntryPrice").value);
      var mode = $("cdTakeMode") ? $("cdTakeMode").value : "PAPER";
      try {
        var res = await api("/take-trade", {
          method: "POST",
          body: JSON.stringify({
            signal_id: takeSignalId,
            entry_price: price,
            trade_mode: mode,
          }),
        });
        closeTake();
        if (res.signal && res.signal.play_trade_audio) playTrade();
        if (res.signal && res.signal.mapping_warning) showBanner(res.signal.mapping_warning, false);
        setTab("in_trade");
        await load();
      } catch (e) {
        showBanner(String(e.message || e), true);
      }
    });

    $("cdEditSaveBtn").addEventListener("click", async function () {
      if (editSignalId == null) return;
      var payload = collectEditPayload();
      if (!payload.trade_taken_at || !(payload.entry_price > 0)) {
        showBanner("Entry date/time and price required", true);
        return;
      }
      try {
        var wasHistory =
          !!(historyById[editSignalId] && historyById[editSignalId].status === "History");
        await api("/signal/update", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        closeEdit();
        showBanner("Trade updated", false);
        if (wasHistory || currentTab === "history") setTab("history");
        await load();
      } catch (e) {
        showBanner(String(e.message || e), true);
      }
    });

    $("cdEditForm").addEventListener("submit", async function (ev) {
      ev.preventDefault();
      if (editSignalId == null) return;
      var payload = collectEditPayload();
      if (!payload.trade_taken_at || !(payload.entry_price > 0)) {
        showBanner("Entry date/time and price required", true);
        return;
      }
      if (!payload.exit_at || !(payload.exit_price > 0)) {
        showBanner("Exit date/time and price required to move to History", true);
        return;
      }
      try {
        await api("/exit-submit", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        closeEdit();
        lastExitAudioForId = null;
        setTab("history");
        await load();
      } catch (e) {
        showBanner(String(e.message || e), true);
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    bind();
    setTab("active");
    load();
    pollTimer = setInterval(load, 15000);
  });
})();
