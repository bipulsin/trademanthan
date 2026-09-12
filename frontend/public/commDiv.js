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
  var exitSignalId = null;

  function $(id) {
    return document.getElementById(id);
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

  function dirChip(direction) {
    var dir = String(direction || "").toUpperCase();
    var cls = dir === "BEAR" ? "cd-chip cd-chip-bear" : "cd-chip cd-chip-bull";
    return '<span class="' + cls + '">' + esc(dir || "—") + "</span>";
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

  function bindActiveActions(row) {
    var takeBtn = $("cdTakeBtn");
    if (takeBtn) {
      takeBtn.addEventListener("click", function () {
        openTake(row);
      });
    }
    var exitBtn = $("cdExitBtn");
    if (exitBtn) {
      exitBtn.addEventListener("click", function () {
        openExit(row);
      });
    }
    var takeBtnM = $("cdTakeBtnM");
    if (takeBtnM) {
      takeBtnM.addEventListener("click", function () {
        openTake(row);
      });
    }
    var exitBtnM = $("cdExitBtnM");
    if (exitBtnM) {
      exitBtnM.addEventListener("click", function () {
        openExit(row);
      });
    }
  }

  function activeActionHtml(row, idSuffix) {
    var suffix = idSuffix || "";
    if (row.status === "Activated") {
      return (
        '<button type="button" class="cd-btn cd-btn-primary" id="cdTakeBtn' +
        suffix +
        '">Take Trade</button>'
      );
    }
    if (row.status === "Exit Trade") {
      return (
        '<button type="button" class="cd-btn cd-btn-danger" id="cdExitBtn' +
        suffix +
        '">Exit</button>'
      );
    }
    return (
      '<button type="button" class="cd-btn" disabled>' +
      (row.status === "In-Trade" ? "In trade…" : "Waiting…") +
      "</button>"
    );
  }

  function renderActive(row) {
    var empty = $("cdActiveEmpty");
    var panel = $("cdActiveRow");
    var mobile = $("cdActiveMobile");
    if (!row) {
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
    var statusClass = "cd-status";
    if (row.status === "Exit Trade") statusClass += " cd-status-exit cd-status-blink";
    var dirClass = row.direction === "BEAR" ? "cd-dir-bear" : "cd-dir-bull";
    var mapWarn =
      row.status === "In-Trade" && !row.instrument_key
        ? '<p class="cd-note">Front-month FUT not resolved yet — LTP retries on the 10-min Upstox sidecar.</p>'
        : "";
    var symHtml =
      fmt(row.symbol_mapped) +
      (row.symbol_raw && row.symbol_raw !== row.symbol_mapped
        ? ' <span class="cd-muted">(' + fmt(row.symbol_raw) + ")</span>"
        : "");

    panel.innerHTML =
      '<div><span class="cd-field-label">Symbol</span><div class="cd-field-val">' +
      symHtml +
      "</div></div>" +
      '<div><span class="cd-field-label">Direction</span><div class="cd-field-val ' +
      dirClass +
      '">' +
      fmt(row.direction) +
      "</div></div>" +
      '<div><span class="cd-field-label">Status</span><div><span class="' +
      statusClass +
      '">' +
      fmt(row.status) +
      "</span></div></div>" +
      '<div><span class="cd-field-label">DIV received</span><div class="cd-field-val">' +
      fmt(row.div_received_at) +
      "</div></div>" +
      '<div><span class="cd-field-label">GO received</span><div class="cd-field-val">' +
      fmt(row.go_received_at) +
      "</div></div>" +
      '<div><span class="cd-field-label">Entry</span><div class="cd-field-val">' +
      fmt(row.entry_price) +
      (row.trade_taken_at ? " @ " + fmt(row.trade_taken_at) : "") +
      "</div></div>" +
      '<div><span class="cd-field-label">LTP</span><div class="cd-field-val">' +
      fmt(row.ltp) +
      (row.ltp_updated_at ? ' <span class="cd-muted">' + fmt(row.ltp_updated_at) + "</span>" : "") +
      "</div></div>" +
      '<div><span class="cd-field-label">Action</span><div>' +
      activeActionHtml(row, "") +
      "</div></div>" +
      mapWarn;

    var timeSrc = row.go_received_at || row.div_received_at || row.trade_taken_at || "";
    if (mobile) {
      mobile.innerHTML =
        '<article class="cd-mcard">' +
        '<button type="button" class="cd-mcard-summary" aria-expanded="false">' +
        '<span class="cd-mcard-time">' +
        esc(cardTimeLabel(timeSrc)) +
        "</span>" +
        '<span class="cd-mcard-sym">' +
        esc(row.symbol_mapped || "—") +
        "</span>" +
        '<span class="cd-mcard-side">' +
        dirChip(row.direction) +
        "</span>" +
        '<i class="fas fa-chevron-down cd-mcard-chev" aria-hidden="true"></i>' +
        "</button>" +
        '<div class="cd-mcard-body" hidden>' +
        fieldHtml("Status", '<span class="' + statusClass + '">' + esc(row.status) + "</span>") +
        fieldHtml("DIV", esc(fmt(row.div_received_at))) +
        fieldHtml("GO", esc(fmt(row.go_received_at))) +
        fieldHtml(
          "Entry",
          esc(fmt(row.entry_price)) +
            (row.trade_taken_at ? " @ " + esc(fmt(row.trade_taken_at)) : "")
        ) +
        fieldHtml(
          "LTP",
          esc(fmt(row.ltp)) +
            (row.ltp_updated_at
              ? ' <span class="cd-muted">' + esc(fmt(row.ltp_updated_at)) + "</span>"
              : "")
        ) +
        '<div class="cd-mcard-actions">' +
        activeActionHtml(row, "M") +
        "</div>" +
        mapWarn +
        "</div></article>";
    }

    bindActiveActions(row);
    if (row.status === "Exit Trade") playExit(row.id);
  }

  function renderHistory(rows) {
    var body = $("cdHistoryBody");
    var empty = $("cdHistoryEmpty");
    var mobile = $("cdHistoryMobile");
    body.innerHTML = "";
    if (mobile) mobile.innerHTML = "";
    if (!rows || !rows.length) {
      empty.hidden = false;
      return;
    }
    empty.hidden = true;
    rows.forEach(function (r) {
      var tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" +
        fmt(r.symbol_mapped) +
        "</td><td>" +
        fmt(r.direction) +
        "</td><td>" +
        fmt(r.div_received_at) +
        "</td><td>" +
        fmt(r.go_received_at) +
        "</td><td>" +
        fmt(r.entry_price) +
        "</td><td>" +
        fmt(r.trade_taken_at) +
        "</td><td>" +
        fmt(r.exit_price) +
        "</td><td>" +
        fmt(r.exit_at) +
        "</td><td>" +
        fmt(r.trade_log_id) +
        "</td>";
      body.appendChild(tr);
    });

    if (!mobile) return;
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
    mobile.innerHTML = order
      .map(function (k) {
        var list = groups[k]
          .map(function (r) {
            var tSrc = r.exit_at || r.trade_taken_at || r.go_received_at || "";
            return (
              '<article class="cd-mcard">' +
              '<button type="button" class="cd-mcard-summary" aria-expanded="false">' +
              '<span class="cd-mcard-time">' +
              esc(cardTimeLabel(tSrc)) +
              "</span>" +
              '<span class="cd-mcard-sym">' +
              esc(r.symbol_mapped || "—") +
              "</span>" +
              '<span class="cd-mcard-side">' +
              dirChip(r.direction) +
              "</span>" +
              '<i class="fas fa-chevron-down cd-mcard-chev" aria-hidden="true"></i>' +
              "</button>" +
              '<div class="cd-mcard-body" hidden>' +
              fieldHtml("DIV", esc(fmt(r.div_received_at))) +
              fieldHtml("GO", esc(fmt(r.go_received_at))) +
              fieldHtml("Entry", esc(fmt(r.entry_price))) +
              fieldHtml("Entry time", esc(fmt(r.trade_taken_at))) +
              fieldHtml("Exit", esc(fmt(r.exit_price))) +
              fieldHtml("Exit time", esc(fmt(r.exit_at))) +
              fieldHtml("trade_log", esc(fmt(r.trade_log_id))) +
              "</div></article>"
            );
          })
          .join("");
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

  function openTake(row) {
    takeSignalId = row.id;
    $("cdTakeMeta").textContent = row.symbol_mapped + " " + row.direction;
    $("cdEntryPrice").value = "";
    $("cdTakeModal").hidden = false;
  }

  function closeTake() {
    $("cdTakeModal").hidden = true;
    takeSignalId = null;
  }

  function toDatetimeLocalValue(serverIst) {
    // serverIst: "YYYY-MM-DD HH:MM:SS"
    if (!serverIst || serverIst.length < 16) {
      var d = new Date();
      var pad = function (n) {
        return n < 10 ? "0" + n : "" + n;
      };
      return (
        d.getFullYear() +
        "-" +
        pad(d.getMonth() + 1) +
        "-" +
        pad(d.getDate()) +
        "T" +
        pad(d.getHours()) +
        ":" +
        pad(d.getMinutes())
      );
    }
    return serverIst.slice(0, 16).replace(" ", "T");
  }

  function openExit(row) {
    exitSignalId = row.id;
    $("cdExitMeta").textContent = row.symbol_mapped + " " + row.direction;
    $("cdExitEntryReadonly").value = row.entry_price != null ? row.entry_price : "";
    $("cdExitPrice").value = "";
    $("cdExitAt").value = toDatetimeLocalValue($("cdServerTime").textContent);
    $("cdExitModal").hidden = false;
  }

  function closeExit() {
    $("cdExitModal").hidden = true;
    exitSignalId = null;
  }

  async function load() {
    try {
      var data = await api("/workspace");
      if (data.server_time_ist) $("cdServerTime").textContent = data.server_time_ist;
      renderActive(data.active);
      renderHistory(data.history || []);
      showBanner("");
    } catch (e) {
      showBanner(String(e.message || e), true);
    }
  }

  function bind() {
    $("cdReloadBtn").addEventListener("click", load);
    $("cdTakeCancel").addEventListener("click", closeTake);
    $("cdExitCancel").addEventListener("click", closeExit);

    document.addEventListener("click", function (ev) {
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

    $("cdTakeForm").addEventListener("submit", async function (ev) {
      ev.preventDefault();
      if (takeSignalId == null) return;
      var price = parseFloat($("cdEntryPrice").value);
      try {
        var res = await api("/take-trade", {
          method: "POST",
          body: JSON.stringify({ signal_id: takeSignalId, entry_price: price }),
        });
        closeTake();
        if (res.signal && res.signal.play_trade_audio) playTrade();
        if (res.signal && res.signal.mapping_warning) showBanner(res.signal.mapping_warning, false);
        await load();
      } catch (e) {
        showBanner(String(e.message || e), true);
      }
    });

    $("cdExitForm").addEventListener("submit", async function (ev) {
      ev.preventDefault();
      if (exitSignalId == null) return;
      var price = parseFloat($("cdExitPrice").value);
      var local = $("cdExitAt").value;
      if (!local) {
        showBanner("Exit date & time required", true);
        return;
      }
      var exitAt = local.replace("T", " ") + (local.length === 16 ? ":00" : "");
      try {
        await api("/exit-submit", {
          method: "POST",
          body: JSON.stringify({
            signal_id: exitSignalId,
            exit_price: price,
            exit_at: exitAt,
          }),
        });
        closeExit();
        lastExitAudioForId = null;
        await load();
      } catch (e) {
        showBanner(String(e.message || e), true);
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    bind();
    load();
    pollTimer = setInterval(load, 15000);
  });
})();
