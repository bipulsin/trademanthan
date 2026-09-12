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

  function renderActive(row) {
    var empty = $("cdActiveEmpty");
    var panel = $("cdActiveRow");
    if (!row) {
      empty.hidden = false;
      panel.hidden = true;
      panel.innerHTML = "";
      return;
    }
    empty.hidden = true;
    panel.hidden = false;
    var statusClass = "cd-status";
    if (row.status === "Exit Trade") statusClass += " cd-status-exit cd-status-blink";
    var dirClass = row.direction === "BEAR" ? "cd-dir-bear" : "cd-dir-bull";
    var actionHtml = "";
    if (row.status === "Activated") {
      actionHtml =
        '<button type="button" class="cd-btn cd-btn-primary" id="cdTakeBtn">Take Trade</button>';
    } else if (row.status === "Exit Trade") {
      actionHtml = '<button type="button" class="cd-btn cd-btn-danger" id="cdExitBtn">Exit</button>';
    } else {
      actionHtml = '<button type="button" class="cd-btn" disabled>' +
        (row.status === "In-Trade" ? "In trade…" : "Waiting…") +
        "</button>";
    }
    var mapWarn =
      row.status === "In-Trade" && !row.instrument_key
        ? '<p class="cd-note">No instrument_key — add mapping for LTP.</p>'
        : "";

    panel.innerHTML =
      '<div><span class="cd-field-label">Symbol</span><div class="cd-field-val">' +
      fmt(row.symbol_mapped) +
      (row.symbol_raw && row.symbol_raw !== row.symbol_mapped
        ? ' <span class="cd-muted">(' + fmt(row.symbol_raw) + ")</span>"
        : "") +
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
      actionHtml +
      "</div></div>" +
      mapWarn;

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

    if (row.status === "Exit Trade") playExit(row.id);
  }

  function renderHistory(rows) {
    var body = $("cdHistoryBody");
    var empty = $("cdHistoryEmpty");
    body.innerHTML = "";
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
  }

  function renderMaps(rows) {
    var body = $("cdMapBody");
    body.innerHTML = "";
    (rows || []).forEach(function (m) {
      var tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" +
        fmt(m.tv_ticker) +
        "</td><td>" +
        fmt(m.upstox_symbol) +
        "</td><td>" +
        fmt(m.exchange) +
        '</td><td><button type="button" class="cd-btn cd-btn-ghost cd-map-del" data-id="' +
        m.id +
        '">Delete</button></td>';
      body.appendChild(tr);
    });
    body.querySelectorAll(".cd-map-del").forEach(function (btn) {
      btn.addEventListener("click", async function () {
        try {
          await api("/mappings/" + btn.getAttribute("data-id"), { method: "DELETE" });
          await load();
        } catch (e) {
          showBanner(String(e.message || e), true);
        }
      });
    });
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
      renderMaps(data.mappings || []);
      showBanner("");
    } catch (e) {
      showBanner(String(e.message || e), true);
    }
  }

  function bind() {
    $("cdReloadBtn").addEventListener("click", load);
    $("cdTakeCancel").addEventListener("click", closeTake);
    $("cdExitCancel").addEventListener("click", closeExit);

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

    $("cdMapForm").addEventListener("submit", async function (ev) {
      ev.preventDefault();
      var fd = new FormData(ev.target);
      try {
        await api("/mappings", {
          method: "PUT",
          body: JSON.stringify({
            tv_ticker: fd.get("tv_ticker"),
            upstox_symbol: fd.get("upstox_symbol"),
            exchange: fd.get("exchange") || "MCX",
          }),
        });
        ev.target.reset();
        ev.target.exchange.value = "MCX";
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
