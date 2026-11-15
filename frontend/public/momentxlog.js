/* MomentX trade log — grouped by date */
(function () {
  "use strict";

  var editRowId = null;

  function apiBase() {
    if (typeof trademanthanApiBase === "function") return trademanthanApiBase();
    var h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }

  function authHeaders() {
    var t = "";
    try { t = localStorage.getItem("trademanthan_token") || ""; } catch (e) { t = ""; }
    var h = { Accept: "application/json", "Content-Type": "application/json" };
    if (t) h.Authorization = "Bearer " + t;
    return h;
  }

  function $(id) { return document.getElementById(id); }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function fmtNum(n, digits) {
    if (n == null || n === "" || Number.isNaN(Number(n))) return "";
    return Number(n).toLocaleString("en-IN", {
      minimumFractionDigits: digits == null ? 2 : digits,
      maximumFractionDigits: digits == null ? 2 : digits,
    });
  }

  function fmtHm(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return "";
      return d.toLocaleString("en-GB", {
        timeZone: "Asia/Kolkata",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });
    } catch (e) {
      return "";
    }
  }

  function toLocalInput(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return "";
      // datetime-local in browser local tz — convert via IST parts for consistency
      var parts = new Intl.DateTimeFormat("en-CA", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }).formatToParts(d);
      var map = {};
      parts.forEach(function (p) { map[p.type] = p.value; });
      return map.year + "-" + map.month + "-" + map.day + "T" + map.hour + ":" + map.minute;
    } catch (e) {
      return "";
    }
  }

  function fromLocalInput(v) {
    if (!v) return null;
    // Treat as IST wall clock
    return String(v) + ":00+05:30";
  }

  function defaultDates() {
    var now = new Date();
    var ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    var y = ist.getFullYear();
    var m = String(ist.getMonth() + 1).padStart(2, "0");
    var d = String(ist.getDate()).padStart(2, "0");
    var today = y + "-" + m + "-" + d;
    var past = new Date(ist.getTime() - 30 * 86400000);
    var py = past.getFullYear();
    var pm = String(past.getMonth() + 1).padStart(2, "0");
    var pd = String(past.getDate()).padStart(2, "0");
    $("mxLogFrom").value = py + "-" + pm + "-" + pd;
    $("mxLogTo").value = today;
  }

  function sectorBadgeHtml(r) {
    var raw = (r && (r.sector_badge_display || r.sector_badge)) || "";
    if (!raw) return "";
    var text = String(raw);
    var kind = /^W/i.test(text) ? "weak" : "strong";
    var title = (r && r.nifty_sector_label)
      ? ("Sector: " + r.nifty_sector_label + (r.sector_star ? " · top within sector" : ""))
      : "Sector strength";
    return (
      ' <span class="mx-sector-badge is-' + kind + '" title="' + esc(title) + '">' +
      esc(text) +
      "</span>"
    );
  }

  function sortValAttr(v) {
    return ' data-sort-val="' + esc(v == null ? "" : v) + '"';
  }

  function isoSortKey(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return "";
      return String(d.getTime());
    } catch (e) {
      return "";
    }
  }

  function rowHtml(r) {
    var name = r.future_symbol || r.stock || "—";
    var traded = !!r.traded_at;
    var side = String(r.side || "bullish").toLowerCase() === "bearish" ? "bearish" : "bullish";
    var sideCls = side === "bearish" ? "mx-log-side-bearish" : "mx-log-side-bullish";
    var sideLabel = side === "bearish" ? "Bearish" : "Bullish";
    var pnlShown = traded || r.pnl != null;
    return (
      "<tr data-id=\"" + esc(r.id) + "\" class=\"mx-bt-row-" + side + "\">" +
      "<td" + sortValAttr(name) + "><button type=\"button\" class=\"mx-sym-link\" data-act=\"chart\" " +
        "data-ik=\"" + esc(r.future_instrument_key || "") + "\" " +
        "data-sym=\"" + esc(name) + "\">" + esc(name) + "</button>" +
        sectorBadgeHtml(r) + "</td>" +
      "<td class=\"" + sideCls + "\"" + sortValAttr(sideLabel) + ">" + esc(sideLabel) + "</td>" +
      "<td" + sortValAttr(isoSortKey(r.filtered_at)) + ">" + esc(fmtHm(r.filtered_at)) + "</td>" +
      "<td" + sortValAttr(isoSortKey(r.first_active_at)) + ">" + esc(fmtHm(r.first_active_at)) + "</td>" +
      "<td" + sortValAttr(r.first_active_ltp != null ? r.first_active_ltp : "") + ">" +
        esc(fmtNum(r.first_active_ltp)) + "</td>" +
      "<td" + sortValAttr(isoSortKey(r.last_active_at)) + ">" + esc(fmtHm(r.last_active_at)) + "</td>" +
      "<td" + sortValAttr(traded ? isoSortKey(r.traded_at) : "") + ">" +
        (traded ? esc(fmtHm(r.traded_at)) : "") + "</td>" +
      "<td" + sortValAttr(traded && r.entry_price != null ? r.entry_price : "") + ">" +
        (traded ? esc(fmtNum(r.entry_price)) : "") + "</td>" +
      "<td" + sortValAttr(traded && r.exit_price != null ? r.exit_price : "") + ">" +
        (traded ? esc(fmtNum(r.exit_price)) : "") + "</td>" +
      "<td class=\"" + (r.pnl != null ? (Number(r.pnl) >= 0 ? "mx-pnl-pos" : "mx-pnl-neg") : "") + "\"" +
        sortValAttr(pnlShown && r.pnl != null ? r.pnl : "") + ">" +
        (pnlShown ? esc(fmtNum(r.pnl)) : "") + "</td>" +
      "<td><button type=\"button\" class=\"mx-icon-btn\" data-act=\"edit\" title=\"Edit\" " +
        "data-id=\"" + esc(r.id) + "\" " +
        "data-entry-price=\"" + esc(r.entry_price != null ? r.entry_price : "") + "\" " +
        "data-entry-time=\"" + esc(r.entry_time || "") + "\" " +
        "data-exit-price=\"" + esc(r.exit_price != null ? r.exit_price : "") + "\" " +
        "data-exit-time=\"" + esc(r.exit_time || "") + "\">" +
        "<i class=\"fas fa-pencil-alt\"></i></button></td>" +
      "</tr>"
    );
  }

  function sortTh(key, type, label) {
    return (
      '<th class="mx-log-sortable" data-sort="' + key + '" data-type="' + type + '" ' +
      'role="button" tabindex="0" title="Sort by ' + esc(label) + '">' +
      esc(label) + "</th>"
    );
  }

  function groupHtml(g) {
    var total = g.total_pnl;
    var totalCls = total == null ? "" : (Number(total) >= 0 ? "mx-pnl-pos" : "mx-pnl-neg");
    return (
      '<section class="mx-log-day">' +
      '<header class="mx-log-day-head">' +
        "<h2>" + esc(g.trade_date) + "</h2>" +
        '<span class="mx-log-day-pnl ' + totalCls + '">Day PnL: ' +
          (total == null ? "—" : esc(fmtNum(total))) + "</span>" +
      "</header>" +
      '<div class="mx-log-table-wrap"><table class="mx-log-table">' +
      "<thead><tr>" +
      sortTh("future", "text", "Future") +
      sortTh("side", "text", "Side") +
      sortTh("filtered", "time", "Filtered") +
      sortTh("first_active", "time", "1st Activ") +
      sortTh("first_active_ltp", "num", "1st Activ LTP") +
      sortTh("last_active", "time", "Last Activ") +
      sortTh("traded", "time", "Traded") +
      sortTh("entry", "num", "Entry") +
      sortTh("exit", "num", "Exit") +
      sortTh("pnl", "num", "PnL") +
      "<th></th>" +
      "</tr></thead><tbody>" +
      (g.rows || []).map(rowHtml).join("") +
      "</tbody></table></div></section>"
    );
  }

  function cellSortRaw(td) {
    if (!td) return "";
    var v = td.getAttribute("data-sort-val");
    return v == null ? "" : String(v);
  }

  function compareSortValues(a, b, type, dir) {
    var emptyA = a === "";
    var emptyB = b === "";
    if (emptyA && emptyB) return 0;
    if (emptyA) return 1;
    if (emptyB) return -1;
    var cmp = 0;
    if (type === "num" || type === "time") {
      var na = Number(a);
      var nb = Number(b);
      if (!Number.isNaN(na) && !Number.isNaN(nb)) cmp = na < nb ? -1 : na > nb ? 1 : 0;
      else cmp = a < b ? -1 : a > b ? 1 : 0;
    } else {
      cmp = a.localeCompare(b, undefined, { sensitivity: "base" });
    }
    return cmp * dir;
  }

  function sortLogTable(th) {
    var table = th.closest("table.mx-log-table");
    if (!table) return;
    var tbody = table.querySelector("tbody");
    if (!tbody) return;
    var key = th.getAttribute("data-sort");
    var type = th.getAttribute("data-type") || "text";
    var colIdx = Array.prototype.indexOf.call(th.parentNode.children, th);
    if (colIdx < 0) return;

    var prevKey = table.getAttribute("data-sort-key");
    var prevDir = table.getAttribute("data-sort-dir") === "desc" ? -1 : 1;
    var dir = prevKey === key ? -prevDir : 1;
    table.setAttribute("data-sort-key", key);
    table.setAttribute("data-sort-dir", dir > 0 ? "asc" : "desc");

    table.querySelectorAll("th.mx-log-sortable").forEach(function (h) {
      h.classList.remove("sort-asc", "sort-desc");
      h.removeAttribute("aria-sort");
    });
    th.classList.add(dir > 0 ? "sort-asc" : "sort-desc");
    th.setAttribute("aria-sort", dir > 0 ? "ascending" : "descending");

    var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr"));
    rows.sort(function (ra, rb) {
      return compareSortValues(
        cellSortRaw(ra.children[colIdx]),
        cellSortRaw(rb.children[colIdx]),
        type,
        dir
      );
    });
    rows.forEach(function (row) { tbody.appendChild(row); });
  }

  function render(data) {
    $("mxLogLoading").hidden = true;
    var groups = (data && data.groups) || [];
    if (!groups.length) {
      $("mxLogEmpty").hidden = false;
      $("mxLogRoot").hidden = true;
      return;
    }
    $("mxLogEmpty").hidden = true;
    $("mxLogRoot").hidden = false;
    $("mxLogRoot").innerHTML = groups.map(groupHtml).join("");
  }

  async function loadLog() {
    $("mxLogLoading").hidden = false;
    $("mxLogError").hidden = true;
    var from = $("mxLogFrom").value;
    var to = $("mxLogTo").value;
    var q = "/api/momentx/log?";
    if (from) q += "date_from=" + encodeURIComponent(from) + "&";
    if (to) q += "date_to=" + encodeURIComponent(to);
    try {
      var res = await fetch(apiBase() + q, { headers: authHeaders() });
      var data = await res.json().catch(function () { return {}; });
      if (!res.ok) throw new Error((data && data.detail) || res.statusText || "Load failed");
      render(data);
    } catch (e) {
      $("mxLogLoading").hidden = true;
      $("mxLogError").hidden = false;
      $("mxLogError").textContent = (e && e.message) || "Could not load log";
    }
  }

  function downloadCsv() {
    var from = $("mxLogFrom").value;
    var to = $("mxLogTo").value;
    var q = "/api/momentx/log.csv?";
    if (from) q += "date_from=" + encodeURIComponent(from) + "&";
    if (to) q += "date_to=" + encodeURIComponent(to);
    var t = "";
    try { t = localStorage.getItem("trademanthan_token") || ""; } catch (e) { t = ""; }
    fetch(apiBase() + q, { headers: authHeaders() })
      .then(function (res) {
        if (!res.ok) throw new Error("CSV download failed");
        return res.blob();
      })
      .then(function (blob) {
        var a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = "momentx_log.csv";
        a.click();
        URL.revokeObjectURL(a.href);
      })
      .catch(function (err) {
        $("mxLogError").hidden = false;
        $("mxLogError").textContent = (err && err.message) || "CSV failed";
      });
  }

  function openEdit(btn) {
    editRowId = Number(btn.getAttribute("data-id"));
    $("mxEditEntryPrice").value = btn.getAttribute("data-entry-price") || "";
    $("mxEditExitPrice").value = btn.getAttribute("data-exit-price") || "";
    $("mxEditEntryTime").value = toLocalInput(btn.getAttribute("data-entry-time"));
    $("mxEditExitTime").value = toLocalInput(btn.getAttribute("data-exit-time"));
    $("mxLogEditModal").hidden = false;
  }

  function closeEdit() {
    editRowId = null;
    $("mxLogEditModal").hidden = true;
  }

  async function saveEdit() {
    if (!editRowId) return;
    var body = {
      entry_price: $("mxEditEntryPrice").value === "" ? null : Number($("mxEditEntryPrice").value),
      exit_price: $("mxEditExitPrice").value === "" ? null : Number($("mxEditExitPrice").value),
      entry_time: fromLocalInput($("mxEditEntryTime").value),
      exit_time: fromLocalInput($("mxEditExitTime").value),
    };
    try {
      var res = await fetch(apiBase() + "/api/momentx/log/" + editRowId, {
        method: "PATCH",
        headers: authHeaders(),
        body: JSON.stringify(body),
      });
      var data = await res.json().catch(function () { return {}; });
      if (!res.ok) throw new Error((data && data.detail) || data.error || "Save failed");
      closeEdit();
      loadLog();
    } catch (e) {
      $("mxLogError").hidden = false;
      $("mxLogError").textContent = (e && e.message) || "Save failed";
    }
  }

  function bind() {
    $("mxLogReload").addEventListener("click", loadLog);
    $("mxLogCsv").addEventListener("click", downloadCsv);
    $("mxEditCancel").addEventListener("click", closeEdit);
    $("mxEditSave").addEventListener("click", saveEdit);
    $("mxLogEditModal").addEventListener("click", function (ev) {
      if (ev.target === $("mxLogEditModal")) closeEdit();
    });
    $("mxLogRoot").addEventListener("click", function (ev) {
      var sortThEl = ev.target.closest("th.mx-log-sortable");
      if (sortThEl) {
        sortLogTable(sortThEl);
        return;
      }
      var editBtn = ev.target.closest("[data-act='edit']");
      if (editBtn) {
        openEdit(editBtn);
        return;
      }
      var chartBtn = ev.target.closest("[data-act='chart']");
      if (chartBtn && typeof openMomentxChart === "function") {
        openMomentxChart({
          instrumentKey: chartBtn.getAttribute("data-ik"),
          displaySymbol: chartBtn.getAttribute("data-sym"),
        });
      }
    });
    $("mxLogRoot").addEventListener("keydown", function (ev) {
      if (ev.key !== "Enter" && ev.key !== " ") return;
      var sortThEl = ev.target.closest("th.mx-log-sortable");
      if (!sortThEl) return;
      ev.preventDefault();
      sortLogTable(sortThEl);
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    defaultDates();
    bind();
    loadLog();
  });
})();
