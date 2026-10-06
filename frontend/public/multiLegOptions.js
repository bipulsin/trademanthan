/* Multi-Leg Options journal */
(function () {
  "use strict";

  var MIN_LEGS = { STRADDLE: 2, IRON_FLY: 4, IRON_CONDOR: 4 };
  var instruments = { indices: [], equities: [], commodities: [] };
  var activeTrades = [];
  var orphanLegs = [];
  var assignableTrades = [];
  var reportRows = [];
  var reportSort = { key: "entry_date", dir: -1 };
  var tab = "active";
  var expandedTrades = {};
  var mode = "new";
  var editingId = null;
  var expiryTouched = false;
  var pollTimer = null;
  var payoffTimer = null;
  var alertLatch = {};
  var PAYOFF_REFRESH_MS = 10 * 60 * 1000;
  var payoffClock = {};
  var lastInstrumentSpot = {};
  var lotCache = {};
  var lotFlight = {};
  var lotMiss = {};
  var modalPayoffTimer = null;

  function $(id) { return document.getElementById(id); }

  function apiBase() {
    if (typeof trademanthanApiBase === "function") return trademanthanApiBase();
    var h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }

  function authHeaders() {
    var t = "";
    try { t = localStorage.getItem("trademanthan_token") || ""; } catch (e) { t = ""; }
    var h = { "Content-Type": "application/json", Accept: "application/json" };
    if (t) h.Authorization = "Bearer " + t;
    return h;
  }

  async function api(path, opts) {
    var res = await fetch(apiBase() + "/api/multi-leg-options" + path, Object.assign({ headers: authHeaders() }, opts || {}));
    var data = await res.json().catch(function () { return {}; });
    if (!res.ok) {
      var detail = data.detail || data.message || res.statusText;
      throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
    }
    return data;
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function inr(n) {
    if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
    return new Intl.NumberFormat("en-IN", {
      style: "currency",
      currency: "INR",
      maximumFractionDigits: 2,
    }).format(Number(n));
  }

  function zoneNum(n) {
    if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
    var num = Number(n);
    if (Math.abs(num - Math.round(num)) < 1e-4) return String(Math.round(num));
    return String(num);
  }

  function futureLtpText(n) {
    if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
    return new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 }).format(Number(n));
  }

  function greenZoneText(t) {
    return "PE " + zoneNum(t && t.green_zone_pe) + " - " + zoneNum(t && t.green_zone_ce) + " CE";
  }

  function pnlClass(n) {
    if (n == null || Number.isNaN(Number(n))) return "";
    return Number(n) >= 0 ? "mlo-pos" : "mlo-neg";
  }

  function todayISO() {
    try {
      var parts = new Intl.DateTimeFormat("en-CA", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
      }).format(new Date());
      if (/^\d{4}-\d{2}-\d{2}$/.test(parts)) return parts;
    } catch (e) { /* local date below */ }
    var d = new Date();
    var m = String(d.getMonth() + 1).padStart(2, "0");
    var day = String(d.getDate()).padStart(2, "0");
    return d.getFullYear() + "-" + m + "-" + day;
  }

  function pad2(n) { return String(n).padStart(2, "0"); }

  var DEFAULT_CLOCK = "03:10 PM";

  function parseClock(text) {
    var match = String(text || "").trim().match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
    if (!match) return null;
    var hour = Number(match[1]);
    var minute = Number(match[2]);
    if (hour < 1 || hour > 12 || minute > 59) return null;
    var ap = match[3].toUpperCase();
    var hour24 = ap === "AM" ? (hour === 12 ? 0 : hour) : (hour === 12 ? 12 : hour + 12);
    return { hour: hour24, minute: minute };
  }

  function formatClock(hour24, minute) {
    var ap = hour24 >= 12 ? "PM" : "AM";
    var hour = hour24 % 12;
    if (hour === 0) hour = 12;
    return pad2(hour) + ":" + pad2(minute) + " " + ap;
  }

  function normalizeClock(text) {
    var parsed = parseClock(text);
    if (!parsed) return "";
    return formatClock(parsed.hour, parsed.minute);
  }

  function composeClock(dateStr, clockText) {
    var shown = normalizeClock(clockText);
    if (!dateStr || !shown) return "";
    return String(dateStr) + "T" + shown;
  }

  function parseISODate(s) {
    var p = String(s || "").slice(0, 10).split("-");
    if (p.length < 3) return NaN;
    return Date.UTC(+p[0], +p[1] - 1, +p[2]);
  }

  function dayDiff(fromIso, toIso) {
    var a = parseISODate(fromIso);
    var b = parseISODate(toIso);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
    return Math.round((b - a) / 86400000);
  }

  function typeLabel(t) {
    return { STRADDLE: "Straddle", IRON_FLY: "Iron Fly", IRON_CONDOR: "Iron Condor", UNCLASSIFIED: "Unclassified" }[t] || t;
  }

  function legTypeLabel(qualifier) {
    var role = String(qualifier || "").trim().toUpperCase();
    if (role === "MAIN") return "Main";
    if (role === "WING") return "Wing";
    if (role === "ADJ") return "Adj";
    return "—";
  }

  function lastWeekday(year, month, weekday) {
    var last = new Date(Date.UTC(year, month, 0));
    while (last.getUTCDay() !== weekday) last.setUTCDate(last.getUTCDate() - 1);
    var m = String(last.getUTCMonth() + 1).padStart(2, "0");
    var d = String(last.getUTCDate()).padStart(2, "0");
    return last.getUTCFullYear() + "-" + m + "-" + d;
  }

  function nextMonth(year, monthIndex) {
    return monthIndex === 11 ? [year + 1, 0] : [year, monthIndex + 1];
  }

  // Client preview. Server /expiry is the source of truth (NSE holiday shift + MCX master).
  // JS weekday: Sun=0 … Tue=2, Thu=4. Step one month at a time until DTE from IST today is 40+.
  var MIN_DTE = 40;

  function clientExpiry(entryIso, instrument) {
    var p = String(entryIso || "").split("-");
    if (p.length < 3) return "";
    var y = +p[0];
    var m = +p[1] - 1;
    var mcx = (instruments.commodities || []).indexOf(String(instrument || "").toUpperCase()) >= 0;
    var weekday = mcx ? 4 : 2;
    var exp = lastWeekday(y, m + 1, weekday);
    if (entryIso > exp) {
      var nm = nextMonth(y, m);
      exp = lastWeekday(nm[0], nm[1] + 1, weekday);
    }
    var today = todayISO();
    for (var i = 0; i < 24 && dayDiff(today, exp) < MIN_DTE; i++) {
      var parts = exp.split("-");
      var nxt = nextMonth(+parts[0], +parts[1] - 1);
      exp = lastWeekday(nxt[0], nxt[1] + 1, weekday);
    }
    return exp;
  }

  function setBanner(msg) {
    var el = $("mloBanner");
    if (!msg) { el.hidden = true; el.textContent = ""; el.classList.remove("mlo-banner-adj"); return; }
    el.hidden = false;
    el.textContent = msg;
  }

  function setAdjBanner(show) {
    var el = $("mloAdjBanner");
    if (!el) return;
    el.hidden = !show;
  }

  function fmtRatio(n) {
    if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
    return Number(n).toFixed(2);
  }

  function isStraddle(t) {
    return String((t && t.trade_type) || "").toUpperCase().replace(/[\s-]/g, "_") === "STRADDLE";
  }

  function legIsExited(leg) {
    return !!(leg && leg.exit_price != null && leg.exit_price !== "" && leg.exit_time);
  }

  function setSyncBanner(msg, isError) {
    var el = $("mloSyncBanner");
    if (!msg) { el.hidden = true; el.textContent = ""; el.classList.remove("is-error"); return; }
    el.hidden = false;
    el.classList.toggle("is-error", !!isError);
    el.textContent = msg;
  }

  function countPhrase(n, singular, plural) {
    var value = Number(n) || 0;
    return value + " " + (value === 1 ? singular : plural);
  }

  function syncSummary(data) {
    var parts = [
      countPhrase(data.new_legs, "new leg", "new legs"),
      countPhrase(data.new_trades, "new trade created", "new trades created"),
      countPhrase(data.orphans, "orphan flagged", "orphans flagged"),
      countPhrase(data.duplicates_skipped, "duplicate skipped", "duplicates skipped"),
    ];
    if (data.skipped_no_lot) parts.push(countPhrase(data.skipped_no_lot, "skipped (no lot)", "skipped (no lot)"));
    if (data.skipped_unmapped) parts.push(countPhrase(data.skipped_unmapped, "skipped (unmapped)", "skipped (unmapped)"));
    if (data.master_empty) parts.push("instrument master had no index option contracts");
    return parts.join(", ") + ".";
  }

  function minForType() {
    if ($("mloType").value === "UNCLASSIFIED") return 1;
    return MIN_LEGS[$("mloType").value] || 2;
  }

  function typeTemplate(kind) {
    if (kind === "IRON_FLY" || kind === "IRON_CONDOR") {
      return [
        { qualifier: "MAIN", option_type: "CE" },
        { qualifier: "MAIN", option_type: "PE" },
        { qualifier: "WING", option_type: "CE" },
        { qualifier: "WING", option_type: "PE" },
      ];
    }
    if (kind === "STRADDLE") {
      return [
        { qualifier: "MAIN", option_type: "CE" },
        { qualifier: "MAIN", option_type: "PE" },
      ];
    }
    return [];
  }

  function applyTypeTemplate() {
    if (mode !== "new") return;
    var slots = typeTemplate($("mloType").value);
    while (legCount() < slots.length) addLeg(null);
    var rows = $("mloLegs").querySelectorAll(".mlo-leg");
    slots.forEach(function (slot, i) {
      var row = rows[i];
      if (!row) return;
      var role = row.querySelector('[data-f="qualifier"]');
      var right = row.querySelector('[data-f="option_type"]');
      if (role) role.value = slot.qualifier;
      if (right) right.value = slot.option_type;
    });
    refreshSaveGate();
  }

  function legCount() {
    return $("mloLegs").querySelectorAll(".mlo-leg").length;
  }

  function refreshSaveGate() {
    var under = legCount() < minForType();
    $("mloSave").disabled = under;
    $("mloSave").title = under ? "Need at least " + minForType() + " legs" : "";
    var closeBtn = $("mloCloseTrade");
    if (!closeBtn.hidden) {
      closeBtn.disabled = !allLegsExited();
    }
    scheduleModalPayoff();
  }

  function refreshDte() {
    var exp = $("mloExpiry").value;
    var days = dayDiff(todayISO(), exp);
    $("mloDte").textContent = days == null ? "DTE —" : "DTE " + days;
  }

  function clockParts(iso) {
    if (!iso) return null;
    var d = new Date(iso);
    if (!Number.isNaN(d.getTime())) {
      var bag = {};
      new Intl.DateTimeFormat("en-GB", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hourCycle: "h23",
      }).formatToParts(d).forEach(function (part) { bag[part.type] = part.value; });
      var hour = Number(bag.hour);
      if (hour === 24) hour = 0;
      return {
        date: bag.year + "-" + bag.month + "-" + bag.day,
        hour: hour,
        minute: Number(bag.minute),
      };
    }
    var match = String(iso).match(/(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2})/);
    if (!match) return null;
    return { date: match[1], hour: Number(match[2]), minute: Number(match[3]) };
  }

  function dateField(name, value, placeholder) {
    var shown = value ? ' value="' + esc(value) + '"' : "";
    var typ = value ? "date" : "text";
    return '<input data-f="' + name + '" data-ph="' + esc(placeholder) + '" type="' + typ +
      '" placeholder="' + esc(placeholder) + '" aria-label="' + esc(placeholder) + '" required' + shown + ">";
  }

  function bindDateField(inp) {
    function show() {
      inp.type = inp.value ? "date" : "text";
    }
    inp.addEventListener("focus", function () { inp.type = "date"; });
    inp.addEventListener("blur", show);
  }

  function bindClockField(inp) {
    inp.addEventListener("blur", function () {
      var shown = normalizeClock(inp.value);
      if (shown) inp.value = shown;
    });
  }

  function legCard(leg, exitMode) {
    var shell = document.createElement("div");
    shell.className = "mlo-leg-wrap";
    var wrap = document.createElement("div");
    wrap.className = "mlo-leg";
    if (leg && leg.id) wrap.dataset.id = leg.id;
    var exited = !exitMode && legIsExited(leg);
    if (exited) shell.classList.add("is-exited");
    var entryClock = clockParts(leg && leg.entry_time);
    var entryText = entryClock ? formatClock(entryClock.hour, entryClock.minute) : DEFAULT_CLOCK;
    var exit = "";
    if (exitMode) {
      var exitClock = clockParts(leg && leg.exit_time);
      var exitDate = exitClock ? exitClock.date : todayISO();
      var exitText = exitClock ? formatClock(exitClock.hour, exitClock.minute) : DEFAULT_CLOCK;
      exit = '<input data-f="exit_price" type="number" min="0" step="0.01" placeholder="Exit price" aria-label="Exit price" value="' + esc(leg && leg.exit_price != null ? leg.exit_price : "") + '">' +
        dateField("exit_date", exitDate, "Exit date") +
        '<input data-f="exit_clock" type="text" placeholder="Time" aria-label="Exit time" value="' + esc(exitText) + '" required>';
    } else if (exited) {
      var doneClock = clockParts(leg.exit_time);
      var doneDate = doneClock ? doneClock.date : todayISO();
      var doneText = doneClock ? formatClock(doneClock.hour, doneClock.minute) : DEFAULT_CLOCK;
      exit = '<input data-f="exit_price" type="hidden" value="' + esc(leg.exit_price) + '">' +
        '<input data-f="exit_date" type="hidden" value="' + esc(doneDate) + '">' +
        '<input data-f="exit_clock" type="hidden" value="' + esc(doneText) + '">' +
        '<span class="mlo-leg-realized ' + pnlClass(leg.leg_pnl) + '">' + esc(inr(leg.leg_pnl)) + "</span>";
    }
    var exitBtn = (!exitMode && !exited)
      ? '<button type="button" class="mlo-icon-btn mlo-leg-exit-btn" aria-label="Exit leg"><i class="fas fa-right-from-bracket" aria-hidden="true"></i></button>'
      : "";
    wrap.innerHTML =
      '<select data-f="qualifier" aria-label="Qualifier"><option value="" hidden>Role</option><option value="MAIN">Main</option><option value="WING">Wing</option><option value="ADJ">Adj</option></select>' +
      '<select data-f="side" aria-label="Side" required><option value="" hidden>Side</option><option>BUY</option><option>SELL</option></select>' +
      '<select data-f="option_type" aria-label="Type" required><option value="" hidden>Type</option><option>CE</option><option>PE</option></select>' +
      '<input data-f="strike_price" type="number" min="0" step="0.01" placeholder="Strike" aria-label="Strike" required>' +
      dateField("leg_expiry_date", "", "Expiry") +
      '<input data-f="entry_price" type="number" min="0" step="0.01" placeholder="Entry price" aria-label="Entry price" required>' +
      '<input data-f="lot_size" type="number" min="1" step="1" placeholder="Lot" aria-label="Lot size">' +
      '<input data-f="entry_clock" type="text" placeholder="Time" aria-label="Time" value="' + esc(entryText) + '" required>' +
      exit +
      exitBtn +
      '<button type="button" class="mlo-icon-btn mlo-remove" aria-label="Remove leg"><i class="fas fa-trash" aria-hidden="true"></i></button>';
    var role = wrap.querySelector('[data-f="qualifier"]');
    if (leg && leg.qualifier) role.value = String(leg.qualifier).toUpperCase();
    wrap.querySelector('[data-f="side"]').value = (leg && leg.side) || "SELL";
    wrap.querySelector('[data-f="option_type"]').value = (leg && leg.option_type) || "CE";
    wrap.querySelector('[data-f="strike_price"]').value = leg && leg.strike_price != null ? leg.strike_price : "";
    var expiryInp = wrap.querySelector('[data-f="leg_expiry_date"]');
    expiryInp.value = (leg && leg.leg_expiry_date) || $("mloExpiry").value || "";
    if (expiryInp.value) expiryInp.type = "date";
    wrap.querySelector('[data-f="entry_price"]').value = leg && leg.entry_price != null ? leg.entry_price : "";
    var lotInp = wrap.querySelector('[data-f="lot_size"]');
    if (leg && leg.lot_size) {
      lotInp.value = String(leg.lot_size);
      lotInp.dataset.auto = "1";
    }
    lotInp.addEventListener("input", function () { lotInp.dataset.auto = ""; });
    wrap.querySelectorAll("input[data-ph]").forEach(bindDateField);
    wrap.querySelectorAll('[data-f="entry_clock"], [data-f="exit_clock"]').forEach(bindClockField);
    if (leg && leg.lot_size) wrap.dataset.lotSize = String(leg.lot_size);
    if (leg && leg.leg_expiry_date) expiryInp.dataset.touched = "1";
    expiryInp.addEventListener("input", function () { expiryInp.dataset.touched = "1"; });
    wrap.querySelector(".mlo-remove").addEventListener("click", function () {
      shell.remove();
      refreshSaveGate();
    });
    if (exited) {
      wrap.querySelectorAll("input, select").forEach(function (inp) {
        if (inp.type === "hidden") return;
        inp.disabled = true;
      });
    } else {
      wrap.querySelectorAll("input, select").forEach(function (inp) {
        inp.addEventListener("input", refreshSaveGate);
        inp.addEventListener("change", refreshSaveGate);
      });
    }
    shell.appendChild(wrap);
    if (!exitMode && !exited) {
      var panel = document.createElement("div");
      panel.className = "mlo-leg-exit-panel";
      panel.hidden = true;
      panel.innerHTML =
        '<input data-panel="exit_price" type="number" min="0" step="0.01" placeholder="Exit price" aria-label="Exit price">' +
        '<input data-panel="exit_clock" type="text" placeholder="Time" aria-label="Exit time" value="' + esc(DEFAULT_CLOCK) + '">' +
        '<button type="button" class="mlo-btn so-modal-btn primary mlo-leg-exit-submit">Submit</button>';
      panel.querySelector('[data-panel="exit_clock"]').addEventListener("blur", function () {
        var shown = normalizeClock(this.value);
        if (shown) this.value = shown;
      });
      wrap.querySelector(".mlo-leg-exit-btn").addEventListener("click", function () {
        panel.hidden = !panel.hidden;
      });
      panel.querySelector(".mlo-leg-exit-submit").addEventListener("click", function () {
        submitPerLegExit(shell, wrap, panel);
      });
      shell.appendChild(panel);
    }
    return shell;
  }

  async function submitPerLegExit(shell, wrap, panel) {
    if (!editingId || mode !== "edit") {
      showFormError("Open Edit to exit a single leg");
      return;
    }
    var pxEl = panel.querySelector('[data-panel="exit_price"]');
    var clockEl = panel.querySelector('[data-panel="exit_clock"]');
    var px = pxEl ? pxEl.value.trim() : "";
    var clock = clockEl ? clockEl.value.trim() : "";
    if (px === "" || Number(px) < 0 || !parseClock(clock)) {
      showFormError("Exit needs a price and a time like 03:10 PM");
      return;
    }
    var clockErr = legClockError();
    if (clockErr) {
      showFormError(clockErr);
      return;
    }
    var roleErr = qualifierError();
    if (roleErr) {
      showFormError(roleErr);
      return;
    }
    // Stash exit onto this leg row so readLegs picks it up, then PUT (trade stays ACTIVE).
    var hiddenPx = wrap.querySelector('[data-f="exit_price"]');
    var hiddenDay = wrap.querySelector('[data-f="exit_date"]');
    var hiddenClock = wrap.querySelector('[data-f="exit_clock"]');
    if (!hiddenPx) {
      hiddenPx = document.createElement("input");
      hiddenPx.type = "hidden";
      hiddenPx.setAttribute("data-f", "exit_price");
      wrap.appendChild(hiddenPx);
    }
    if (!hiddenDay) {
      hiddenDay = document.createElement("input");
      hiddenDay.type = "hidden";
      hiddenDay.setAttribute("data-f", "exit_date");
      wrap.appendChild(hiddenDay);
    }
    if (!hiddenClock) {
      hiddenClock = document.createElement("input");
      hiddenClock.type = "hidden";
      hiddenClock.setAttribute("data-f", "exit_clock");
      wrap.appendChild(hiddenClock);
    }
    hiddenPx.value = px;
    hiddenDay.value = todayISO();
    hiddenClock.value = normalizeClock(clock) || clock;
    showFormError("");
    try {
      var body = payload();
      var data = await api("/trades/" + editingId, { method: "PUT", body: JSON.stringify(body) });
      openModal("edit", data.trade);
      await loadActive(true);
    } catch (e) {
      showFormError(e.message || "Leg exit failed");
    }
  }

  function toLocal(iso) {
    var parts = clockParts(iso);
    if (!parts) return "";
    return parts.date + " " + formatClock(parts.hour, parts.minute);
  }

  function addLeg(leg) {
    $("mloLegs").appendChild(legCard(leg, mode === "exit"));
    refreshSaveGate();
  }

  function allLegsExited() {
    var rows = $("mloLegs").querySelectorAll(".mlo-leg");
    if (!rows.length) return false;
    for (var i = 0; i < rows.length; i++) {
      var px = rows[i].querySelector('[data-f="exit_price"]');
      var day = rows[i].querySelector('[data-f="exit_date"]');
      var clock = rows[i].querySelector('[data-f="exit_clock"]');
      if (!px || !day || !clock) return false;
      if (px.value === "" || !day.value || !parseClock(clock.value)) return false;
    }
    return true;
  }

  function readLegs() {
    return Array.prototype.map.call($("mloLegs").querySelectorAll(".mlo-leg"), function (row) {
      function val(name) {
        var el = row.querySelector('[data-f="' + name + '"]');
        return el ? el.value : "";
      }
      var leg = {
        qualifier: val("qualifier") || null,
        side: val("side"),
        option_type: val("option_type"),
        strike_price: Number(val("strike_price")),
        leg_expiry_date: val("leg_expiry_date"),
        entry_price: Number(val("entry_price")),
        entry_time: composeClock($("mloEntryDate").value, val("entry_clock")),
      };
      if (row.dataset.id) leg.id = row.dataset.id;
      var exitPx = val("exit_price");
      var exitDay = val("exit_date");
      if (exitPx !== "") {
        leg.exit_price = Number(exitPx);
        leg.exit_time = composeClock(exitDay, val("exit_clock")) || null;
      } else {
        leg.exit_price = null;
        leg.exit_time = null;
      }
      return leg;
    });
  }

  function legClockError() {
    var rows = $("mloLegs").querySelectorAll(".mlo-leg");
    for (var i = 0; i < rows.length; i++) {
      var entry = rows[i].querySelector('[data-f="entry_clock"]');
      if (!entry || !parseClock(entry.value)) return "Time must look like 03:10 PM";
      var exitPx = rows[i].querySelector('[data-f="exit_price"]');
      var exitClock = rows[i].querySelector('[data-f="exit_clock"]');
      if (exitPx && exitPx.value !== "" && exitClock && !parseClock(exitClock.value)) {
        return "Exit time must look like 03:10 PM";
      }
    }
    return "";
  }

  function qualifierError() {
    var rows = $("mloLegs").querySelectorAll(".mlo-leg");
    for (var i = 0; i < rows.length; i++) {
      var role = rows[i].querySelector('[data-f="qualifier"]');
      if (role && role.value) continue;
      if (rows[i].dataset.id) continue;
      return "Each new leg needs Main, Wing, or Adj";
    }
    return "";
  }

  function payload() {
    var body = {
      trade_type: $("mloType").value,
      instrument: $("mloInstrument").value.trim().toUpperCase(),
      spot_price_entry: Number($("mloSpot").value),
      entry_date: $("mloEntryDate").value,
      expiry_date: $("mloExpiry").value,
      legs: readLegs(),
    };
    // Exit saves omit the field so the stored value stays.
    if (mode !== "exit") {
      var rawMax = $("mloMaxProfit").value.trim();
      body.max_profit = rawMax === "" ? null : Number(rawMax);
      function zone(id) {
        var raw = $(id).value.trim();
        return raw === "" ? null : Number(raw);
      }
      body.green_zone_ce = zone("mloGreenCe");
      body.green_zone_pe = zone("mloGreenPe");
    }
    return body;
  }

  function showFormError(msg) {
    var el = $("mloFormErr");
    el.hidden = !msg;
    el.textContent = msg || "";
  }

  async function loadExpiry() {
    if (mode !== "new" || expiryTouched) return;
    var inst = $("mloInstrument").value.trim();
    var entry = $("mloEntryDate").value;
    if (!inst || !entry) return;
    $("mloExpiry").value = clientExpiry(entry, inst);
    try {
      var data = await api("/expiry?instrument=" + encodeURIComponent(inst) + "&entry_date=" + encodeURIComponent(entry));
      if (!expiryTouched && data.expiry_date) $("mloExpiry").value = data.expiry_date;
    } catch (e) { /* keep client preview */ }
    syncLegExpiries();
    refreshDte();
    scheduleModalPayoff();
  }

  function syncLegExpiries() {
    var exp = $("mloExpiry").value;
    $("mloLegs").querySelectorAll('[data-f="leg_expiry_date"]').forEach(function (inp) {
      if (!inp.dataset.touched) {
        inp.value = exp;
        inp.type = exp ? "date" : "text";
      }
    });
  }

  function ensureTypeOptions(trade) {
    var sel = $("mloType");
    var opt = sel.querySelector('option[value="UNCLASSIFIED"]');
    if (trade && trade.trade_type === "UNCLASSIFIED") {
      if (!opt) {
        opt = document.createElement("option");
        opt.value = "UNCLASSIFIED";
        opt.textContent = "Unclassified";
        sel.appendChild(opt);
      }
    } else if (opt) {
      opt.remove();
    }
  }

  function openModal(nextMode, trade) {
    mode = nextMode;
    editingId = trade ? trade.id : null;
    expiryTouched = !!trade;
    ensureTypeOptions(trade);
    $("mloModalTitle").textContent = nextMode === "new" ? "New Trade" : nextMode === "exit" ? "Exit Trade" : "Edit Trade";
    $("mloCloseTrade").hidden = nextMode !== "exit";
    var tradeNo = $("mloTradeNo");
    if (nextMode === "new") {
      tradeNo.hidden = true;
    } else {
      tradeNo.hidden = false;
      $("mloTradeNoVal").textContent = trade && trade.trade_no != null ? String(trade.trade_no) : "—";
    }
    $("mloType").value = (trade && trade.trade_type) || "STRADDLE";
    $("mloInstrument").value = (trade && trade.instrument) || "";
    $("mloSpot").value = trade && trade.spot_price_entry != null ? trade.spot_price_entry : "";
    $("mloEntryDate").value = (trade && trade.entry_date) || todayISO();
    $("mloExpiry").value = (trade && trade.expiry_date) || "";
    var showHeaderExtras = nextMode !== "exit";
    $("mloMaxProfitField").hidden = !showHeaderExtras;
    $("mloMaxProfit").value = showHeaderExtras && trade && trade.max_profit != null ? trade.max_profit : "";
    $("mloGreenCeField").hidden = !showHeaderExtras;
    $("mloGreenPeField").hidden = !showHeaderExtras;
    $("mloGreenCe").value = showHeaderExtras && trade && trade.green_zone_ce != null ? trade.green_zone_ce : "";
    $("mloGreenPe").value = showHeaderExtras && trade && trade.green_zone_pe != null ? trade.green_zone_pe : "";
    $("mloLegs").innerHTML = "";
    showFormError("");
    var legs = (trade && trade.legs) || [];
    if (!legs.length) {
      var n = minForType();
      for (var i = 0; i < n; i++) addLeg(null);
    } else {
      legs.forEach(function (leg) { addLeg(leg); });
    }
    if (nextMode === "new") applyTypeTemplate();
    if (!trade) loadExpiry();
    refreshDte();
    refreshSaveGate();
    $("mloModal").hidden = false;
    scheduleModalPayoff();
  }

  function closeModal() {
    $("mloModal").hidden = true;
    if ($("mloPayoff")) $("mloPayoff").hidden = true;
  }

  function assignFieldFocused() {
    var el = document.activeElement;
    return !!(el && el.closest && el.closest(".mlo-orphan-assign"));
  }

  function tradeChoiceLabel(t) {
    var num = t.trade_no != null ? String(t.trade_no) : "—";
    return num + " · " + t.instrument + " · " + t.expiry_date;
  }

  function rowField(label, value, extraClass) {
    return '<span class="mlo-row-field"><span class="mlo-row-k">' + label + "</span>" +
      '<span class="' + (extraClass || "") + '">' + esc(value) + "</span></span>";
  }

  function iconButton(act, label, icon, expanded) {
    return '<button type="button" class="mlo-icon-btn" data-act="' + act + '" aria-label="' + esc(label) + '"' +
      (act === "toggle" ? ' aria-expanded="' + (expanded ? "true" : "false") + '"' : "") + ">" +
      '<i class="fas ' + icon + '" aria-hidden="true"></i></button>';
  }

  function renderOrphans(orphans) {
    if (!orphans || !orphans.length) return "";
    var cards = orphans.map(function (leg) {
      return '<article class="mlo-card mlo-orphan so-card" data-leg="' + esc(leg.id) + '">' +
        '<div class="mlo-leg-ro">' +
          '<span data-label="Underlying">' + esc(leg.instrument || "—") + "</span>" +
          '<span data-label="Expiry">' + esc(leg.expiry_date) + "</span>" +
          '<span data-label="Strike">' + esc(leg.strike_price) + "</span>" +
          '<span data-label="Side">' + esc(leg.side) + "</span>" +
          '<span data-label="Type">' + esc(leg.option_type) + "</span>" +
          '<span data-label="Entry">' + esc(leg.entry_price) + "</span>" +
          '<span data-label="Entry time">' + esc(toLocal(leg.entry_time) || leg.entry_time || "—") + "</span>" +
          '<span class="mlo-badge">Orphan</span>' +
        "</div>" +
        '<div class="mlo-combo mlo-orphan-assign">' +
          '<input type="text" placeholder="Assign to trade — search number, instrument, or expiry" autocomplete="off" data-leg="' + esc(leg.id) + '" />' +
          '<div class="mlo-combo-list" hidden></div>' +
        "</div></article>";
    }).join("");
    return '<section class="mlo-orphans"><h2 class="mlo-orphans-title">Orphan legs</h2>' +
      '<p class="mlo-meta">These fills are not on a trade. Assign one to count it in that trade’s P&amp;L.</p>' +
      cards + "</section>";
  }

  function renderActive(trades, orphans) {
    activeTrades = trades || [];
    if (orphans) orphanLegs = orphans;
    if (assignFieldFocused()) return;
    var host = $("mloActive");
    var empty = '<p class="mlo-empty">No active trades. Use + New Trade to record a straddle, iron fly, or iron condor.</p>';
    var cards = activeTrades.length ? activeTrades.map(function (t) {
      var open = !!expandedTrades[t.id];
      var legs = (t.legs || []).map(function (leg) {
        var mark = leg.exited ? leg.exit_price : leg.ltp;
        var markLabel = leg.exited ? "Exit" : "LTP";
        return '<div class="mlo-leg-ro">' +
          '<span data-label="Leg type">' + esc(legTypeLabel(leg.qualifier)) + "</span>" +
          '<span data-label="Side">' + esc(leg.side) + "</span>" +
          '<span data-label="Type">' + esc(leg.option_type) + "</span>" +
          '<span data-label="Strike">' + esc(leg.strike_price) + "</span>" +
          '<span data-label="Entry">' + esc(leg.entry_price) + "</span>" +
          '<span data-label="' + markLabel + '">' + esc(mark == null ? "—" : mark) + "</span>" +
          '<span data-label="Delta">' + (leg.delta == null ? "—" : esc(Number(leg.delta).toFixed(4))) + "</span>" +
          '<span data-label="P&L" class="' + pnlClass(leg.leg_pnl) + '">' + esc(inr(leg.leg_pnl)) + "</span>" +
          "</div>";
      }).join("");
      var head = '<div class="mlo-leg-ro mlo-leg-head"><span>Leg type</span><span>Side</span><span>CE/PE</span><span>Strike</span><span>Entry</span><span>LTP</span><span>Delta</span><span>Leg P&L</span></div>';
      var ratioField = "";
      if (isStraddle(t)) {
        var hot = t.ltp_ratio != null && Number(t.ltp_ratio) >= 3;
        ratioField = rowField(
          "LTP Ratio",
          fmtRatio(t.ltp_ratio),
          hot ? "mlo-ratio-hot" : "mlo-ratio-ok"
        );
      }
      return '<article class="mlo-trade so-card" data-id="' + esc(t.id) + '">' +
        '<div class="mlo-row-summary">' +
          rowField("Trade No", t.trade_no == null ? "—" : t.trade_no) +
          rowField("Instrument and Type", t.instrument + " · " + typeLabel(t.trade_type)) +
          rowField("Expiry date", t.expiry_date || "—") +
          rowField("DTE", t.dte == null ? "—" : t.dte) +
          rowField("Future LTP", futureLtpText(t.future_ltp)) +
          rowField("Green zone", greenZoneText(t), "mlo-row-zone") +
          ratioField +
          rowField("P&L", inr(t.total_pnl), pnlClass(t.total_pnl)) +
          '<span class="mlo-row-actions">' +
            iconButton("edit", "Edit", "fa-pen") +
            iconButton("delete", "Delete", "fa-trash") +
            iconButton("exit", "Exit Trade", "fa-right-from-bracket") +
            iconButton("toggle", open ? "Collapse" : "Expand", open ? "fa-chevron-up" : "fa-chevron-down", open) +
          "</span></div>" +
        '<div class="mlo-row-body"' + (open ? "" : " hidden") + ">" + head + legs +
          '<div class="mlo-payoff" data-payoff="' + esc(t.id) + '"></div></div></article>';
    }).join("") : empty;
    host.innerHTML = renderOrphans(orphanLegs) + cards;
    activeTrades.forEach(function (t) {
      if (t.spot_ltp != null && !isNaN(Number(t.spot_ltp))) {
        lastInstrumentSpot[String(t.instrument || "").toUpperCase()] = Number(t.spot_ltp);
      }
    });
    paintExpandedPayoffs(false);
  }

  function cmp(a, b, key) {
    var av = a[key];
    var bv = b[key];
    if (key === "total_pnl") {
      av = av == null ? -Infinity : Number(av);
      bv = bv == null ? -Infinity : Number(bv);
    } else {
      av = av == null ? "" : String(av);
      bv = bv == null ? "" : String(bv);
    }
    if (av < bv) return -1;
    if (av > bv) return 1;
    return 0;
  }

  function renderReport() {
    var rows = reportRows.slice().sort(function (a, b) {
      return cmp(a, b, reportSort.key) * reportSort.dir;
    });
    var body = $("mloReportBody");
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="5" class="mlo-empty">No trades for these filters.</td></tr>';
      return;
    }
    body.innerHTML = rows.map(function (t, idx) {
      var legs = (t.legs || []).map(function (leg) {
        var mark = leg.exited ? leg.exit_price : leg.ltp;
        return "<tr><td>" + esc(leg.side) + "</td><td>" + esc(leg.option_type) + "</td><td>" + esc(leg.strike_price) +
          "</td><td>" + esc(leg.entry_price) + "</td><td>" + esc(mark == null ? "—" : mark) +
          '</td><td class="' + pnlClass(leg.leg_pnl) + '">' + esc(inr(leg.leg_pnl)) + "</td></tr>";
      }).join("");
      return '<tr class="mlo-report-row" data-idx="' + idx + '">' +
        '<td><button type="button" class="mlo-btn mlo-expand" data-idx="' + idx + '">' + esc(t.instrument) + "</button> " +
        '<span class="mlo-meta">' + esc(typeLabel(t.trade_type)) + "</span></td>" +
        "<td>" + esc(t.expiry_date) + "</td><td>" + esc(t.entry_date) + "</td><td>" + esc(t.exit_date || "") + "</td>" +
        '<td class="' + pnlClass(t.total_pnl) + '">' + esc(inr(t.total_pnl)) + "</td></tr>" +
        '<tr class="mlo-legs-detail" data-detail="' + idx + '" hidden><td colspan="5"><table class="mlo-table"><thead><tr>' +
        "<th>Side</th><th>CE/PE</th><th>Strike</th><th>Entry</th><th>Exit or LTP</th><th>Leg P&L</th></tr></thead><tbody>" +
        legs + "</tbody></table></td></tr>";
    }).join("");
  }

  async function loadActive(refreshQuotes) {
    try {
      var data = refreshQuotes ? await api("/quotes") : await api("/trades/active");
      setBanner(data.quote_error ? "Live quotes unavailable. Showing last saved LTP. " + data.quote_error : "");
      assignableTrades = data.assignable_trades || [];
      renderActive(data.trades || [], data.orphans || []);
      handleAlerts(data.trades || []);
    } catch (e) {
      setBanner(e.message || "Could not load active trades");
    }
  }

  async function loadReport() {
    var q = [];
    function add(name, id) {
      var v = $(id).value.trim();
      if (v) q.push(name + "=" + encodeURIComponent(v));
    }
    add("instrument", "mloFInstrument");
    add("trade_type", "mloFType");
    add("status", "mloFStatus");
    add("date_from", "mloFFrom");
    add("date_to", "mloFTo");
    try {
      var data = await api("/trades/report" + (q.length ? "?" + q.join("&") : ""));
      reportRows = data.trades || [];
      renderReport();
    } catch (e) {
      setBanner(e.message || "Could not load report");
    }
  }

  function showTab(name) {
    tab = name;
    document.querySelectorAll(".bf-tab").forEach(function (btn) {
      btn.classList.toggle("active", btn.dataset.tab === name);
    });
    $("mloActive").hidden = name !== "active";
    $("mloReport").hidden = name !== "report";
    if (name === "report") loadReport();
    else loadActive(true);
  }

  function fillCombo(query) {
    var list = $("mloInstrumentList");
    var q = String(query || "").trim().toUpperCase();
    function group(title, names) {
      var hits = names.filter(function (n) { return !q || n.indexOf(q) >= 0; }).slice(0, 40);
      if (!hits.length) return "";
      return '<div class="mlo-combo-group">' + esc(title) + "</div>" + hits.map(function (n) {
        return '<button type="button" data-sym="' + esc(n) + '">' + esc(n) + "</button>";
      }).join("");
    }
    var html = group("Index", instruments.indices || []) +
      group("Equity", instruments.equities || []) +
      group("MCX", instruments.commodities || []);
    list.innerHTML = html || '<div class="mlo-combo-group">No match</div>';
    list.hidden = false;
  }

  async function loadInstruments() {
    try {
      var data = await api("/instruments");
      instruments.indices = data.indices || [];
      instruments.equities = data.equities || [];
      instruments.commodities = data.commodities || [];
    } catch (e) {
      instruments.indices = ["NIFTY", "BANKNIFTY", "SENSEX"];
    }
  }

  function tradeById(id) {
    return activeTrades.filter(function (t) { return t.id === id; })[0] || null;
  }

  $("mloNewTrade").addEventListener("click", function () { openModal("new", null); });
  $("mloSyncUpstox").addEventListener("click", openSyncModal);
  $("mloCancel").addEventListener("click", closeModal);
  $("mloModalClose").addEventListener("click", closeModal);
  $("mloAddLeg").addEventListener("click", function () {
    if (mode !== "new") {
      addLeg({ qualifier: "ADJ" });
      return;
    }
    var slots = typeTemplate($("mloType").value);
    if (legCount() >= slots.length) addLeg({ qualifier: "ADJ" });
    else addLeg(slots[legCount()]);
  });
  $("mloType").addEventListener("change", function () {
    if (mode === "new") applyTypeTemplate();
    refreshSaveGate();
  });
  $("mloEntryDate").addEventListener("change", function () {
    if (mode !== "new") return;
    expiryTouched = false;
    loadExpiry();
  });
  $("mloExpiry").addEventListener("input", function () { expiryTouched = true; refreshDte(); syncLegExpiries(); scheduleModalPayoff(); });
  $("mloInstrument").addEventListener("focus", function () { fillCombo(this.value); });
  $("mloInstrument").addEventListener("input", function () {
    fillCombo(this.value);
    scheduleModalPayoff();
    if (mode !== "new") return;
    expiryTouched = false;
    loadExpiry();
  });
  $("mloInstrumentList").addEventListener("click", function (ev) {
    var btn = ev.target.closest("button[data-sym]");
    if (!btn) return;
    $("mloInstrument").value = btn.getAttribute("data-sym");
    $("mloInstrumentList").hidden = true;
    scheduleModalPayoff();
    if (mode !== "new") return;
    expiryTouched = false;
    loadExpiry();
  });
  document.addEventListener("click", function (ev) {
    if (ev.target.closest(".mlo-combo")) return;
    $("mloInstrumentList").hidden = true;
    document.querySelectorAll(".mlo-orphan-assign .mlo-combo-list").forEach(function (el) { el.hidden = true; });
  });

  $("mloForm").addEventListener("submit", async function (ev) {
    ev.preventDefault();
    showFormError("");
    if (legCount() < minForType()) {
      showFormError(typeLabel($("mloType").value) + " needs at least " + minForType() + " legs");
      return;
    }
    var clockErr = legClockError();
    if (clockErr) {
      showFormError(clockErr);
      return;
    }
    var roleErr = qualifierError();
    if (roleErr) {
      showFormError(roleErr);
      return;
    }
    var body = payload();
    try {
      if (mode === "new") await api("/trades", { method: "POST", body: JSON.stringify(body) });
      else await api("/trades/" + editingId, { method: "PUT", body: JSON.stringify(body) });
      closeModal();
      await loadActive(true);
    } catch (e) {
      showFormError(e.message || "Save failed");
    }
  });

  $("mloCloseTrade").addEventListener("click", async function () {
    showFormError("");
    if (!allLegsExited()) {
      showFormError("Close requires every leg to have an exit price and exit time");
      return;
    }
    try {
      await api("/trades/" + editingId + "/close", { method: "POST", body: JSON.stringify({ legs: readLegs() }) });
      closeModal();
      await loadActive(true);
    } catch (e) {
      showFormError(e.message || "Close failed");
    }
  });

  function showAssignList(input) {
    var list = input.parentElement.querySelector(".mlo-combo-list");
    if (!list) return;
    var q = input.value.trim().toUpperCase();
    var hits = assignableTrades.filter(function (t) {
      if (!q) return true;
      var hay = ((t.trade_no == null ? "" : t.trade_no) + " " + t.instrument + " " + t.expiry_date + " " + t.trade_type).toUpperCase();
      return hay.indexOf(q) >= 0;
    }).slice(0, 30);
    list.innerHTML = hits.length ? hits.map(function (t) {
      return '<button type="button" data-trade="' + esc(t.id) + '" data-leg="' + esc(input.getAttribute("data-leg")) + '">' +
        esc(tradeChoiceLabel(t)) + "</button>";
    }).join("") : '<div class="mlo-combo-group">No matching trade</div>';
    list.hidden = false;
  }

  async function assignOrphan(legId, tradeId) {
    try {
      await api("/orphans/" + encodeURIComponent(legId) + "/assign", {
        method: "POST",
        body: JSON.stringify({ trade_id: tradeId }),
      });
      await loadActive(true);
    } catch (e) {
      setBanner(e.message || "Could not assign the leg");
    }
  }

  function openSyncModal() {
    $("mloSyncDate").value = todayISO();
    $("mloSyncErr").hidden = true;
    $("mloSyncErr").textContent = "";
    $("mloSyncModal").hidden = false;
  }

  function closeSyncModal() {
    $("mloSyncModal").hidden = true;
  }

  async function syncFromUpstox(tradeDate) {
    var btn = $("mloSyncUpstox");
    var label = btn.querySelector(".text");
    btn.disabled = true;
    if (label) label.textContent = "Syncing…";
    try {
      var data = await api("/sync/upstox", {
        method: "POST",
        body: JSON.stringify({ trade_date: tradeDate }),
      });
      var dated = data.trade_date ? data.trade_date + ": " : "";
      setSyncBanner(dated + syncSummary(data), false);
      closeSyncModal();
      await loadActive(true);
    } catch (e) {
      $("mloSyncErr").hidden = false;
      $("mloSyncErr").textContent = e.message || "Sync failed";
      setSyncBanner(e.message || "Sync failed", true);
    } finally {
      btn.disabled = false;
      if (label) label.textContent = "Sync frm Broker";
    }
  }

  $("mloSyncCancel").addEventListener("click", closeSyncModal);
  $("mloSyncClose").addEventListener("click", closeSyncModal);
  $("mloSyncForm").addEventListener("submit", function (ev) {
    ev.preventDefault();
    var day = $("mloSyncDate").value;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) {
      $("mloSyncErr").hidden = false;
      $("mloSyncErr").textContent = "Choose a trade date";
      return;
    }
    syncFromUpstox(day);
  });

  $("mloActive").addEventListener("input", function (ev) {
    var input = ev.target.closest(".mlo-orphan-assign input");
    if (input) showAssignList(input);
  });
  $("mloActive").addEventListener("focusin", function (ev) {
    var input = ev.target.closest && ev.target.closest(".mlo-orphan-assign input");
    if (input) showAssignList(input);
  });

  $("mloActive").addEventListener("click", async function (ev) {
    var pick = ev.target.closest("button[data-trade]");
    if (pick) {
      var legId = pick.getAttribute("data-leg");
      var tradeId = pick.getAttribute("data-trade");
      if (legId && tradeId) await assignOrphan(legId, tradeId);
      return;
    }
    var btn = ev.target.closest("button[data-act]");
    if (!btn) return;
    var card = btn.closest(".mlo-trade");
    if (!card) return;
    var trade = tradeById(card.getAttribute("data-id"));
    if (!trade) return;
    var act = btn.getAttribute("data-act");
    if (act === "toggle") {
      var body = card.querySelector(".mlo-row-body");
      var willOpen = !!(body && body.hidden);
      if (body) body.hidden = !willOpen;
      if (willOpen) {
        expandedTrades[trade.id] = true;
        paintTradePayoff(trade, false);
      } else delete expandedTrades[trade.id];
      var icon = btn.querySelector("i");
      if (icon) icon.className = willOpen ? "fas fa-chevron-up" : "fas fa-chevron-down";
      btn.setAttribute("aria-expanded", willOpen ? "true" : "false");
      btn.setAttribute("aria-label", willOpen ? "Collapse" : "Expand");
      return;
    }
    if (act === "edit") openModal("edit", trade);
    if (act === "exit") openModal("exit", trade);
    if (act === "delete") {
      if (!window.confirm("Delete this " + trade.instrument + " trade?")) return;
      try {
        await api("/trades/" + trade.id, { method: "DELETE" });
        await loadActive(true);
      } catch (e) {
        setBanner(e.message || "Delete failed");
      }
    }
  });

  document.querySelectorAll(".bf-tab").forEach(function (btn) {
    btn.addEventListener("click", function () { showTab(btn.dataset.tab); });
  });
  $("mloApplyFilters").addEventListener("click", loadReport);
  $("mloReportTable").querySelector("thead").addEventListener("click", function (ev) {
    var th = ev.target.closest("th[data-sort]");
    if (!th) return;
    var key = th.getAttribute("data-sort");
    if (reportSort.key === key) reportSort.dir *= -1;
    else { reportSort.key = key; reportSort.dir = 1; }
    renderReport();
  });
  $("mloReportBody").addEventListener("click", function (ev) {
    var btn = ev.target.closest(".mlo-expand");
    if (!btn) return;
    var detail = $("mloReportBody").querySelector('[data-detail="' + btn.getAttribute("data-idx") + '"]');
    if (detail) detail.hidden = !detail.hidden;
  });

  function playAlert(file) {
    try {
      var audio = new Audio(file);
      var played = audio.play();
      if (played && typeof played.catch === "function") played.catch(function () {});
    } catch (e) { /* autoplay can be blocked until a click */ }
  }

  function considerAlert(key, active, message, file, useBanner) {
    if (!active) {
      alertLatch[key] = false;
      return;
    }
    if (alertLatch[key]) return;
    alertLatch[key] = true;
    playAlert(file);
    if (useBanner) return;
    if (message) window.alert(message);
  }

  function handleAlerts(trades) {
    var seen = {};
    var straddleAdj = false;
    (trades || []).forEach(function (t) {
      if (!t || (t.status && t.status !== "ACTIVE")) return;
      var id = t.id || ((t.trade_type || "") + ":" + (t.instrument || ""));
      var pairKey = (t.ltp_ratio_ce_leg_id || "") + "+" + (t.ltp_ratio_pe_leg_id || "");
      var adjKey = isStraddle(t) ? (id + ":adj:" + pairKey) : (id + ":adj");
      var exitKey = id + ":exit";
      seen[adjKey] = true;
      seen[exitKey] = true;
      if (isStraddle(t) && t.adjustment_alert) straddleAdj = true;
      considerAlert(
        adjKey,
        !!t.adjustment_alert,
        t.adjustment_message || "Adjustment",
        "adjustment.mp3",
        isStraddle(t)
      );
      considerAlert(exitKey, !!t.exit_adjustment_alert, "Exit Adjustment", "adjustment_exit.mp3", false);
    });
    setAdjBanner(straddleAdj);
    Object.keys(alertLatch).forEach(function (key) {
      if (!seen[key]) alertLatch[key] = false;
    });
  }

  function pollAlertsOnly() {
    api("/trades/active").then(function (data) {
      handleAlerts(data.trades || []);
    }).catch(function () {});
  }

  $("mloSpot").addEventListener("input", scheduleModalPayoff);

  function payoffSnapshot(key, spot, livePnl, force) {
    var now = Date.now();
    var spotNum = spot != null && spot !== "" && !isNaN(Number(spot)) ? Number(spot) : null;
    var row = payoffClock[key];
    if (!row) {
      payoffClock[key] = { spot: spotNum, livePnl: livePnl, at: now };
      return payoffClock[key];
    }
    if (force || now - row.at >= PAYOFF_REFRESH_MS) {
      if (spotNum != null) row.spot = spotNum;
      row.livePnl = livePnl;
      row.at = now;
    } else if (row.spot == null && spotNum != null) {
      row.spot = spotNum;
      row.at = now;
    }
    if (row.livePnl == null && livePnl != null && !force && now - row.at < PAYOFF_REFRESH_MS) {
      row.livePnl = livePnl;
    }
    return row;
  }

  function paintTradePayoff(trade, force) {
    if (!trade || !window.MultiLegPayoff) return;
    var host = document.querySelector('[data-payoff="' + trade.id + '"]');
    if (!host) return;
    var body = host.closest(".mlo-row-body");
    if (body && body.hidden) return;
    var spot = trade.spot_ltp;
    var snap = payoffSnapshot(String(trade.id), spot, trade.total_pnl, force);
    window.MultiLegPayoff.renderPayoff(host, {
      legs: trade.legs || [],
      spot: snap.spot,
      instrument: trade.instrument,
      livePnl: snap.livePnl,
    });
  }

  function paintExpandedPayoffs(force) {
    activeTrades.forEach(function (trade) {
      if (!expandedTrades[trade.id]) return;
      paintTradePayoff(trade, force);
    });
  }

  function modalClockKey() {
    var inst = $("mloInstrument").value.trim().toUpperCase();
    return "modal:" + (editingId || "new") + ":" + inst;
  }

  function modalLivePnl() {
    if (!editingId) return null;
    var trade = tradeById(editingId);
    return trade ? trade.total_pnl : null;
  }

  function modalSpotCandidate() {
    var inst = $("mloInstrument").value.trim().toUpperCase();
    if (editingId) {
      var trade = tradeById(editingId);
      if (trade && trade.spot_ltp != null && !isNaN(Number(trade.spot_ltp))) return Number(trade.spot_ltp);
    }
    if (lastInstrumentSpot[inst] != null) return lastInstrumentSpot[inst];
    return null;
  }

  function contractKey(inst, strike, right, expiry) {
    return [inst, strike, right, expiry].join("|");
  }

  function ensureFormLots() {
    var inst = $("mloInstrument").value.trim().toUpperCase();
    $("mloLegs").querySelectorAll(".mlo-leg").forEach(function (row) {
      var strike = row.querySelector('[data-f="strike_price"]').value;
      var right = row.querySelector('[data-f="option_type"]').value;
      var expiryEl = row.querySelector('[data-f="leg_expiry_date"]');
      var expiry = (expiryEl && expiryEl.value) || $("mloExpiry").value;
      if (!inst || !strike || !right || !expiry || !(Number(strike) > 0)) return;
      var key = contractKey(inst, strike, right, expiry);
      var lotField = row.querySelector('[data-f="lot_size"]');
      if (row.dataset.lotKey && row.dataset.lotKey !== key) {
        row.dataset.lotSize = "";
        delete row.dataset.lotKey;
        if (lotField && lotField.dataset.auto === "1") lotField.value = "";
      }
      if (lotCache[key]) {
        row.dataset.lotSize = String(lotCache[key]);
        row.dataset.lotKey = key;
        if (lotField && lotField.value === "" && lotField.dataset.auto !== "") {
          lotField.value = String(lotCache[key]);
          lotField.dataset.auto = "1";
        }
        return;
      }
      if (row.dataset.lotSize && !row.dataset.lotKey) {
        lotCache[key] = Number(row.dataset.lotSize);
        row.dataset.lotKey = key;
        return;
      }
      if (row.dataset.lotKey === key && Number(row.dataset.lotSize) > 0) return;
      if (lotMiss[key] === true) return;
      if (typeof lotMiss[key] === "number" && Date.now() < lotMiss[key]) return;
      if (lotFlight[key]) return;
      var path = "/quote?instrument=" + encodeURIComponent(inst) +
        "&strike=" + encodeURIComponent(strike) +
        "&option_type=" + encodeURIComponent(right) +
        "&expiry=" + encodeURIComponent(expiry);
      lotFlight[key] = api(path).then(function (data) {
        var lot = data && data.quote && Number(data.quote.lot_size);
        if (lot > 0) {
          lotCache[key] = lot;
          delete lotMiss[key];
          if (row.dataset.lotKey === key || !row.dataset.lotKey) {
            row.dataset.lotSize = String(lot);
            row.dataset.lotKey = key;
            var field = row.querySelector('[data-f="lot_size"]');
            if (field && field.value === "" && field.dataset.auto !== "") {
              field.value = String(lot);
              field.dataset.auto = "1";
            }
          }
        } else {
          lotMiss[key] = true;
        }
      }).catch(function () {
        lotMiss[key] = Date.now() + 30000;
      }).then(function () {
        delete lotFlight[key];
        if ($("mloModal").hidden) return;
        renderModalPayoff(false);
      });
    });
  }

  function formPayoffLegs() {
    return Array.prototype.map.call($("mloLegs").querySelectorAll(".mlo-leg"), function (row) {
      function val(name) {
        var field = row.querySelector('[data-f="' + name + '"]');
        return field ? field.value : "";
      }
      var exitPx = val("exit_price");
      var exitDay = val("exit_date");
      var exitClock = val("exit_clock");
      var exited = exitPx !== "" && !!exitDay && !!parseClock(exitClock);
      var lotField = row.querySelector('[data-f="lot_size"]');
      var typedLot = val("lot_size").trim();
      var cleared = !!(lotField && lotField.dataset.auto === "" && typedLot === "");
      var lot = typedLot !== "" ? Number(typedLot) : (cleared ? NaN : Number(row.dataset.lotSize));
      return {
        side: val("side"),
        option_type: val("option_type"),
        strike_price: val("strike_price") === "" ? null : Number(val("strike_price")),
        entry_price: val("entry_price") === "" ? null : Number(val("entry_price")),
        lot_size: lot,
        exit_price: exited ? Number(exitPx) : null,
        exit_time: exited ? "set" : null,
      };
    });
  }

  function renderModalPayoff(force) {
    var host = $("mloPayoff");
    if (!host || $("mloModal").hidden || !window.MultiLegPayoff) return;
    ensureFormLots();
    var legs = formPayoffLegs();
    var lotPending = legs.some(function (leg) {
      return leg.strike_price > 0 && leg.entry_price != null && !(leg.lot_size > 0);
    });
    var lotLoading = lotPending && Object.keys(lotFlight).length > 0;
    var liveCandidate = modalSpotCandidate();
    var typed = Number($("mloSpot").value);
    var spot;
    var livePnl;
    if (liveCandidate == null) {
      spot = typed > 0 ? typed : null;
      var fresh = payoffSnapshot(modalClockKey(), null, modalLivePnl(), force);
      livePnl = fresh.livePnl;
    } else {
      var snap = payoffSnapshot(modalClockKey(), liveCandidate, modalLivePnl(), force);
      spot = snap.spot;
      livePnl = snap.livePnl;
    }
    window.MultiLegPayoff.renderPayoff(host, {
      legs: legs,
      spot: spot,
      instrument: $("mloInstrument").value.trim(),
      livePnl: livePnl,
      note: lotPending
        ? (lotLoading
          ? "Lot size is still loading for a leg."
          : "Lot size is unavailable for a leg, so it is left off the curve.")
        : "",
    });
  }

  function scheduleModalPayoff() {
    if (!$("mloPayoff") || $("mloModal").hidden) return;
    clearTimeout(modalPayoffTimer);
    modalPayoffTimer = setTimeout(function () { renderModalPayoff(false); }, 200);
  }

  function refreshPayoffClock() {
    var apply = function () {
      paintExpandedPayoffs(true);
      if (!$("mloModal").hidden) renderModalPayoff(true);
    };
    if (!$("mloModal").hidden) {
      api("/trades/active").then(function (data) {
        (data.trades || []).forEach(function (trade) {
          var current = tradeById(trade.id);
          if (current) {
            current.spot_ltp = trade.spot_ltp;
            current.total_pnl = trade.total_pnl;
          }
          if (trade.spot_ltp != null && !isNaN(Number(trade.spot_ltp))) {
            lastInstrumentSpot[String(trade.instrument || "").toUpperCase()] = Number(trade.spot_ltp);
          }
        });
      }).catch(function () {}).then(apply);
      return;
    }
    activeTrades.forEach(function (trade) {
      if (trade.spot_ltp != null && !isNaN(Number(trade.spot_ltp))) {
        lastInstrumentSpot[String(trade.instrument || "").toUpperCase()] = Number(trade.spot_ltp);
      }
    });
    apply();
  }

  loadInstruments().then(function () { return loadActive(true); });
  pollTimer = setInterval(function () {
    if (tab === "active" && $("mloModal").hidden) loadActive(true);
    else pollAlertsOnly();
  }, 8000);
  payoffTimer = setInterval(refreshPayoffClock, PAYOFF_REFRESH_MS);
})();
