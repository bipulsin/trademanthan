(function () {
  "use strict";

  function apiBase() {
    var h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }

  function headers() {
    var t = localStorage.getItem("trademanthan_token") || "";
    return { "Content-Type": "application/json", Authorization: "Bearer " + t };
  }

  var LABELS = {
    commdiv: "CommDiv",
    stock_options: "Stock Options",
    multi_leg_options: "Multi-Leg Options",
    kavach: "Kavach",
    breakfast: "Breakfast",
    premium_futures: "Premium Futures",
    tarang: "Kosmic Tarang"
  };

  function $(id) { return document.getElementById(id); }

  function renderAlgos(algos) {
    var host = $("tickerAlgos");
    if (!host) return;
    host.innerHTML = Object.keys(LABELS).map(function (k) {
      var on = !algos || algos[k] !== false;
      return '<label style="display:flex;gap:8px;align-items:center;"><input type="checkbox" data-algo="' + k + '"' +
        (on ? " checked" : "") + " /> " + LABELS[k] + "</label>";
    }).join("");
  }

  function collectAlgos() {
    var out = {};
    document.querySelectorAll("#tickerAlgos input[data-algo]").forEach(function (el) {
      out[el.getAttribute("data-algo")] = el.checked;
    });
    return out;
  }

  async function load() {
    if (!$("tickerEnabled")) return;
    var res = await fetch(apiBase() + "/api/ticker/settings", { headers: headers() });
    if (!res.ok) return;
    var data = await res.json();
    $("tickerEnabled").checked = data.enabled !== false;
    renderAlgos(data.algos || {});
  }

  async function save() {
    var res = await fetch(apiBase() + "/api/ticker/settings", {
      method: "PUT",
      headers: headers(),
      body: JSON.stringify({ enabled: $("tickerEnabled").checked, algos: collectAlgos() })
    });
    var data = await res.json().catch(function () { return {}; });
    $("tickerTokenOut").textContent = res.ok ? "Ticker settings saved." : (data.detail || "Save failed");
  }

  function detailText(data, fallback) {
    var d = data && data.detail;
    if (Array.isArray(d)) {
      return d.map(function (x) { return (x && x.msg) || String(x); }).join("; ");
    }
    if (d && typeof d !== "string") return JSON.stringify(d);
    return d || fallback;
  }

  async function mint() {
    var btn = $("tickerNewToken");
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Creating…";
    }
    try {
      var res = await fetch(apiBase() + "/api/ticker/token", { method: "POST", headers: headers() });
      var data = await res.json().catch(function () { return {}; });
      if (!res.ok || !data.token) {
        $("tickerTokenOut").textContent = detailText(data, "Could not create token (HTTP " + res.status + ")");
        return;
      }
      $("tickerTokenOut").textContent = "Copy this into the Mac app now. It will not be shown again: " + data.token;
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = "Create app token";
      }
    }
  }

  async function revoke() {
    var res = await fetch(apiBase() + "/api/ticker/token", { method: "DELETE", headers: headers() });
    var data = await res.json().catch(function () { return {}; });
    $("tickerTokenOut").textContent = res.ok
      ? ("Revoked " + (data.revoked || 0) + " token(s).")
      : (data.detail || "Revoke failed");
  }

  document.addEventListener("DOMContentLoaded", function () {
    load().catch(function () {});
    var saveBtn = $("tickerSave");
    if (saveBtn) saveBtn.addEventListener("click", function () { save().catch(function (e) { $("tickerTokenOut").textContent = String(e); }); });
    var mintBtn = $("tickerNewToken");
    if (mintBtn) mintBtn.addEventListener("click", function () { mint().catch(function (e) { $("tickerTokenOut").textContent = String(e); }); });
    var revBtn = $("tickerRevoke");
    if (revBtn) revBtn.addEventListener("click", function () { revoke().catch(function (e) { $("tickerTokenOut").textContent = String(e); }); });
  });
})();
