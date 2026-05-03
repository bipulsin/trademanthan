(function () {
  var API_BASE =
    window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
      ? "http://localhost:8000"
      : window.location.origin;

  function authHeaders() {
    var t = localStorage.getItem("trademanthan_token") || "";
    return {
      Authorization: "Bearer " + t,
      "Content-Type": "application/json",
      Accept: "application/json",
    };
  }

  async function fetchJson(paths, opts) {
    var lastErr;
    for (var i = 0; i < paths.length; i++) {
      try {
        var r = await fetch(API_BASE + paths[i], opts);
        if (r.ok) return await r.json();
        lastErr = new Error(paths[i] + " HTTP " + r.status);
      } catch (e) {
        lastErr = e;
      }
    }
    throw lastErr || new Error("Request failed");
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function gateClass(g) {
    var u = String(g || "").toUpperCase();
    if (u === "VALID") return "ic-gate-valid";
    if (u === "WARN") return "ic-gate-warn";
    if (u === "BLOCK") return "ic-gate-block";
    return "";
  }

  var lastAnalysis = null;

  function renderAnalysis(a) {
    var el = document.getElementById("icAnalysis");
    if (!a) {
      el.innerHTML = "";
      return;
    }
    var g = a.hedge_gate || "";
    el.innerHTML =
      '<div class="ic-grid">' +
      "<div><strong>Spot</strong><br>" +
      esc(a.spot) +
      "</div>" +
      "<div><strong>Monthly ATR(14)</strong><br>" +
      esc(a.monthly_atr_14) +
      "</div>" +
      "<div><strong>Strike distance (1.25×ATR)</strong><br>" +
      esc(a.strike_distance) +
      "</div>" +
      "<div><strong>Expiry</strong><br>" +
      esc(a.expiry_date) +
      "</div>" +
      "</div>" +
      "<p><strong>Strikes</strong></p>" +
      '<div class="ic-pre">' +
      "Sell CE: " +
      a.strikes.sell_call +
      " | Buy CE: " +
      a.strikes.buy_call +
      "\n" +
      "Sell PE: " +
      a.strikes.sell_put +
      " | Buy PE: " +
      a.strikes.buy_put +
      "\nPremiums collected (shorts): " +
      a.premium_collected +
      "\nHedge cost (longs): " +
      a.hedge_cost +
      "\nHedge ratio: " +
      a.hedge_ratio +
      "</div>" +
      '<p class="' +
      gateClass(g) +
      '">' +
      esc(g) +
      " — " +
      esc(a.hedge_gate_message) +
      "</p>" +
      '<p class="ic-muted">' +
      esc((a.position_sizing && a.position_sizing.reject_reason) || "") +
      "</p>" +
      "<p>Suggested allocation: " +
      esc((a.position_sizing && a.position_sizing.suggested_allocation_pct) || "—") +
      "% ⇒ ₹" +
      esc((a.position_sizing && a.position_sizing.suggested_capital_rupees) || "—") +
      "</p>" +
      '<button type="button" class="ic-btn" id="icSavePosition">Save advisory snapshot</button> ' +
      '<span class="ic-muted">' +
      esc(a.disclaimer) +
      "</span>";
    var btn = document.getElementById("icSavePosition");
    if (btn)
      btn.onclick = function () {
        savePosition();
      };
  }

  async function loadUniverse() {
    var sel = document.getElementById("icSymbol");
    try {
      var j = await fetchJson(["/api/iron-condor/universe", "/iron-condor/universe"], {
        headers: authHeaders(),
        cache: "no-store",
      });
      var syms = (j && j.symbols) || [];
      sel.innerHTML =
        '<option value="">Select symbol…</option>' +
        syms
          .map(function (x) {
            return '<option value="' + esc(x.symbol) + '">' + esc(x.symbol + " — " + x.sector) + "</option>";
          })
          .join("");
    } catch (e) {
      sel.innerHTML = '<option value="">Failed to load universe</option>';
    }
  }

  async function loadWorkspace() {
    var note = document.getElementById("icSettingsNote");
    var body = document.getElementById("icPosBody");
    try {
      var w = await fetchJson(["/api/iron-condor/workspace", "/iron-condor/workspace"], {
        headers: authHeaders(),
        cache: "no-store",
      });
      var st = (w && w.settings) || {};
      document.getElementById("icCapital").value = st.trading_capital != null ? st.trading_capital : "";
      document.getElementById("icSlots").value = st.target_position_slots != null ? st.target_position_slots : 5;
      document.getElementById("icPt").value =
        st.profit_target_pct_of_credit != null ? st.profit_target_pct_of_credit : "";
      document.getElementById("icSl").value =
        st.stop_loss_pct_of_credit != null ? st.stop_loss_pct_of_credit : "";

      var rows = (w && w.positions) || [];
      if (!rows.length)
        body.innerHTML = '<tr><td colspan="9" class="ic-muted">No saved positions.</td></tr>';
      else {
        body.innerHTML = rows
          .map(function (r) {
            var isOpen = String(r.status).toUpperCase() === "OPEN";
            var id = r.id;
            return (
              "<tr>" +
              "<td>" +
              esc(r.underlying) +
              "</td>" +
              "<td>" +
              esc(r.sector) +
              "</td>" +
              '<td class="num">' +
              esc(r.sell_call_strike) +
              "</td>" +
              '<td class="num">' +
              esc(r.buy_call_strike) +
              "</td>" +
              '<td class="num">' +
              esc(r.sell_put_strike) +
              "</td>" +
              '<td class="num">' +
              esc(r.buy_put_strike) +
              "</td>" +
              "<td>" +
              esc(r.hedge_gate) +
              "</td>" +
              "<td>" +
              esc(r.expiry_date) +
              "</td>" +
              "<td>" +
              (isOpen
                ? '<button type="button" class="ic-btn ic-eval" data-id="' +
                  id +
                  '" style="font-size:0.75rem;margin-right:6px;padding:4px 8px">Alerts</button>' +
                  '<button type="button" class="ic-btn ic-close" data-id="' +
                  id +
                  '" style="font-size:0.75rem;padding:4px 8px;background:#b45309">Close</button>'
                : "") +
              "</td>" +
              "</tr>"
            );
          })
          .join("");
      }

      body.querySelectorAll("button.ic-eval").forEach(function (b) {
        b.onclick = function () {
          evalAlerts(Number(b.getAttribute("data-id")));
        };
      });
      body.querySelectorAll("button.ic-close").forEach(function (b) {
        b.onclick = function () {
          closePos(Number(b.getAttribute("data-id")));
        };
      });

      var alerts = (w && w.alerts) || [];
      document.getElementById("icAlerts").innerHTML = alerts.length
        ? alerts
            .slice(0, 25)
            .map(function (a) {
              return (
                '<div style="margin-bottom:6px"><strong>' +
                esc(a.created_at) +
                "</strong> [" +
                esc(a.rule_code) +
                "] " +
                esc(a.message) +
                "</div>"
              );
            })
            .join("")
        : "No alerts yet.";
      note.textContent = "";
    } catch (e) {
      note.textContent = "Could not load workspace (check login).";
      body.innerHTML = '<tr><td colspan="9">Error</td></tr>';
    }
  }

  async function saveSettings() {
    var cap = parseFloat(document.getElementById("icCapital").value);
    var slots = parseInt(document.getElementById("icSlots").value, 10);
    var pt = document.getElementById("icPt").value;
    var sl = document.getElementById("icSl").value;
    var body = {};
    if (!Number.isNaN(cap) && cap >= 0) body.trading_capital = cap;
    if (!Number.isNaN(slots)) body.target_position_slots = slots;
    if (pt !== "") body.profit_target_pct_of_credit = parseFloat(pt);
    if (sl !== "") body.stop_loss_pct_of_credit = parseFloat(sl);
    try {
      await fetchJson(["/api/iron-condor/settings", "/iron-condor/settings"], {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify(body),
      });
      document.getElementById("icSettingsNote").textContent = "Settings saved.";
      loadWorkspace();
    } catch (e) {
      document.getElementById("icSettingsNote").textContent = "Save failed.";
    }
  }

  async function analyze() {
    var u = document.getElementById("icSymbol").value.trim();
    if (!u) return;
    document.getElementById("icAnalyze").disabled = true;
    try {
      var j = await fetchJson(["/api/iron-condor/analyze", "/iron-condor/analyze"], {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ underlying: u }),
      });
      lastAnalysis = j.analysis;
      renderAnalysis(lastAnalysis);
    } catch (e) {
      lastAnalysis = null;
      document.getElementById("icAnalysis").innerHTML =
        '<p class="ic-gate-block">Analyze failed — ' + esc(String(e.message || e)) + "</p>";
    } finally {
      document.getElementById("icAnalyze").disabled = false;
    }
  }

  async function savePosition() {
    if (!lastAnalysis) return;
    try {
      await fetchJson(["/api/iron-condor/positions", "/iron-condor/positions"], {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ analysis: lastAnalysis }),
      });
      document.getElementById("icAnalysis").appendChild(document.createElement("p")).textContent =
        "Saved to your workspace.";
      loadWorkspace();
    } catch (e) {
      alert("Save failed: " + (e.message || e));
    }
  }

  async function evalAlerts(id) {
    try {
      var j = await fetchJson(
        ["/api/iron-condor/positions/" + id + "/evaluate-alerts", "/iron-condor/positions/" + id + "/evaluate-alerts"],
        { method: "POST", headers: authHeaders(), body: "{}" },
      );
      alert("New alerts: " + (((j.new_alerts && j.new_alerts.length) || 0) ? JSON.stringify(j.new_alerts, null, 2) : "none"));
      loadWorkspace();
    } catch (e) {
      alert("Evaluate failed: " + (e.message || e));
    }
  }

  async function closePos(id) {
    if (!confirm("Mark this advisory position closed?")) return;
    try {
      await fetchJson(["/api/iron-condor/positions/" + id + "/close", "/iron-condor/positions/" + id + "/close"], {
        method: "POST",
        headers: authHeaders(),
        body: "{}",
      });
      loadWorkspace();
    } catch (e) {
      alert("Close failed: " + (e.message || e));
    }
  }

  document.getElementById("icSaveSettings").onclick = saveSettings;
  document.getElementById("icAnalyze").onclick = analyze;

  loadUniverse().then(loadWorkspace);
})();
