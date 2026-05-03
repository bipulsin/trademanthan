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

  async function fj(paths, opts) {
    var err;
    for (var i = 0; i < paths.length; i++) {
      try {
        var r = await fetch(API_BASE + paths[i], opts);
        var j = r.ok ? await r.json() : null;
        if (r.ok) return j;
        err = paths[i] + " " + r.status;
      } catch (e) {
        err = e.message || String(e);
      }
    }
    throw new Error(err || "fetch failed");
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  var state = {
    symbol: "",
    detailed: null,
    checklist: null,
    pollTimer: null,
  };

  function showPane(n) {
    document.querySelectorAll("[data-pane]").forEach(function (el) {
      el.style.display = el.getAttribute("data-pane") === String(n) ? "block" : "none";
    });
    document.querySelectorAll(".ic-step-pill").forEach(function (b) {
      b.setAttribute("data-active", b.getAttribute("data-step") === String(n) ? "1" : "0");
    });
  }

  function playRedSound() {
    try {
      var ctx = new (window.AudioContext || window.webkitAudioContext)();
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.connect(g);
      g.connect(ctx.destination);
      o.frequency.value = 880;
      o.type = "sine";
      g.gain.value = 0.05;
      o.start();
      setTimeout(function () {
        o.stop();
      }, 180);
    } catch (_e) {}
  }

  function renderAlertsBar(alerts) {
    var host = document.getElementById("alertStack");
    if (!host || !alerts || !alerts.length) {
      host.innerHTML = "";
      return;
    }
    var html = alerts
      .filter(function (a) {
        return !a.acknowledged;
      })
      .slice(0, 15)
      .map(function (a) {
        var sev = a.severity || "default";
        if (!a.severity && /STOP|CRITICAL/i.test(String(a.rule_code || a.alert_type || "")))
          sev = "RED";
        var cls = "ic-sev-" + (sev || "default");
        var id = a.id;
        return (
          '<div class="ic-alert-bar ' +
          cls +
          '">' +
          "<span>" +
          esc(a.message || "") +
          '</span><span style="opacity:0.85;font-weight:600;">' +
          esc(a.rule_code || a.alert_type || "") +
          "</span>" +
          (id ? '<button type="button" class="ic-btn ic-btn-ghost" data-aid="' + id + '">Ack</button>' : "")
        );
      })
      .join("");
    host.innerHTML = html;
    host.querySelectorAll("button[data-aid]").forEach(function (btn) {
      btn.onclick = function () {
        ackAlert(Number(btn.getAttribute("data-aid")));
      };
    });
    alerts.some(function (a) {
      if (!a.severity) return String(a.rule_code || "").indexOf("STOP") >= 0;
      return /^RED|CRITICAL/.test(String(a.severity || ""));
    }) && playRedSound();
  }

  async function ackAlert(id) {
    await fj(["/api/iron-condor/alerts/" + id + "/acknowledge", "/iron-condor/alerts/" + id + "/acknowledge"], {
      method: "POST",
      headers: authHeaders(),
      body: "{}",
    }).catch(function () {});
    refreshWorkspaceQuiet();
  }

  async function loadSessionLine() {
    try {
      var s = await fj(["/api/iron-condor/session", "/iron-condor/session"], { headers: authHeaders(), cache: "no-store" });
      document.getElementById("sessionLine").textContent =
        (s.market_poll_active ? "Session: IST market window (polling eligible)." : s.banner) || "";
    } catch (_) {
      document.getElementById("sessionLine").textContent = "Session unavailable";
    }
  }

  async function loadPicker() {
    var tbody = document.getElementById("pickerBody");
    tbody.innerHTML = "<tr><td colspan='6'>Loading…</td></tr>";
    try {
      var u = await fj(["/api/iron-condor/universe-with-quotes", "/iron-condor/universe-with-quotes"], {
        headers: authHeaders(),
        cache: "no-store",
      });
      tbody.innerHTML = (u.symbols || [])
        .map(function (row) {
          var act = row.active_position ? "Yes" : "—";
          var warn = row.active_position ? '<span class="ic-chip-warn">Active</span>' : "";
          return (
            "<tr data-sym=\"" +
            esc(row.symbol) +
            "\">" +
            "<td><strong>" +
            esc(row.symbol) +
            "</strong>" +
            warn +
            "</td>" +
            "<td><span class='ic-chip-pass'>" +
            esc(row.sector) +
            "</span></td>" +
            "<td>" +
            (row.ltp != null ? Number(row.ltp).toFixed(2) : "—") +
            "</td>" +
            "<td>" +
            (row.change_pct_day != null ? Number(row.change_pct_day).toFixed(2) + "%" : "—") +
            "</td>" +
            "<td>" +
            esc(act) +
            "</td>" +
            "<td><button type='button' class='ic-btn ic-btn-ghost pickRow'>Analyze</button></td>"
          );
        })
        .join("");
      tbody.querySelectorAll("button.pickRow").forEach(function (b) {
        b.onclick = function () {
          var tr = b.closest("tr");
          state.symbol = tr.getAttribute("data-sym") || "";
          document.getElementById("gotoChecklistBtn").disabled = false;
          tr.style.outline = "2px solid #2563eb";
        };
      });
    } catch (e) {
      tbody.innerHTML = "<tr><td colspan='6'>Failed: " + esc(e.message) + "</td></tr>";
    }
  }

  function chipCls(st) {
    if (st === "PASS") return "ic-chip-pass";
    if (st === "FAIL") return "ic-chip-fail";
    if (st === "WARN") return "ic-chip-warn";
    return "ic-chip-info";
  }

  async function runChecklist() {
    if (!state.symbol) return;
    var capEst = Number(document.getElementById("icCapital").value) || 0;
    var pct = Number(document.getElementById("icSlots").value) >= 5 ? 3 : 5;
    var estimate = capEst > 0 ? (capEst * pct) / 100 : 0;
    var j = await fj(["/api/iron-condor/checklist", "/iron-condor/checklist"], {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ underlying: state.symbol, new_capital_estimate: estimate }),
    }).catch(function (e) {
      throw e;
    });
    state.checklist = j;
    var chips = j.chips || [];
    document.getElementById("checklistArea").innerHTML =
      chips
        .map(function (c) {
          return "<div style='margin:6px 0'><span class='" + chipCls(c.status) + "'>" + esc(c.code) + " · " + esc(c.status) + "</span> " + esc(c.message) + "</div>";
        })
        .join("") || "—";

    document.getElementById("toStrikesBtn").disabled = !!(j.may_proceed_blocked);
  }

  function fmtLeg(l) {
    if (!l) return "—";
    var bd = l.bid != null ? Number(l.bid).toFixed(2) : "—";
    var ak = l.ask != null ? Number(l.ask).toFixed(2) : "—";
    return Number(l.ltp || 0).toFixed(2) + " (Bid/Ask: " + bd + "/" + ak + ")";
  }

  async function analyzeDetailed() {
    if (!state.symbol) throw new Error("No symbol");
    var j = await fj(["/api/iron-condor/analyze-detailed", "/iron-condor/analyze-detailed"], {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ underlying: state.symbol }),
    });
    state.detailed = j.analysis;
    var a = state.detailed;
    var econ = a.economics || {};
    var lq = a.legs_quote || {};
    var hk = econ.hedge_gate_color === "GREEN" ? "#22c55e" : econ.hedge_gate_color === "YELLOW" ? "#eab308" : "#ef4444";
    document.getElementById("strikeCard").innerHTML =
      "<p><strong>" +
      esc(a.underlying) +
      "</strong> · Spot ₹" +
      esc(a.live && a.live.spot_ltp) +
      (a.live && a.live.underlying_change_pct_today != null ? " · Day " + a.live.underlying_change_pct_today + "%" : "") +
      " · Sector " +
      esc(a.sector) +
      "</p>" +
      "<p>Monthly ATR(14): ₹" +
      esc(a.monthly_atr_14) +
      " · Strike distance: ₹" +
      esc(a.strike_distance) +
      "</p>" +
      "<hr style='border-color:#334155'/>" +
      "<p><strong>SELL (short strangle)</strong></p>" +
      "<p>Sell Call: " +
      a.strikes.sell_call +
      " CE @ " +
      fmtLeg(lq.sell_call) +
      "</p>" +
      "<p>Sell Put: " +
      a.strikes.sell_put +
      " PE @ " +
      fmtLeg(lq.sell_put) +
      "</p>" +
      "<p><strong>BUY (hedge)</strong></p>" +
      "<p>Buy Call: " +
      a.strikes.buy_call +
      " CE @ " +
      fmtLeg(lq.buy_call) +
      " · OI max in 5–6 step range</p>" +
      "<p>Buy Put: " +
      a.strikes.buy_put +
      " PE @ " +
      fmtLeg(lq.buy_put) +
      "</p>" +
      "<p><strong>Economics (× lot qty)</strong></p>" +
      "<p>Premium collected (pts): " +
      esc(econ.premium_collected_pts) +
      " · Hedge cost (pts): " +
      esc(econ.hedge_cost_pts) +
      " · Net credit (pts): " +
      esc(econ.net_credit_pts) +
      "</p>" +
      "<p>Hedge ratio " +
      Number(a.hedge_ratio).toFixed(2) +
      " — <strong style='color:" +
      hk +
      "'>" +
      esc(a.hedge_gate) +
      "</strong></p>" +
      "<p>Max profit est ₹" +
      esc(econ.max_profit_rupees_est) +
      " · Max loss est ₹" +
      esc(econ.max_loss_rupees_est) +
      "</p>" +
      "<p>R:R proxy " +
      (econ.risk_reward_net_to_max_loss != null ? econ.risk_reward_net_to_max_loss : "—") +
      "</p>" +
      "<p>Breakeven range: ₹" +
      econ.breakeven_lower +
      " ↔ ₹" +
      econ.breakeven_upper +
      "</p>";

    document.getElementById("fsc").value = a.premiums.sell_call || "";
    document.getElementById("fbc").value = a.premiums.buy_call || "";
    document.getElementById("fsp").value = a.premiums.sell_put || "";
    document.getElementById("fbp").value = a.premiums.buy_put || "";
  }

  async function confirmEntrySave() {
    var a = state.detailed;
    if (!a) return;
    await fj(["/api/iron-condor/confirm-entry", "/iron-condor/confirm-entry"], {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        analysis: a,
        fills: {
          sell_call_fill: Number(document.getElementById("fsc").value),
          buy_call_fill: Number(document.getElementById("fbc").value),
          sell_put_fill: Number(document.getElementById("fsp").value),
          buy_put_fill: Number(document.getElementById("fbp").value),
        },
        lot_size: (state.detailed && state.detailed.economics && state.detailed.economics.lot_size) || null,
        num_lots: Number(document.getElementById("flots").value) || 1,
      }),
    });
    showPane(5);
    refreshWorkspaceQuiet();
    alert("Stored as ACTIVE. Roll 4 legs on Upstox as advised.");
  }

  async function refreshWorkspaceQuiet() {
    var w = await fj(["/api/iron-condor/workspace", "/iron-condor/workspace"], { headers: authHeaders(), cache: "no-store" });
    renderAlertsBar(w.alerts || []);

    var d = w.dashboard || {};
    document.getElementById("kpiDash").innerHTML =
      "<div>Capital ₹" +
      Number(d.trading_capital || 0).toFixed(0) +
      "</div>" +
      "<div>Deployed ₹" +
      Number(d.deployed_capital_rupees || 0).toFixed(0) +
      " (" +
      (d.deployed_pct != null ? d.deployed_pct + "%)" : "") +
      "</div>" +
      "<div>Open MTM est ₹" +
      Number(d.open_mtm_sum_rupees || 0).toFixed(0) +
      "</div>" +
      "<div>This month ₹" +
      Number(d.realized_month_rupees || 0).toFixed(0) +
      "</div>" +
      "<div>YTD ₹" +
      Number(d.realized_year_rupees || 0).toFixed(0) +
      "</div>" +
      "<div>Avail est ₹" +
      (d.capital_available_est != null ? Number(d.capital_available_est).toFixed(0) : "—") +
      "</div>";

    var pos = (w.positions || []).filter(function (p) {
      return String(p.status).toUpperCase() !== "CLOSED";
    });
    document.getElementById("posCards").innerHTML = pos.length
      ? pos
          .map(function (p) {
            var h = esc(p.position_health || "");
            return (
              '<div class="ic-pos-card" data-h="' +
              esc(p.position_health) +
              '"><strong>' +
              esc(p.underlying) +
              "</strong> · " +
              esc(p.sector) +
              "<div class=\"ic-pos-detail\">DTE/expiry · " +
              esc(p.expiry_date) +
              "</div><div class=\"ic-pos-detail\">SL call ref ₹" +
              esc(p.stop_sl_call_px) +
              " · SL put ₹" +
              esc(p.stop_sl_put_px) +
              "</div><div class=\"ic-pos-detail\">Health: <strong>" +
              h +
              "</strong></div></div>"
            );
          })
          .join("")
      : "<span class=\"ic-muted\">No active positions.</span>";

    var sel = document.getElementById("closePick");
    sel.innerHTML =
      '<option value="">—</option>' +
      (w.positions || [])
        .filter(function (p) {
          return String(p.status).toUpperCase() !== "CLOSED";
        })
        .map(function (p) {
          return '<option value="' + esc(p.id) + '">' + esc(p.underlying + " #" + p.id) + "</option>";
        })
        .join("");
  }

  async function pollTick() {
    try {
      var w = await fj(["/api/iron-condor/session", "/iron-condor/session"], { headers: authHeaders(), cache: "no-store" });
      if (!w.market_poll_active) return;
      await fj(["/api/iron-condor/poll", "/iron-condor/poll"], { method: "POST", headers: authHeaders(), body: "{}" });
      await refreshWorkspaceQuiet();
    } catch (_e) {}
  }

  function startPolling() {
    if (state.pollTimer) clearInterval(state.pollTimer);
    state.pollTimer = setInterval(function () {
      pollTick();
    }, 5 * 60 * 1000);
    pollTick();
  }

  document.querySelectorAll(".ic-step-pill").forEach(function (pill) {
    pill.onclick = function () {
      showPane(Number(pill.getAttribute("data-step")));
    };
  });

  document.getElementById("gotoChecklistBtn").onclick = async function () {
    showPane(2);
    try {
      await runChecklist();
    } catch (e) {
      document.getElementById("checklistArea").innerHTML = "Error " + esc(e.message);
    }
  };

  document.getElementById("toStrikesBtn").onclick = async function () {
    if (
      state.checklist &&
      state.checklist.warnings_require_ack &&
      !document.getElementById("warnAck").checked
    ) {
      alert("Check the acknowledgment box for WARN items.");
      return;
    }
    showPane(3);
    document.getElementById("strikeCard").textContent = "Computing strikes…";
    try {
      await analyzeDetailed();
    } catch (e) {
      document.getElementById("strikeCard").textContent = "Error: " + e.message;
    }
  };

  document.getElementById("back1").onclick = function () {
    showPane(1);
  };
  document.getElementById("back2").onclick = function () {
    showPane(2);
  };
  document.getElementById("toConfirmBtn").onclick = function () {
    showPane(4);
  };
  document.getElementById("back3").onclick = function () {
    showPane(3);
  };
  document.getElementById("confirmEntryBtn").onclick = async function () {
    try {
      await confirmEntrySave();
    } catch (e) {
      alert(e.message);
    }
  };

  document.getElementById("icSaveCap").onclick = async function () {
    var cap = Number(document.getElementById("icCapital").value);
    var slots = Number(document.getElementById("icSlots").value);
    await fj(["/api/iron-condor/settings", "/iron-condor/settings"], {
      method: "PUT",
      headers: authHeaders(),
      body: JSON.stringify({
        trading_capital: Number.isFinite(cap) ? cap : undefined,
        target_position_slots: Number.isFinite(slots) ? slots : undefined,
      }),
    }).catch(function () {});
  };

  document.getElementById("journalCloseBtn").onclick = async function () {
    var pid = Number(document.getElementById("closePick").value);
    if (!pid) return alert("Pick position.");
    await fj(["/api/iron-condor/close-with-journal", "/iron-condor/close-with-journal"], {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        position_id: pid,
        exit_reason: document.getElementById("jxReason").value,
        emotion: document.getElementById("jxEmo").value,
        followed_rules: document.getElementById("jxFollow").checked,
        deviation_notes: document.getElementById("jxDev").value,
        lesson_learned: document.getElementById("jxLes").value,
        exit_fills: {
          sell_call_exit: Number(document.getElementById("xsc").value),
          buy_call_exit: Number(document.getElementById("xbc").value),
          sell_put_exit: Number(document.getElementById("xsp").value),
          buy_put_exit: Number(document.getElementById("xbp").value),
        },
      }),
    });
    alert("Saved.");
    refreshWorkspaceQuiet();
  };

  fj(["/api/iron-condor/workspace", "/iron-condor/workspace"], { headers: authHeaders() })
    .then(function (w) {
      var st = (w && w.settings) || {};
      if (st.trading_capital != null) document.getElementById("icCapital").value = st.trading_capital;
      if (st.target_position_slots != null) document.getElementById("icSlots").value = st.target_position_slots;
    })
    .catch(function () {})
    .finally(function () {
      loadSessionLine();
      loadPicker();
      refreshWorkspaceQuiet();
      startPolling();
      showPane(1);
    });
})();
