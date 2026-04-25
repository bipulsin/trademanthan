(function () {
  const API_BASE =
    window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
      ? "http://localhost:8000"
      : window.location.origin;

  const POLL_MS = 15000;

  function authHeaders() {
    const t = localStorage.getItem("trademanthan_token") || "";
    return t ? { Authorization: "Bearer " + t } : {};
  }

  function istDateYmd() {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: "Asia/Kolkata",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date());
  }

  async function fetchFirstJson(paths, useAuth) {
    let lastErr = null;
    for (let i = 0; i < paths.length; i += 1) {
      try {
        const res = await fetch(API_BASE + paths[i], {
          cache: "no-store",
          headers: useAuth ? authHeaders() : undefined,
        });
        if (!res.ok) {
          lastErr = new Error("HTTP " + res.status);
          continue;
        }
        return await res.json();
      } catch (e) {
        lastErr = e;
      }
    }
    throw lastErr || new Error("Request failed");
  }

  function toNum(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  function chipHtml(v) {
    if (v == null) return '<span class="pnl-chip pnl-chip-neutral">—</span>';
    const cls = v > 0 ? "pnl-chip-positive" : v < 0 ? "pnl-chip-negative" : "pnl-chip-neutral";
    const sign = v > 0 ? "+" : "";
    return (
      '<span class="pnl-chip ' +
      cls +
      '">' +
      sign +
      "₹" +
      v.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) +
      "</span>"
    );
  }

  function calcDailyFuturesTotals(doc) {
    const rows = (doc && Array.isArray(doc.running) ? doc.running : []);
    let un = 0;
    rows.forEach(function (r) {
      const ltp = toNum(r && r.ltp);
      const ep = toNum(r && r.entry_price);
      const qty = toNum(r && r.lot_size);
      if (ltp == null || ep == null || qty == null) return;
      un += (ltp - ep) * qty;
    });
    const realized = toNum(doc && doc.summary && doc.summary.cumulative_pnl_rupees) || 0;
    return { realized: realized, unrealized: un };
  }

  function calcSmartFuturesTotals(doc) {
    const rows = (doc && Array.isArray(doc.rows) ? doc.rows : []);
    let realized = 0;
    let unrealized = 0;
    rows.forEach(function (r) {
      const st = String((r && r.order_status) || "").trim().toLowerCase();
      if (st === "sold") {
        const rp = toNum(r && r.realized_pnl);
        if (rp != null) realized += rp;
        return;
      }
      if (st !== "bought") return;
      const ltp = toNum(r && r.current_ltp);
      const entry = toNum(r && r.buy_price);
      const lots = toNum(r && r.calculated_lots);
      if (ltp == null || entry == null || lots == null || lots < 1) return;
      const lotSize = toNum(r && r.instrument_lot_size);
      const units = lotSize != null && lotSize > 0 ? lots * lotSize : lots;
      const side = String((r && r.side) || "").trim().toUpperCase();
      const pts = side === "SHORT" ? entry - ltp : ltp - entry;
      unrealized += pts * units;
    });
    return { realized: realized, unrealized: unrealized };
  }

  function calcIntradayTotals(doc) {
    const trades = (doc && Array.isArray(doc.trades) ? doc.trades : []);
    let realized = 0;
    let unrealized = 0;
    trades.forEach(function (t) {
      const pnl = toNum(t && t.pnl);
      if (pnl == null) return;
      const st = String((t && t.status) || "").trim().toLowerCase();
      const isRealized = st === "sold" || st.indexOf("exit") !== -1;
      if (isRealized) realized += pnl;
      else unrealized += pnl;
    });
    return { realized: realized, unrealized: unrealized };
  }

  function cardHtml(label, totals) {
    const total = totals.realized + totals.unrealized;
    return (
      '<article class="pnl-day-item">' +
      "<h4><span>" +
      label +
      "</span>" +
      chipHtml(total) +
      "</h4>" +
      "</article>"
    );
  }

  function renderPnlCards(data) {
    const grid = document.getElementById("dashboardPnlDayGrid");
    const head = document.getElementById("dashboardPnlDayTotal");
    if (!grid || !head) return;
    const intradayTotal = data.intraday.realized + data.intraday.unrealized;
    const smartTotal = data.smart.realized + data.smart.unrealized;
    const dailyTotal = data.daily.realized + data.daily.unrealized;
    const grandTotal = intradayTotal + smartTotal + dailyTotal;
    grid.innerHTML =
      cardHtml("Intraday PnL", data.intraday) +
      cardHtml("Smart Futures PnL", data.smart) +
      cardHtml("Premium Futures PnL", data.daily);
    head.innerHTML = "Total: " + chipHtml(grandTotal);
  }

  async function refreshPnlDay() {
    try {
      const today = encodeURIComponent(istDateYmd());
      const [intradayDoc, smartDoc, dailyDoc] = await Promise.all([
        fetchFirstJson(["/scan/daily-trades/" + today], false),
        fetchFirstJson(["/api/smart-futures/daily", "/smart-futures/daily"], true),
        fetchFirstJson(["/api/daily-futures/workspace", "/daily-futures/workspace"], true),
      ]);
      renderPnlCards({
        intraday: calcIntradayTotals(intradayDoc),
        smart: calcSmartFuturesTotals(smartDoc),
        daily: calcDailyFuturesTotals(dailyDoc),
      });
    } catch (e) {
      const head = document.getElementById("dashboardPnlDayTotal");
      if (head) {
        head.innerHTML = 'Total: <span class="pnl-chip pnl-chip-neutral">Unavailable</span>';
      }
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    refreshPnlDay();
    setInterval(refreshPnlDay, POLL_MS);
  });
})();
