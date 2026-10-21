/* Ladder Report date groups. Sums the PnL each row already shows (total_pnl). */
(function (root, factory) {
  var api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  root.ladderReportGroups = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";

  var MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
  ];
  var MONTHS_SHORT = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

  function ladderTradeDate(trade) {
    var raw = trade && trade.entry_date;
    if (raw == null || String(raw).trim() === "") return "";
    var text = String(raw).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
    var dt = new Date(text);
    if (Number.isNaN(dt.getTime())) return "";
    try {
      var parts = new Intl.DateTimeFormat("en-CA", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
      }).format(dt);
      if (/^\d{4}-\d{2}-\d{2}$/.test(parts)) return parts;
    } catch (e) { /* fall through */ }
    return "";
  }

  function ladderDisplayedPnl(trade) {
    if (!trade) return null;
    var n = trade.total_pnl;
    if (n == null || n === "") return null;
    var num = Number(n);
    if (!Number.isFinite(num)) return null;
    return num;
  }

  function sumDisplayedPnl(values) {
    var total = null;
    for (var i = 0; i < values.length; i++) {
      var n = values[i];
      if (n == null || !Number.isFinite(Number(n))) continue;
      total = (total == null ? 0 : total) + Number(n);
    }
    return total;
  }

  function tradeNo(trade) {
    var n = trade && trade.trade_no;
    if (n == null || n === "" || !Number.isFinite(Number(n))) return null;
    return Number(n);
  }

  function dayLabel(iso) {
    if (!iso) return "—";
    var p = iso.split("-");
    var month = MONTHS_SHORT[Number(p[1]) - 1];
    if (!month || p.length < 3) return iso;
    return Number(p[2]) + " " + month + " " + p[0];
  }

  function monthLabel(key) {
    if (!key) return "Undated";
    var p = key.split("-");
    var month = MONTHS[Number(p[1]) - 1];
    if (!month) return key;
    return month + " " + p[0];
  }

  function groupLadderReport(trades) {
    var indexed = (trades || []).map(function (trade, index) {
      return { trade: trade, index: index };
    });
    indexed.sort(function (a, b) {
      var da = ladderTradeDate(a.trade);
      var db = ladderTradeDate(b.trade);
      if (da !== db) {
        if (!da) return 1;
        if (!db) return -1;
        return da < db ? 1 : -1;
      }
      var na = tradeNo(a.trade);
      var nb = tradeNo(b.trade);
      if (na !== nb) {
        if (na == null) return 1;
        if (nb == null) return -1;
        return nb - na;
      }
      return a.index - b.index;
    });

    var days = [];
    var byDate = {};
    indexed.forEach(function (row) {
      var date = ladderTradeDate(row.trade);
      if (!byDate[date]) {
        byDate[date] = {
          date: date,
          label: dayLabel(date),
          trades: []
        };
        days.push(byDate[date]);
      }
      byDate[date].trades.push(row.trade);
    });
    days.forEach(function (day) {
      day.pnl = sumDisplayedPnl(day.trades.map(ladderDisplayedPnl));
    });

    var months = [];
    var byMonth = {};
    days.forEach(function (day) {
      var key = day.date ? day.date.slice(0, 7) : "";
      if (!byMonth[key]) {
        byMonth[key] = { key: key, label: monthLabel(key), days: [] };
        months.push(byMonth[key]);
      }
      byMonth[key].days.push(day);
    });
    months.forEach(function (month) {
      month.pnl = sumDisplayedPnl(month.days.map(function (day) { return day.pnl; }));
    });
    return { months: months };
  }

  return {
    ladderTradeDate: ladderTradeDate,
    ladderDisplayedPnl: ladderDisplayedPnl,
    sumDisplayedPnl: sumDisplayedPnl,
    formatLadderDay: dayLabel,
    groupLadderReport: groupLadderReport
  };
});
