const test = require("node:test");
const assert = require("node:assert/strict");
const g = require("./public/ladderReportGroups.js");

test("groups by entry date, newest first, trade number descending", () => {
  const grouped = g.groupLadderReport([
    { id: "a", entry_date: "2026-10-20", trade_no: 1, total_pnl: 10 },
    { id: "b", entry_date: "2026-10-21", trade_no: 2, total_pnl: null },
    { id: "c", entry_date: "2026-10-21", trade_no: 5, total_pnl: 7 },
    { id: "d", entry_date: "2026-09-30", trade_no: 9, total_pnl: 3 },
    { id: "e", entry_date: "2026-10-20", trade_no: 4, total_pnl: 0 },
  ]);
  assert.deepEqual(grouped.months.map((m) => m.key), ["2026-10", "2026-09"]);
  assert.equal(grouped.months[0].label, "October 2026");
  assert.deepEqual(grouped.months[0].days.map((d) => d.date), ["2026-10-21", "2026-10-20"]);
  assert.equal(grouped.months[0].days[0].label, "21 Oct 2026");
  assert.deepEqual(grouped.months[0].days[0].trades.map((t) => t.id), ["c", "b"]);
  assert.equal(grouped.months[0].days[0].pnl, 7);
  assert.equal(grouped.months[0].days[1].pnl, 10);
  assert.equal(grouped.months[0].pnl, 17);
  assert.equal(grouped.months[1].label, "September 2026");
  assert.equal(grouped.months[1].pnl, 3);
});

test("omits a missing row PnL and shows no daily or monthly total when every trade is unpriced", () => {
  const grouped = g.groupLadderReport([
    { entry_date: "2026-10-02", trade_no: 2, total_pnl: null },
    { entry_date: "2026-10-02", trade_no: 1, total_pnl: "" },
    { entry_date: "2026-10-02", trade_no: 3, total_pnl: "nope" },
  ]);
  assert.equal(grouped.months[0].days[0].pnl, null);
  assert.equal(grouped.months[0].pnl, null);
});

test("a day with no PnL does not add zero to the monthly sum", () => {
  const grouped = g.groupLadderReport([
    { entry_date: "2026-10-03", trade_no: 2, total_pnl: 8 },
    { entry_date: "2026-10-02", trade_no: 1, total_pnl: null },
  ]);
  assert.equal(grouped.months[0].days[1].pnl, null);
  assert.equal(grouped.months[0].pnl, 8);
});

test("a calendar date is kept and a timestamp uses the IST date", () => {
  const dated = g.groupLadderReport([
    { entry_date: "2026-10-01", trade_no: 1, total_pnl: 1 },
  ]);
  assert.equal(dated.months[0].days[0].date, "2026-10-01");

  const stamped = g.groupLadderReport([
    { entry_date: "2026-09-30T20:00:00Z", trade_no: 1, total_pnl: 4 },
  ]);
  assert.equal(stamped.months[0].key, "2026-10");
  assert.equal(stamped.months[0].days[0].date, "2026-10-01");
  assert.equal(g.ladderDisplayedPnl({ total_pnl: 4 }), 4);
  assert.equal(g.ladderDisplayedPnl({ total_pnl: null }), null);
});

test("equal trade numbers keep their original order", () => {
  const rows = [
    { id: "first", entry_date: "2026-10-21", trade_no: 3, total_pnl: 1 },
    { id: "high", entry_date: "2026-10-21", trade_no: 8, total_pnl: 1 },
    { id: "second", entry_date: "2026-10-21", trade_no: 3, total_pnl: 1 },
  ];
  const grouped = g.groupLadderReport(rows);
  assert.deepEqual(grouped.months[0].days[0].trades.map((t) => t.id), ["high", "first", "second"]);
  assert.deepEqual(rows.map((r) => r.id), ["first", "high", "second"]);
});

test("undated trades sit after dated months", () => {
  const grouped = g.groupLadderReport([
    { id: "none", entry_date: null, trade_no: 4, total_pnl: 2 },
    { id: "oct", entry_date: "2026-10-21", trade_no: 1, total_pnl: 5 },
  ]);
  assert.deepEqual(grouped.months.map((m) => m.label), ["October 2026", "Undated"]);
  assert.equal(grouped.months[1].days[0].label, "—");
  assert.equal(grouped.months[1].pnl, 2);
});
