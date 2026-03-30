#!/usr/bin/env python3
"""Export intraday_stock_options for today's trade_date (IST) to stdout or a text file."""
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytz
from sqlalchemy import text

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from backend.database import engine  # noqa: E402


def run_dump(out) -> int:
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = today + timedelta(days=1)

    q = text(
        """
        SELECT id, stock_name, alert_type, status, exit_reason, no_entry_reason,
               option_contract, option_type, option_strike, qty,
               buy_price, sell_price, pnl,
               buy_order_id, sell_order_id, instrument_key,
               alert_time, buy_time, sell_time,
               trade_date, created_date_time, updated_at
        FROM intraday_stock_options
        WHERE trade_date >= :t0 AND trade_date < :t1
        ORDER BY id
        """
    )
    with engine.connect() as conn:
        r = conn.execute(q, {"t0": today, "t1": end})
        rows = r.fetchall()
        colnames = list(r.keys())

    def w(s: str = "") -> None:
        out.write(s + "\n")

    w(f"trade_date range (IST): {today.date()} 00:00 -> next day")
    w(f"exported_at_server: {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
    w("")

    if not rows:
        w("No rows.")
        return 0

    w("| " + " | ".join(colnames) + " |")
    w("| " + " | ".join(["---"] * len(colnames)) + " |")
    for row in rows:
        cells = []
        for v in row:
            if v is None:
                cells.append("")
            elif isinstance(v, datetime):
                cells.append(v.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                s = str(v).replace("|", "/")
                if len(s) > 64:
                    s = s[:61] + "..."
                cells.append(s)
        w("| " + " | ".join(cells) + " |")
    w("")
    w(f"Row count: {len(rows)}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Write UTF-8 text here (default: stdout)",
    )
    args = p.parse_args()

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            return run_dump(f)
    return run_dump(sys.stdout)


if __name__ == "__main__":
    raise SystemExit(main())
