"""NSE Equity Futures cost defaults and market microstructure constants."""

from __future__ import annotations

# Brokerage (₹ per order, flat — configurable in settings)
DEFAULT_BROKERAGE_PER_ORDER = 20.0

# Securities Transaction Tax — equity futures: charged on sell-side turnover
STT_SELL_RATE = 0.0002  # 0.02%

# NSE futures exchange turnover charge
EXCHANGE_TURNOVER_RATE = 0.000019  # 0.0019%

# SEBI charges: ₹10 per crore of turnover
SEBI_PER_CRORE = 10.0

# Stamp duty — buy-side turnover (equity futures)
STAMP_DUTY_BUY_RATE = 0.00002  # 0.002%

# GST on (brokerage + exchange + SEBI)
GST_RATE = 0.18

# Margin / sizing defaults
DEFAULT_INITIAL_MARGIN_PCT = 0.22  # ~22% of contract value
DEFAULT_TICK_SIZE = 0.05
DEFAULT_LOT_SIZE = 1

# Session (IST)
NSE_SESSION_OPEN = (9, 15)
NSE_SESSION_CLOSE = (15, 30)

# Upstox historical interval tokens (v2 path)
UPSTOX_V2_INTERVALS = {
    "1minute": "1minute",
    "3minute": "3minute",
    "5minute": "5minute",
    "15minute": "15minute",
    "30minute": "30minute",
    "day": "day",
}

# Map CLI intervals → internal UpstoxService interval keys
INTERVAL_TO_UPSTOX = {
    "1minute": "minutes/1",
    "3minute": "minutes/3",
    "5minute": "minutes/5",
    "15minute": "minutes/15",
    "30minute": "minutes/30",
    "day": "days/1",
}
