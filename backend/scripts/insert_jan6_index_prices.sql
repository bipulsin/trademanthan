-- One-time SQL script to insert index prices for January 6th, 2026 at 9:15 AM IST
-- NIFTY50: 26189.70
-- BANKNIFTY: 59957.80

-- First, delete any existing records for this time (if any)
DELETE FROM index_prices 
WHERE index_name = 'NIFTY50' 
  AND price_time = '2026-01-06 09:15:00+05:30';

DELETE FROM index_prices 
WHERE index_name = 'BANKNIFTY' 
  AND price_time = '2026-01-06 09:15:00+05:30';

-- Insert NIFTY50 price
INSERT INTO index_prices (
    index_name,
    instrument_key,
    ltp,
    day_open,
    close_price,
    trend,
    change,
    change_percent,
    price_time,
    is_market_open,
    is_special_time,
    created_at
) VALUES (
    'NIFTY50',
    'NSE_INDEX|Nifty 50',
    26189.70,
    26189.70,
    NULL,
    'neutral',
    0.0,
    0.0,
    '2026-01-06 09:15:00+05:30',
    true,
    true,
    NOW()
);

-- Insert BANKNIFTY price
INSERT INTO index_prices (
    index_name,
    instrument_key,
    ltp,
    day_open,
    close_price,
    trend,
    change,
    change_percent,
    price_time,
    is_market_open,
    is_special_time,
    created_at
) VALUES (
    'BANKNIFTY',
    'NSE_INDEX|Nifty Bank',
    59957.80,
    59957.80,
    NULL,
    'neutral',
    0.0,
    0.0,
    '2026-01-06 09:15:00+05:30',
    true,
    true,
    NOW()
);

-- Verify the inserts
SELECT 
    id,
    index_name,
    ltp,
    day_open,
    price_time,
    is_special_time,
    created_at
FROM index_prices
WHERE price_time = '2026-01-06 09:15:00+05:30'
ORDER BY index_name;

