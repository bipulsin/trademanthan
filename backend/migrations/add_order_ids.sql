-- Add Upstox order ID audit fields for live trading
ALTER TABLE intraday_stock_options
    ADD COLUMN IF NOT EXISTS buy_order_id VARCHAR(100);

ALTER TABLE intraday_stock_options
    ADD COLUMN IF NOT EXISTS sell_order_id VARCHAR(100);
