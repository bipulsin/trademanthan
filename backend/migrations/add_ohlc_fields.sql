-- Migration: Add OHLC and Previous Hour VWAP Fields
-- Date: 2025-11-25
-- Description: Adds fields for option OHLC candles and previous hour stock VWAP

-- Add previous hour VWAP fields
ALTER TABLE intraday_stock_options 
ADD COLUMN IF NOT EXISTS stock_vwap_previous_hour FLOAT,
ADD COLUMN IF NOT EXISTS stock_vwap_previous_hour_time TIMESTAMP;

-- Add option current candle OHLC fields
ALTER TABLE intraday_stock_options
ADD COLUMN IF NOT EXISTS option_current_candle_open FLOAT,
ADD COLUMN IF NOT EXISTS option_current_candle_high FLOAT,
ADD COLUMN IF NOT EXISTS option_current_candle_low FLOAT,
ADD COLUMN IF NOT EXISTS option_current_candle_close FLOAT,
ADD COLUMN IF NOT EXISTS option_current_candle_time TIMESTAMP;

-- Add option previous candle OHLC fields
ALTER TABLE intraday_stock_options
ADD COLUMN IF NOT EXISTS option_previous_candle_open FLOAT,
ADD COLUMN IF NOT EXISTS option_previous_candle_high FLOAT,
ADD COLUMN IF NOT EXISTS option_previous_candle_low FLOAT,
ADD COLUMN IF NOT EXISTS option_previous_candle_close FLOAT,
ADD COLUMN IF NOT EXISTS option_previous_candle_time TIMESTAMP;

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_intraday_stock_options_stock_vwap_prev_time 
ON intraday_stock_options(stock_vwap_previous_hour_time);

CREATE INDEX IF NOT EXISTS idx_intraday_stock_options_option_current_candle_time 
ON intraday_stock_options(option_current_candle_time);

CREATE INDEX IF NOT EXISTS idx_intraday_stock_options_option_previous_candle_time 
ON intraday_stock_options(option_previous_candle_time);

