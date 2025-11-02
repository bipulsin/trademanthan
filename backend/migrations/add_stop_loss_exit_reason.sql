-- Migration: Add stop_loss and exit_reason columns to intraday_stock_options table
-- Date: 2025-11-02
-- Purpose: Implement Stop Loss functionality and track exit reasons

-- Add stop_loss column
ALTER TABLE intraday_stock_options 
ADD COLUMN IF NOT EXISTS stop_loss FLOAT;

-- Add exit_reason column
ALTER TABLE intraday_stock_options 
ADD COLUMN IF NOT EXISTS exit_reason VARCHAR(50);

-- Add comments
COMMENT ON COLUMN intraday_stock_options.stop_loss IS 'Stop loss price for risk management (~₹3,100 loss target)';
COMMENT ON COLUMN intraday_stock_options.exit_reason IS 'Reason for trade exit: profit_target, stop_loss, time_based, manual';

-- Verify columns were added
SELECT 
    column_name, 
    data_type, 
    is_nullable 
FROM information_schema.columns 
WHERE table_name = 'intraday_stock_options' 
    AND column_name IN ('stop_loss', 'exit_reason')
ORDER BY column_name;

