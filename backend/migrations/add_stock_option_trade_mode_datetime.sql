-- Stock Options: PAPER|LIVE mode + datetime entry/exit (IST-naive timestamps).
-- Historical / untagged rows default to PAPER.

ALTER TABLE stock_option_signals
    ADD COLUMN IF NOT EXISTS trade_mode TEXT NOT NULL DEFAULT 'PAPER';

UPDATE stock_option_signals
SET trade_mode = 'PAPER'
WHERE trade_mode IS NULL
   OR BTRIM(trade_mode) = ''
   OR UPPER(BTRIM(trade_mode)) NOT IN ('PAPER', 'LIVE');

COMMENT ON COLUMN stock_option_signals.trade_mode IS
    'PAPER or LIVE. Existing/historical rows default PAPER (no prior tagging).';

-- Promote DATE → TIMESTAMP WITHOUT TIME ZONE (legacy dates become midnight).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'stock_option_signals'
          AND column_name = 'date_traded'
          AND data_type = 'date'
    ) THEN
        ALTER TABLE stock_option_signals
            ALTER COLUMN date_traded TYPE TIMESTAMP WITHOUT TIME ZONE
            USING date_traded::timestamp;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'stock_option_signals'
          AND column_name = 'exit_date'
          AND data_type = 'date'
    ) THEN
        ALTER TABLE stock_option_signals
            ALTER COLUMN exit_date TYPE TIMESTAMP WITHOUT TIME ZONE
            USING exit_date::timestamp;
    END IF;
END $$;
