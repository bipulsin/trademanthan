-- Sticky options contract month (MMM-YYYY) frozen at arm time from currmth FUT expiry.
ALTER TABLE stock_option_signals
    ADD COLUMN IF NOT EXISTS contract_mmm_yyyy TEXT;

COMMENT ON COLUMN stock_option_signals.contract_mmm_yyyy IS
    'Current-month FUT expiry as MMM-YYYY at armed_at; sticky across month roll';
