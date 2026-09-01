-- Daily wick class for Breakfast Live confirmation (filled by prev-close jobs).
ALTER TABLE arbitrage_master
    ADD COLUMN IF NOT EXISTS wick TEXT;
