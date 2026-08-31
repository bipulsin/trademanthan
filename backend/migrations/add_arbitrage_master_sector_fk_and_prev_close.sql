-- 3a: sector_instrument_key FK to nifty_benchmark_reference
-- 3b: Stock prev-session close columns (filled by breakfast prev-close job)
-- sector_index column retained for soak / dual-read (Phase 4 drop later)

ALTER TABLE arbitrage_master
    ADD COLUMN IF NOT EXISTS sector_instrument_key TEXT,
    ADD COLUMN IF NOT EXISTS prev_session_close          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS prev_session_close_for_date DATE,
    ADD COLUMN IF NOT EXISTS prev_session_close_source   TEXT;

-- Backfill sector FK from legacy text
UPDATE arbitrage_master
SET sector_instrument_key = TRIM(sector_index)
WHERE sector_index IS NOT NULL
  AND TRIM(sector_index) <> ''
  AND (sector_instrument_key IS NULL OR TRIM(sector_instrument_key) = '');

-- Alias cleanup (idempotent)
UPDATE arbitrage_master SET sector_instrument_key = 'NSE_INDEX|Nifty Fin Service'
    WHERE TRIM(sector_index) = 'NSE_INDEX|Nifty Financial Services';
UPDATE arbitrage_master SET sector_instrument_key = 'NSE_INDEX|Nifty Pvt Bank'
    WHERE TRIM(sector_index) = 'NSE_INDEX|Nifty Private Bank';
UPDATE arbitrage_master SET sector_instrument_key = 'NSE_INDEX|NIFTY CONSR DURBL'
    WHERE TRIM(sector_index) = 'NSE_INDEX|Nifty Consumer Durables';
UPDATE arbitrage_master SET sector_instrument_key = 'NSE_INDEX|Nifty Trans Logis'
    WHERE TRIM(sector_index) = 'NSE_INDEX|Nifty Logistics';
UPDATE arbitrage_master SET sector_instrument_key = 'NSE_INDEX|Nifty Serv Sector'
    WHERE TRIM(sector_index) = 'NSE_INDEX|Nifty Services';
UPDATE arbitrage_master SET sector_instrument_key = 'NSE_INDEX|Nifty MS IT Telcm'
    WHERE TRIM(sector_index) = 'NSE_INDEX|Nifty Telecom';

-- RADICO → Nifty FMCG
UPDATE arbitrage_master
SET sector_index = 'NSE_INDEX|Nifty FMCG',
    sector = 'FMCG',
    sector_instrument_key = 'NSE_INDEX|Nifty FMCG'
WHERE UPPER(TRIM(stock)) = 'RADICO';

-- FK + NOT NULL only when zero orphans (sector_index set but sector_instrument_key unmapped)
DO $$
DECLARE
    orphan_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO orphan_count
    FROM arbitrage_master am
    WHERE am.sector_index IS NOT NULL
      AND TRIM(am.sector_index) <> ''
      AND (
          am.sector_instrument_key IS NULL
          OR TRIM(am.sector_instrument_key) = ''
          OR NOT EXISTS (
              SELECT 1 FROM nifty_benchmark_reference nbr
              WHERE nbr.instrument_key = TRIM(am.sector_instrument_key)
          )
      );

    IF orphan_count = 0 THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'fk_arbitrage_master_sector_benchmark'
        ) THEN
            ALTER TABLE arbitrage_master
                ADD CONSTRAINT fk_arbitrage_master_sector_benchmark
                FOREIGN KEY (sector_instrument_key)
                REFERENCES nifty_benchmark_reference (instrument_key);
        END IF;

        CREATE INDEX IF NOT EXISTS idx_arbitrage_master_sector_instrument_key
            ON arbitrage_master (sector_instrument_key)
            WHERE sector_instrument_key IS NOT NULL;

        -- NOT NULL only when every row has a mapped sector_instrument_key
        IF NOT EXISTS (
            SELECT 1 FROM arbitrage_master
            WHERE sector_instrument_key IS NULL OR TRIM(sector_instrument_key) = ''
        ) THEN
            ALTER TABLE arbitrage_master
                ALTER COLUMN sector_instrument_key SET NOT NULL;
        END IF;
    ELSE
        RAISE NOTICE 'Skipping FK/NOT NULL: % orphan sector_instrument_key rows', orphan_count;
    END IF;
END $$;
