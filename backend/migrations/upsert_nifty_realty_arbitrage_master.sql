-- Nifty Realty F&O basket → arbitrage_master.
-- Inverse of RADICO-style master → lowliquid: insert with NOT NULL sector
-- fields first, copy overlapping instrument columns from lowliquid, then
-- delete those rows from lowliquid.
-- Sector label matches existing master rows (DLF/LODHA): REALITY.
-- sector_index / sector_instrument_key: NSE_INDEX|Nifty Realty
--
-- Official Nifty Realty (10, NSE/Upstox as of 2026-06): DLF, PHOENIXLTD, LODHA,
-- GODREJPROP, PRESTIGE, OBEROIRLTY, BRIGADE, ANANTRAJ, SOBHA, ABREL.
-- Only the first six have NSE stock futures. Non-F&O names are not inserted.
--
-- After this SQL, run metadata roll so EQ/FUT keys stay current:
--   PYTHONPATH=. python backend/scripts/upsert_nifty_realty_arbitrage_master.py
-- Idempotent. Wrapped in a transaction.

BEGIN;

INSERT INTO arbitrage_master (stock, sector, sector_index, sector_instrument_key)
VALUES
    ('GODREJPROP', 'REALITY', 'NSE_INDEX|Nifty Realty', 'NSE_INDEX|Nifty Realty'),
    ('OBEROIRLTY', 'REALITY', 'NSE_INDEX|Nifty Realty', 'NSE_INDEX|Nifty Realty'),
    ('PHOENIXLTD', 'REALITY', 'NSE_INDEX|Nifty Realty', 'NSE_INDEX|Nifty Realty'),
    ('PRESTIGE', 'REALITY', 'NSE_INDEX|Nifty Realty', 'NSE_INDEX|Nifty Realty'),
    ('DLF', 'REALITY', 'NSE_INDEX|Nifty Realty', 'NSE_INDEX|Nifty Realty'),
    ('LODHA', 'REALITY', 'NSE_INDEX|Nifty Realty', 'NSE_INDEX|Nifty Realty')
ON CONFLICT (stock) DO UPDATE SET
    sector = 'REALITY',
    sector_index = 'NSE_INDEX|Nifty Realty',
    sector_instrument_key = 'NSE_INDEX|Nifty Realty';

UPDATE arbitrage_master m
SET
    stock_instrument_key = COALESCE(l.stock_instrument_key, m.stock_instrument_key),
    stock_ltp = COALESCE(l.stock_ltp, m.stock_ltp),
    currmth_future_symbol = COALESCE(l.currmth_future_symbol, m.currmth_future_symbol),
    currmth_future_instrument_key = COALESCE(l.currmth_future_instrument_key, m.currmth_future_instrument_key),
    currmth_future_ltp = COALESCE(l.currmth_future_ltp, m.currmth_future_ltp),
    nextmth_future_symbol = COALESCE(l.nextmth_future_symbol, m.nextmth_future_symbol),
    nextmth_future_instrement_key = COALESCE(l.nextmth_future_instrement_key, m.nextmth_future_instrement_key),
    nextmth_future_ltp = COALESCE(l.nextmth_future_ltp, m.nextmth_future_ltp)
FROM arbitrage_lowliquid l
WHERE UPPER(TRIM(m.stock)) = UPPER(TRIM(l.stock))
  AND UPPER(TRIM(m.stock)) IN (
      'GODREJPROP', 'OBEROIRLTY', 'PHOENIXLTD', 'PRESTIGE', 'DLF', 'LODHA'
  );

DELETE FROM arbitrage_lowliquid
WHERE UPPER(TRIM(stock)) IN (
    'GODREJPROP', 'OBEROIRLTY', 'PHOENIXLTD', 'PRESTIGE', 'DLF', 'LODHA'
);

UPDATE arbitrage_master
SET sector = 'REALITY',
    sector_index = 'NSE_INDEX|Nifty Realty',
    sector_instrument_key = 'NSE_INDEX|Nifty Realty'
WHERE UPPER(TRIM(stock)) IN (
    'DLF', 'LODHA', 'GODREJPROP', 'OBEROIRLTY', 'PHOENIXLTD', 'PRESTIGE'
);

COMMIT;
