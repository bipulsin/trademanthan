-- Index underlyings → arbitrage_master (NIFTY / BANKNIFTY).
-- stock names match Upstox trading_symbol and FO FUT underliers.
-- stock_instrument_key = NSE_INDEX spot; sector_index = self (own index).
--
-- After this SQL, refresh EQ/FUT keys via metadata roll:
--   PYTHONPATH=. python backend/scripts/upsert_nifty_banknifty_arbitrage_master.py
--
-- Stock-FUT consumers (Breakfast / OI / Smart / Vajra / HA VWAP) exclude these
-- symbols via INDEX_FUT_UNDERLYINGS — see backend/services/arbitrage_universe.py.
-- Idempotent. Wrapped in a transaction.

BEGIN;

INSERT INTO arbitrage_master (stock, sector, sector_index, sector_instrument_key, stock_instrument_key)
VALUES
    (
        'NIFTY',
        'Nifty 50',
        'NSE_INDEX|Nifty 50',
        'NSE_INDEX|Nifty 50',
        'NSE_INDEX|Nifty 50'
    ),
    (
        'BANKNIFTY',
        'Nifty Bank',
        'NSE_INDEX|Nifty Bank',
        'NSE_INDEX|Nifty Bank',
        'NSE_INDEX|Nifty Bank'
    )
ON CONFLICT (stock) DO UPDATE SET
    sector = EXCLUDED.sector,
    sector_index = EXCLUDED.sector_index,
    sector_instrument_key = EXCLUDED.sector_instrument_key,
    stock_instrument_key = COALESCE(
        NULLIF(TRIM(arbitrage_master.stock_instrument_key), ''),
        EXCLUDED.stock_instrument_key
    );

COMMIT;
