-- The modelling table: one row per symbol per hour, the seven candle columns.
-- build_warehouse.py exports this to data/warehouse/bars/ as Parquet, and that
-- export is what notebooks/data.py loads.
--
-- Candles only, for now. tick_bars joins once candles.py has closed the 27-day
-- hole between the backfill and the live feed. Before that, features.py would
-- score the jump across the hole as a single hour's return (+23.85%), which on
-- its own moves RMSE from 0.0045 to 0.0072.
SELECT time, symbol, open, high, low, close, volume
FROM raw_candles;
