-- Phase 6: Snowflake Bronze DDL — raw_prices, raw_trades (+ staging tables)
-- Run as transformer_role or loader_role (whichever owns table creation) in a Snowsight worksheet.
-- Translated from sql/001_setup_raw_prices.sql (Postgres).
--
-- Type/design notes vs. Postgres original:
--   TIMESTAMPTZ -> TIMESTAMP_NTZ: Alpaca daily OHLCV timestamps are UTC-normalized with no
--     per-row timezone variance, so timezone-aware storage isn't needed. Treated as naive UTC.
--   TEXT / FLOAT8 map directly (Snowflake aliases TEXT -> VARCHAR, FLOAT8 -> FLOAT/DOUBLE).
--   "timestamp" stays quoted (reserved word in Snowflake, same as Postgres).
--   open/close/volume unquoted — not reserved words in Snowflake.

USE ROLE loader_role;
USE WAREHOUSE trading_pipeline_wh;
USE DATABASE trading_pipeline;
USE SCHEMA bronze;

CREATE TABLE IF NOT EXISTS raw_prices (
    symbol TEXT,
    "timestamp" TIMESTAMP_NTZ,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    trade_count FLOAT,
    vwap FLOAT
);

CREATE TABLE IF NOT EXISTS raw_prices_staging LIKE raw_prices;

CREATE TABLE IF NOT EXISTS raw_trades (
    ticker TEXT,
    "date" TIMESTAMP_NTZ,
    side TEXT,
    quantity FLOAT,
    price FLOAT,
    strategy_used TEXT,
    signal_strength FLOAT
);

CREATE TABLE IF NOT EXISTS raw_trades_staging LIKE raw_trades;