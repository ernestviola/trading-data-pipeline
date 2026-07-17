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