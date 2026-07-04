-- psql -h localhost -U ernest -d trading_pipeline -f sql/001_setup_raw_prices.sql

CREATE TABLE IF NOT EXISTS raw_prices (
    symbol TEXT,
    "timestamp" TIMESTAMPTZ,
    open FLOAT8,
    high FLOAT8,
    low FLOAT8,
    close FLOAT8,
    volume FLOAT8,
    trade_count FLOAT8,
    vwap FLOAT8
);

CREATE TABLE IF NOT EXISTS raw_prices_staging (LIKE raw_prices INCLUDING ALL);

CREATE TABLE IF NOT EXISTS raw_trades (
    symbol TEXT,
    "date" TIMESTAMPTZ,
    price FLOAT8,
    high FLOAT8,
    low FLOAT8,
    close FLOAT8,
    volume FLOAT8,
    trade_count FLOAT8,
    vwap FLOAT8,
    rolling_mean FLOAT8,
    rolling_std FLOAT8,
    z_score FLOAT8,
    side TEXT,
    ticker TEXT,
    strategy_used TEXT,
    quantity FLOAT8
);

CREATE TABLE IF NOT EXISTS raw_trades_staging (LIKE raw_trades INCLUDING ALL);