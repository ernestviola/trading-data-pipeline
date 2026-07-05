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

CREATE TABLE IF NOT EXISTS public.raw_trades (
    ticker TEXT,
    "date" TIMESTAMPTZ,
    side TEXT,
    quantity FLOAT8,
    price FLOAT8,
    strategy_used TEXT,
    signal_strength FLOAT
);

CREATE TABLE IF NOT EXISTS public.raw_trades_staging (LIKE raw_trades INCLUDING ALL);