# Simulated Trading Data Pipeline

A data engineering portfolio project simulating a mean-reversion trading strategy, built to demonstrate incremental loading, slowly changing dimensions, and orchestration patterns relevant to fintech data.

## Overview

This project pulls real historical stock price data, uses it to generate synthetic trades based on a configurable trading strategy, and models the resulting portfolio state changes over time — the kind of pattern found in real trading/position-tracking systems where historical state matters (audit, backtesting, reporting).

The core design goal: separate the **immutable event stream** (trades) from the **derived, mutable state** (holdings), and treat the trading strategy as a pluggable input rather than a fixed pipeline behavior.

## Getting Started

1. Create a localstack account https://app.localstack.cloud/ and sign up for their snowflake local service
2. Run localstack snowflake `docker compose up -d`
3. Download the snow CLI from snowflake
4. Add a connection

```
snow connection add \
  --connection-name localstack \
  --user test \
  --password test \
  --account test \
  --host snowflake.localhost.localstack.cloud
```

4. Run sql files `snow sql -f <filename> --connection localstack`
5. Stop localstack `docker compose down`

6. `cd trading-api/` and create a python venv `python3 -m venv .venv`
7. Install python requirements in trading-api/

## Architecture

```
Alpaca API (historical OHLCV)
        │
        ▼
  raw_prices (landed once, append-only)
        │
        ▼
  Trade generator (mean-reversion logic)
        │
        ▼
  raw_trades (append-only fact — trade events)
        │
        ▼
     dbt models
   staging → intermediate → marts
        │
        ▼
  holdings_scd2 (position/cost-basis history)
  portfolio_value (daily fact)
        │
        ▼
   Airflow DAG orchestrates the above,
   with retries + incremental logic
```

## Key design decisions

- **Source data pulled once, not on every run.** Alpaca free tier has no daily cap, but there's no reason to re-hit the API repeatedly for static historical data — it's landed once into a raw table and treated as the source of truth downstream.
- **Trades are append-only.** Each generated trade is an immutable event. No updates, no deletes — this is the fact stream.
- **Holdings use SCD Type 2.** Position quantity and average cost basis change as new trades arrive. Instead of overwriting the current state, each change closes out the prior row (`end_date`, `is_current = false`) and inserts a new one — preserving full history and enabling point-in-time queries ("what did the portfolio look like on date X").
- **Strategy is a parameter, not a branch.** The trade generator takes a `strategy` argument (starting with `mean_reversion`, with `momentum` supported later) and stamps each trade with `strategy_used`. Downstream models don't care which strategy produced a trade — this keeps the modeling/orchestration layer decoupled from trading logic, so adding a new strategy later requires no pipeline changes.
- **Airflow orchestrates meaningfully, not trivially.** The DAG separates ingestion from transformation as distinct tasks, includes retry/failure handling, and drives incremental dbt runs rather than full-refreshing every time.

## Todo

### Phase 1 — Setup & raw data

- [x] Setup localstack snowflake for local development
- [x] Create Alpaca account, get API key
- [x] Pick a small set of tickers (5–10) to keep the project scoped
- [ ] Pull historical daily OHLCV for chosen tickers, land in `raw_prices` (Snowflake)
- [x] Confirm schema: date, ticker, open, high, low, close, volume

### Phase 2 — Trade generation

- [ ] Write script to compute rolling mean + stddev per ticker
- [ ] Implement mean-reversion signal (buy below threshold, sell above threshold)
- [ ] Parameterize `strategy` argument (even if only `mean_reversion` is implemented now)
- [ ] Generate `raw_trades` output with `strategy_used`, ticker, date, side, quantity, price
- [ ] Load `raw_trades` into Snowflake

### Phase 3 — dbt modeling

- [ ] Set up dbt project, connect to Snowflake
- [ ] Define sources + freshness checks for `raw_prices` and `raw_trades`
- [ ] Staging models: `stg_prices`, `stg_trades` (clean/rename/cast)
- [ ] Intermediate model: compute running position/cost-basis changes from trades
- [ ] Marts: `holdings_scd2` (SCD Type 2 dimension)
- [ ] Marts: `portfolio_value` (daily fact — position × price)
- [ ] Add generic tests (not null, unique, relationships) on key models
- [ ] Add at least one custom test (e.g. no overlapping SCD2 date ranges)
- [ ] Implement one model as incremental (merge strategy, not full refresh)

### Phase 4 — Orchestration

- [ ] Set up Airflow (local/Docker)
- [ ] DAG task 1: ingest/generate new trades
- [ ] DAG task 2: run dbt (staging → marts)
- [ ] Add retry/failure handling (not just Airflow defaults)
- [ ] Add a sensor or dependency check (e.g. don't run dbt until new trade data lands)
- [ ] Confirm DAG supports incremental runs, not full reprocessing each time

### Phase 5 — Polish

- [ ] Write README section explaining _why_, not just what (this doc is the start)
- [ ] dbt docs generated and reviewed
- [ ] Sanity-check SCD2 output with a manual point-in-time query
- [ ] (Stretch) Add `momentum` strategy as a second option to prove the pluggable design works
- [ ] Clean repo structure, remove dead code/experiments before sharing

## Stack

- **Source**: Alpaca API (historical daily OHLCV, US equities)
- **Warehouse**: Snowflake
- **Transformation**: dbt
- **Orchestration**: Airflow
