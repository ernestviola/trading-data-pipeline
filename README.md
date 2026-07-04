# Simulated Trading Data Pipeline

A data engineering portfolio project simulating a mean-reversion trading strategy, built to demonstrate incremental loading, slowly changing dimensions, and orchestration patterns relevant to fintech data.

## Overview

This project pulls real historical stock price data, uses it to generate synthetic trades based on a configurable trading strategy, and models the resulting portfolio state changes over time — the kind of pattern found in real trading/position-tracking systems where historical state matters (audit, backtesting, reporting).

The core design goal: separate the **immutable event stream** (trades) from the **derived, mutable state** (holdings), and treat the trading strategy as a pluggable input rather than a fixed pipeline behavior.

## Getting Started

1. Create a [LocalStack account](https://app.localstack.cloud/) and sign up for their Snowflake local service (Trial plan, or apply for the free non-commercial OSS license since this repo is public).
2. Copy `.exampleenv` to `.env` and fill in all required values (see below).
3. Start the local Snowflake emulator:
   ```bash
   docker compose up -d
   ```
4. Install the [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation) (`snow`), then add a connection pointed at the emulator:
   ```bash
   snow connection add \
     --connection-name localstack \
     --user test \
     --password test \
     --account test \
     --host snowflake.localhost.localstack.cloud
   ```
5. Run the setup SQL to create the database, table, file format, and stage:
   ```bash
   snow sql -f sql/001_setup_raw_prices.sql --connection localstack
   ```
6. Set up the Python environment:
   ```bash
   cd trading-scripts/
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
7. Create an [Alpaca account](https://alpaca.markets/) and generate API keys (paper trading is sufficient — no funding needed, this project only pulls historical market data).
8. Run the pipeline:
   ```bash
   python main.py
   ```
9. When done, stop the emulator:
   ```bash
   docker compose down
   ```

### Required `.env` values

`.exampleenv` documents these — copy it to `.env` (gitignored) and fill in:

```
LOCALSTACK_AUTH_TOKEN=

ALPACA_API_KEY=
ALPACA_API_SECRET=

SNOWFLAKE_USER=test
SNOWFLAKE_PASSWORD=test
SNOWFLAKE_ACCOUNT=test
SNOWFLAKE_HOST=snowflake.localhost.localstack.cloud
```

> Note: LocalStack is ephemeral by default — data doesn't persist across container restarts unless volume persistence is configured (this repo's `docker-compose.yml` mounts `./volume:/var/lib/localstack` for this reason). If you `docker compose down` without that volume, you'll need to re-run step 5 on your next `docker compose up`.

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

**Local dev stack**, distinct from the pipeline architecture above:

- **Snowflake** is emulated locally via [LocalStack for Snowflake](https://docs.localstack.cloud/snowflake/), running in Docker (`docker-compose.yml`) rather than a real cloud account — keeps the project runnable by anyone cloning the repo, no Snowflake trial account required.
- Objects live under the `TRADING_PIPELINE` database (see `sql/001_setup_raw_prices.sql`), not the emulator's `test` default — keeps the schema intentional and matches how dbt will reference sources later.

## Key design decisions

- **Source data pulled once, not on every run.** Alpaca free tier has no daily cap, but there's no reason to re-hit the API repeatedly for static historical data — it's landed once into a raw table and treated as the source of truth downstream.
- **Trades are append-only.** Each generated trade is an immutable event. No updates, no deletes — this is the fact stream.
- **Holdings use SCD Type 2.** Position quantity and average cost basis change as new trades arrive. Instead of overwriting the current state, each change closes out the prior row (`end_date`, `is_current = false`) and inserts a new one — preserving full history and enabling point-in-time queries ("what did the portfolio look like on date X").
- **Strategy is a parameter, not a branch.** The trade generator takes a `strategy` argument (starting with `mean_reversion`, with `momentum` supported later) and stamps each trade with `strategy_used`. Downstream models don't care which strategy produced a trade — this keeps the modeling/orchestration layer decoupled from trading logic, so adding a new strategy later requires no pipeline changes.
- **Airflow orchestrates meaningfully, not trivially.** The DAG separates ingestion from transformation as distinct tasks, includes retry/failure handling, and drives incremental dbt runs rather than full-refreshing every time.
- **Ingestion files are matched by column name, not position.** `COPY INTO` uses `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`, so CSVs are loaded using Alpaca's own response headers directly — no manual column-order mapping to keep in sync if Alpaca's schema changes.
- **Load deduplication uses a staging table + `MERGE`, not raw file/stage tracking.** CSVs land in a staging table via `COPY INTO`, then a `MERGE INTO` the target table (matched on natural keys like symbol/timestamp for prices, or ticker/date for trades) inserts only genuinely new rows. This was chosen over relying on Snowflake's stage/file-load tracking alone, since that only prevents re-loading an identical file — it doesn't catch two different files covering overlapping date ranges, which caused duplicate rows in practice.

## Todo

### Phase 1 — Setup & raw data

- [x] Setup LocalStack Snowflake for local development
- [x] Create Alpaca account, get API key
- [x] Pick a small set of tickers (5–10) to keep the project scoped _(currently just `AAPL` in `main.py`)_
- [x] Pull historical daily OHLCV for chosen tickers, land in `raw_prices` (Snowflake)
- [x] Confirm schema: symbol, timestamp, open, high, low, close, volume, trade_count, vwap

### Phase 2 — Trade generation

- [x] Write script to compute rolling mean + stddev per ticker
- [x] Implement mean-reversion signal (buy below threshold, sell above threshold)
- [ ] Parameterize `strategy` argument (even if only `mean_reversion` is implemented now)
- [ ] Track simulated cash balance, derived by replaying trades in order (starting cash − buys + sells) — not stored as a column, kept consistent with the append-only design
- [ ] Size each trade using signal-strength-scaled quantity: position size scales with how far price deviates from the rolling mean (larger z-score → larger position), rather than a fixed share count or dollar amount
- [ ] Cap trade size to available cash on buys; skip or reduce sells if the simulated position doesn't hold enough shares
- [ ] Document the sizing formula and its parameters (base position size, max multiplier, starting cash) in the README, since they're arbitrary choices that should be defensible/explained
- [x] Generate `raw_trades` output with `strategy_used`, ticker, date, side, quantity, price
- [x] Load `raw_trades` into Snowflake

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
- [ ] Clean repo structure, remove dead code/experiments before sharing _(e.g. `example/` folder — LocalStack quickstart scratch work, not part of the actual pipeline)_
- [ ] Apply for LocalStack OSS/non-commercial license, since this repo is public

## Repo structure

```
sql/                   DDL — database, table, file format, stage setup
trading-scripts/
  gather_historicals.py  Pulls historical OHLCV from Alpaca, writes to data/
  load_to_snowflake.py   PUT + COPY INTO raw_prices
  main.py                Entry point — loops tickers, gathers + loads
  requirements.txt
  strategies/
   mean_reversion.py
example/               LocalStack Snowflake quickstart scratch work (not part of the pipeline — candidate for removal in Phase 5)
docker-compose.yml      LocalStack Snowflake emulator
.exampleenv             Template for required environment variables
```

## Stack

- **Source**: Alpaca API (historical daily OHLCV, US equities)
- **Warehouse**: Snowflake (emulated locally via [LocalStack for Snowflake](https://docs.localstack.cloud/snowflake/))
- **Transformation**: dbt
- **Orchestration**: Airflow
- **Local dev tools**: Docker Compose, Snowflake CLI (`snow`), DBeaver (optional, for browsing/querying the emulator visually)
