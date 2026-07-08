# Simulated Trading Data Pipeline

A data engineering portfolio project simulating a mean-reversion trading strategy, built to demonstrate incremental loading, slowly changing dimensions, and orchestration patterns relevant to fintech data.

## Overview

This project pulls real historical stock price data, uses it to generate synthetic trades based on a configurable trading strategy, and models the resulting portfolio state changes over time — the kind of pattern found in real trading/position-tracking systems where historical state matters (audit, backtesting, reporting).

The core design goal: separate the **immutable event stream** (trades) from the **derived, mutable state** (holdings), and treat the trading strategy as a pluggable input rather than a fixed pipeline behavior.

## Getting Started

1. Make sure you have a Postgres instance running (Docker is easiest — see below if you don't already have one).
2. Copy `.exampleenv` to `.env` and fill in all required values (see below).
3. Create the database and load the schema:
   ```bash
   docker exec -it <container_name> psql -U <user> -d <default_db> -c "CREATE DATABASE trading_pipeline;"
   docker exec -i <container_name> psql -U <user> -d trading_pipeline < sql/001_setup_raw_prices.sql
   ```
4. Set up the Python environment:
   ```bash
   cd trading-scripts/
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
5. Create an [Alpaca account](https://alpaca.markets/) and generate API keys (paper trading is sufficient — no funding needed, this project only pulls historical market data).
6. Run the pipeline:
   ```bash
   python main.py
   ```

### Don't have Postgres running yet?

A minimal `docker-compose.yml` works fine:

```yaml
services:
  postgres:
    image: postgres:16
    container_name: trading-postgres
    environment:
      - POSTGRES_USER=trading
      - POSTGRES_PASSWORD=trading
      - POSTGRES_DB=trading_pipeline
    ports:
      - '127.0.0.1:5432:5432'
    volumes:
      - './volume:/var/lib/postgresql/data'
```

Volume-mounted, so data persists across `docker compose down`/`up` with no extra config needed.

### Required `.env` values

`.exampleenv` documents these — copy it to `.env` (gitignored) and fill in:

```
ALPACA_API_KEY=
ALPACA_API_SECRET=

DATABASE_URL=postgresql://trading:trading@localhost:5432/trading_pipeline
```

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

- **Postgres** runs locally in Docker (`docker-compose.yml`) — keeps the project runnable by anyone cloning the repo, no cloud account or trial license required.
- Objects live in the `trading_pipeline` database (see `sql/001_setup_raw_prices.sql`), created explicitly rather than reused from another local project's database — keeps the schema intentional and matches how dbt will reference sources later.

## Key design decisions

- **Source data pulled once, not on every run.** Alpaca free tier has no daily cap, but there's no reason to re-hit the API repeatedly for static historical data — it's landed once into a raw table and treated as the source of truth downstream.
- **Trades are append-only.** Each generated trade is an immutable event. No updates, no deletes — this is the fact stream.
- **Trade sizing scales with signal strength, not fixed.** Each trade's quantity is derived from how far the price deviates from the rolling mean, capped by available cash/position:

  ```
  quantity = (base_position_size * min(abs(z_score) / z_threshold, max_multiplier)) / price
  ```

  - `starting_cash = $10,000` — arbitrary round number, easy to sanity-check by hand.
  - `base_position_size = $500` — dollar-denominated (not a fixed share count) so trade size scales consistently across tickers at different price points.
  - `z_threshold = 1.5` — the same threshold used to trigger a buy/sell signal; a trade right at the threshold gets exactly `base_position_size`.
  - `max_multiplier = 3` — caps position size at 3x base ($1,500) for extreme z-scores, preventing one outlier signal from disproportionately sizing a single trade.

  After computing the desired quantity, buys are capped to `available_cash / price` and sells are capped to current shares held — trades are reduced (not skipped outright, unless the cap is 0) if the simulated portfolio can't fully support the desired size. Cash and position are tracked in-memory as trades are generated, replaying in chronological order per ticker — but neither is stored as a column in `raw_trades`, consistent with the append-only, immutable-event design. Position/cash state is meant to be re-derived downstream (in dbt) from the trade event stream itself, not trusted from a Python-side precomputation.

- **Holdings use SCD Type 2.** Position quantity and average cost basis change as new trades arrive. Instead of overwriting the current state, each change closes out the prior row (`end_date`, `is_current = false`) and inserts a new one — preserving full history and enabling point-in-time queries ("what did the portfolio look like on date X").
- **Strategy is a parameter, not a branch.** The trade generator takes a `strategy` argument (starting with `mean_reversion`, with `momentum` supported later) and stamps each trade with `strategy_used`. Downstream models don't care which strategy produced a trade — this keeps the modeling/orchestration layer decoupled from trading logic, so adding a new strategy later requires no pipeline changes.
- **Airflow orchestrates meaningfully, not trivially.** The DAG separates ingestion from transformation as distinct tasks, includes retry/failure handling, and drives incremental dbt runs rather than full-refreshing every time.
- **Load deduplication uses a staging table + `MERGE`.** CSVs land in a staging table via `COPY`, then a `MERGE INTO` the target table (matched on natural keys like symbol/timestamp for prices, or ticker/date for trades) inserts only genuinely new rows. This catches overlapping-date-range files that a simple "have I loaded this exact file before" check wouldn't — which caused duplicate rows in practice during early development.
- **Ingestion loads by explicit column list, not implicit header matching.** Postgres's `COPY ... HEADER true` verifies a header row exists but loads positionally rather than matching by column name — unlike Snowflake's `MATCH_BY_COLUMN_NAME`, there's no built-in reordering safety net here if Alpaca's CSV column order ever drifts, so this is a known tradeoff of the current implementation.

## Todo

### Phase 1 — Setup & raw data

- [x] Setup local Postgres for development
- [x] Create Alpaca account, get API key
- [x] Pick a small set of tickers (5–10) to keep the project scoped _(currently just `AAPL` in `main.py`)_
- [x] Pull historical daily OHLCV for chosen tickers, land in `raw_prices` (Postgres)
- [x] Confirm schema: symbol, timestamp, open, high, low, close, volume, trade_count, vwap

### Phase 2 — Trade generation

- [x] Write script to compute rolling mean + stddev per ticker
- [x] Implement mean-reversion signal (buy below threshold, sell above threshold)
- [x] Generate `raw_trades` output with `strategy_used`, ticker, date, side, quantity, price
- [x] Load `raw_trades` into Postgres

### Phase 3 — dbt modeling

- [x] Set up dbt project (`dbt-postgres` adapter), connect to Postgres
- [x] Define sources for `raw_prices` and `raw_trades`
- [x] Staging models: `stg_prices`, `stg_trades` (clean/rename/cast)
- [x] Intermediate model: compute running position/cost-basis changes from trades
- [x] Marts: `holdings_scd2` (SCD Type 2 dimension)
- [x] Marts: `portfolio_value` (daily fact — position × price)
- [x] Add generic tests (not null, unique, relationships) on key models
- [x] Add at least one custom test (e.g. no overlapping SCD2 date ranges)
- [x] Implement one model as incremental (merge strategy, not full refresh)

### Phase 4 — Orchestration

- [ ] Set up Airflow (local/Docker)
- [ ] DAG task 1: ingest/generate new trades
- [ ] DAG task 2: run dbt (staging → marts)
- [ ] Add retry/failure handling (not just Airflow defaults)
- [ ] Add a sensor or dependency check (e.g. don't run dbt until new trade data lands)
- [ ] Confirm DAG supports incremental runs, not full reprocessing each time

### Phase 4.5 — Revisit before polish

From Phase 2 (trade sizing realism):

- [x] Parameterize `strategy` argument (even if only `mean_reversion` is implemented now)
- [x] Track simulated cash balance, derived by replaying trades in order (starting cash − buys + sells)
- [x] Size each trade using signal-strength-scaled quantity (larger z-score → larger position)
- [x] Cap trade size to available cash on buys; skip/reduce sells if position doesn't hold enough shares
- [x] Document the sizing formula and its parameters in the README

From Phase 3 (source freshness — deferred since raw data isn't pulled on a schedule):

- [ ] Revisit source freshness checks IF ingestion becomes periodic rather than one-time
- [ ] Until then, replace with a simpler row-count-based source test

Validation note:

- [ ] Once Phase 4.5 sizing changes land, re-validate `holdings_scd2` output values — the row-versioning mechanism built in Phase 3 doesn't need to change, but the quantity/cost-basis numbers running through it will

### Phase 5 — Polish

- [ ] Write README section explaining _why_, not just what (this doc is the start)
- [ ] dbt docs generated and reviewed
- [ ] Sanity-check SCD2 output with a manual point-in-time query
- [ ] (Stretch) Add `momentum` strategy as a second option to prove the pluggable design works
- [ ] Clean repo structure, remove dead code/experiments before sharing

### Phase 6 — Snowflake Migration

- [ ] Set up Snowflake trial account (warehouse, database, schema, role)
- [ ] Swap Postgres connection (psycopg2) for Snowflake Python connector
- [ ] Update DDL for Snowflake types/syntax (`TIMESTAMP_NTZ`, no `SERIAL`, identity columns)
- [ ] Replace `COPY`-from-file load with internal stage + `PUT` + `COPY INTO`
- [ ] Swap `dbt-postgres` adapter for `dbt-snowflake`, update `profiles.yml`
- [ ] Verify existing models (recursive CTEs, window functions) run unmodified on Snowflake; document any syntax deltas
- [ ] Update Airflow connection from Postgres type to Snowflake type

### Phase 7 — Snowflake Streams & Tasks (incremental processing)

- [ ] Add a Stream on `raw_trades` to track new rows since last consumption
- [ ] Add a Task to trigger downstream dbt processing when the Stream has data
- [ ] Document how Stream/Task-driven triggering compares to Airflow's schedule-driven incremental runs — be ready to explain when you'd use which

### Phase 8 — Fivetran / ELT tooling exposure

- [ ] Decide approach (Alpaca isn't a native Fivetran connector):
  - Option A: Build a custom connector via the Fivetran Connector SDK for Alpaca (closer to real hands-on Fivetran dev experience)
  - Option B: Add a second, genuinely Fivetran-native source (e.g. Google Sheets, S3/CSV, a small Postgres source) alongside the existing Alpaca pipeline
- [ ] Set up Fivetran free tier and land data into Snowflake's raw/bronze layer
- [ ] Document how Fivetran's managed sync (schema drift handling, scheduling) differs from the custom Python ingestion already in the project

### Phase 9 — Streamlit client-facing app

- [ ] Small app reading from mart-layer tables (`portfolio_value`, `holdings_scd2`)
- [ ] Portfolio value over time chart, current holdings table, data quality/test status indicator
- [ ] Scope as a thin client-facing view, not a full app — revisit scope before building

### Phase 10 — Data quality / QC framing

- [ ] Reframe existing dbt tests (SCD2 no-overlap, not-null, relationships) explicitly as client-facing trust/QC rules, not just correctness checks
- [ ] Add a lightweight data dictionary / lineage doc
- [ ] Note current raw → staging → marts naming vs. Bronze/Silver/Gold (Medallion) terminology

## Repo structure

```
sql/                   DDL — database and table setup
trading-scripts/
  gather_historicals.py  Pulls historical OHLCV from Alpaca, writes to data/
  load_to_database.py    COPY + staging-table MERGE into raw_prices / raw_trades
  main.py                Entry point — loops tickers, gathers + loads
  requirements.txt
  strategies/
   mean_reversion.py
docker-compose.yml      Local Postgres (optional, if not already running one)
.exampleenv             Template for required environment variables
```

## Stack

- **Source**: Alpaca API (historical daily OHLCV, US equities)
- **Warehouse**: Postgres (local, via Docker)
- **Transformation**: dbt (`dbt-postgres`)
- **Orchestration**: Airflow
- **Local dev tools**: Docker Compose, `psql` (via `docker exec`), DBeaver (optional, for browsing/querying visually)

## Note

Airflow runs earlier versions of
pandas
numpy
psycopg2-binary
python-dotenv

We may swap the trading-scripts versions down to match constraints
