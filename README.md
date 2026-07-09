# Trading Strategy Backtest & Execution Platform

A data engineering portfolio project centered on comparing algorithmic trading
strategies against historical data, with a path toward live paper execution
and, eventually, intelligent strategy switching. Built to demonstrate
incremental loading, SCD Type 2 modeling, role-based access control, and
orchestration patterns relevant to fintech data engineering.

## Project vision — three layers

This project is being built in three distinct layers. Each is a genuinely
different engineering problem, not a bigger version of the last one.

### Layer 1 — Backtesting / strategy comparison (current focus)

Pull historical OHLCV data, run multiple pluggable trading strategies against
it independently, and compare their simulated performance side by side —
per ticker, and across tickers. This is the layer currently being built out
on Snowflake/dbt/Airflow.

Key design choice: each `(strategy, ticker)` pair is an **independent
simulated portfolio** — its own starting cash, its own position history. This
is what makes "mean_reversion vs. momentum on AAPL" or "best strategy across
all tickers" an apples-to-apples comparison, and it's why `cash_position` and
`holdings_scd2` are partitioned by `(strategy_used, ticker)` rather than
tracking one global portfolio.

### Layer 2 — Live paper execution (not started)

Once a strategy is validated in backtesting, submit its signals as real
orders to Alpaca's paper trading API rather than simulating trades in Python.
Key differences from Layer 1, noted here so they aren't lost:

- Alpaca's paper account enforces buying power itself — no need to replicate
  `sizing.py`'s affordability logic as a synchronous ledger/lock the way a
  real live-money OMS would need. The broker is the ledger.
- New Bronze sources: `raw_orders` (submitted) and `raw_fills`/`raw_positions`
  (what Alpaca actually executed), pulled from Alpaca's API rather than
  generated in Python.
- `holdings_scd2`/`portfolio_value` shift from synthetic trade replay to
  real fill events — same SCD2/recursive-CTE pattern, different source.
- Asset scope is constrained by what Alpaca actually offers: **US equities
  and crypto only**. No forex, no true commodities (commodity _exposure_ is
  only available via ETF proxy, e.g. `GLD` for gold).
- Cash becomes a **single shared pool across tickers** again (not partitioned
  per strategy/ticker like backtesting) — this is a deliberate, later
  divergence from Layer 1's design, not a contradiction of it. A "mixed
  strategy" that switches between strategies mid-flight is just another
  `strategy_used` value under this shared-cash model — no separate
  architecture needed for it.

### Layer 3 — Regime switching / intelligent strategy allocation (not started, hardest)

A meta-strategy that decides _which_ underlying strategy to run based on
market conditions, to maximize profit. This is an open-ended quant research
problem (regime detection / strategy allocation), not a small feature — to
be scoped seriously once Layers 1 and 2 exist and there's real comparative
performance data to make switching decisions from.

## Architecture (Layer 1 — current)

```
Alpaca API (historical OHLCV)
        │
        ▼
  Bronze (Snowflake, loader_role)
  raw_prices, raw_trades — landed via PUT + COPY INTO + staging/MERGE dedup
        │
        ▼
  Silver (Snowflake, transformer_role — dbt)
  stg_prices, stg_trades → int_portfolio_cash, int_position_cost_basis
  (recursive CTEs, partitioned by strategy_used + ticker)
        │
        ▼
  Gold (Snowflake, transformer_role — dbt)
  holdings_scd2, cash_position, portfolio_value (incremental),
  strategy_performance_summary (view)
        │
        ▼
   Airflow DAG orchestrates ingestion + dbt runs,
   with retries + dynamic per-ticker task mapping
```

**Role-based access control:**

- `loader_role` — write access to Bronze only. Used by Python ingestion.
- `transformer_role` — read-only on Bronze, read/write Silver + Gold. Used by dbt.
- `SYSADMIN` — read access across all three schemas, for manual browsing
  without switching roles. `ACCOUNTADMIN` is left untouched, reserved for
  account-level operations.

## Key design decisions (Layer 1)

- **Strategy comparison requires independent portfolios, not one shared
  ledger.** `cash_position` and `holdings_scd2` are partitioned by
  `(strategy_used, ticker)` — each combination replays its own trades against
  its own $10,000 starting cash. This also sidesteps a real bug we found:
  a single shared cash pool across tickers has no natural way to arbitrate
  which ticker's buy "wins" when two tickers compete for the same cash in
  the same run — a live-execution problem this project intentionally isn't
  solving in the backtesting layer.
- **`strategy_performance_summary` is a view, not a table.** It's a cheap
  `MAX(date)`-per-group read on top of already-computed `portfolio_value`
  data — no incremental/materialization cost to justify storing it.
  Comparison metric is **percent return** (`(ending_value - starting_cash) /
starting_cash`), not raw dollar gain, so it's fair across strategies.
- **Reserved words need explicit quoting in Snowflake, and it bites in more
  places than DDL.** `"timestamp"` and `"date"` were quoted at table-creation
  time, which means every later _reference_ to them — in dbt models, in
  hand-written `MERGE` matching SQL, in ad hoc queries — must also be quoted
  and case-matched, or Snowflake's uppercase-folding of unquoted identifiers
  will silently look for a different (nonexistent) column.
- **`COPY INTO` + staging + `MERGE` needs staging truncated at the _start_
  of a load, not the end.** A failed `MERGE` (e.g. the reserved-word bug
  above) used to leave staging un-truncated, so the next run's `COPY INTO`
  appended on top of leftover rows — silently creating duplicate rows in the
  target table once a run finally succeeded. Fixed by truncating staging as
  the first step of `load_csv_to_snowflake()`, independent of how the
  previous run ended.
- **Dynamic per-ticker Airflow task mapping is safe for computation, not for
  shared-resource writes.** `compute_trades` can be `.expand()`-ed per ticker
  (each instance reads Gold independently, no shared state). `load_trades`
  cannot — all tickers currently share one `raw_trades_staging` table, so
  parallel loaders would race on truncate/load. `load_trades` stays a single
  sequential task that loops over each ticker's CSV path from
  `compute_trades`'s XCom output. (`pull_prices`/price-loading has the same
  latent risk if it's ever split the same way — not yet hit in practice,
  revisit if it becomes a real task.)
- **Snowflake connections are role-scoped, not schema-locked by default.**
  `snowflake_connection(role, schema=None)` is a shared helper — `schema` is
  only hardcoded at the call site for connections that structurally can only
  ever target one schema (e.g. `loader_role` → always `bronze`).
  `transformer_role` connections leave schema unset since dbt/analytics reads
  span Silver and Gold.

## Open design questions — momentum strategy (MACD), paused mid-design

Second strategy chosen: **MACD-based momentum** (rejected z-score — that's a
mean-reversion concept, not a momentum one). Three forks were identified but
not yet resolved:

1. **Crossover event vs. threshold on histogram magnitude.** True MACD usage
   is a crossover event (histogram flips sign — buy on negative→positive,
   sell on positive→negative), which needs a `shift()`-based comparison to
   the previous row's sign. This is structurally different from
   `mean_reversion.py`'s current per-row-only threshold check. Alternative:
   threshold the histogram's raw magnitude per-row (simpler, reuses the
   existing conditional pattern, but not how MACD is conventionally used).
   **Leaning toward true crossover detection** as the more defensible
   approach — not yet implemented.

2. **Scale mismatch between MACD histogram and existing threshold logic.**
   `z_score` is unitless (standardized); MACD's histogram is in raw price
   units (a difference of EMAs of price), so a fixed `z_threshold`-style
   value doesn't transfer — $1.50 means something different for a $10 stock
   vs. a $500 stock. Open question: normalize the histogram (e.g. as a
   percentage of price) or accept per-ticker threshold tuning.

3. **Strategy function signature needs to generalize.** `STRATEGIES` registry
   and `step_2()` currently call every strategy with the same fixed
   parameters (`window, starting_cash, base_position_size, z_threshold,
max_multiplier, shares_held`). MACD needs different inputs entirely
   (`fast_period=12, slow_period=26, signal_period=9`, plus whatever
   threshold #2 lands on). This will likely require `**kwargs` or a
   per-strategy config dict rather than a fixed positional signature.

Related refactor this will force regardless of how the forks resolve:
**`sizing.py` currently hardcodes `row.z_score`** in its strength
calculation. Since MACD won't produce a z-score, `sizing.py` needs to
generalize to consume a neutral `signal_strength` column directly (computed
per-strategy upstream) rather than deriving `abs(z_score)` itself.

## Todo

### Phase 1–5 — Setup, trade generation, dbt modeling, orchestration, polish

_(Complete on Postgres — see git history for detail. Superseded by Phase 6
migration below.)_

### Phase 6 — Snowflake Migration

- [x] Set up Snowflake trial account (warehouse, database, schema, role)
- [x] Bronze/Silver/Gold schemas, `loader_role`/`transformer_role` with
      scoped grants, `SYSADMIN` read access for manual browsing
- [x] Key-pair auth (`snowflake_connection.py`), retired password auth
- [x] Swap Postgres connection (psycopg2) for Snowflake Python connector
      (`PUT` + `COPY INTO`, staging + `MERGE` dedup pattern preserved)
- [x] Update DDL for Snowflake types/syntax (`TIMESTAMP_NTZ`, no `SERIAL`)
- [x] Swap `dbt-postgres` adapter for `dbt-snowflake`, update `profiles.yml`
      (key-pair auth, `transformer_role`, custom `generate_schema_name` macro
      for clean Bronze/Silver/Gold schema names)
- [x] Verify existing models (recursive CTEs, window functions) run
      unmodified on Snowflake — confirmed, only syntax delta was reserved
      word quoting (`"timestamp"`, `"date"`), not the CTE/window logic itself
- [x] Repartition `cash_position`/`holdings_scd2`/`portfolio_value` by
      `(strategy_used, ticker)` for independent strategy comparison
- [x] Add `strategy_performance_summary` view (percent-return leaderboard)
- [x] Harden `load_csv_to_snowflake()` against partial-failure duplicate rows
- [ ] Update Airflow connection from Postgres type to Snowflake type
- [ ] Split `generate_trades` into `compute_trades` (dynamic per-ticker
      mapping, `transformer_role`) + `load_trades` (single sequential task,
      `loader_role`) — design finalized, not yet implemented in the DAG

### Phase 6.5 — Second strategy (MACD momentum)

- [ ] Resolve the three open forks above (crossover vs. threshold, scale
      normalization, strategy signature generalization)
- [ ] Generalize `sizing.py` to consume `signal_strength` directly instead
      of hardcoded `z_score`
- [ ] Implement `momentum.py`, register in `STRATEGIES`
- [ ] Run both strategies against AAPL, confirm independent cash/holdings
      trajectories via `(strategy_used, ticker)` partitioning
- [ ] Sanity-check `strategy_performance_summary` leaderboard output

### Phase 7 — Snowflake Streams & Tasks (incremental processing)

- [ ] Add a Stream on `raw_trades` to track new rows since last consumption
- [ ] Add a Task to trigger downstream dbt processing when the Stream has data
- [ ] Document how Stream/Task-driven triggering compares to Airflow's
      schedule-driven incremental runs

### Phase 8 — Fivetran / ELT tooling exposure

- [ ] Decide approach (Alpaca isn't a native Fivetran connector) — Option A:
      custom Fivetran Connector SDK for Alpaca. Option B: genuinely
      Fivetran-native second source. No decision yet.
- [ ] Set up Fivetran free tier, land data into Bronze
- [ ] Document how Fivetran's managed sync differs from custom Python ingestion

### Phase 9 — Streamlit client-facing app

- [ ] Comparison chart: all strategies against one chosen ticker
- [ ] Leaderboard view: best strategy per ticker, across all tickers
      (reads `strategy_performance_summary` directly)
- [ ] Current holdings table, data quality/test status indicator
- [ ] Scope as a thin client-facing view, not a full app

### Phase 10 — Data quality / QC framing

- [ ] Reframe existing dbt tests explicitly as client-facing trust/QC rules
- [ ] Lightweight data dictionary / lineage doc
- [ ] Document raw/staging/marts → Bronze/Silver/Gold naming mapping

### Layer 2 — Live paper execution (scoping only, not started)

- [ ] Alpaca order-submission client
- [ ] `raw_orders`/`raw_fills`/`raw_positions` Bronze sources
- [ ] Shared (non-partitioned) cash model for live execution, distinct from
      backtesting's per-`(strategy, ticker)` partitioning
- [ ] Next-day execution scheduling (signal computed after close, order fills
      at next market open)

### Layer 3 — Regime switching (scoping only, not started)

- [ ] Not yet scoped — depends on having real comparative performance data
      from Layers 1 and 2 first

## Stack

- **Source**: Alpaca API (historical daily OHLCV, US equities + crypto)
- **Warehouse**: Snowflake (Bronze/Silver/Gold, role-based access control)
- **Transformation**: dbt (`dbt-snowflake`)
- **Orchestration**: Airflow (Docker Compose, LocalExecutor)
- **Planned**: Streamlit (client-facing dashboard), Fivetran (ELT exposure)
