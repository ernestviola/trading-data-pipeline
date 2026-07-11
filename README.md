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
- **Recalibrating thresholds and rerunning `main.py` had no effect, because
  `MERGE` never updates existing rows.** Same `WHEN NOT MATCHED THEN INSERT`
  (no `UPDATE`) as the bug above, different symptom: once a row exists for a
  `(ticker, strategy_used, date)`, changing the config and rerunning doesn't
  touch it - it was computed under the old thresholds and just sits there.
  For `macd_momentum` this was total (crossover dates are threshold-
  independent, so the exact same dates come back "already matched" every
  time - sizing changes had zero effect). For `mean_reversion` it was
  partial (only genuinely new/removed trade dates reflected the new
  config). Root cause: `main.py` recomputes each strategy's _entire_ trade
  history from scratch every run, which means a config change needs
  full-replace semantics for that `(strategy_used, ticker)` scope, not
  incremental-append semantics. Fixed via new `delete_target_where_sql`/
  `delete_target_params` on `load_csv_to_snowflake()` - clears matching
  rows from the _target_ table (not just staging) before the `MERGE`,
  scoped the same way as the staging delete. Defaults to `None` (original
  append-only behavior) for callers where that's actually correct, e.g.
  `raw_prices`' genuinely incremental daily loads - only `main.py`'s
  `raw_trades` call opts in.
- **`MERGE` match key must include every column that partitions the table,
  not just what looks unique.** `raw_trades`'s `MERGE` matched on
  `ticker + date` only — correct while `mean_reversion` was the only
  strategy, since strategy_used was constant across every row. Once
  `macd_momentum` shared the same table, any date where both strategies
  traded the same ticker collided: `WHEN NOT MATCHED` saw an existing row
  for that `ticker + date` (from whichever strategy loaded first) and
  silently skipped the other strategy's row for that date. Surfaced as
  `macd_momentum` showing negative `market_value` in
  `strategy_performance_summary` — missing buy rows meant
  `int_position_cost_basis`'s recursive `shares_held - quantity` (no floor
  at zero) went negative. Fixed by adding `strategy_used` to the match key
  in both `main.py` and the DAG. Since `MERGE` here only has
  `WHEN NOT MATCHED THEN INSERT` (no `UPDATE`), no existing row was ever
  corrupted — only some rows were never inserted — so re-running `main.py`
  after the fix backfills what's missing with no manual cleanup needed.
- **`COPY INTO` + staging + `MERGE` needs staging cleared at the _start_
  of a load, not the end.** A failed `MERGE` (e.g. the reserved-word bug
  above) used to leave staging un-truncated, so the next run's `COPY INTO`
  appended on top of leftover rows — silently creating duplicate rows in the
  target table once a run finally succeeded. Fixed by clearing staging as
  the first step of `load_csv_to_snowflake()`, independent of how the
  previous run ended.
- **Scoped `DELETE` instead of blanket `TRUNCATE`, for callers sharing a
  staging table.** `main.py` now runs `mean_reversion` and `macd_momentum`
  back-to-back same-day, both writing through the same `raw_trades_staging`
  table — a full `TRUNCATE` would wipe one strategy's just-loaded,
  not-yet-merged rows out from under the other. `load_csv_to_snowflake()`
  takes an optional `delete_where_sql`/`delete_params` pair; `raw_trades`
  loads pass `WHERE strategy_used = %s AND ticker = %s` so each load only
  clears its own scope. `raw_prices` still defaults to a full `TRUNCATE`
  since nothing shares its staging table yet.
  **Concurrent load retries — still open.** Scoped `DELETE` makes concurrent
  calls _correctness_-safe (no cross-scope data loss) but not necessarily
  _executable_ concurrently: Snowflake's concurrent-DML conflict detection
  works at the micro-partition level, not exact row level, so two truly
  concurrent `DELETE`+`COPY INTO`+`MERGE` transactions against a small
  staging table can still collide and one gets rejected, even though their
  `WHERE` predicates don't overlap. Real parallel loads would need
  retry-on-conflict handling wrapped around `load_csv_to_snowflake()` — not
  yet implemented. Today this doesn't block anything: `main.py`'s two
  `step_2()` calls run sequentially in one process, not concurrently.
- **Dynamic per-ticker Airflow task mapping is safe for computation, not yet
  for shared-resource writes.** `compute_trades` can be `.expand()`-ed per
  ticker (each instance reads Gold independently, no shared state).
  `load_trades` still can't safely be parallelized despite the scoped-
  `DELETE` fix above, for the same concurrent-DML reason — `load_trades`
  stays a single sequential task that loops over each ticker's CSV path from
  `compute_trades`'s XCom output until retry-on-conflict logic exists.
  (`pull_prices`/price-loading has the same latent risk if it's ever split
  the same way — not yet hit in practice, revisit if it becomes a real
  task.)
- **Snowflake connections are role-scoped, not schema-locked by default.**
  `snowflake_connection(role, schema=None)` is a shared helper — `schema` is
  only hardcoded at the call site for connections that structurally can only
  ever target one schema (e.g. `loader_role` → always `bronze`).
  `transformer_role` connections leave schema unset since dbt/analytics reads
  span Silver and Gold.
- **`mean_reversion` has no position-state awareness — known characteristic,
  not a bug.** It re-evaluates the z_score threshold every row with no
  memory of whether it's already flat, so during a sustained trend (e.g.
  AAPL climbing ~259→315 over a real backtest window) it keeps re-firing the
  same-direction signal well after it's already sold every share, producing
  zero-quantity trade rows once inventory hits zero. `momentum`'s crossover
  design (fork 1) doesn't have this failure mode by construction — a sign
  flip only happens once. Left as-is deliberately rather than adding
  position-awareness or filtering zero-quantity rows; worth surfacing if
  strategy performance comparison ever needs to explain a divergence between
  the two.

## Design decisions — momentum strategy (MACD), resolved

Second strategy chosen: **MACD-based momentum** (rejected z-score — that's a
mean-reversion concept, not a momentum one). Three forks were identified;
all three are now resolved:

1. **Crossover event, not threshold-on-magnitude.** True MACD usage is a
   crossover event (histogram flips sign — buy on negative→positive, sell on
   positive→negative), via a `shift()`-based comparison to the previous
   row's sign. This is structurally different from `mean_reversion.py`'s
   current per-row-only threshold check, and is independent of fork 2 below
   — a sign flip is unaffected by whether the histogram is normalized,
   since normalizing divides by a positive value.

2. **Normalize via PPO-style percentage, not raw price units.** `z_score` is
   unitless; MACD's histogram is in raw price units (a difference of EMAs of
   price), so a fixed threshold doesn't transfer across tickers at different
   price levels. Resolved by normalizing the same way a real-world
   Percentage Price Oscillator does: `(fast_EMA − slow_EMA) / slow_EMA *
100`. The raw (non-normalized) histogram still drives the crossover
   trigger per fork 1; the normalized value feeds `signal_strength` as
   `abs(normalized_histogram)` — mirroring `abs(z_score)`'s role for
   mean-reversion.

3. **Per-strategy typed config, not `**kwargs`or a plain dict.**`STRATEGIES`and`step_2()` used to call every strategy with the same fixed positional
parameters (`window, starting_cash, base_position_size, z_threshold,
   max_multiplier, shares_held`), which didn't fit MACD's entirely different
inputs (`fast_period=12, slow_period=26, signal_period=9`). With a target
of 100+ strategies, plain `\*\*kwargs`doesn't namespace (two strategies
both wanting a`threshold`param collide) and doesn't validate (a typo in
a dict key silently produces a wrong value instead of failing loudly).
Implemented instead as`strategies/configs.py`: each strategy is paired
with its own dataclass config in the registry —
`STRATEGIES = {"mean_reversion": (mean_reversion, MeanReversionConfig),
   "macd_momentum": (momentum, MACDConfig)}`— giving namespacing plus
validation/autocomplete, and letting`step_2()`pass a strategy's config
through untouched without knowing its shape. Both configs share the
field name`strength_threshold`for their sizing-normalization role
(mean-reversion's also doubles as its buy/sell trigger cutoff; MACD's
trigger is the crossover event in fork 1, so its`strength_threshold`
   only feeds sizing).

Related refactor this forces regardless: **`sizing.py` currently hardcodes
`row.z_score`** in its strength calculation. Since MACD won't produce a
z-score, `sizing.py` needs to generalize to consume a neutral
`signal_strength` column directly (computed per-strategy upstream, per fork
2 above) rather than deriving `abs(z_score)` itself.

## Agent layer — scoping (paused, sequenced after Phase 9 / Streamlit)

Goal: a conversational "what and why" layer over the pipeline's data — not a
replacement for Phase 9's Streamlit dashboard, but a chat panel embedded in
it. Motivation is partly resume-driven (MCP/agent tool-calling is asked for
directly in AI-engineering and forward-deployed-engineering postings), but
the design is scoped for defensibility, not buzzword coverage.

**Decisions made:**

- **Tools must be strategy-agnostic from the start.** No tool or resource
  gets named after a single strategy (e.g. `get_strategy_signal` takes
  `strategy_used` as a parameter, not a hardcoded "mean-reversion"). This
  mirrors the same principle behind `signal_strength` in the data layer —
  the multi-strategy comparison is the actual differentiator for this
  feature, not single-strategy Q&A. This was originally the phase's only
  blocker (**Phase 6.5**, now done). It's since moved to **after Phase 9**
  too — a standalone agent layer with no Streamlit app to embed into is
  speculative rather than a real integration, so it's sequenced as
  Phase 9.5. The MCP server itself is still independently demoable via
  Claude Desktop regardless of Streamlit; only the in-process chat-panel
  embedding specifically needs Phase 9 done first.
- **No LangGraph, at least initially.** The example questions ("why did the
  strategy exit AAPL," "how did performance change since the threshold
  changed") are sequential multi-tool lookups with no branching and no need
  to persist state across turns — a plain tool-calling loop (model sequences
  its own calls) covers this. LangGraph's explicit graph structure earns its
  place when control flow itself needs to branch, retry with backtracking,
  or checkpoint a long-running task — not preemptively. Revisit only if a
  real question surfaces that a simple loop can't handle.
- **MCP over raw SQL access, transport kept minimal.** Tools are deliberately
  scoped and read-only (no trade execution). Stdio transport is sufficient
  for the stated clients (Claude Desktop, an in-process Streamlit chat
  panel) — no FastAPI/HTTP layer unless a client requiring it shows up.
- **Evals/tracing built alongside tool development, not bolted on at the
  end.** A handful of question/expected-answer pairs and basic tracing from
  day one matter more for defensibility than a fully-featured tool surface
  with no evidence it's ever wrong — if time runs short, this is the part
  that should survive, not get cut as "Phase 3."
- **Streamlit integration is in-process, not another MCP round-trip.** The
  MCP server is the single source of truth for tools; the tool-calling agent
  consumes them and is embedded directly (Python import) as a chat panel in
  the Phase 9 Streamlit app. Claude Desktop can independently connect to the
  same MCP server as a second, separate demo.

**Guardrails (what NOT to build):** no write access via MCP (read-only
tools only), no fine-tuning, no LangGraph/multi-agent orchestration unless a
concrete need for branching or persisted state actually shows up.

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

- [x] Resolve the three design forks above (crossover detection, PPO-style
      normalization, per-strategy typed config)
- [x] Generalize `sizing.py` to consume `signal_strength` directly instead
      of hardcoded `z_score` (`strength_threshold` param, reads
      `row.signal_strength`)
- [x] Define `MACDConfig`/`MeanReversionConfig` dataclasses
      (`strategies/configs.py`) and update `STRATEGIES` registry to pair
      each strategy function with its config class
- [x] Implement `momentum.py`: crossover-based buy/sell (raw histogram sign
      flip via `shift()`, no lookahead — decision uses yesterday's closed
      histogram, executes at today's open) + `signal_strength` from the
      PPO-style normalized histogram. Verified against synthetic price data:
      crossovers fire as sparse events (not per-row), signal_strength is
      non-negative and finite.
- [x] Ran both strategies against AAPL via `main.py`, confirmed independent
      cash/holdings trajectories via `(strategy_used, ticker)` partitioning
- [x] Sanity-checked `strategy_performance_summary` leaderboard output —
      caught and fixed the `MERGE` match-key bug (missing `strategy_used`)
      and the MERGE-never-updates staleness bug along the way; final output
      shows both strategies with plausible independent returns
- [x] Split `strength_threshold` into `buy_strength_threshold`/
      `sell_strength_threshold` (both configs, `sizing.py` picks by
      `row.side`) - buy/sell sensitivity isn't necessarily symmetric.
      Defaults unchanged (both sides equal to the old single value) so
      behavior didn't shift until calibrated.
- [x] Built `calibrate_thresholds.py`: grid-sweeps buy/sell thresholds per
      strategy with an in-sample (pre-2025)/out-of-sample (2025+) date
      split, each split simulated as its own independent portfolio (fresh
      cash, shares_held=0) rather than slicing one continuous run - avoids
      picking a threshold that just curve-fits the full window. Read-only
      against raw_prices, never touches raw_trades/dbt. Verified against
      synthetic regime-shifting price data before handoff; the overfitting
      guard fired as intended (a combo that looked best in-sample lost
      money out-of-sample).
- [x] Ran `calibrate_thresholds.py` against real AAPL data; updated both
      configs' defaults in `main.py` with the sweep + out-of-sample-checked
      thresholds

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

### Phase 9.5 — Agent layer (MCP tools + tool-calling loop)

Moved after Phase 9, not before it - it doesn't make sense as a standalone
phase with nothing to integrate into yet. The chat panel's whole point is
to embed _into_ the Streamlit app for "what and why" questions about what's
on screen, so Streamlit needs to exist first for that integration point to
be real rather than speculative. The MCP server itself is still
independently demoable via Claude Desktop regardless of Streamlit, but the
in-process embedding step specifically needs Phase 9 done.

- [ ] Blocked on Phase 6.5 (already done) and Phase 9 — tools must be
      strategy-parameterized from the start (`strategy_used` as an
      argument, not hardcoded), and there needs to be a Streamlit app to
      embed the chat panel into
- [ ] Scope MCP tool contracts: `get_holdings(strategy_used, ticker,
  as_of_date)`, `get_trade_history(strategy_used, symbol, start_date,
  end_date)`, `get_strategy_signal(strategy_used, symbol, date)`,
      `get_performance_summary(strategy_used, start_date, end_date)`
- [ ] Build MCP server (Python `mcp` SDK, stdio transport — no FastAPI/HTTP
      layer unless a non-Claude-Desktop client actually requires it)
- [ ] Verify tools work via `claude mcp add` in Claude Desktop
- [ ] Plain tool-calling loop on top of the MCP tools (single agent loop,
      model sequences its own tool calls) — no LangGraph graph unless a real
      branching/stateful need shows up once built against real questions
- [ ] Write 5-10 eval question/expected-answer pairs alongside tool
      development, not deferred to a later phase
- [ ] Basic tracing (LangSmith or Langfuse) wired in from the start
- [ ] Embed the agent as a chat panel in the Phase 9 Streamlit app
      (in-process, direct Python import — not another MCP round-trip)

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
- **Planned**: Streamlit (client-facing dashboard), Fivetran (ELT exposure),
  MCP server + tool-calling agent (chat layer over pipeline data)
