# Build Log

Detailed build history, bugs found and fixed, and design-decision rationale
for this project — moved out of the top-level README to keep that scannable
for someone evaluating the project rather than following its history.

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
- **Negative `shares_held` in `holdings_scd2`, root-caused to the DAG
  feeding stale Gold state into what is actually a full-history replay
  every run — fixed by making the DAG a thin wrapper around `main.py`.**
  Surfaced via `select * from holdings_scd2 where shares_held < 0`
  returning real rows. Traced through the stack: `int_position_cost_basis`'s
  recursive CTE has no floor on `shares_held - quantity` for a sell (by
  design — see `size_trades` below), so a sell that legitimately exceeds
  current holdings goes negative and that bad running total propagates
  through every later row for that `(strategy_used, ticker)`. `sizing.py`'s
  `size_trades()` itself is correct in isolation — `qty = min(desired_qty,
shares_held)` guarantees a fresh position's first sell is always `qty=0`
  — _provided_ it's called with the right starting `shares_held`. The DAG's
  old `compute_trades` task read `cash_position`/`holdings_scd2`'s
  `is_current` row as the starting state for each run, intended for
  incremental processing — but every strategy function (`mean_reversion()`,
  etc.) actually recomputes signals and sizes trades across the **entire**
  price history from scratch every call, same as `main.py`. So the DAG was
  feeding a previous run's _ending_ balance into `size_trades()` as the
  _starting_ balance for a full replay from 2023 - meaning trade #1 of the
  replay (chronologically first, but not actually "first" from
  `size_trades()`'s point of view) could see a non-zero `shares_held` and
  produce a real, non-zero sell where a fresh run would have correctly
  produced zero. Confirmed by cross-checking `main.py` (hardcodes
  `shares_held=0`, unaffected) against the DAG (read stale Gold state,
  affected) and by observing the corrupted value stayed pinned across many
  months of subsequent rows with no new trades in between, consistent with
  a bad running total rather than fresh data.
  **Fix, in two parts:**
  1. **`portfolio_value` needed `dbt run --select portfolio_value
--full-refresh`** to actually clear the corrupted historical rows -
     its `is_incremental()` filter (`where p.price_date > max(price_date)
in this`) is a single global watermark, not scoped per
     `(strategy_used, ticker)`, so a plain `dbt run` can never revisit
     dates already "covered" by that watermark even after the upstream
     data is fixed. This is a recurring operational quirk, not a one-off -
     any future backfill of historical data for a new ticker (see GOOGL
     below) hits the same gap and needs the same manual `--full-refresh`.
  2. **The DAG no longer reads Gold for starting state at all.** Decided
     to stop treating the DAG as incremental and instead have it do a
     full replay each run, same as `main.py` — the DAG became a thin
     wrapper around `main.py`'s `step_1`/`step_2` rather than its own
     separate incremental logic.
- **`mean_reversion`'s zero-quantity trade rows, left as a known,
  documented tradeoff rather than fixed.** In a sustained one-directional
  price trend (e.g. AAPL climbing ~259→315 over a real backtest window) it
  keeps re-firing the same-direction signal well after it's already sold
  every share, producing zero-quantity trade rows once inventory hits zero.
  `momentum`'s crossover design doesn't have this failure mode by
  construction — a sign flip only happens once. Left as-is deliberately
  rather than adding position-awareness or filtering zero-quantity rows;
  worth surfacing if strategy performance comparison ever needs to explain
  a divergence between the two.
- **Adding a new ticker needs a one-time manual historical backfill —
  the DAG alone won't do it.** `pull_prices` only ever pulls Airflow's
  current `data_interval` window; there's no mechanism for a ticker newly
  added to `TICKERS` to retroactively get 2023-to-now price history just
  because it's new to the list (confirmed when adding GOOGL only pulled
  the latest day). `main.py`'s `step_1` already does the right thing (hard-
  coded `2023-01-01` to now) - the fix is running `step_1` manually once
  for the new ticker before the DAG's daily runs take over. `airflow dags
backfill` is the wrong tool for this specifically - it replays a DAG
  across historical _scheduled_ windows, which would mean replaying the
  now-full-history `step_2` computation once per historical day, not
  backfilling one ticker's price history once.

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

3. **Per-strategy typed config, not `**kwargs`or a plain dict.**`STRATEGIES`and`step_2()` used to call every strategy with the same
fixed positional parameters (`window, starting_cash, base_position_size,
   z_threshold, max_multiplier, shares_held`), which didn't fit MACD's
entirely different inputs (`fast_period=12, slow_period=26,
   signal_period=9`). With a target of 100+ strategies, plain `**kwargs`doesn't namespace (two strategies both wanting a`threshold`param
collide) and doesn't validate (a typo in a dict key silently produces a
wrong value instead of failing loudly). Implemented instead as`strategies/configs.py`: each strategy is paired with its own dataclass
config in the registry — `STRATEGIES = {"mean_reversion": (mean_reversion,
   MeanReversionConfig), "macd_momentum": (momentum, MACDConfig)}`— giving
namespacing plus validation/autocomplete, and letting`step_2()`pass a
strategy's config through untouched without knowing its shape. Both
configs share the field name`strength_threshold`for their
sizing-normalization role (mean-reversion's also doubles as its buy/sell
trigger cutoff; MACD's trigger is the crossover event in fork 1, so its`strength_threshold`only feeds sizing). Calibrated values from`calibrate_thresholds.py` live as the dataclass **defaults\*\* in
   `configs.py` — the single source of truth `main.py`'s `STRATEGIES` loop
   and the DAG both construct fresh instances from (`config_cls()`).

Related refactor this forced regardless: `sizing.py` used to hardcode
`row.z_score` in its strength calculation. Since MACD doesn't produce a
z-score, `sizing.py` was generalized to consume a neutral `signal_strength`
column directly (computed per-strategy upstream, per fork 2 above) rather
than deriving `abs(z_score)` itself.

## Agent layer — scoping (paused, sequenced after Phase 10 / QC)

Goal: a conversational "what and why" layer over the pipeline's data — not a
replacement for the Streamlit dashboard, but a chat panel embedded in it.
Motivation is partly resume-driven (MCP/agent tool-calling is asked for
directly in AI-engineering and forward-deployed-engineering postings), but
the design is scoped for defensibility, not buzzword coverage.

**Decisions made:**

- **Tools must be strategy-agnostic from the start.** No tool or resource
  gets named after a single strategy (e.g. `get_strategy_signal` takes
  `strategy_used` as a parameter, not a hardcoded "mean-reversion"). This
  mirrors the same principle behind `signal_strength` in the data layer —
  the multi-strategy comparison is the actual differentiator for this
  feature, not single-strategy Q&A.
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
  the Streamlit app. Claude Desktop can independently connect to the same
  MCP server as a second, separate demo.

**Guardrails (what NOT to build):** no write access via MCP (read-only
tools only), no fine-tuning, no LangGraph/multi-agent orchestration unless a
concrete need for branching or persisted state actually shows up.

## Completed phases

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
- [x] Update Airflow connection from Postgres type to Snowflake type
      (`load_csv_to_snowflake`/`snowflake_connection`)
- [x] Generalize the DAG to run all strategies for every ticker in
      `TICKERS`, mirroring `main.py`'s loop over the `STRATEGIES` registry —
      superseded the original `compute_trades`/`load_trades` `.expand()`
      split entirely; see the negative-`shares_held` writeup above for why
      the DAG became a thin wrapper around `main.py`'s `step_1`/`step_2`
      instead

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
      configs' defaults in `configs.py` with the sweep + out-of-sample-
      checked thresholds (previously drifted into `main.py`'s inline calls
      instead — fixed as part of the DAG-refactor work above)

### Phase 7 — Snowflake Streams & Tasks (incremental processing)

Deliberately built as a standalone path, not wired into Airflow or dbt - the
target JD asks for Streams/Tasks specifically as the Snowflake-native
incremental mechanism, distinct from schedule-driven batch orchestration
(which Airflow/dbt already demonstrate elsewhere in this project). See
`sql/snowflake/002_setup_streams_and_tasks.sql`.

- [x] Add a Stream on `raw_trades` to track new rows since last consumption
      (`bronze.raw_trades_stream`, `APPEND_ONLY` - `raw_trades` only ever
      receives inserts, never updates/deletes, so a full standard stream's
      before/after tracking isn't needed) - deployed to the live account;
      end-to-end behavior (new data landing → stream flips true → task
      fires → `stg_trades_streaming` populates) not yet confirmed
- [x] Add a Task that runs a stored procedure MERGE-ing only the Stream's
      changed rows into `silver.stg_trades_streaming` when the Stream has
      data (`WHEN SYSTEM$STREAM_HAS_DATA(...)`) - MERGE instead of INSERT
      because `main.py`/the DAG full-replace `raw_trades` on a config change
      (delete + re-insert), which an append-only stream sees as new rows,
      not updates; without a MERGE keyed on `(ticker, strategy_used,
    trade_date)` a full-replace upstream would duplicate rows here rather
      than overwrite them - written, not yet run against a live account
- [x] Document how Stream/Task-driven triggering compares to Airflow's
      schedule-driven incremental runs (below)

**Streams/Tasks vs. Airflow - comparison**

- **What triggers work.** Airflow's `check_new_trades`/`new_data_landed`
  polls on a fixed schedule (`@daily`) and short-circuits if nothing landed
  - the DAG still wakes up and evaluates even when there's nothing to do. A
    Task's `WHEN SYSTEM$STREAM_HAS_DATA(...)` clause still evaluates on its
    own schedule (here, every minute), but skips spinning up warehouse compute
    entirely when the stream is empty - the "poll" itself is metadata-only and
    free; only the actual `CALL` costs compute.
- **What "incremental" means.** `dbt run` re-evaluates each model's full
  defined query every run - for `stg_trades` that's a full scan/reload of
  `raw_trades`, not just what's new, even though the transformation itself
  is simple. A Stream tracks exact row-level offsets since last consumption,
  so the Task's `MERGE` only ever touches genuinely new rows - the "keeping
  normalized datasets fresh without full reprocessing" framing directly.
- **Where the transformation logic lives.** dbt models are declarative SQL
  with lineage, tests, and docs generation built around them - strong for
  anything client-facing or governed. The stored procedure here is
  imperative SQL with none of that tooling - fine for a narrow, fast,
  internal freshness path, but it doesn't get dbt's testing/documentation
  for free, and duplicating real transformation logic (e.g. the recursive
  CTEs in `int_portfolio_cash`) into a procedure would mean maintaining the
  same logic twice.
- **Failure handling.** Airflow retries (`retries: 3`,
  `retry_delay: timedelta(minutes=5)`) are explicit, visible in the UI, and
  alertable. A Task's failure history lives in
  `INFORMATION_SCHEMA.TASK_HISTORY` - queryable, but there's no
  out-of-the-box UI/alerting layer the way Airflow has one built in.
- **When each earns its place.** Streams/Tasks fit a narrow, low-latency,
  single-table-to-single-table freshness problem where spinning up an
  orchestrator's overhead isn't worth it. Airflow/dbt fit anything with
  real dependency chains, multi-step lineage, or where testing/docs/alerting
  matter more than shaving latency - which is most of this project's Silver/
  Gold layer, hence why this stays a standalone comparison rather than a
  replacement for the existing DAG.

### Phase 8 — Fivetran / ELT tooling exposure

- [x] Decide approach (Alpaca isn't a native Fivetran connector) — **Option
      A chosen**: build a custom Fivetran Connector SDK connector for
      Alpaca, rather than Option B (a genuinely Fivetran-native second
      source like S3/Google Sheets). Reasoning: Option A demonstrates
      _building_ ELT tooling, which is a stronger match for "hands-on
      experience with Fivetran or equivalent ELT tooling" than configuring
      an existing connector would be.
- [x] Scope: pull Alpaca's **corporate actions** endpoint — expanded during
      build from the original dividends/splits-only scope to **all 14**
      corporate action types Alpaca supports (splits, mergers, spinoffs,
      name changes, etc.), landed unfiltered. Reasoning: `symbols` is
      filtered at extract time (bounded by `TICKERS` — a real entity-scope
      boundary), but `types` isn't, since excluding a type at extract means
      discarding a category permanently with no principled reason to rule
      it out yet — that's Transform's job, not Extract's, per ELT
      philosophy. Not another OHLCV bars pull — re-fetching price bars
      through a second pipeline would just duplicate `gather_historicals.py`
      → `raw_prices`, not add anything. Corporate actions maps directly onto
      **NAV components** (assets, liabilities, accrued income,
      corporate-action adjustments) - one of the fund-admin entities the
      target JD names explicitly - and this project's actual end goal (see
      Layer 2 in the roadmap) needs dividend-aware NAV modeling eventually
      anyway, so building the data source for it now, even at extra upfront
      cost, sets up that future work instead of deferring it.
- [x] Land corporate actions data into Bronze via the custom connector -
      lands in `alpaca_corporate_actions.raw_corporate_actions`, **not**
      `bronze.raw_corporate_actions` as originally planned (see "Fivetran
      forces one schema per connection" below) - this phase is scoped to
      _landing the data_, not yet wiring it into `cash_position`/
      `portfolio_value` math. Today those models don't account for
      dividends at all (a known simplification) - integrating corporate
      actions into the actual NAV/cash calculations is separate, later
      work, not part of this phase.
- [ ] Document how Fivetran's managed sync + Connector SDK differs from the
      custom Python ingestion path (`gather_historicals.py`)

**Design decisions — Fivetran corporate actions connector, resolved**

- **Endpoint: `CorporateActionsClient.get_corporate_actions()`, not the
  deprecated `TradingClient.get_corporate_announcements()`.** The
  deprecated endpoint caps date ranges at 90 days per request - unworkable
  for a multi-year historical backfill without manual windowing. The
  current endpoint has no such cap.
- **Cursor: `process_date` (Alpaca's own ingestion date), not any business
  date** (`ex_date`/`declaration_date`/etc.). Alpaca's docs explicitly warn
  corporate actions can arrive from their data providers well after the
  event itself occurred; cursoring on a business date risks silently
  dropping late-arriving records once the sync window moves past them.
  `start`/`end` on `CorporateActionsRequest` filter and sort on
  `process_date` under the hood, confirmed against the underlying REST
  reference - the SDK just doesn't name the field explicitly in its own
  docstring.
- **7-day lookback window on every incremental sync**, checkpoint capped at
  `today - 7` rather than `today`. Same-day stragglers (a correction
  landing hours after that day's sync already ran) get silently missed
  otherwise. Dedup on re-fetched records is free via `id`-keyed
  `op.upsert()` - no custom dedup logic needed. Known gap: a delay longer
  than 7 days would still be missed; accepted as a documented limitation
  rather than solved, same spirit as the `mean_reversion`
  zero-quantity-trades tradeoff.
- **`CorporateActionsRequest(limit=None)`, explicitly** - the field
  defaults to `1000` if unset, which silently caps the _total_ records
  returned across every symbol and type combined, with no error and no
  signal more exist. Traced directly from `alpaca-py`'s internal
  `_get_marketdata()` pagination loop: it walks `next_page_token` correctly,
  but only continues while `total_items < limit` - so an unset `limit`
  defaulting to `1000` stops the loop exactly at that ceiling. Passing
  `limit=None` disables the total cap; per-request page size is still
  capped by the SDK internally, but pagination continues until Alpaca
  itself returns no further `next_page_token`.
- **Bronze table shape: narrow real columns + one `VARIANT`/`JSON` payload
  column, not a wide table or per-type tables.** The 14 corporate action
  types are a genuine union of differently-shaped records (a cash
  dividend's `rate` and a split's `old_rate`/`new_rate` aren't the same
  kind of number), so a single wide table would mean pre-deciding a
  relational shape - real interpretation work - before Silver ever runs,
  and would break the moment Alpaca adds a 15th type. `id`, `symbol`,
  `ca_type`, `process_date` are declared as real columns (enough to
  identify, dedup, and cursor on); the full raw record lands untouched in
  one `payload` column. Every type takes the identical path with zero
  type-specific decisions made in Bronze - shaping is deferred entirely to
  a future Silver staging model, mirroring how `stg_trades` already shapes
  `raw_trades`.
- **New `fivetran_role`, not a reuse of `loader_role`.** Same reasoning as
  the existing `loader_role`/`transformer_role` split - auditable,
  independently revocable blast radius per ingestion tool. Authenticates as
  a dedicated Snowflake `SERVICE` user with its own key pair
- **Fivetran forces one destination schema per connection - a real,
  structural limit discovered mid-build, not a config choice.** The
  original plan was landing directly in `bronze.raw_corporate_actions`,
  alongside `raw_prices`/`raw_trades`. Fivetran's Connector SDK doesn't
  support this: each connection gets its own auto-named destination schema
  (named from the connection name), enforced for write isolation between
  connectors, and un-renameable after creation without deploying an
  entirely new connection. A multi-destination workaround exists (separate
  Fivetran destinations, both pointed at the same Snowflake database/role,
  independently configured to use the schema name `bronze`) but was
  rejected - Fivetran's own docs flag exactly the failure mode already
  documented above under the concurrent-`DELETE`-retry bug: two independent
  writers assuming exclusive control of shared destination state.
  Resolution: accept per-connector schemas
  (`alpaca_corporate_actions.raw_corporate_actions`) and unify at the
  transformation layer instead. "Bronze" becomes a logical raw-data layer
  spanning schemas, not one physical schema name. **As of this writing,
  that unification hasn't actually been written yet** — see "Known open
  items" in the README roadmap.

### Phase 9 — Streamlit client-facing app

- [x] Comparison chart: all strategies against one chosen ticker
- [x] Leaderboard view: best strategy per ticker, across all tickers
      (reads `strategy_performance_summary` directly)
- [x] Current holdings table (pivoted: metrics as rows, ticker/strategy as
      two-level columns, filterable by ticker)
- [x] Scope as a thin client-facing view, not a full app
- [x] Deployed to Streamlit in Snowflake (Workspaces), alongside local dev
      version — `streamlit_role`/`streamlit_svc` (Gold-only read access,
      own key pair) for local; app-owner role + `GRANT USAGE ON STREAMLIT`
      RBAC for SiS access gating

### Phase 10 — Data quality / QC framing

- [x] Reframe existing dbt tests explicitly as client-facing trust/QC rules
      — descriptions added on both singular tests
      (`assert_no_overlapping_scd2_ranges`, `assert_single_current_cash_position`)
      and the `unique_combination_of_columns` test on `int_portfolio_cash`,
      explaining why each rule matters. Also fixed three pre-existing bugs
      surfaced along the way, all sharing the same root cause: tests written
      before this project went multi-strategy, never updated to include
      `strategy_used` in their join/grouping/uniqueness logic.
- [x] Lightweight data dictionary / lineage doc — `description:` fields
      added to every model/seed/column (judgement-call coverage) across
      `staging/`, `intermediate/`, `marts/` schema.yml files and the
      `raw_market_calendar` seed; `dbt docs generate && dbt docs serve`
      produces the actual dictionary/lineage artifact from these.
- [x] Document raw/staging/marts → Bronze/Silver/Gold naming mapping —
      `docs/naming-conventions.md`
- [x] Streamlit QC status indicator — persistent sidebar/banner element
      (`st.success`/`st.warning`, cached with the same `ttl=300` as other
      queries) plus a dedicated `views/qc_status.py` detail page

**Freshness check design:**

- `qc_portfolio_value_freshness` (Gold), grain `(strategy_used, ticker)`,
  Type 1 (overwrite — no point-in-time history needed for a check that only
  cares about "right now"). Compares each pair's latest `price_date` in
  `portfolio_value` against the most recent trading day on or before today,
  sourced from a market-calendar seed (`raw_market_calendar`, pulled from
  Alpaca's `TradingClient.get_calendar()`) rather than naive wall-clock date
  comparison — the pipeline runs at midnight, when "today" can never have
  data yet, and a plain `today() == max(price_date)` check would also
  false-alarm every weekend/holiday.
- **Known limitation:** a `(strategy_used, ticker)` pair with zero rows
  ever loaded won't appear in this table at all, since it's derived from
  `portfolio_value` rather than an enumerated expected-pairs list. Catches
  staleness, not total absence.
- **Source vs. seed distinction:** `raw_market_calendar` is dbt-owned (built
  via `dbt seed` from a checked-in CSV, referenced downstream via `ref()`)
  — not a `source()`, since no external loader is involved. An earlier
  version of this table was mistakenly declared as both, producing a
  duplicate node in the dbt lineage graph.
