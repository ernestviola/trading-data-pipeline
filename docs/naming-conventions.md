# Naming Conventions & Schema Mapping

## Bronze / Silver / Gold → physical Snowflake schemas

| Medallion layer | Snowflake schema | Set via                                                        |
| --------------- | ---------------- | -------------------------------------------------------------- |
| Bronze          | `bronze`         | Explicit `schema:` on sources/seeds                            |
| Silver          | `silver`         | `+schema: silver` in `dbt_project.yml` (staging, intermediate) |
| Gold            | `gold`           | `+schema: gold` in `dbt_project.yml` (marts)                   |

`generate_schema_name()` is a full override (not dbt's default concatenation
behavior) — it returns whatever custom schema it's given, verbatim, with no
`target.schema_` prefixing. This applies uniformly regardless of resource
type (model, seed, source), which is why seeds and models are configured
the same way.

## Prefix conventions

| Prefix | Layer  | Meaning                                                                                                             |
| ------ | ------ | ------------------------------------------------------------------------------------------------------------------- |
| `raw_` | Bronze | Landed data, untouched — either externally loaded (source) or dbt-seeded                                            |
| `stg_` | Silver | 1:1-ish staging model off a single raw table (renaming/typing/light filtering only)                                 |
| `int_` | Silver | Intermediate model combining/transforming staging output, not yet a client-facing mart                              |
| `qc_`  | Gold   | Data-quality/trust-check model, not a business-facing fact/dim                                                      |
| (none) | Gold   | Business-facing fact/dim/view (`holdings_scd2`, `portfolio_value`, `cash_position`, `strategy_performance_summary`) |

## Bronze: source vs. seed

Bronze contains two structurally different kinds of tables, both prefixed
`raw_`, that must not be confused:

- **Sources** (`raw_prices`, `raw_trades`) — landed by something _outside_
  dbt's control (`loader_role` via Airflow's PUT/COPY INTO/MERGE). dbt only
  declares these exist via `source()`; it never creates or owns them.
- **Seeds** (`raw_market_calendar`) — landed _by_ dbt itself via `dbt seed`,
  from a checked-in CSV. Referenced downstream via `ref()`, same as any
  model — never `source()`, since there's no external loader involved.

A table should only ever be declared as one or the other, not both — an
earlier version of this project declared `raw_market_calendar` as both a
source and a seed simultaneously, which produced two separate (duplicate)
nodes in the dbt lineage graph for what was physically one table.

## Known exception: Fivetran corporate actions

`raw_corporate_actions` does not follow the schema mapping above. Fivetran's
Connector SDK enforces one destination schema per connection with no
post-creation rename, so this data physically lands in
`alpaca_corporate_actions.raw_corporate_actions`, not `bronze.raw_corporate_actions`.

**Status: not yet unified into dbt.** The original plan (documented in
Phase 8) was to declare this as a dbt `source()` pointing at its actual
schema, making "Bronze" a logical layer spanning physical schemas rather
than one schema name — the same pattern already used for corporate actions
in Fivetran generally. As of this doc, that `source()` declaration hasn't
been written yet; `alpaca_corporate_actions.raw_corporate_actions` exists in
Snowflake but has no dbt lineage node.
