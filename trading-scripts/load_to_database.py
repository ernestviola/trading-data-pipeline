from pathlib import Path
from utils.snowflake_connection import snowflake_connection

conn = snowflake_connection(role="loader_role", schema="bronze")
cs = conn.cursor()


def load_csv_to_snowflake(
    table_name,
    matching_sql,
    csv_path: Path,
    delete_where_sql=None,
    delete_params=None,
    delete_target_where_sql=None,
    delete_target_params=None,
):
    """
    delete_where_sql/delete_params scope staging cleanup to just the rows
    this call is about to load (e.g. "WHERE strategy_used = %s AND ticker =
    %s"), instead of truncating the whole staging table. Needed once more
    than one caller shares a staging table (e.g. raw_trades_staging across
    strategies/tickers) - a full TRUNCATE would wipe another in-flight
    caller's just-loaded, not-yet-merged rows. Defaults to the old
    whole-table TRUNCATE for callers (e.g. raw_prices) that don't share
    their staging table across concurrent callers.

    delete_target_where_sql/delete_target_params optionally clear matching
    rows from the TARGET table (not staging) before the MERGE, turning this
    call into a full replace of that scope instead of an incremental
    append. Needed for callers that recompute their entire history from
    scratch every run (e.g. main.py's strategy functions, which redo the
    full trade signal history against the current config every call) -
    MERGE's WHEN NOT MATCHED THEN INSERT (no UPDATE clause) means a row
    computed under an old config just sits there forever once it exists,
    so recalibrating a threshold and rerunning silently has no effect on
    dates that already have a row. Defaults to None (append-only, the
    original behavior) for callers where that's actually correct, e.g.
    raw_prices' genuinely incremental daily loads.

    NOTE: this makes concurrent calls correctness-safe (no cross-scope data
    loss), but Snowflake's concurrent-DML conflict detection works at the
    micro-partition level, not exact row level - a small staging table can
    still reject one of two truly concurrent transactions even when their
    WHERE predicates don't overlap. See README "Concurrent load retries" for
    the still-open retry-handling gap.
    """
    print("Processing: ", csv_path)

    if delete_target_where_sql:
        cs.execute(
            f"DELETE FROM {table_name} {delete_target_where_sql}", delete_target_params
        )

    # Clear only this call's scope of staging first, not last - guarantees a
    # clean slate before this run's load regardless of whether the previous
    # run for this same scope failed partway through (e.g. a MERGE error
    # after COPY INTO already landed rows).
    if delete_where_sql:
        cs.execute(
            f"DELETE FROM {table_name}_staging {delete_where_sql}", delete_params
        )
    else:
        cs.execute(f"TRUNCATE TABLE {table_name}_staging;")

    # Upload local CSV into a per-table folder on the shared stage.
    # Snowflake's PUT doesn't take a bind variable for the file path.
    stage_path = f"@bronze_load_stage/{table_name}"
    cs.execute(f"PUT file://{csv_path.resolve()} {stage_path}")

    # Load from stage into the staging table. No FORCE — rely on Snowflake's
    # built-in load history (file name + size) to skip files already loaded.
    # PURGE removes the staged file after a successful load.
    cs.execute(f"""
        COPY INTO {table_name}_staging
        FROM {stage_path}/{csv_path.name}
        FILE_FORMAT = (FORMAT_NAME = csv_with_header)
        PURGE = TRUE
        """)

    # Snowflake folds unquoted identifiers to uppercase, unlike Postgres
    # (which lowercases them) — match case-insensitively either way.
    cs.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE UPPER(table_name) = UPPER(%s)
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    column_names = cs.fetchall()
    target = ['"' + row[0] + '"' for row in column_names]
    source = ['source."' + row[0] + '"' for row in column_names]

    cs.execute(f"""
            MERGE INTO {table_name} AS target
            USING {table_name}_staging AS source
            {matching_sql}
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(target)})
                VALUES ({", ".join(source)});
            """)
