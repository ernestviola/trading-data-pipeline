from pathlib import Path
from utils.snowflake_connection import snowflake_connection

conn = snowflake_connection(role="loader_role")
cs = conn.cursor()


def load_csv_to_snowflake(table_name, matching_sql, csv_path: Path):
    print("Processing: ", csv_path)

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

    cs.execute(f"TRUNCATE TABLE {table_name}_staging;")
