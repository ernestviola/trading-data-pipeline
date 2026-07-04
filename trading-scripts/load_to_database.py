from pathlib import Path
from utils.postgres_connection import postgres_connection

conn = postgres_connection()
cs = conn.cursor()


def load_csv_to_postgres(table_name, matching_sql, csv_path: Path):
    print("Processing: ", csv_path)
    with open(csv_path, "r") as f:
        cs.copy_expert(
            f"COPY {table_name}_staging FROM STDIN WITH (FORMAT csv, HEADER true)",
            f,
        )

    cs.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name.lower(),),
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

    conn.commit()

    cs.execute(f"TRUNCATE TABLE {table_name}_staging;")
