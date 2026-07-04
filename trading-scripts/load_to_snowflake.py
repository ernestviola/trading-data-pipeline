import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    host=os.getenv("SNOWFLAKE_HOST"),
    database="TRADING_PIPELINE",
)

cs = conn.cursor()


def load_csv_to_snowflake(table_name, matching_sql, csv_path: Path):
    print("Processing: ", csv_path)
    put_result = cs.execute(
        f"PUT file://{csv_path} @{table_name}_stage AUTO_COMPRESS=TRUE"
    )

    copy_result = cs.execute(f"""
               COPY INTO {table_name}_STAGING
               FROM @{table_name}_stage
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
               PATTERN = '.*{csv_path.name}.*'
               ON_ERROR = 'CONTINUE'
               """)

    cs.execute(f"DESCRIBE TABLE {table_name}")
    column_names = cs.fetchall()
    target = ['"' + row[0] + '"' for row in column_names]
    source = ['source."' + row[0] + '"' for row in column_names]

    cs.execute(f"""
            MERGE INTO {table_name} AS target
            USING {table_name}_STAGING AS source
            {matching_sql}
            WHEN NOT MATCHED THEN
                INSERT(
                    {", ".join(target)})
                VALUES(
                    {", ".join(source)});
            """)

    # cs.execute(f"TRUNCATE TABLE {table_name}_STAGING;")
