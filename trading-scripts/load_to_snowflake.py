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


def load_to_snowflake(csv_path: Path):
    put_result = cs.execute(
        f"PUT file://{csv_path} @raw_prices_stage AUTO_COMPRESS=TRUE"
    )
    # print("Put result:", put_result.fetchall())

    copy_result = cs.execute(f"""
               COPY INTO RAW_PRICES
               FROM @raw_prices_stage
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
               PATTERN = '.*{csv_path.name}.*'
               ON_ERROR = 'CONTINUE'
               """)

    # print("Copy result:", copy_result.fetchall())
