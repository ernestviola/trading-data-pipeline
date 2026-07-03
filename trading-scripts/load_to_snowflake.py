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

    copy_result = cs.execute(f"""
               COPY INTO RAW_PRICES_STAGING
               FROM @raw_prices_stage
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
               PATTERN = '.*{csv_path.name}.*'
               ON_ERROR = 'CONTINUE'
               """)

    cs.execute("""
            MERGE INTO RAW_PRICES AS target
            USING RAW_PRICES_STAGING AS source
            on target.symbol = source.symbol AND target.timestamp = source.timestamp
            WHEN NOT MATCHED THEN
                INSERT(
                    SYMBOL,
                    TIMESTAMP,
                    "OPEN",
                    HIGH,
                    LOW,
                    "CLOSE",
                    "VOLUME",
                    TRADE_COUNT,
                    VWAP)
                VALUES(
                    source.SYMBOL,
                    source.TIMESTAMP,
                    source.OPEN,
                    source.HIGH,
                    source.LOW,
                    source.CLOSE,
                    source.VOLUME,
                    source.TRADE_COUNT,
                    source.VWAP);
            """)

    cs.execute("TRUNCATE TABLE RAW_PRICES_STAGING;")
