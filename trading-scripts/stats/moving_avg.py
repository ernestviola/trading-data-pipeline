import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    host=os.getenv("SNOWFLAKE_HOST"),
    database="TRADING_PIPELINE",
)

cs = conn.cursor()


def moving_avg(ticker, window):
    cs.execute("USE DATABASE TRADING_PIPELINE;")
    cs.execute(f"""
      SELECT * FROM RAW_PRICES
      where symbol = '{ticker}'
      ORDER BY timestamp
      ;
    """)

    dat = cs.fetch_pandas_all()
    df = pd.DataFrame(dat)

    df["ROLLING_MEAN"] = df["CLOSE"].rolling(window).mean()
    df["ROLLING_STD"] = df["CLOSE"].rolling(window).std()
    print(df[["TIMESTAMP", "OPEN", "CLOSE", "ROLLING_MEAN", "ROLLING_STD"]])
