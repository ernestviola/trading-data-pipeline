import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime

data_dir = Path(__file__).resolve().parent.parent.parent / "data"
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


def mean_reversion(ticker, window, threshold):
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

    df["Z_SCORE"] = (df["OPEN"] - df["ROLLING_MEAN"].shift(1)) / df[
        "ROLLING_STD"
    ].shift(1)

    conditions = [df["Z_SCORE"] < -threshold, df["Z_SCORE"] > threshold]
    choices = ["buy", "sell"]

    df["TRADE_TYPE"] = np.select(conditions, choices, default="hold")

    filtered = df[df["TRADE_TYPE"] != "hold"].copy()
    filtered["TICKER"] = ticker
    filtered["STRATEGY_USED"] = "mean_reversion"
    filtered["QUANTITY"] = np.nan

    raw_trades = filtered.rename(
        columns={"TIMESTAMP": "DATE", "TRADE_TYPE": "SIDE", "OPEN": "PRICE"}
    )
    filepath = (
        data_dir
        / f"raw_trades_{datetime.now().year}-{datetime.now().month}-{datetime.now().day}.csv"
    )

    raw_trades.to_csv(filepath, index=False)
    return filepath
