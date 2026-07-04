from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import os
from sqlalchemy import create_engine

data_dir = Path(__file__).resolve().parent.parent.parent / "data"

engine = create_engine(os.getenv("DATABASE_URL"))


def mean_reversion(ticker, window, threshold):
    df = pd.read_sql(
        "SELECT * FROM raw_prices WHERE symbol = %s ORDER BY timestamp;",
        engine,
        params=(ticker,),
    )

    df["rolling_mean"] = df["close"].rolling(window).mean()
    df["rolling_std"] = df["close"].rolling(window).std()

    df["z_score"] = (df["open"] - df["rolling_mean"].shift(1)) / df[
        "rolling_std"
    ].shift(1)

    conditions = [df["z_score"] < -threshold, df["z_score"] > threshold]
    choices = ["buy", "sell"]

    df["trade_type"] = np.select(conditions, choices, default="hold")

    filtered = df[df["trade_type"] != "hold"].copy()
    filtered["ticker"] = ticker
    filtered["strategy_used"] = "mean_reversion"
    filtered["quantity"] = np.nan

    raw_trades = filtered.rename(
        columns={"timestamp": "date", "trade_type": "side", "open": "price"}
    )
    filepath = (
        data_dir
        / f"raw_trades_{datetime.now().year}-{datetime.now().month}-{datetime.now().day}.csv"
    )

    raw_trades.to_csv(filepath, index=False)
    return filepath
