from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import os
from sqlalchemy import create_engine
from strategies.sizing import size_trades

data_dir = Path(__file__).resolve().parent.parent.parent / "data"

engine = create_engine(os.getenv("DATABASE_URL"))


def mean_reversion(
    ticker,
    window,
    starting_cash,
    base_position_size,
    z_threshold,
    max_multiplier,
    shares_held,
    strategy_used,
):
    filepath = (
        data_dir
        / f"raw_trades_{datetime.now().year}-{datetime.now().month}-{datetime.now().day}.csv"
    )

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

    conditions = [df["z_score"] < -z_threshold, df["z_score"] > z_threshold]
    choices = ["buy", "sell"]

    df["trade_type"] = np.select(conditions, choices, default="hold")

    filtered = df[df["trade_type"] != "hold"].copy()
    filtered["ticker"] = ticker
    filtered["strategy_used"] = strategy_used
    filtered["quantity"] = np.nan

    raw_trades = filtered.rename(
        columns={"timestamp": "date", "trade_type": "side", "open": "price"}
    )

    raw_trades = raw_trades.sort_values("date").reset_index(drop=True)

    raw_trades = size_trades(
        trades_df=raw_trades,
        cash_on_hand=starting_cash,
        base_position_size=base_position_size,
        z_threshold=z_threshold,
        max_multiplier=max_multiplier,
        shares_held=shares_held,
    )

    raw_trades["signal_strength"] = abs(raw_trades["z_score"])

    output_columns = [
        "ticker",
        "date",
        "side",
        "quantity",
        "price",
        "strategy_used",
        "signal_strength",
    ]
    raw_trades[output_columns].to_csv(filepath, index=False)
    return filepath
