from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import os
from sqlalchemy import create_engine

data_dir = Path(__file__).resolve().parent.parent.parent / "data"

engine = create_engine(os.getenv("DATABASE_URL"))


def size_trades(
    trades_df,
    starting_cash,
    base_position_size,
    z_threshold,
    max_multiplier,
    shares_held,
):
    """
    trades_df must be sorted chronologically per ticker before calling this.
    Expects columns: side ('buy'/'sell'), z_score, price.
    Adds: quantity, cash_after, shares_held_after.
    """
    quantities = []
    cash_trace = []
    shares_trace = []

    for row in trades_df.itertuples():
        dollar_size = base_position_size * min(
            abs(row.z_score) / z_threshold, max_multiplier
        )
        desired_qty = dollar_size / row.price

        if row.side == "buy":
            affordable_qty = starting_cash / row.price
            qty = min(desired_qty, affordable_qty)
            starting_cash -= qty * row.price
            shares_held += qty
        elif row.side == "sell":
            qty = min(desired_qty, shares_held)
            starting_cash += qty * row.price
            shares_held -= qty
        else:
            qty = 0  # shouldn't happen, hold rows already filtered out

        quantities.append(qty)
        cash_trace.append(starting_cash)
        shares_trace.append(shares_held)

    trades_df["quantity"] = quantities
    trades_df["cash_after"] = cash_trace
    trades_df["shares_held_after"] = shares_trace
    return trades_df


def mean_reversion(
    ticker,
    window,
    starting_cash,
    base_position_size,
    z_threshold,
    max_multiplier,
    shares_held,
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
    filtered["strategy_used"] = "mean_reversion"
    filtered["quantity"] = np.nan

    raw_trades = filtered.rename(
        columns={"timestamp": "date", "trade_type": "side", "open": "price"}
    )

    raw_trades = raw_trades.sort_values("date").reset_index(drop=True)

    raw_trades = size_trades(
        raw_trades,
        starting_cash,
        base_position_size,
        z_threshold,
        max_multiplier,
        shares_held,
    )

    output_columns = ["ticker", "date", "side", "quantity", "price", "strategy_used"]
    raw_trades[output_columns].to_csv(filepath, index=False)
    return filepath
