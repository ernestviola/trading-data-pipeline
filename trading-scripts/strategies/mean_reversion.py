from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from utils.snowflake_connection import snowflake_connection
from strategies.sizing import size_trades

data_dir = Path(__file__).resolve().parent.parent.parent / "data"

conn = snowflake_connection(role="transformer_role", schema="bronze")


def mean_reversion(
    ticker,
    starting_cash,
    base_position_size,
    max_multiplier,
    shares_held,
    strategy_used,
    config,
):
    filepath = (
        data_dir
        / f"raw_trades_{strategy_used}_{ticker}_{datetime.now().year}-{datetime.now().month}-{datetime.now().day}.csv"
    )

    df = pd.read_sql(
        'SELECT * FROM raw_prices WHERE symbol = %s ORDER BY "timestamp";',
        conn,
        params=(ticker,),
    )

    # Snowflake folds unquoted DDL identifiers to uppercase, so columns come
    # back as SYMBOL/TIMESTAMP/OPEN/CLOSE etc. Lowercase them to match the
    # rest of this function, which was written against Postgres's lowercase
    # fold behavior.
    df.columns = df.columns.str.lower()

    df["rolling_mean"] = df["close"].rolling(config.window).mean()
    df["rolling_std"] = df["close"].rolling(config.window).std()

    df["z_score"] = (df["open"] - df["rolling_mean"].shift(1)) / df[
        "rolling_std"
    ].shift(1)

    conditions = [
        df["z_score"] < -config.strength_threshold,
        df["z_score"] > config.strength_threshold,
    ]
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

    # signal_strength is the strategy-agnostic magnitude sizing.py consumes;
    # for mean-reversion that's abs(z_score). Computed before size_trades()
    # since sizing.py no longer knows what z_score is.
    raw_trades["signal_strength"] = abs(raw_trades["z_score"])

    raw_trades = size_trades(
        trades_df=raw_trades,
        cash_on_hand=starting_cash,
        base_position_size=base_position_size,
        strength_threshold=config.strength_threshold,
        max_multiplier=max_multiplier,
        shares_held=shares_held,
    )

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
