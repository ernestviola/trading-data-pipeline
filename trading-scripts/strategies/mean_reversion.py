from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from utils.snowflake_connection import snowflake_connection
from strategies.sizing import size_trades

data_dir = Path(__file__).resolve().parent.parent.parent / "data"

conn = snowflake_connection(role="transformer_role", schema="bronze")


def compute_mean_reversion_signals(df, ticker, strategy_used, config):
    """
    Pure signal generation, no I/O and no sizing. df must already be a
    lowercase-columned price dataframe (as fetched/lowercased in
    mean_reversion() below), sorted by timestamp. Returns the buy/sell
    (never 'hold') rows with signal_strength computed, but no quantity/
    cash_after/shares_held_after yet - callers decide how to size (e.g.
    mean_reversion() sizes the whole history in one continuous run;
    calibrate_thresholds.py sizes train/test splits as independent
    portfolios). Split out specifically so a threshold sweep can reuse the
    exact same signal logic instead of re-deriving it and risking drift
    from the production path.
    """
    df = df.copy()
    df["rolling_mean"] = df["close"].rolling(config.window).mean()
    df["rolling_std"] = df["close"].rolling(config.window).std()

    df["z_score"] = (df["open"] - df["rolling_mean"].shift(1)) / df[
        "rolling_std"
    ].shift(1)

    conditions = [
        df["z_score"] < -config.buy_strength_threshold,
        df["z_score"] > config.sell_strength_threshold,
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
    # for mean-reversion that's abs(z_score).
    raw_trades["signal_strength"] = abs(raw_trades["z_score"])

    return raw_trades


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

    raw_trades = compute_mean_reversion_signals(df, ticker, strategy_used, config)

    raw_trades = size_trades(
        trades_df=raw_trades,
        cash_on_hand=starting_cash,
        base_position_size=base_position_size,
        buy_strength_threshold=config.buy_strength_threshold,
        sell_strength_threshold=config.sell_strength_threshold,
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
