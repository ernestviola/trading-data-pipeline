from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from utils.snowflake_connection import snowflake_connection
from strategies.sizing import size_trades

data_dir = Path(__file__).resolve().parent.parent.parent / "data"

conn = snowflake_connection(role="transformer_role", schema="bronze")


def compute_momentum_signals(df, ticker, strategy_used, config):
    """
    Pure signal generation, no I/O and no sizing - mirrors
    compute_mean_reversion_signals(). df must already be a lowercase-
    columned price dataframe, sorted by timestamp. Returns the buy/sell
    rows with signal_strength computed; no quantity/cash_after/
    shares_held_after yet. Note MACD's buy/sell classification (the
    crossover event) doesn't depend on buy/sell_strength_threshold at all -
    those only affect sizing, computed downstream by size_trades().
    """
    df = df.copy()

    # Standard MACD: EMAs of close price.
    ema_fast = df["close"].ewm(span=config.fast_period, adjust=False).mean()
    ema_slow = df["close"].ewm(span=config.slow_period, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=config.signal_period, adjust=False).mean()
    histogram = macd_line - signal_line

    # PPO-style normalization (fork 2 resolution): a percentage rather than
    # raw price units, so a fixed strength_threshold is comparable across
    # tickers at different price levels.
    ppo_histogram = (macd_line / ema_slow) * 100

    # Shift by 1 so today's trade decision is based on yesterday's *closed*
    # histogram (known before today's open) - mirrors mean_reversion.py's
    # shift(1) convention to avoid lookahead. Trades execute at today's open.
    hist_shifted = histogram.shift(1)
    hist_prev_shifted = histogram.shift(2)
    ppo_shifted = ppo_histogram.shift(1)

    # Crossover event (fork 1 resolution): buy when the histogram flips
    # negative->positive, sell when it flips positive->negative. This is a
    # sign-flip check against the prior row, not a magnitude threshold -
    # structurally different from mean_reversion.py's per-row threshold
    # check. The sign flip is unaffected by the PPO normalization above,
    # since dividing by a positive ema_slow never changes histogram's sign.
    conditions = [
        (hist_prev_shifted < 0) & (hist_shifted > 0),
        (hist_prev_shifted > 0) & (hist_shifted < 0),
    ]
    choices = ["buy", "sell"]

    df["signal_strength_raw"] = ppo_shifted
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
    # for MACD that's abs(normalized histogram).
    raw_trades["signal_strength"] = abs(raw_trades["signal_strength_raw"])

    return raw_trades


def momentum(
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
    # rest of this function, mirroring mean_reversion.py.
    df.columns = df.columns.str.lower()

    raw_trades = compute_momentum_signals(df, ticker, strategy_used, config)

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
