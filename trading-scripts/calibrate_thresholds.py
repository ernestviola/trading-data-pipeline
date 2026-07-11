"""
Grid-sweep buy/sell strength thresholds for both strategies, validated with
an in-sample/out-of-sample date split.

Why the split matters: a threshold chosen to maximize return over the
*entire* backtest window tends to just curve-fit noise particular to that
window (classic backtest overfitting). Instead: pick the threshold that
performs best on an earlier "train" slice, then check whether that same
threshold still holds up on a later "test" slice it never saw.

Why each split is its own independent portfolio: naively slicing one
continuous cash/shares_held trace at the split date would let the test
period inherit whatever cash/position the train period ended with, which
defeats the point of an out-of-sample check. Each split starts fresh
(shares_held=0, the same starting_cash), mirroring how
strategy_performance_summary already treats each (strategy, ticker) as its
own independent portfolio.

Signal generation (rolling z-score / MACD EMAs) still runs over the *full*
continuous price history so rolling windows and EMAs are properly warmed
up - only the resulting trade rows get split by date before sizing.

This never writes to raw_trades or touches dbt - it's read-only against
raw_prices and pure in-memory pandas/sizing.py from there. Safe to run as
often as you want.
"""

from datetime import datetime
import itertools

import pandas as pd

from strategies.mean_reversion import (
    compute_mean_reversion_signals,
    conn as prices_conn,
)
from strategies.momentum import compute_momentum_signals
from strategies.sizing import size_trades
from strategies.configs import MeanReversionConfig, MACDConfig


def fetch_prices(ticker):
    df = pd.read_sql(
        'SELECT * FROM raw_prices WHERE symbol = %s ORDER BY "timestamp";',
        prices_conn,
        params=(ticker,),
    )
    df.columns = df.columns.str.lower()
    return df


def portfolio_pct_return(sized_trades, starting_cash, last_price):
    """
    ending_value = cash on hand + remaining shares valued at last_price,
    mirroring strategy_performance_summary's ending_value definition.
    """
    if sized_trades.empty:
        return 0.0
    final_cash = sized_trades["cash_after"].iloc[-1]
    final_shares = sized_trades["shares_held_after"].iloc[-1]
    ending_value = final_cash + final_shares * last_price
    return (ending_value - starting_cash) / starting_cash


def evaluate_combo(
    signals,
    df,
    split_date,
    starting_cash,
    base_position_size,
    max_multiplier,
    buy_threshold,
    sell_threshold,
):
    train_signals = signals[signals["date"] <= split_date].copy()
    test_signals = signals[signals["date"] > split_date].copy()

    train_prices = df[df["timestamp"] <= split_date]
    test_prices = df

    if train_prices.empty or test_prices.empty:
        return None

    train_last_price = train_prices["close"].iloc[-1]
    test_last_price = test_prices["close"].iloc[-1]

    train_sized = size_trades(
        trades_df=train_signals,
        cash_on_hand=starting_cash,
        base_position_size=base_position_size,
        buy_strength_threshold=buy_threshold,
        sell_strength_threshold=sell_threshold,
        max_multiplier=max_multiplier,
        shares_held=0,
    )
    test_sized = size_trades(
        trades_df=test_signals,
        cash_on_hand=starting_cash,
        base_position_size=base_position_size,
        buy_strength_threshold=buy_threshold,
        sell_strength_threshold=sell_threshold,
        max_multiplier=max_multiplier,
        shares_held=0,
    )

    return {
        "buy_strength_threshold": buy_threshold,
        "sell_strength_threshold": sell_threshold,
        "train_pct_return": portfolio_pct_return(
            train_sized, starting_cash, train_last_price
        ),
        "test_pct_return": portfolio_pct_return(
            test_sized, starting_cash, test_last_price
        ),
        "train_trade_count": len(train_signals),
        "test_trade_count": len(test_signals),
    }


def sweep_mean_reversion(
    df,
    ticker,
    split_date,
    starting_cash,
    base_position_size,
    max_multiplier,
    window,
    buy_candidates,
    sell_candidates,
):
    results = []
    for buy_t, sell_t in itertools.product(buy_candidates, sell_candidates):
        config = MeanReversionConfig(
            window=window,
            buy_strength_threshold=buy_t,
            sell_strength_threshold=sell_t,
        )
        signals = compute_mean_reversion_signals(df, ticker, "mean_reversion", config)
        row = evaluate_combo(
            signals,
            df,
            split_date,
            starting_cash,
            base_position_size,
            max_multiplier,
            buy_t,
            sell_t,
        )
        if row:
            results.append(row)
    return pd.DataFrame(results)


def sweep_momentum(
    df,
    ticker,
    split_date,
    starting_cash,
    base_position_size,
    max_multiplier,
    fast_period,
    slow_period,
    signal_period,
    buy_candidates,
    sell_candidates,
):
    results = []
    for buy_t, sell_t in itertools.product(buy_candidates, sell_candidates):
        config = MACDConfig(
            fast_period=fast_period,
            slow_period=slow_period,
            signal_period=signal_period,
            buy_strength_threshold=buy_t,
            sell_strength_threshold=sell_t,
        )
        # MACD's buy/sell classification (the crossover event) doesn't
        # depend on the thresholds at all - only sizing does - so this
        # recomputes the same signals each time. Harmless at this grid
        # size, just not maximally efficient.
        signals = compute_momentum_signals(df, ticker, "macd_momentum", config)
        row = evaluate_combo(
            signals,
            df,
            split_date,
            starting_cash,
            base_position_size,
            max_multiplier,
            buy_t,
            sell_t,
        )
        if row:
            results.append(row)
    return pd.DataFrame(results)


def report(name, results_df):
    if results_df.empty:
        print(f"\n{name}: no results (check date range / split_date)")
        return

    sorted_df = results_df.sort_values("train_pct_return", ascending=False).reset_index(
        drop=True
    )
    print(f"\n=== {name}: top 5 by train_pct_return ===")
    # pct_return columns are raw fractions ((ending_value - starting_cash) /
    # starting_cash), same as strategy_performance_summary - format as a
    # percentage here so this table can't be misread as already-percent or
    # some other unit.
    print(
        sorted_df.head(5).to_string(
            index=False,
            formatters={
                "train_pct_return": "{:.2%}".format,
                "test_pct_return": "{:.2%}".format,
            },
        )
    )

    winner = sorted_df.iloc[0]
    print(
        f"\nBest on train: buy={winner['buy_strength_threshold']}, "
        f"sell={winner['sell_strength_threshold']} "
        f"-> train {winner['train_pct_return']:.2%}, "
        f"test {winner['test_pct_return']:.2%}"
    )
    if winner["test_pct_return"] < 0 < winner["train_pct_return"]:
        print(
            "WARNING: this combo looks good in-sample but lost money "
            "out-of-sample - likely overfit to the train window, not a "
            "threshold worth committing to as-is."
        )


if __name__ == "__main__":
    TICKER = "AAPL"
    STARTING_CASH = 10000
    BASE_POSITION_SIZE = 500
    MAX_MULTIPLIER = 3
    SPLIT_DATE = pd.Timestamp("2025-01-01")

    df = fetch_prices(TICKER)

    mr_results = sweep_mean_reversion(
        df=df,
        ticker=TICKER,
        split_date=SPLIT_DATE,
        starting_cash=STARTING_CASH,
        base_position_size=BASE_POSITION_SIZE,
        max_multiplier=MAX_MULTIPLIER,
        window=20,
        buy_candidates=[1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5],
        sell_candidates=[1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5],
    )
    report("mean_reversion", mr_results)

    macd_results = sweep_momentum(
        df=df,
        ticker=TICKER,
        split_date=SPLIT_DATE,
        starting_cash=STARTING_CASH,
        base_position_size=BASE_POSITION_SIZE,
        max_multiplier=MAX_MULTIPLIER,
        fast_period=12,
        slow_period=26,
        signal_period=9,
        buy_candidates=[0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0],
        sell_candidates=[0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0],
    )
    report("macd_momentum", macd_results)
