from gather_historicals import gather_historicals
from load_to_database import load_csv_to_postgres
from datetime import datetime
from strategies import STRATEGIES
import os


def step_1(tickers, start, end):
    for ticker in tickers:
        csv_path = gather_historicals(ticker, start, end)
        load_csv_to_postgres(
            "raw_prices",
            "on target.symbol = source.symbol AND target.timestamp = source.timestamp",
            csv_path,
        )


def step_2(
    tickers,
    strategy,
    window,
    starting_cash,
    base_position_size,
    z_threshold,
    max_multiplier,
    shares_held,
):
    strategy_fn = STRATEGIES[strategy]
    for ticker in tickers:
        csv_path = strategy_fn(
            ticker,
            window,
            starting_cash,
            base_position_size,
            z_threshold,
            max_multiplier,
            shares_held,
            strategy_used=strategy,
        )
        load_csv_to_postgres(
            "raw_trades",
            "on target.ticker = source.ticker AND target.date = source.date",
            csv_path,
        )


def main():
    STARTING_CASH = int(os.getenv("STARTING_CASH", 10000))

    tickers = ["AAPL"]
    start = datetime(2023, 1, 1)
    end = datetime.now()
    window = 20
    base_position_size = 500
    z_threshold = 1.5
    max_multiplier = 3
    shares_held = 0

    step_1(tickers, start, end)
    step_2(
        tickers,
        "mean_reversion",
        window,
        STARTING_CASH,
        base_position_size,
        z_threshold,
        max_multiplier,
        shares_held,
    )


if __name__ == "__main__":
    main()
