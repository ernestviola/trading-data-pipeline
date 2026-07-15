from gather_historicals import gather_historicals
from load_to_database import load_csv_to_snowflake
from datetime import datetime
from strategies import STRATEGIES
import os


def step_1(tickers, start, end):
    for ticker in tickers:
        csv_path = gather_historicals(ticker, start, end)
        load_csv_to_snowflake(
            "raw_prices",
            'on target.symbol = source.symbol AND target."timestamp" = source."timestamp"',
            csv_path,
        )


def step_2(
    tickers,
    starting_cash,
    base_position_size,
    max_multiplier,
    shares_held,
):

    for strategy in STRATEGIES:
        strategy_fn, config = STRATEGIES[strategy]

        for ticker in tickers:
            csv_path = strategy_fn(
                ticker,
                starting_cash,
                base_position_size,
                max_multiplier,
                shares_held,
                strategy_used=strategy,
                config=config(),
            )

            load_csv_to_snowflake(
                "raw_trades",
                "on target.ticker = source.ticker AND target.strategy_used = source.strategy_used "
                'AND target."date" = source."date"',
                csv_path,
                delete_where_sql="WHERE strategy_used = %s AND ticker = %s",
                delete_params=(strategy, ticker),
                # main.py recomputes this strategy's entire trade history every
                # run - replace its rows in raw_trades outright rather than only
                # inserting missing dates, so a config change (e.g. calibrated
                # thresholds) actually takes effect on rerun instead of leaving
                # stale rows computed under the old config.
                delete_target_where_sql="WHERE strategy_used = %s AND ticker = %s",
                delete_target_params=(strategy, ticker),
            )


def main():
    STARTING_CASH = int(os.getenv("STARTING_CASH", 10000))

    tickers = ["AAPL", "GOOGL"]
    start = datetime(2023, 1, 1)
    end = datetime.now()

    base_position_size = 500
    max_multiplier = 3
    shares_held = 0

    step_1(tickers, start, end)

    # step_2(
    #     tickers,
    #     STARTING_CASH,
    #     base_position_size,
    #     max_multiplier,
    #     shares_held,
    # )


if __name__ == "__main__":
    main()
