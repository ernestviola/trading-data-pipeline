from gather_historicals import gather_historicals
from load_to_snowflake import load_csv_to_snowflake
from datetime import datetime
from strategies.mean_reversion import mean_reversion


def step_1(tickers, start, end):
    for ticker in tickers:
        csv_path = gather_historicals(ticker, start, end)
        load_csv_to_snowflake(
            "RAW_PRICES",
            "on target.symbol = source.symbol AND target.timestamp = source.timestamp",
            csv_path,
        )


def step_2():
    csv_path = mean_reversion("AAPL", 20, 1.5)
    load_csv_to_snowflake(
        "RAW_TRADES",
        'on target.TICKER = source.TICKER AND target."DATE" = source."DATE"',
        csv_path,
    )


def main():
    tickers = ["AAPL"]
    start = datetime(2023, 1, 1)
    end = datetime.now()

    step_1(tickers, start, end)
    step_2()


#     - [ ] Write script to compute rolling mean + stddev per ticker
# - [ ] Implement mean-reversion signal (buy below threshold, sell above threshold)
# - [ ] Parameterize `strategy` argument (even if only `mean_reversion` is implemented now)
# - [ ] Generate `raw_trades` output with `strategy_used`, ticker, date, side, quantity, price
# - [ ] Load `raw_trades` into Snowflake

# compute moving avg and stddev

# mean reversion signal buy sell

# generate raw_trades

# load raw_trades


if __name__ == "__main__":
    main()
