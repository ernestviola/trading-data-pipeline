from gather_historicals import gather_historicals
from load_to_snowflake import load_to_snowflake
from datetime import datetime


def main():
    tickers = ["AAPL"]
    start = datetime(2023, 1, 1)
    end = datetime.now()

    for ticker in tickers:
        csv_path = gather_historicals(ticker, start, end)
        load_to_snowflake(csv_path)


if __name__ == "__main__":
    main()
