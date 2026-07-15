import os
from pathlib import Path
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient

from alpaca.trading.requests import GetCalendarRequest
import pandas as pd

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

seed_dir = Path(__file__).resolve().parent.parent / "dbt/seeds"


if __name__ == "__main__":
    client = TradingClient(os.getenv("ALPACA_API_KEY"), os.getenv("ALPACA_API_SECRET"))
    request_params = GetCalendarRequest()
    calendar_days = client.get_calendar(request_params)

    # transform pydantic objects into a list of dictionaries to be consumed by pandas
    calendar_dicts = [c.model_dump() for c in calendar_days]

    calendar_df = pd.DataFrame(calendar_dicts)
    calendar_df = calendar_df.rename(columns={"date": "market_date"})
    filename = seed_dir / "raw_market_calendar.csv"
    calendar_df.to_csv(filename, index=False)
