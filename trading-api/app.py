import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

data_dir = Path(__file__).resolve().parent.parent/ "data"
data_dir.mkdir(parents=True, exist_ok=True)

client = StockHistoricalDataClient(
    os.getenv("ALPACA_API_KEY"),
    os.getenv("ALPACA_API_SECRET")
)

def gatherHistoricals(ticker,start,end):
  request_params = StockBarsRequest(
      symbol_or_symbols=[ticker],
      timeframe=TimeFrame.Day,
      start=start,
      end=end
  )

  bars = client.get_stock_bars(request_params)
  df = bars.df
  df.to_csv(data_dir / f"{ticker}_{start.date()}_{end.date()}.csv")

def main():
  tickers = ["AAPL"]
  start = datetime(2023,1,1)
  end = datetime.now()

  for ticker in tickers:
    gatherHistoricals(ticker, start,end)

if (__name__ == "__main__"):
  main()