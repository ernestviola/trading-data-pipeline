select
  symbol,
  timestamp::date as price_date,
  open,
  high,
  low,
  close,
  volume,
  trade_count,
  vwap
from {{source('raw', 'raw_prices')}}