select
  ticker,
  date::date as trade_date,
  side,
  quantity,
  price,
  strategy_used
from {{ source('raw','raw_trades') }}