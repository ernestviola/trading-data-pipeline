select
  market_date
from 
  {{source('raw',('raw_market_calendar'))}}