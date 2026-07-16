select
1
from {{ref('holdings_scd2')}} a
join {{ref('holdings_scd2')}} b
  on a.ticker = b.ticker
  and a.strategy_used = b.strategy_used
  and a.start_date < b.start_date
  and (a.end_date > b.start_date or a.end_date is null)
  