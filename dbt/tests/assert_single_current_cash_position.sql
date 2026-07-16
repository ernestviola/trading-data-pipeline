select
count(*)
from {{ref('cash_position')}} where is_current = True
group by strategy_used, ticker
having count(*) != 1