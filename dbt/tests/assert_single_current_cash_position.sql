select
count(*)
from {{ref('cash_position')}} where is_current = True
having count(*) != 1