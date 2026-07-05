{{ config(materialized='table') }}

select
    ticker,
    trade_date as start_date,
    lead(trade_date) over (partition by ticker order by trade_date) as end_date,
    shares_held,
    cost_basis,
    avg_cost,
    case
        when lead(trade_date) over (partition by ticker order by trade_date) is null
        then true
        else false
    end as is_current
from {{ ref('int_position_cost_basis') }}
order by ticker, start_date