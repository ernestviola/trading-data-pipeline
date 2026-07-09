{{ config(materialized='table') }}

select
    strategy_used,
    ticker,
    trade_date as start_date,
    lead(trade_date) over (partition by strategy_used, ticker order by trade_date) as end_date,
    shares_held,
    cost_basis,
    avg_cost,
    case
        when lead(trade_date) over (partition by strategy_used, ticker order by trade_date) is null
        then true
        else false
    end as is_current
from {{ ref('int_position_cost_basis') }}
order by strategy_used, ticker, start_date