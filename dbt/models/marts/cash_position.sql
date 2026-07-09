{{ config(materialized='table') }}

select
    strategy_used,
    ticker,
    trade_date as start_date,
    lead(trade_date) over (partition by strategy_used, ticker order by rn) as end_date,
    side,
    quantity,
    price,
    cash_after,
    case
        when lead(trade_date) over (partition by strategy_used, ticker order by rn) is null
        then true
        else false
    end as is_current
from {{ ref('int_portfolio_cash') }}
order by strategy_used, ticker, rn