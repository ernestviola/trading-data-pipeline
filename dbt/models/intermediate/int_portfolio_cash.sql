{{ config(materialized='table') }}

with recursive ranked_trades as (
    select
        *,
        row_number() over (order by trade_date asc, signal_strength desc) as rn
    from {{ ref('stg_trades') }}
),

running_cash as (

    -- base case: first trade across the whole portfolio
    select
        ticker,
        trade_date,
        side,
        quantity,
        price,
        rn,
        case
            when side = 'buy' then {{ var('starting_cash', 10000) }} - (quantity * price)
            else {{ var('starting_cash', 10000) }} + (quantity * price)
        end as cash_after
    from ranked_trades
    where rn = 1

    union all

    -- recursive case: build off the previous row's running cash
    select
        rt.ticker,
        rt.trade_date,
        rt.side,
        rt.quantity,
        rt.price,
        rt.rn,
        case
            when rt.side = 'buy' then rc.cash_after - (rt.quantity * rt.price)
            else rc.cash_after + (rt.quantity * rt.price)
        end as cash_after
    from ranked_trades rt
    join running_cash rc
        on rt.rn = rc.rn + 1
)

select
    ticker,
    trade_date,
    side,
    quantity,
    price,
    cash_after,
    rn
from running_cash
order by rn