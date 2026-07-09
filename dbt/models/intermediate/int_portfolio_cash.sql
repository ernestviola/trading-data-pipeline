{{ config(materialized='table') }}

with recursive ranked_trades as (
    select
        *,
        row_number() over (
            partition by strategy_used, ticker
            order by trade_date asc, signal_strength desc
        ) as rn
    from {{ ref('stg_trades') }}
),

running_cash as (

    -- base case: first trade per (strategy_used, ticker) — each strategy/ticker
    -- combination gets its own independent starting cash
    select
        strategy_used,
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

    -- recursive case: build off the previous row's running cash,
    -- staying within the same strategy_used + ticker partition
    select
        rt.strategy_used,
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
        on rt.strategy_used = rc.strategy_used
        and rt.ticker = rc.ticker
        and rt.rn = rc.rn + 1
)

select
    strategy_used,
    ticker,
    trade_date,
    side,
    quantity,
    price,
    cash_after,
    rn
from running_cash
order by strategy_used, ticker, rn