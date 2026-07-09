{{ config(materialized='table') }}

with recursive ranked_trades as (
    select
        *,
        row_number() over (partition by strategy_used, ticker order by trade_date) as rn
    from {{ ref('stg_trades') }}
),

running_position as (

    -- base case: first trade per (strategy_used, ticker)
    -- assumes shares_held starts at 0, enforced upstream in size_trades
    select
        strategy_used,
        ticker,
        trade_date,
        side,
        quantity,
        price,
        rn,
        case when side = 'buy' then quantity else 0 end as shares_held,
        case when side = 'buy' then quantity * price else 0 end as cost_basis
    from ranked_trades
    where rn = 1

    union all

    -- recursive case: build off the previous row's state,
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
            when rt.side = 'buy' then rp.shares_held + rt.quantity
            else rp.shares_held - rt.quantity
        end as shares_held,
        case
          when rt.side = 'buy' then rp.cost_basis + (rt.quantity * rt.price)
          else coalesce(rp.cost_basis * (rp.shares_held - rt.quantity) / nullif(rp.shares_held, 0), 0)
        end as cost_basis
    from ranked_trades rt
    join running_position rp
        on rt.strategy_used = rp.strategy_used
        and rt.ticker = rp.ticker
        and rt.rn = rp.rn + 1
)

select
    strategy_used,
    ticker,
    trade_date,
    side,
    quantity,
    price,
    shares_held,
    cost_basis,
    case when shares_held = 0 then 0 else cost_basis / shares_held end as avg_cost
from running_position
order by strategy_used, ticker, trade_date