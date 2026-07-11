{{ config(materialized='view') }}

-- Percent return, not raw dollar gain, so comparison is fair across
-- strategies regardless of how much of starting_cash ended up deployed.
-- Cheap MAX(date)-per-group read on top of already-computed portfolio_value
-- and cash_position - no incremental/materialization cost to justify
-- storing this as a table.

with latest_value_date as (
    select
        strategy_used,
        ticker,
        max(price_date) as price_date
    from {{ ref('portfolio_value') }}
    group by strategy_used, ticker
),

latest_portfolio_value as (
    select
        pv.strategy_used,
        pv.ticker,
        pv.price_date as as_of_date,
        pv.market_value
    from {{ ref('portfolio_value') }} pv
    join latest_value_date lvd
        on pv.strategy_used = lvd.strategy_used
        and pv.ticker = lvd.ticker
        and pv.price_date = lvd.price_date
),

current_cash as (
    -- cash_after doesn't change between trades, so "current" cash on hand
    -- is just the most recent row per (strategy_used, ticker), regardless
    -- of whether its date matches portfolio_value's latest priced date.
    select
        strategy_used,
        ticker,
        cash_after
    from {{ ref('cash_position') }}
    where is_current = true
)

select
    lpv.strategy_used,
    lpv.ticker,
    lpv.as_of_date,
    cc.cash_after as cash_on_hand,
    lpv.market_value,
    cc.cash_after + lpv.market_value as ending_value,
    {{ var('starting_cash', 10000) }} as starting_cash,
    (cc.cash_after + lpv.market_value - {{ var('starting_cash', 10000) }})
        / {{ var('starting_cash', 10000) }} as pct_return
from latest_portfolio_value lpv
join current_cash cc
    on lpv.strategy_used = cc.strategy_used
    and lpv.ticker = cc.ticker
order by pct_return desc