{{ config(materialized='table') }}

with expected_date as (
    select max(market_date) as market_date
    from {{ ref('stg_market_calendar') }}
    where market_date < current_date()
),

latest_by_pair as (
    select
        strategy_used,
        ticker,
        max(price_date) as actual_date
    from {{ ref('portfolio_value') }}
    group by strategy_used, ticker
)

select
    l.strategy_used,
    l.ticker,
    l.actual_date,
    e.market_date as expected_date,
    coalesce(l.actual_date = e.market_date, false) as is_fresh
from latest_by_pair l
cross join expected_date e