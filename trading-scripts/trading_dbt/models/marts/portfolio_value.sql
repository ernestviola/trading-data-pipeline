{{ config(
  materialized='table',
  unique_key=['ticker','price_date']
) }}

select
    p.symbol as ticker,
    p.price_date,
    p.close as price,
    h.shares_held,
    h.avg_cost,
    h.shares_held * p.close as market_value,
    h.shares_held * h.avg_cost as cost_basis,
    (h.shares_held * p.close) - (h.shares_held * h.avg_cost) as unrealized_gain_loss
from {{ ref('stg_prices') }} p
join {{ ref('holdings_scd2') }} h
    on p.symbol = h.ticker
    and p.price_date >= h.start_date
    and (p.price_date < h.end_date or h.end_date is null)

{% if is_incremental() %}
where p.price_date > (select max(price_date) from {{this}})
{% endif %}

order by p.symbol, p.price_date