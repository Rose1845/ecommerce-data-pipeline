-- marts/dim_customers.sql
-- Customer dimension with lifetime metrics.

{{
  config(
    materialized='table',
    indexes=[{'columns': ['customer_id']}]
  )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

order_stats as (
    select
        customer_id,
        count(distinct order_id)                        as total_orders,
        round(sum(net_amount)::numeric, 2)              as lifetime_net_spend,
        min(order_date)                                 as first_order_date,
        max(order_date)                                 as last_order_date,
        count(distinct category)                        as categories_purchased
    from {{ ref('int_enriched_orders') }}
    group by 1
),

return_stats as (
    select
        o.customer_id,
        count(r.return_id)                              as total_returns,
        coalesce(round(sum(r.refund_amount)::numeric, 2), 0) as total_refunded
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_returns') }} r on o.order_id = r.order_id
    group by 1
)

select
    c.customer_id,
    c.customer_name,
    c.email,
    c.country,
    c.customer_tier,
    c.signup_date,
    c.days_since_signup,
    coalesce(s.total_orders,         0) as total_orders,
    coalesce(s.lifetime_net_spend,   0) as lifetime_net_spend,
    s.first_order_date,
    s.last_order_date,
    coalesce(s.categories_purchased, 0) as categories_purchased,
    coalesce(rt.total_returns,       0) as total_returns,
    coalesce(rt.total_refunded,      0) as total_refunded,
    case
        when coalesce(s.total_orders, 0) > 0
        then round(
            (coalesce(rt.total_returns, 0)::numeric / s.total_orders) * 100, 2
        )
        else 0
    end                                 as personal_return_rate_pct
from customers c
left join order_stats  s  on c.customer_id = s.customer_id
left join return_stats rt on c.customer_id = rt.customer_id