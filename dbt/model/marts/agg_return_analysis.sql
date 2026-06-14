-- marts/agg_return_analysis.sql
-- Return rates and anomalies by category and customer tier.

{{ config(materialized='table') }}

with enriched as (
    select * from {{ ref('int_enriched_orders') }}
),

returns as (
    select * from {{ ref('stg_returns') }}
),

joined as (
    select
        e.category,
        e.customer_tier,
        e.country,
        r.return_id,
        r.reason,
        r.refund_amount,
        r.refund_exceeds_order,
        e.net_amount,
        e.order_id
    from returns r
    left join enriched e on r.order_id = e.order_id
),

by_category as (
    select
        'category'               as dimension_type,
        category                 as dimension_value,
        count(distinct order_id) as orders_with_returns,
        count(return_id)         as total_returns,
        round(sum(refund_amount)::numeric, 2) as total_refunded,
        sum(case when refund_exceeds_order then 1 else 0 end) as anomalous_refunds
    from joined
    group by 1, 2
),

by_tier as (
    select
        'customer_tier'          as dimension_type,
        customer_tier            as dimension_value,
        count(distinct order_id) as orders_with_returns,
        count(return_id)         as total_returns,
        round(sum(refund_amount)::numeric, 2) as total_refunded,
        sum(case when refund_exceeds_order then 1 else 0 end) as anomalous_refunds
    from joined
    group by 1, 2
)

select * from by_category
union all
select * from by_tier
order by dimension_type, total_refunded desc