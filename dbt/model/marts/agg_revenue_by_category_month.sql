-- marts/agg_revenue_by_category_month.sql
-- Monthly revenue by category — primary Power BI data source.

{{ config(materialized='table') }}

with base as (
    select * from {{ ref('int_enriched_orders') }}
    where net_amount is not null
),

monthly as (
    select
        order_month,
        extract(year  from order_month)::int  as year,
        extract(month from order_month)::int  as month,
        category,
        count(distinct order_id)                          as order_count,
        count(distinct customer_id)                       as unique_customers,
        round(sum(net_amount)::numeric, 2)                as total_net_revenue,
        round(avg(net_amount)::numeric, 2)                as avg_order_value,
        round(sum(total_amount)::numeric, 2)              as total_gross_revenue,
        round(sum(total_amount - net_amount)::numeric, 2) as total_discount_given
    from base
    group by 1, 2, 3, 4
),

with_share as (
    select
        *,
        round(
            total_net_revenue
            / nullif(sum(total_net_revenue) over (partition by order_month), 0)
            * 100,
            2
        ) as revenue_share_pct
    from monthly
)

select * from with_share
order by order_month desc, total_net_revenue desc