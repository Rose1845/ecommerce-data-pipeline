
{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['order_id']},
      {'columns': ['customer_id']},
      {'columns': ['order_date']},
      {'columns': ['category']},
    ]
  )
}}

with base as (
    select * from {{ ref('int_enriched_orders') }}
),

returns as (
    select
        order_id,
        count(*)                        as return_count,
        sum(refund_amount)              as total_refunded,
        bool_or(refund_exceeds_order)   as has_anomalous_refund
    from {{ ref('stg_returns') }}
    group by 1
),

final as (
    select
        b.*,
        coalesce(r.return_count, 0)         as return_count,
        coalesce(r.total_refunded, 0)       as total_refunded,
        coalesce(r.has_anomalous_refund, false) as has_anomalous_refund,
        case when r.order_id is not null then true else false end as has_return
    from base b
    left join returns r on b.order_id = r.order_id
)

select * from final