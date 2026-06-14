-- staging/stg_orders.sql
-- Lightweight view on raw.orders with convenience columns added.

with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        discount_pct,
        is_negative_amount,
        net_amount,
        ingested_at,
        pipeline_run_id,
        date_trunc('month', order_date)::date  as order_month,
        date_trunc('year',  order_date)::date  as order_year,
        extract(dow from order_date)::int       as order_day_of_week
    from source
    where order_id     is not null
      and customer_id  is not null
)

select * from renamed