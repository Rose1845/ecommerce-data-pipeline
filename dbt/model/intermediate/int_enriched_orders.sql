-- intermediate/int_enriched_orders.sql
-- Ephemeral: joins orders + customers + order_items into one wide table.
-- Used as the base for all mart models — never written to disk.

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

joined as (
    select
        -- Order fields
        o.order_id,
        o.order_date,
        o.order_month,
        o.order_year,
        o.order_day_of_week,
        o.status,
        o.total_amount,
        o.discount_pct,
        o.net_amount,
        o.is_negative_amount,

        -- Customer fields
        c.customer_id,
        c.customer_name,
        c.email,
        c.country,
        c.customer_tier,
        c.signup_date,
        c.days_since_signup,

        -- Item fields
        i.item_id,
        i.product_name,
        i.category,
        i.quantity,
        i.unit_price,
        i.line_total

    from orders o
    -- inner: only orders with a known customer
    inner join customers c on o.customer_id = c.customer_id
    -- left: keep orders even without line items
    left  join order_items i on o.order_id = i.order_id
)

select * from joined