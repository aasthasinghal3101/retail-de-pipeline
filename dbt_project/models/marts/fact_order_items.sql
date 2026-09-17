-- Grain: one row per order line item. This is the atomic fact table that
-- every downstream aggregate mart rolls up from.
select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    o.order_ts,
    cast(o.order_ts as date) as order_date,
    o.order_status,
    o.payment_method,
    oi.quantity,
    oi.unit_price,
    oi.discount_pct,
    oi.line_total
from {{ ref('stg_order_items') }} oi
inner join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id
