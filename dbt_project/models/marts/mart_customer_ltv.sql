-- Customer lifetime value mart: one row per customer with order history
-- rolled up. Powers churn/segmentation analysis and feeds the loyalty-tier
-- recommendation logic referenced in the GenAI assistant demo.
with order_level as (
    select
        o.customer_id,
        o.order_id,
        o.order_ts,
        o.order_status,
        o.order_total
    from {{ ref('stg_orders') }} o
    where o.order_status not in ('CANCELLED')
),

agg as (
    select
        customer_id,
        count(distinct order_id)          as total_orders,
        round(sum(order_total), 2)        as lifetime_spend,
        round(avg(order_total), 2)        as avg_order_value,
        min(order_ts)                     as first_order_ts,
        max(order_ts)                     as last_order_ts
    from order_level
    group by customer_id
)

select
    c.customer_id,
    c.customer_name,
    c.region,
    c.loyalty_tier,
    coalesce(a.total_orders, 0)                              as total_orders,
    coalesce(a.lifetime_spend, 0)                             as lifetime_spend,
    a.avg_order_value,
    a.first_order_ts,
    a.last_order_ts,
    date_diff('day', a.last_order_ts, current_timestamp)      as days_since_last_order
from {{ ref('dim_customers') }} c
left join agg a on c.customer_id = a.customer_id
