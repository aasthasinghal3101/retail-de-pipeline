-- Business-facing aggregate: daily revenue and volume by region and category.
-- This is the table a BI dashboard (or the GenAI query assistant in
-- genai_assistant/) reads from — pre-aggregated so consumers never have to
-- re-derive revenue logic (e.g. what counts as "completed revenue").
with items as (
    select * from {{ ref('fact_order_items') }}
    where order_status not in ('CANCELLED')
),

enriched as (
    select
        items.order_date,
        c.region,
        p.category,
        items.quantity,
        items.line_total
    from items
    inner join {{ ref('dim_customers') }} c on items.customer_id = c.customer_id
    inner join {{ ref('dim_products') }} p on items.product_id = p.product_id
)

select
    order_date,
    region,
    category,
    count(*)                       as line_item_count,
    sum(quantity)                  as units_sold,
    round(sum(line_total), 2)      as total_revenue,
    round(avg(line_total), 2)      as avg_line_value
from enriched
group by order_date, region, category
order by order_date, region, category
