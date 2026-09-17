with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        cast(order_ts as timestamp) as order_ts,
        upper(status)               as order_status,
        payment_method,
        cast(order_total as decimal(12, 2)) as order_total
    from source
)

select * from cleaned
