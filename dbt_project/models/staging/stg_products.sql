with source as (
    select * from {{ source('raw', 'products') }}
),

cleaned as (
    select
        product_id,
        product_name,
        category,
        cast(unit_price as decimal(10, 2)) as unit_price,
        supplier,
        is_active
    from source
)

select * from cleaned
