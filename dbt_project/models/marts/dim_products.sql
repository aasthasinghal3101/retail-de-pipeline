select
    product_id,
    product_name,
    category,
    unit_price,
    supplier,
    is_active
from {{ ref('stg_products') }}
