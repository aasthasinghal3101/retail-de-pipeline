select
    customer_id,
    customer_name,
    email,
    is_missing_email,
    region,
    city,
    signup_ts,
    loyalty_tier,
    _source_system
from {{ ref('stg_customers') }}
