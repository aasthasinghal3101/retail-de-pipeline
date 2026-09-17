-- Cleans and standardizes raw customer records:
--   * trims whitespace introduced by upstream CRM exports
--   * de-duplicates on customer_id (raw zone can contain re-sent records)
--   * flags rows with missing email so they're visible, not silently dropped
with source as (
    select * from {{ source('raw', 'customers') }}
),

deduped as (
    select *,
        row_number() over (
            partition by customer_id order by _loaded_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        customer_id,
        trim(full_name)            as customer_name,
        lower(trim(email))         as email,
        (email is null)            as is_missing_email,
        phone,
        region,
        city,
        cast(signup_ts as timestamp) as signup_ts,
        loyalty_tier,
        _source_system
    from deduped
    where _row_num = 1
)

select * from cleaned
