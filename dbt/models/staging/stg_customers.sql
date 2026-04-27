{{
    config(
        materialized = 'view',
        schema = 'staging'
    )
}}

select
    customer_id,
    first_name,
    last_name,
    dob,
    province,
    postal_code,
    email,
    phone,
    created_at,
    _ingest_date,
    effective_start,
    effective_end,
    is_current
from {{ source('staging', 'customers') }}
where is_current = true
  and customer_id is not null
