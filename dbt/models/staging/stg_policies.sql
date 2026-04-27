{{
    config(
        materialized = 'view',
        schema = 'staging'
    )
}}

select
    policy_id,
    customer_id,
    product_type,
    coverage_amount,
    premium,
    start_date,
    end_date,
    status,
    _ingest_date,
    effective_start,
    effective_end,
    is_current,
    -- derived
    datediff('day', start_date, coalesce(end_date, current_date())) as policy_duration_days,
    case
        when end_date < current_date() then 'expired'
        when status = 'active'         then 'active'
        else status
    end as policy_state
from {{ source('staging', 'policies') }}
where policy_id is not null
  and customer_id is not null
