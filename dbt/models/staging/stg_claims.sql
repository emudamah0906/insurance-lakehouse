{{
    config(
        materialized = 'view',
        schema = 'staging'
    )
}}

select
    claim_id,
    policy_id,
    claim_date,
    loss_date,
    claim_amount,
    claim_status,
    claim_type,
    adjuster_id,
    fraud_flag,
    _ingest_date,
    -- derived
    datediff('day', loss_date, claim_date)         as days_to_report,
    case when fraud_flag = true then 1 else 0 end  as is_fraud
from {{ source('staging', 'claims') }}
where claim_id is not null
  and policy_id is not null
  and claim_amount > 0
  -- quarantine rule: loss cannot be after claim date
  and (loss_date is null or loss_date <= claim_date)
