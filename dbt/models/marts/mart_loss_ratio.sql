{{
    config(
        materialized = 'table',
        schema = 'marts'
    )
}}

/*
  Loss ratio = total claims paid / total premiums collected.
  Broken down by product_type and province — primary profitability KPI
  for actuaries and underwriters.
*/

with policies as (
    select
        policy_id,
        customer_id,
        product_type,
        premium,
        status,
        policy_state,
        c.province
    from {{ ref('stg_policies') }} p
    join {{ ref('stg_customers') }} c using (customer_id)
),

claims as (
    select
        policy_id,
        claim_amount,
        claim_status,
        claim_type,
        is_fraud
    from {{ ref('stg_claims') }}
    -- only count settled / closed claims in loss ratio
    where claim_status in ('closed', 'denied')
),

base as (
    select
        p.product_type,
        p.province,
        count(distinct p.policy_id)           as policy_count,
        sum(p.premium)                         as total_premium,
        count(distinct c.policy_id)            as policies_with_claims,
        count(c.policy_id)                     as claim_count,
        coalesce(sum(c.claim_amount), 0)       as total_claims_paid,
        sum(c.is_fraud)                        as fraud_claim_count
    from policies p
    left join claims c using (policy_id)
    group by 1, 2
)

select
    product_type,
    province,
    policy_count,
    total_premium,
    policies_with_claims,
    claim_count,
    total_claims_paid,
    fraud_claim_count,
    round(total_claims_paid / nullif(total_premium, 0), 4)          as loss_ratio,
    round(policies_with_claims / nullif(policy_count, 0) * 100, 2)  as claims_frequency_pct,
    round(total_claims_paid / nullif(claim_count, 0), 2)            as avg_claim_severity,
    current_timestamp()                                              as _dbt_updated_at
from base
