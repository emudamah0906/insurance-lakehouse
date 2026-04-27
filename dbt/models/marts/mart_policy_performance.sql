{{
    config(
        materialized = 'table',
        schema = 'marts'
    )
}}

/*
  Policy-level performance — combines premium, claims, and claim frequency
  per policy for portfolio risk assessment.
*/

with policy_claims as (
    select
        policy_id,
        count(*)                          as claim_count,
        sum(claim_amount)                 as total_claim_amount,
        min(claim_date)                   as first_claim_date,
        max(claim_date)                   as last_claim_date,
        sum(is_fraud)                     as fraud_count,
        avg(days_to_report)               as avg_days_to_report
    from {{ ref('stg_claims') }}
    group by 1
)

select
    p.policy_id,
    p.customer_id,
    p.product_type,
    c.province,
    p.coverage_amount,
    p.premium,
    p.start_date,
    p.end_date,
    p.status,
    p.policy_state,
    p.policy_duration_days,
    coalesce(pc.claim_count, 0)                                      as claim_count,
    coalesce(pc.total_claim_amount, 0)                               as total_claim_amount,
    pc.first_claim_date,
    pc.last_claim_date,
    coalesce(pc.fraud_count, 0)                                      as fraud_count,
    round(pc.avg_days_to_report, 1)                                  as avg_days_to_report,
    -- profitability
    round(p.premium - coalesce(pc.total_claim_amount, 0), 2)        as net_profit,
    round(
        coalesce(pc.total_claim_amount, 0) / nullif(p.premium, 0), 4
    )                                                                as policy_loss_ratio,
    -- exposure (claims per year of coverage)
    round(
        coalesce(pc.claim_count, 0) / nullif(p.policy_duration_days / 365.0, 0), 4
    )                                                                as claims_per_year,
    -- risk flag
    case
        when coalesce(pc.fraud_count, 0) > 0                       then 'fraud_flagged'
        when coalesce(pc.total_claim_amount, 0) > p.coverage_amount then 'over_limit'
        when coalesce(pc.total_claim_amount, 0) / nullif(p.premium, 0) > 2 then 'high_loss'
        else 'normal'
    end                                                              as risk_flag,
    current_timestamp()                                              as _dbt_updated_at
from {{ ref('stg_policies') }} p
join {{ ref('stg_customers') }} c using (customer_id)
left join policy_claims pc using (policy_id)
