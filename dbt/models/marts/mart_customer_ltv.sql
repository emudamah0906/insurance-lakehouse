{{
    config(
        materialized = 'table',
        schema = 'marts'
    )
}}

/*
  Customer Lifetime Value (LTV) model.
  LTV = cumulative premiums paid - cumulative claims cost.
  Segments customers by value tier for targeted retention campaigns.
*/

with customer_policies as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.province,
        c.dob,
        c.created_at                              as customer_since,
        datediff('year', c.dob, current_date())   as age_years,
        p.policy_id,
        p.product_type,
        p.premium,
        p.status,
        p.policy_state,
        p.policy_duration_days
    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_policies') }} p using (customer_id)
),

customer_claims as (
    select
        cp.customer_id,
        coalesce(sum(cl.claim_amount), 0)   as total_claims,
        count(cl.claim_id)                  as claim_count,
        sum(cl.is_fraud)                    as fraud_count
    from customer_policies cp
    left join {{ ref('stg_claims') }} cl using (policy_id)
    group by 1
),

aggregated as (
    select
        cp.customer_id,
        max(cp.first_name)                                       as first_name,
        max(cp.last_name)                                        as last_name,
        max(cp.province)                                         as province,
        max(cp.age_years)                                        as age_years,
        max(cp.customer_since)                                   as customer_since,
        datediff('year', max(cp.customer_since), current_date()) as tenure_years,
        count(distinct cp.policy_id)                             as policy_count,
        count(distinct cp.product_type)                          as product_diversity,
        sum(cp.premium)                                          as lifetime_premium,
        max(cc.total_claims)                                     as lifetime_claims,
        max(cc.claim_count)                                      as total_claim_count,
        max(cc.fraud_count)                                      as fraud_flag_count
    from customer_policies cp
    left join customer_claims cc using (customer_id)
    group by 1
)

select
    customer_id,
    first_name,
    last_name,
    province,
    age_years,
    customer_since,
    tenure_years,
    policy_count,
    product_diversity,
    round(lifetime_premium, 2)                                      as lifetime_premium,
    round(lifetime_claims, 2)                                       as lifetime_claims,
    round(lifetime_premium - lifetime_claims, 2)                    as net_ltv,
    total_claim_count,
    fraud_flag_count,
    round(lifetime_claims / nullif(lifetime_premium, 0), 4)         as personal_loss_ratio,
    -- value tier
    case
        when lifetime_premium - lifetime_claims >= 5000  then 'platinum'
        when lifetime_premium - lifetime_claims >= 2000  then 'gold'
        when lifetime_premium - lifetime_claims >= 500   then 'silver'
        when lifetime_premium - lifetime_claims >= 0     then 'bronze'
        else 'unprofitable'
    end                                                              as ltv_tier,
    current_timestamp()                                              as _dbt_updated_at
from aggregated
