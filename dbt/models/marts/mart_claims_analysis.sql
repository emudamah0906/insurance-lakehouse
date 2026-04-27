{{
    config(
        materialized = 'table',
        schema = 'marts'
    )
}}

/*
  Claims analysis — frequency, severity, fraud rate, and reporting lag
  by claim_type and month.  Feeds the Metabase claims dashboard.
*/

with claims as (
    select
        c.claim_id,
        c.policy_id,
        c.claim_date,
        c.claim_amount,
        c.claim_status,
        c.claim_type,
        c.adjuster_id,
        c.is_fraud,
        c.days_to_report,
        p.product_type,
        cu.province,
        p.premium,
        date_trunc('month', c.claim_date) as claim_month
    from {{ ref('stg_claims') }} c
    join {{ ref('stg_policies') }} p using (policy_id)
    join {{ ref('stg_customers') }} cu using (customer_id)
),

monthly as (
    select
        claim_month,
        claim_type,
        product_type,
        province,
        count(*)                                    as claim_count,
        sum(claim_amount)                           as total_claim_amount,
        avg(claim_amount)                           as avg_claim_amount,
        max(claim_amount)                           as max_claim_amount,
        avg(days_to_report)                         as avg_days_to_report,
        sum(is_fraud)                               as fraud_count,
        round(sum(is_fraud) / count(*) * 100, 2)   as fraud_rate_pct,
        count(distinct policy_id)                   as distinct_policies_affected,
        count(distinct adjuster_id)                 as distinct_adjusters
    from claims
    group by 1, 2, 3, 4
)

select
    *,
    round(avg_claim_amount / nullif(
        sum(avg_claim_amount) over (partition by claim_month), 0
    ) * 100, 2)  as pct_of_monthly_total,
    current_timestamp() as _dbt_updated_at
from monthly
order by claim_month desc, total_claim_amount desc
