with claims as (
    select * from {{ ref('stg_claims__submitted') }}
),

daily as (
    select
        provider_id,
        date_trunc('day', submitted_at) as claim_date,
        count(claim_id)                 as claims_count,
        sum(billed_amount)              as total_billed,
        avg(billed_amount)              as avg_billed_amount
    from claims
    group by provider_id, date_trunc('day', submitted_at)
),

with_baseline as (
    select
        *,
        avg(avg_billed_amount) over (
            partition by provider_id
            order by claim_date
            rows between 7 preceding and 1 preceding
        ) as rolling_7day_avg_billed
    from daily
)

select
    *,
    case
        when rolling_7day_avg_billed > 0
             and avg_billed_amount > rolling_7day_avg_billed * 3
        then true
        else false
    end as is_billing_spike
from with_baseline