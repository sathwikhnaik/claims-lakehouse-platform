with claims as (
    select * from {{ ref('stg_claims__submitted') }}
)

select
    provider_id,
    count(distinct claim_id)      as total_claims,
    sum(billed_amount)            as total_billed,
    avg(billed_amount)            as avg_billed_amount,
    min(submitted_at)             as first_claim_at,
    max(submitted_at)             as most_recent_claim_at
from claims
group by provider_id