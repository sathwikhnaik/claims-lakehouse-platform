with billing as (
    select * from {{ ref('stg_provider_daily_billing') }}
)

select
    provider_id,
    claim_date,
    claims_count,
    total_billed,
    avg_billed_amount,
    rolling_7day_avg_billed,
    is_billing_spike,
    case
        when rolling_7day_avg_billed > 0
        then round(avg_billed_amount / rolling_7day_avg_billed, 2)
        else null
    end as spike_ratio
from billing
