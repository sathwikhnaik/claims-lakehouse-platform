with source as (
    select * from {{ source('serving_raw', 'fct_provider_daily_billing') }}
),

cleaned as (
    select
        provider_id,
        cast(claim_date as date)              as claim_date,
        claims_count,
        total_billed,
        avg_billed_amount,
        rolling_7day_avg_billed,
        is_billing_spike
    from source
    where provider_id is not null
      and claim_date is not null
)

select * from cleaned
