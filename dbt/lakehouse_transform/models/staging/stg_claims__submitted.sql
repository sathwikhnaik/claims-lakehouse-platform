with source as (
    select * from {{ source('bronze', 'claims_submitted') }}
),

deduped as (
    select *,
        row_number() over (
            partition by claim_id
            order by submitted_at desc
        ) as rn
    from source
),

cleaned as (
    select
        claim_id,
        trim(provider_id)       as provider_id,
        trim(patient_id)        as patient_id,
        trim(procedure_code)    as procedure_code,
        procedure_desc,
        cast(billed_amount as decimal(10,2)) as billed_amount,
        cast(submitted_at as timestamp)      as submitted_at
    from deduped
    where rn = 1
      and billed_amount > 0
      and claim_id is not null
      and provider_id is not null
)

select * from cleaned