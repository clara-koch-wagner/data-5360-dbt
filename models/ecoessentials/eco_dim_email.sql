{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['emailid']) }} as email_key,
    emailid as email_id,
    emailname as email_name
from (
    select distinct emailid, emailname
    from {{ source('ecoessentials_marketing_landing', 'marketingemails') }}
)