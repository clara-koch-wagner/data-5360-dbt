{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['eventtype']) }} as event_key,
    eventtype as event_name
from (
    select distinct eventtype
    from {{ source('ecoessentials_marketing_landing', 'marketingemails') }}
)