{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['hour', 'minute', 'second']) }} as time_key,
    hour,
    minute,
    second
from (
    select distinct
        extract(hour from eventtimestamp) as hour,
        extract(minute from eventtimestamp) as minute,
        extract(second from eventtimestamp) as second
    from {{ source('ecoessentials_marketing_landing', 'marketingemails') }}
    where eventtimestamp is not null
)