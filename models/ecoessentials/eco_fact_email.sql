{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
) }}
select
{{ dbt_utils.generate_surrogate_key(['emaileventid']) }} as email_fact_key,
dd.date_key,
dt.time_key,
dpe.person_key,
dc.campaign_key,
de.event_key,
dem.email_key
from {{ source('ecoessentials_marketing_landing', 'marketingemails') }} m
left join {{ ref('eco_dim_person') }} dpe
    on try_cast(m.customerid as integer) = dpe.person_id
left join {{ ref('eco_dim_campaign') }} dc
    on m.campaignid = dc.campaign_id
left join {{ ref('eco_dim_event') }} de
    on m.eventtype = de.event_name
left join {{ ref('eco_dim_email') }} dem
    on m.emailid = dem.email_id
left join {{ ref('eco_dim_date') }} dd
    on cast(m.eventtimestamp as date) = dd.date_key
left join {{ ref('eco_dim_time') }} dt
    on extract(hour from m.eventtimestamp) = dt.hour
    and extract(minute from m.eventtimestamp) = dt.minute
    and extract(second from m.eventtimestamp) = dt.second