{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

with customers as (
    select distinct
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_phone,
        customer_address,
        customer_city,
        customer_state,
        customer_zip,
        customer_country,
        customer_email
    from {{ source('ecoessentials_landing', 'customer') }}
),

subscribers as (
    select distinct
        customerid as subscriber_customer_id,
        subscriberid,
        subscriberfirstname,
        subscriberlastname,
        subscriberemail
    from {{ source('ecoessentials_marketing_landing', 'marketingemails') }}
),

joined as (
    select
        coalesce(cast(c.customer_id as varchar), s.subscriberid) as person_id,
        s.subscriberid as subscriber_id,
        coalesce(c.customer_first_name, s.subscriberfirstname) as person_first_name,
        coalesce(c.customer_last_name, s.subscriberlastname) as person_last_name,
        c.customer_phone as person_phone,
        c.customer_address as person_address,
        c.customer_city as person_city,
        c.customer_state as person_state,
        c.customer_zip as person_zip,
        c.customer_country as person_country,
        coalesce(c.customer_email, s.subscriberemail) as person_email
    from customers c
    full outer join subscribers s
        on c.customer_email = s.subscriberemail
)

select
    {{ dbt_utils.generate_surrogate_key(['person_id']) }} as person_key,
    person_id,
    subscriber_id,
    person_first_name,
    person_last_name,
    person_phone,
    person_address,
    person_city,
    person_state,
    person_zip,
    person_country,
    person_email
from joined