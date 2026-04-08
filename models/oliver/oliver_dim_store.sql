{{ config(
    materialized = 'table',
    schema = 'oliver_dw'
    )
}}

SELECT
    store_id as store_key,
    store_id,
    store_name,
    street,
    city,
    state
FROM {{ source('oliver', 'store') }}