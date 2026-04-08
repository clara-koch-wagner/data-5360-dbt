{{ config(
    materialized = 'table',
    schema = 'oliver_dw'
    )
}}

SELECT
    product_id as product_key,
    product_id,
    product_name,
    description,
    unit_price
FROM {{ source('oliver', 'product') }}