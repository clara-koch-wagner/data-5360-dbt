{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_type,
    product_name
from {{ source('ecoessentials_landing', 'product') }}