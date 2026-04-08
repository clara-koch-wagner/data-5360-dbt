--test
{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

with orders as (
    select *
    from {{ source('ecoessentials_landing', 'order') }}),

order_lines as (
    select *
    from {{ source('ecoessentials_landing', 'order_line') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['ol.order_line_id']) }} as sales_key,
    dp.product_key,
    dpe.person_key as customer_key,
    dc.campaign_key,
    dd.date_key,
    o.order_id,
    ol.order_line_id,
    ol.quantity,
    ol.discount,
    ol.price_after_discount
from order_lines ol
left join orders o
    on ol.order_id = o.order_id
left join {{ ref('eco_dim_product') }} dp
    on ol.product_id = dp.product_id
left join {{ ref('eco_dim_person') }} dpe
    on o.customer_id = dpe.person_id
left join {{ ref('eco_dim_campaign') }} dc
    on ol.campaign_id = dc.campaign_id
left join {{ ref('eco_dim_date') }} dd
    on cast(o.order_timestamp as date) = dd.date_key