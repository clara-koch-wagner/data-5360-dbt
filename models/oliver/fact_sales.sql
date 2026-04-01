{{ config(
    materialized = 'table',
    schema = 'oliver_dw'
    )
}}

SELECT
    ol.order_line_id,
    c.customer_key,
    e.employee_key,
    s.store_key,
    p.product_key,
    d.date_key,
    o.total_amount,
    ol.quantity,
    ol.unit_price
FROM {{ source('oliver', 'orders') }} o
INNER JOIN {{ source('oliver', 'orderline') }} ol ON o.order_id = ol.order_id
INNER JOIN {{ ref('oliver_dim_customer') }} c ON o.customer_id = c.customer_id
INNER JOIN {{ ref('oliver_dim_employee') }} e ON o.employee_id = e.employee_id
INNER JOIN {{ ref('oliver_dim_store') }} s ON o.store_id = s.store_id
INNER JOIN {{ ref('oliver_dim_product') }} p ON ol.product_id = p.product_id
INNER JOIN {{ ref('oliver_dim_date') }} d ON d.date_day = o.order_date