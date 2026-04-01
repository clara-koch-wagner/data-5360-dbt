{{ config(
    materialized = 'table',
    schema = 'oliver_dw'
    )
}}

SELECT
    c.first_name as customer_first_name,
    c.last_name as customer_last_name,
    e.first_name as employee_first_name,
    e.last_name as employee_last_name,
    s.store_name,
    s.city as store_city,
    s.state as store_state,
    p.product_name,
    p.description as product_description,
    d.date_day,
    d.month_name,
    d.year_number,
    f.quantity,
    f.unit_price,
    f.total_amount
FROM {{ ref('fact_sales') }} f
LEFT JOIN {{ ref('oliver_dim_customer') }} c ON f.customer_key = c.customer_key
LEFT JOIN {{ ref('oliver_dim_employee') }} e ON f.employee_key = e.employee_key
LEFT JOIN {{ ref('oliver_dim_store') }} s ON f.store_key = s.store_key
LEFT JOIN {{ ref('oliver_dim_product') }} p ON f.product_key = p.product_key
LEFT JOIN {{ ref('oliver_dim_date') }} d ON f.date_key = d.date_key