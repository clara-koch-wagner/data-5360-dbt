{{ config(
    materialized = 'table',
    schema = 'oliver_dw'
    )
}}

SELECT
    employee_id as employee_key,
    employee_id,
    first_name,
    last_name,
    email,
    phone_number,
    position,
    hire_date
FROM {{ source('oliver', 'employee') }}