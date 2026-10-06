{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    customer_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS customer_name,
    email,
    city,
    country,
    signup_date,
    loyalty_tier,
    marketing_opt_in
FROM {{ ref('stg_customers') }}