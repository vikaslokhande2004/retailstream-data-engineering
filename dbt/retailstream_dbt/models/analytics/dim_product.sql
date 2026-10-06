{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    product_id,
    sku,
    product_name,
    category_id,
    unit_price,
    unit_cost,
    discontinued_at
FROM {{ ref('stg_products') }}