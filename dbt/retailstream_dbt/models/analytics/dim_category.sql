{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    category_id,
    category_name,
    parent_category_id
FROM {{ ref('stg_categories') }}