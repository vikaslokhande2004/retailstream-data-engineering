{{ config(
    materialized='table',
    schema='analytics'
) }}

WITH date_range AS (
    SELECT
        MIN(ordered_at::date) AS min_date,
        MAX(ordered_at::date) AS max_date
    FROM {{ ref('stg_orders') }}
),

dates AS (
    SELECT
        generate_series(
            min_date,
            max_date,
            interval '1 day'
        )::date AS date_day
    FROM date_range
)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::integer AS date_key,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day)::integer AS year,
    EXTRACT(QUARTER FROM date_day)::integer AS quarter,
    EXTRACT(MONTH FROM date_day)::integer AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(WEEK FROM date_day)::integer AS week_of_year,
    EXTRACT(DAY FROM date_day)::integer AS day_of_month,
    EXTRACT(DOW FROM date_day)::integer AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name
FROM dates
ORDER BY date_day