{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    o.order_id,
    o.customer_id,
    o.ordered_at,
    o.shipped_at,
    o.status,
    o.channel,
    o.shipping_country,
    o.coupon_code,

    o.ordered_at::date AS order_date,
    o.shipped_at::date AS shipped_date,

    CASE
        WHEN o.shipped_at IS NOT NULL
        THEN o.shipped_at::date - o.ordered_at::date
        ELSE NULL
    END AS shipping_days

FROM {{ ref('stg_orders') }} AS o