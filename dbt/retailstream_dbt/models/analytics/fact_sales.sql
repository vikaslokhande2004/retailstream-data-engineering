{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,

    o.ordered_at::date AS order_date,

    oi.quantity,
    oi.unit_price,
    oi.discount_amount,

    oi.quantity * oi.unit_price AS gross_amount,

    COALESCE(oi.discount_amount, 0) AS discount_amount_total,

    (oi.quantity * oi.unit_price)
        - COALESCE(oi.discount_amount, 0) AS net_sales_amount

FROM {{ ref('stg_order_items') }} AS oi

INNER JOIN {{ ref('stg_orders') }} AS o
    ON oi.order_id = o.order_id