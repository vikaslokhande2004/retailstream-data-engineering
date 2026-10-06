{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    r.return_id,
    r.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,

    r.returned_at,
    r.returned_at::date AS return_date,

    r.reason,
    r.refund_amount,
    r.restocked

FROM {{ ref('stg_returns') }} AS r

INNER JOIN {{ ref('stg_order_items') }} AS oi
    ON r.order_item_id = oi.order_item_id

INNER JOIN {{ ref('stg_orders') }} AS o
    ON oi.order_id = o.order_id