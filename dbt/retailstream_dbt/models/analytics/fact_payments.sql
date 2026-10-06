{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    p.payment_id,
    p.order_id,
    o.customer_id,

    p.method AS payment_method,
    p.amount AS payment_amount,
    p.paid_at,
    p.status AS payment_status,
    p.failure_reason,

    p.paid_at::date AS payment_date

FROM {{ ref('stg_payments') }} AS p

INNER JOIN {{ ref('stg_orders') }} AS o
    ON p.order_id = o.order_id