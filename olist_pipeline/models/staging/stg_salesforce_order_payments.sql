{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT 
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    FROM {{ ref('olist_order_payments_dataset') }}
)

SELECT 
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM raw_data