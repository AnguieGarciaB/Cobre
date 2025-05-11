{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM {{ ref('olist_orders_dataset') }}
)

SELECT 
    order_id AS opportunity_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM raw_data
WHERE order_status = 'delivered'