with orders as (
    select 
        order_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from {{ ref('stg_salesforce_orders') }}
),
order_items as (
    select 
        order_id,
        seller_id,
        sum(price) as total_price,
        sum(freight_value) as total_freight
    from {{ ref('stg_salesforce_order_items') }}
    group by order_id, seller_id
),
order_payments as (
    select 
        order_id,
        sum(payment_value) as total_payment
    from {{ ref('stg_salesforce_order_payments') }}
    group by order_id
)

select 
    o.order_id,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    oi.seller_id,
    oi.total_price,
    oi.total_freight,
    op.total_payment

from order_items oi
left join orders o
    on o.order_id = oi.order_id

left join order_payments op
    on o.order_id = op.order_id