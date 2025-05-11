{{ config(materialized='table') }}

with hubspot_leads as (
    select 
        lead_id,
        first_contact_date,
        lead_source
    from {{ ref('in_hubspot_leads_enriched') }}
),
salesforce_orders as (
    select 
        order_id,
        customer_id,
        total_price,
        total_freight,
        total_payment,
        order_delivered_customer_date
    from {{ ref('in_salesforce_orders') }}
)

select 
    h.lead_source as marketing_source,
    count(h.lead_id) as total_leads,
    count(s.order_id) as total_converted,
    (count(s.order_id) / nullif(count(h.lead_id), 0)) as conversion_rate
from hubspot_leads h
left join salesforce_orders s
    on h.lead_id = s.customer_id
group by h.lead_source
