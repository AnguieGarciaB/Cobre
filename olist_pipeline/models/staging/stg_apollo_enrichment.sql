{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT 
        mql_id,
        seller_id,
        sdr_id,
        sr_id,
        won_date,
        business_segment,
        lead_type,
        lead_behaviour_profile,
        has_company,
        has_gtin,
        average_stock,
        business_type,
        declared_product_catalog_size,
        declared_monthly_revenue
    FROM {{ ref('olist_closed_deals_dataset') }}
)


SELECT 
    mql_id,
    seller_id,
    sdr_id,
    sr_id,
    won_date,
    business_segment,
    lead_type,
    lead_behaviour_profile,
    has_company,
    has_gtin,
    average_stock,
    business_type,
    declared_product_catalog_size,
    declared_monthly_revenue
FROM raw_data
WHERE won_date IS NOT NULL