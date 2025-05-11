{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT 
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM {{ ref('olist_sellers_dataset') }}
)

SELECT 
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM raw_data