{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT 
        mql_id,
        first_contact_date,
        landing_page_id,
        origin
    FROM {{ ref('olist_marketing_qualified_leads_dataset') }}
)

SELECT 
    mql_id AS lead_id,
    first_contact_date,
    landing_page_id,
    origin AS lead_source
FROM raw_data
WHERE first_contact_date IS NOT NULL