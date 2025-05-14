{{ config(materialized='table') }}
with lead_data as (
    select
        mql_id,
        lead_source,
        lead_type,
        days_to_conversion
    from {{ ref('in_hubspot_leads_enriched') }}
    where is_won = 1
)

select
    lead_source,
    lead_type,
    avg(days_to_conversion) as avg_days_to_conversion
from lead_data
group by lead_source, lead_type
