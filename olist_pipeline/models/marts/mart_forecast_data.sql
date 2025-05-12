{{ config(materialized='table') }}

with leads_data as (
    select
        date_trunc('week', first_contact_date) as week_start,
        count(*) as leads_created
    from {{ ref('in_hubspot_leads_enriched') }}
    group by 1
)

select * from leads_data
order by week_start asc