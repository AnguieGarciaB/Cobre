{{ config(materialized='table') }}

select
  date(first_contact_date) as day,
  count(distinct lead_id) as leads,
  count(distinct case when won_date is not null then lead_id end) as conversions
from {{ ref('in_hubspot_leads_enriched') }}
group by date(first_contact_date)
order by day
