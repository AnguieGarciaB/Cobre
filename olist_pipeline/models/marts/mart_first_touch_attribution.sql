{{ config(materialized='table') }}

with leads as (
    select *
    from {{ ref('in_hubspot_leads_enriched') }}
),

attribution as (
    select
        lead_source,
        count(*) as total_leads,
        count(won_date) as total_won_leads,
        round(count(won_date) * 100 / count(*), 2) as win_rate,
        round(avg(datediff(day, first_contact_date, won_date)), 1) as avg_days_to_win
    from leads
    group by lead_source
)

select *
from attribution
order by win_rate desc
