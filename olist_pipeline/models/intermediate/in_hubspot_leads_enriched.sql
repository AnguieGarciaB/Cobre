{{ config(materialized='table') }}

with hubspot as (
    select *
    from {{ ref('stg_hubspot_leads') }}
),
apollo as (
    select *
    from {{ ref('stg_apollo_enrichment') }}
),


final_data as (
    select
        h.mql_id,
        h.first_contact_date,
        h.lead_source,
        h.landing_page_id,
        
        a.seller_id,
        a.won_date,
        datediff(day, h.first_contact_date, a.won_date) as days_to_conversion,
        case when a.won_date is not null then 1 else 0 end as is_won,
        a.business_segment,
        a.lead_type,
        a.business_type,
        a.lead_behaviour_profile

    from hubspot h
    left join apollo a on h.mql_id = a.mql_id
)

select * from final_data
