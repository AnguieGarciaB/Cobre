with hubspot as (
    select *
    from {{ ref('stg_hubspot_leads') }}
),
apollo as (
    select *
    from {{ ref('stg_apollo_enrichment') }}
)

select
    h.lead_id,
    h.first_contact_date,
    h.landing_page_id,
    h.lead_source,
    a.mql_id,
    a.seller_id,
    a.won_date,
    a.business_segment,
    a.lead_type,
    a.business_type

from hubspot h
left join apollo a
    on h.lead_id = a.mql_id