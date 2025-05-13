{{ config(materialized='table') }}

with hubspot as (
    select *
    from {{ ref('stg_hubspot_leads') }}
),
apollo as (
    select *
    from {{ ref('stg_apollo_enrichment') }}
),

-- MODE business_type
business_type_mode as (
    select business_type
    from (
        select business_type, count(*) as freq
        from {{ ref('stg_apollo_enrichment') }}
        where business_type is not null
        group by business_type
        order by freq desc
        limit 1
    )
),

-- MODE landing_page_id
landing_page_id_mode as (
    select landing_page_id
    from (
        select landing_page_id, count(*) as freq
        from {{ ref('stg_hubspot_leads') }}
        where landing_page_id is not null
        group by landing_page_id
        order by freq desc
        limit 1
    )
),

-- MODE landing_page_id
lead_type_mode as (
    select lead_type
    from (
        select lead_type, count(*) as freq
        from {{ ref('stg_apollo_enrichment') }}
        where lead_type is not null
        group by lead_type
        order by freq desc
        limit 1
    )
),

-- MODE landing_page_id
business_segment_mode as (
    select business_segment
    from (
        select business_segment, count(*) as freq
        from {{ ref('stg_apollo_enrichment') }}
        where business_segment is not null
        group by business_segment
        order by freq desc
        limit 1
    )
),


final_data as (
    select
        h.lead_id,
        h.first_contact_date,
        coalesce(h.landing_page_id, lp.landing_page_id) as landing_page_id,
        case
            when h.lead_source = 'null' or h.lead_source is null then 'unknown'
            else h.lead_source
        end as lead_source,
        a.seller_id,

        -- Conversion info
        a.won_date,
        datediff(day, h.first_contact_date, a.won_date) as days_to_conversion,
        case when a.won_date is not null then 1 else 0 end as is_won,

        -- Enrichment info with imputation
        coalesce(a.business_segment, bs.business_segment) as business_segment,
        coalesce(a.lead_type, lt.lead_type) as lead_type,
        coalesce(a.business_type, bt.business_type) as business_type,
        a.lead_behaviour_profile
        
    from hubspot h
    left join apollo a on h.lead_id = a.mql_id
    cross join business_type_mode bt
    cross join landing_page_id_mode lp
    cross join lead_type_mode lt
    cross join business_segment_mode bs
)

select * from final_data
