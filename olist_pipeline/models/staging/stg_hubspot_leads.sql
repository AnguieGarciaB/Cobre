{{ config(materialized='view') }}

with raw_data as (
    select 
        mql_id,
        first_contact_date,
        landing_page_id,
        origin
    from {{ ref('olist_marketing_qualified_leads_dataset') }}
),

-- MODE landing_page_id
landing_page_id_mode as (
    select landing_page_id
    from (
        select landing_page_id, count(*) as freq
        from {{ ref('olist_marketing_qualified_leads_dataset') }}
        where landing_page_id is not null
        group by landing_page_id
        order by freq desc
        limit 1
    )
),

final_data as (
    select 
        mql_id,
        first_contact_date,
        case
            when origin = 'null' or origin is null then 'unknown'
            else origin
        end as lead_source,
        coalesce(rd.landing_page_id, lp.landing_page_id) as landing_page_id

    from  raw_data rd 
    cross join landing_page_id_mode lp
    where first_contact_date is not null

)
select * from final_data

