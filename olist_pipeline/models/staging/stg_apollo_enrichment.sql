{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT 
        mql_id,
        seller_id,
        sdr_id,
        sr_id,
        won_date,
        business_segment,
        lead_type,
        lead_behaviour_profile,
        has_company,
        has_gtin,
        average_stock,
        business_type,
        declared_product_catalog_size,
        declared_monthly_revenue
    FROM {{ ref('olist_closed_deals_dataset') }}
),

-- MODE business_type
business_type_mode as (
    select business_type
    from (
        select business_type, count(*) as freq
        from {{ ref('olist_closed_deals_dataset') }}
        where business_type is not null
        group by business_type
        order by freq desc
        limit 1
    )
),

-- MODE lead_type_mode
lead_type_mode as (
    select lead_type
    from (
        select lead_type, count(*) as freq
        from {{ ref('olist_closed_deals_dataset') }}
        where lead_type is not null
        group by lead_type
        order by freq desc
        limit 1
    )
),

-- MODE business_segment_mode
business_segment_mode as (
    select business_segment
    from (
        select business_segment, count(*) as freq
        from {{ ref('olist_closed_deals_dataset') }}
        where business_segment is not null
        group by business_segment
        order by freq desc
        limit 1
    )
),

final_data as (
    select 
        mql_id,
        seller_id,
        sdr_id,
        sr_id,
        won_date,
        lead_behaviour_profile,
        has_company,
        has_gtin,
        average_stock,
        declared_product_catalog_size,
        declared_monthly_revenue,

        -- Enrichment info with imputation
        -- business_segment -> 0.118% nulls
        -- lead_type -> 0.71% nulls
        -- business_type -> 1.18% nulls
        
        coalesce(rd.business_segment, bs.business_segment) as business_segment,
        coalesce(rd.lead_type, lt.lead_type) as lead_type,
        coalesce(rd.business_type, bt.business_type) as business_type,

    from  raw_data rd
    cross join business_type_mode bt
    cross join lead_type_mode lt
    cross join business_segment_mode bs

)
select * from final_data