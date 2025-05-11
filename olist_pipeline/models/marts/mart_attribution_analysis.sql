{{ config(materialized='table') }}

with conversion_rates as (
    select 
        marketing_source,
        total_leads,
        total_converted,
        conversion_rate
    from {{ ref('mart_conversion_rates') }}
)

select 
    marketing_source,
    total_leads,
    total_converted,
    conversion_rate,
    case 
        when conversion_rate > 0.5 then 'High'
        when conversion_rate between 0.2 and 0.5 then 'Medium'
        else 'Low' 
    end as attribution_level
from conversion_rates
