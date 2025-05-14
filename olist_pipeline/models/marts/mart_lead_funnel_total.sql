{{ config(materialized='table') }}

with total_funnel as (
    select
        sum(total_mqls) as total_mqls,
        sum(total_sqls) as total_sqls,
        sum(total_wons) as total_wons
    from {{ ref('mart_lead_funnel') }}
)

select
    total_mqls,
    total_sqls,
    total_wons,
    round(total_sqls * 100 / nullif(total_mqls, 0), 2) as mql_to_sql_rate,
    round(total_wons * 100 / nullif(total_sqls, 0), 2) as sql_to_won_rate,
    round(total_wons * 100 / nullif(total_mqls, 0), 2) as mql_to_won_rate
from total_funnel
order by mql_to_won_rate desc
