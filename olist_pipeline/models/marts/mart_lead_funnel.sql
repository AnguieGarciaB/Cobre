{{ config(materialized='table') }}

with leads as (
    select *
    from {{ ref('in_hubspot_leads_enriched') }}
),

-- Calculamos el estado del lead
leads_status as (
    select
        lead_id,
        lead_source,
        -- MQL: todos los registros de hubspot
        1 as is_mql,
        -- SQL: si tiene una fila en leads_closed
        case when seller_id is not null then 1 else 0 end as is_sql,
        -- Won: si tiene fecha de won_date
        case when won_date is not null then 1 else 0 end as is_won
    from leads
),

-- Agrupamos por canal y calculamos tasas de conversión
conversion_rates as (
    select
        lead_source,
        count(*) as total_mqls,
        sum(is_sql) as total_sqls,
        sum(is_won) as total_wons,
        round(sum(is_sql) * 1.0 / count(*), 2) as mql_to_sql_rate,
        round(sum(is_won) * 1.0 / nullif(sum(is_sql), 0), 2) as sql_to_won_rate,
        round(sum(is_won) * 1.0 / count(*), 2) as mql_to_won_rate
    from leads_status
    group by lead_source
)

select *
from conversion_rates
