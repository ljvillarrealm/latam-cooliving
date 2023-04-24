/* The data for the datamart mart_latam-cooliving
*/

{{ config(
    materialized='table'
    , partition_by = {
        'field': 'period'
        , 'data_type': 'date'
        , 'granularity': 'day'
    }
    , cluster_by = ['category', 'country']
) }}

with d_cost as (
    select * from {{ ref('dim_cost') }} 
)
, d_currency as (
    select * from {{ ref('dim_currency') }} 
)
, d_lang as (
    select * from {{ ref('dim_language') }} 
)
, d_clatam as (
    select * from {{ ref('dim_country_latam') }} 
)
, f_costliv as (
    select
        period
        , country_id
        , cost_id
        , AVG(amount_USD) as amount_USD
    from {{ ref('fact_cost_of_living') }} 
    group by period, country_id, cost_id
)
, f_cinfo as (
    select * from {{ ref('fact_country_info') }} 
)

select
    f_costliv.period
    
    , d_cost.category
    , d_cost.cost_id
    , d_cost.description
    , d_clatam.country_name as country
    , d_currency.currency_iso
    , d_lang.language_name as language

    , f_costliv.amount_USD

from d_clatam
inner join f_costliv
    on f_costliv.country_id = d_clatam.country_id
left join d_cost
    on d_cost.cost_id = f_costliv.cost_id

inner join f_cinfo
    on f_cinfo.country_id = d_clatam.country_id
left join d_currency
    on d_currency.currency_id = f_cinfo.official_currency_id
left join d_lang
    on d_lang.language_id = f_cinfo.official_language_id

-- option
-- for production purposes excecute: dbt run --var 'is_test: false'
{% if var('is_test', default=true) %}

limit 1000

{% endif %}