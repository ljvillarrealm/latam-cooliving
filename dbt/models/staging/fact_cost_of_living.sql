/* The factual info of the countries.
*/

{{ config(
    materialized='table'
    , partition_by = {
        'field': 'period'
        , 'data_type': 'date'
        , 'granularity': 'day'
    }
) }}


select distinct
    -- controls
    DATE(cl.ingestion_time) as period

    -- identifiers
    , {{ dbt_utils.surrogate_key(['UPPER(TRIM(cl.country))']) }} as country_id
    , UPPER(TRIM(cl.item)) as cost_id

    -- facts
    , cl.amount as amount_USD
    , cl.data_quality as data_quality


from {{ source('staging','cost_of_living') }} as cl
-- order by period asc, country_id asc

-- option
-- for production purposes excecute: dbt run --var 'is_test: false'
{% if var('is_test', default=true) %}

limit 1000

{% endif %}