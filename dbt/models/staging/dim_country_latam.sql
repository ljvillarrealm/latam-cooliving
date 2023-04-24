/* The country dimension. Countries of latam are considered.
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
    DATE(ingestion_time) as period

    -- identifiers
    , {{ dbt_utils.surrogate_key(['UPPER(TRIM(country))']) }} as country_id

    -- descriptions
    , UPPER(TRIM(country)) as country_name
    --, country as country_name_raw

from {{ source('staging','latam_countries') }}
-- order by country_name asc

-- option
-- for production purposes excecute: dbt run --var 'is_test: false'
{% if var('is_test', default=true) %}

limit 1000

{% endif %}
