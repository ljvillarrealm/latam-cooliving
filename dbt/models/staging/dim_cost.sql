/* The cost dimension.
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
    , UPPER(TRIM(item)) as cost_id

    -- descriptions
    , UPPER(TRIM(category)) as category
    , TRIM(description) as description

from {{ source('staging','cost_of_living_dic') }}
-- order by cost_id asc

-- option
-- for production purposes excecute: dbt run --var 'is_test: false'
{% if var('is_test', default=true) %}

limit 1000

{% endif %}
