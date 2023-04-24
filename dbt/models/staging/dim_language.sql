/* The language dimension.
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
    , {{ dbt_utils.surrogate_key(['UPPER(TRIM(language))']) }} as language_id

    -- descriptions
    , UPPER(TRIM(language)) as language_name
    --, language as language_name_raw

from {{ source('staging','languages') }}
-- order by language_name asc

-- option
-- for production purposes excecute: dbt run --var 'is_test: false'
{% if var('is_test', default=true) %}

limit 1000

{% endif %}
