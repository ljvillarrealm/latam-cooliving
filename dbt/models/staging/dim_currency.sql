/* The currency dimension.
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
    , UPPER(TRIM(currency_ISO)) as currency_id

    -- descriptions
    , currency_ISO as currency_iso

from {{ source('staging','currencies') }}
-- order by currency_id asc

-- option
-- for production purposes excecute: dbt run --var 'is_test: false'
{% if var('is_test', default=true) %}

limit 1000

{% endif %}