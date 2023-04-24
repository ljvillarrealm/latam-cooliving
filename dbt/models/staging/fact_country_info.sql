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
    DATE(cld.ingestion_time) as period

    -- identifiers
    , {{ dbt_utils.surrogate_key(['UPPER(TRIM(cld.country))']) }} as country_id
    , {{ dbt_utils.surrogate_key(['UPPER(TRIM(language))']) }} as official_language_id
    , UPPER(TRIM(currency_ISO)) as official_currency_id

    -- facts
    , cld.GDP_thousands as gdp_thousands
    , cld.GDP_per_capita as gdp_per_capita


from {{ source('staging','latam_countries') }} as cld
left join {{ source('staging','languages') }} as l
    on l.country = cld.country
left join {{ source('staging','currencies') }} as c
    on c.country = cld.country
-- order by period asc, country_id asc, official_language_id asc, official_currency_id asc

-- option
-- for production purposes excecute: dbt run --var 'is_test: false'
{% if var('is_test', default=true) %}

limit 1000

{% endif %}
