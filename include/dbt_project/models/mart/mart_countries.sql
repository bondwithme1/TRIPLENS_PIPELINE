WITH staging AS (
    SELECT * FROM {{ ref('stg_countries') }}
)

SELECT
    country_code_3          AS country_id,
    country_name,
    official_name,
    region,
    subregion,
    capital,
    population,
    area_sq_km,
    primary_timezone,
    flag_url,
    country_code_2,
    country_code_3,
    primary_language,
    primary_currency,
    currency_code,
    currency_symbol
FROM staging
WHERE country_name IS NOT NULL
ORDER BY country_name