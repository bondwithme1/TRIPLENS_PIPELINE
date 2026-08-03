WITH source AS (
    SELECT raw_data
    FROM TRIPLENS_DB.RAW.COUNTRIES_RAW
)

SELECT
    raw_data:names:common::STRING           AS country_name,
    raw_data:names:official::STRING         AS official_name,
    raw_data:region::STRING                 AS region,
    raw_data:subregion::STRING              AS subregion,
    raw_data:capitals[0]:name::STRING       AS capital,
    raw_data:population::NUMBER             AS population,
    raw_data:area:kilometers::FLOAT         AS area_sq_km,
    raw_data:timezones[0]::STRING           AS primary_timezone,
    raw_data:flag:url_png::STRING           AS flag_url,
    raw_data:codes:alpha_2::STRING          AS country_code_2,
    raw_data:codes:alpha_3::STRING          AS country_code_3,
    raw_data:languages[0]:name::STRING      AS primary_language,
    raw_data:currencies[0]:name::STRING     AS primary_currency,
    raw_data:currencies[0]:code::STRING     AS currency_code,
    raw_data:currencies[0]:symbol::STRING   AS currency_symbol
FROM source