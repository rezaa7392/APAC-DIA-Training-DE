{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension']
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),

final AS (
    SELECT
        {{ surrogate_key(['supplier_id']) }} AS supplier_sk,
        supplier_id,
        name,
        country_code,
        supplier_code,
        lead_time_days,
        preferred,

        CASE
            WHEN lead_time_days <= 7 THEN 'Tier A'
            WHEN lead_time_days > 7 AND lead_time_days <= 14 THEN 'Tier B'
            ELSE 'Tier C'
        END AS supplier_tier,

        CASE
            WHEN country_code IN ('AU', 'NZ') THEN 'Oceania'
            WHEN country_code IN ('US', 'CA', 'MX') THEN 'North America'
            WHEN country_code IN ('GB', 'FR', 'DE', 'ES') THEN 'Europe'
            ELSE 'Rest of World'
        END AS country_region

    FROM stg
)

SELECT * FROM final