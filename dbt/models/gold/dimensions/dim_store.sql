{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension']
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_stores') }}
),

final AS (
    SELECT
        {{ surrogate_key(['store_id']) }} AS store_sk,
        store_id,
        store_code,
        name,
        channel,
        region,
        state,
        latitude,
        longitude,
        open_dt,
        close_dt,

        CAST(date_diff('day', open_dt, CURRENT_DATE) AS INT) AS store_age_days,
        
        CASE
            WHEN close_dt IS NOT NULL THEN 'Closed'
            ELSE 'Open'
        END AS operational_status,

        CASE
            WHEN lower(channel) = 'online' THEN 'Small'
            WHEN lower(channel) = 'physical' THEN 'Large'
            ELSE 'Unknown'
        END AS store_size_category

    FROM stg
)

SELECT * FROM final