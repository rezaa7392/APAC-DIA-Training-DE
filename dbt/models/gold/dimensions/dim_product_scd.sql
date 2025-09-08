{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH products_snapshot AS (
    SELECT * FROM {{ ref('products_snapshot') }}
),

products_enhanced AS (
    SELECT
        product_id,
        name,
        category,
        subcategory,
        current_price,
        currency,
        is_discontinued,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to,
        
        CASE
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current,

        -- Calculate price change indicator
        LAG(current_price, 1) OVER (PARTITION BY product_id ORDER BY dbt_valid_from) AS previous_price,
        CASE
            WHEN current_price > LAG(current_price, 1) OVER (PARTITION BY product_id ORDER BY dbt_valid_from) THEN 'Price Increased'
            WHEN current_price < LAG(current_price, 1) OVER (PARTITION BY product_id ORDER BY dbt_valid_from) THEN 'Price Decreased'
            ELSE 'No Price Change'
        END AS price_change_indicator
        
    FROM products_snapshot
)

SELECT
    -- Create a surrogate key for the dimension
    {{ surrogate_key(['dbt_scd_id']) }} AS product_scd_sk,
    product_id,
    name,
    category,
    subcategory,
    current_price,
    currency,
    is_discontinued,
    dbt_valid_from AS effective_from_date,
    dbt_valid_to AS effective_to_date,
    is_current,
    price_change_indicator
    
FROM products_enhanced