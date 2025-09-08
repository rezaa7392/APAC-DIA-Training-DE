-- models/gold/dimensions/dim_date.sql

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH date_spine AS (
  SELECT d::date AS date_day
  FROM generate_series(date '2018-01-01', date '2028-12-31', interval 1 day) AS g(d)
),

-- Holiday data for AU, US, and UK (as a temporary CTE or a separate seed file)
holidays AS (
    SELECT * FROM (
        VALUES
            (date '2023-01-01', 'New Year''s Day', 'AU'),
            (date '2023-01-01', 'New Year''s Day', 'US'),
            (date '2023-01-01', 'New Year''s Day', 'UK'),
            (date '2023-01-26', 'Australia Day', 'AU'),
            (date '2023-02-20', 'Presidents'' Day', 'US'),
            (date '2023-04-07', 'Good Friday', 'AU'),
            (date '2023-04-07', 'Good Friday', 'UK'),
            (date '2023-04-10', 'Easter Monday', 'AU'),
            (date '2023-04-10', 'Easter Monday', 'UK'),
            (date '2023-05-01', 'Early May bank holiday', 'UK'),
            (date '2023-05-29', 'Spring bank holiday', 'UK'),
            (date '2023-05-29', 'Memorial Day', 'US'),
            (date '2023-06-19', 'Juneteenth', 'US'),
            (date '2023-07-04', 'Independence Day', 'US'),
            (date '2023-08-28', 'Summer bank holiday', 'UK'),
            (date '2023-09-04', 'Labor Day', 'US'),
            (date '2023-10-09', 'Indigenous Peoples'' Day', 'US'),
            (date '2023-11-11', 'Veterans Day', 'US'),
            (date '2023-11-23', 'Thanksgiving Day', 'US'),
            (date '2023-12-25', 'Christmas Day', 'AU'),
            (date '2023-12-25', 'Christmas Day', 'US'),
            (date '2023-12-25', 'Christmas Day', 'UK'),
            (date '2023-12-26', 'Boxing Day', 'AU'),
            (date '2023-12-26', 'Boxing Day', 'UK')
    ) AS T (holiday_date, holiday_name, country_code)
),

final AS (
    SELECT
        cast(date_day as date) AS date,
        CAST(EXTRACT(YEAR FROM date_day) AS INT)                                   AS year,
        CAST(EXTRACT(MONTH FROM date_day) AS INT)                                  AS month_of_year,
        CAST(EXTRACT(DAYOFYEAR FROM date_day) AS INT)                              AS day_of_year,
        CAST(EXTRACT(DAYOFMONTH FROM date_day) AS INT)                             AS day_of_month,
        CAST(EXTRACT(WEEK FROM date_day) AS INT)                                   AS week_of_year,
        CAST(EXTRACT(DAYOFWEEK FROM date_day) AS INT)                              AS day_of_week,
        CAST(EXTRACT(DAYOFWEEK FROM date_day) IN (6, 7) AS BOOLEAN)                AS is_weekend,
        
        -- Fiscal Periods (assuming a fiscal year starts on July 1st)
        CASE
            WHEN EXTRACT(MONTH FROM date_day) >= 7 THEN EXTRACT(YEAR FROM date_day) + 1
            ELSE EXTRACT(YEAR FROM date_day)
        END AS fiscal_year,

        CASE
            WHEN EXTRACT(MONTH FROM date_day) IN (7, 8, 9) THEN 'Q1'
            WHEN EXTRACT(MONTH FROM date_day) IN (10, 11, 12) THEN 'Q2'
            WHEN EXTRACT(MONTH FROM date_day) IN (1, 2, 3) THEN 'Q3'
            ELSE 'Q4'
        END AS fiscal_quarter,

        -- Quarter Boundaries (fixed for DuckDB)
        CAST(DATE_TRUNC('quarter', date_day) AS DATE) AS quarter_start_date,
        CAST(DATE_TRUNC('quarter', date_day) + INTERVAL '3 month' - INTERVAL '1 day' AS DATE) AS quarter_end_date,

        -- ISO Week Number
        CAST(EXTRACT(ISOYEAR FROM date_day) AS INT) AS iso_year,
        CAST(EXTRACT(WEEK FROM date_day) AS INT)    AS iso_week_of_year,
        
        -- Holiday Flag and Name
        CASE WHEN h.holiday_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_holiday,
        h.holiday_name,
        h.country_code AS holiday_country_code

    FROM date_spine
    LEFT JOIN holidays h
        ON date_spine.date_day = h.holiday_date
)

SELECT * FROM final