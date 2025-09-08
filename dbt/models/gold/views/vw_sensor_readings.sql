-- models/gold/views/vw_sensor_readings.sql

{{
  config(
    materialized='view',
    schema='gold'
  )
}}

with readings as (
    select
        sensor_hour_ts,
        store_id,
        shelf_id,
        avg_temperature_c,
        avg_humidity_pct,
        avg_battery_mv,
        temperature_anomaly_flag,
        humidity_anomaly_flag,
        battery_anomaly_flag,
        rolling_avg_24h_temperature_c,
        rolling_avg_24h_humidity_pct,
        rolling_avg_24h_battery_mv
    from {{ ref('fact_sensor_readings') }}
),

stores as (
    select
        store_id,
        name as store_name,
        region as store_region,
        channel as store_channel
    from {{ ref('dim_store') }}
)

select
    -- Timestamp and Location Details
    r.sensor_hour_ts as reading_timestamp_utc,
    s.store_id,
    s.store_name,
    s.store_region,
    s.store_channel,



    -- Measures
    r.avg_temperature_c as average_temperature_celsius,
    r.avg_humidity_pct as average_humidity_percent,
    r.avg_battery_mv as average_battery_millivolts,
    
    -- Rolling Averages (for comparison)
    r.rolling_avg_24h_temperature_c as rolling_24h_avg_temperature,
    r.rolling_avg_24h_humidity_pct as rolling_24h_avg_humidity,
    r.rolling_avg_24h_battery_mv as rolling_24h_avg_battery,

    -- Anomaly Flags
    r.temperature_anomaly_flag,
    r.humidity_anomaly_flag,
    r.battery_anomaly_flag

from readings r
left join stores s
    on r.store_id = s.store_id
