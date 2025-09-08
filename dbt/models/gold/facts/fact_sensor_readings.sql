{{
  config(
    materialized='table',
    schema='gold',
    contract={'enforced': true}
  )
}}

--
-- Sources from Silver layer
--
with stg_sensors as (
  select
    sensor_ts,
    shelf_id,
    store_id,
    temperature_c,
    humidity_pct,
    battery_mv,
    ingestion_ts
  from {{ ref('stg_sensors') }}
),

--
-- Aggregate sensor data to an hourly grain
--
hourly_summary as (
    select
        date_trunc('hour', sensor_ts) as sensor_hour_ts,
        store_id,
        shelf_id,
        
        -- Aggregated measures
        avg(temperature_c) as avg_temperature_c,
        avg(humidity_pct) as avg_humidity_pct,
        avg(battery_mv) as avg_battery_mv,
        max(ingestion_ts) as latest_ingestion_ts
    from stg_sensors
    group by 1, 2, 3
),

--
-- Calculate rolling averages and anomaly flags
--
final_with_metrics as (
    select
        {{ dbt_utils.generate_surrogate_key(['sensor_hour_ts', 'store_id', 'shelf_id']) }} as sensor_reading_sk,
        
        sensor_hour_ts,
        store_id,
        {{ surrogate_key(['store_id']) }} AS store_sk,
        shelf_id,

        -- Anomaly Flags (assumed reasonable ranges)
        case
            when avg_temperature_c < 0 or avg_temperature_c > 50 then true
            else false
        end as temperature_anomaly_flag,
        case
            when avg_humidity_pct < 0 or avg_humidity_pct > 100 then true
            else false
        end as humidity_anomaly_flag,
        case
            when avg_battery_mv < 2000 then true
            else false
        end as battery_anomaly_flag,

        -- Measures
        avg_temperature_c,
        avg_humidity_pct,
        avg_battery_mv,

        -- Rolling Averages (24-hour window)
        avg(avg_temperature_c) over (
            partition by store_id, shelf_id
            order by sensor_hour_ts
            rows between 23 preceding and current row
        ) as rolling_avg_24h_temperature_c,

        avg(avg_humidity_pct) over (
            partition by store_id, shelf_id
            order by sensor_hour_ts
            rows between 23 preceding and current row
        ) as rolling_avg_24h_humidity_pct,

        avg(avg_battery_mv) over (
            partition by store_id, shelf_id
            order by sensor_hour_ts
            rows between 23 preceding and current row
        ) as rolling_avg_24h_battery_mv,

        latest_ingestion_ts
    from hourly_summary
)

select * from final_with_metrics