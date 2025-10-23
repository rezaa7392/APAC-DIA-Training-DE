{{
  config(
    materialized='table',
    contract={'enforced': true}
  )
}}

with src as (
  select * from bronze_sensors_parquet
),

-- 1) Normalize to new typed columns with robust casting
normalized as (
  select
    try_cast(sensor_ts as timestamp)    as sensor_ts,
    try_cast(shelf_id as bigint)        as shelf_id,
    try_cast(store_id as bigint)        as store_id,
    try_cast(temperature_c as double)   as temperature_c,
    try_cast(humidity_pct as double)    as humidity_pct,
    try_cast(battery_mv as int)         as battery_mv,
    
    ingestion_ts,
    src_filename,
    src_row_hash
  from src
),

-- 2) Enforce key and filter out bad rows
filtered as (
  select
    *
  from normalized
  where sensor_ts is not null
    and shelf_id is not null
    and store_id is not null
),

-- 3) Deduplicate based on the composite primary key
dedup as (
  select
    *,
    row_number() over (
      partition by sensor_ts, shelf_id, store_id
      order by ingestion_ts desc, src_row_hash asc
    ) as _rn
  from filtered
)

-- 4) Final projection with canonical names
select
  sensor_ts,
  shelf_id,
  store_id,
  temperature_c,
  humidity_pct,
  battery_mv,
  
  ingestion_ts,
  src_filename,
  src_row_hash
from dedup
where _rn = 1