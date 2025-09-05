{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_stores_parquet
)
select
  try_cast(store_id as bigint)                    as store_id,
  upper(trim(store_code))                         as store_code,
  lower(trim(name))                               as name,
  lower(trim(channel))                            as channel,
  upper(trim(region))                             as region,
  upper(trim(state))                              as state,
  try_cast(latitude as double)                    as latitude,
  try_cast(longitude as double)                   as longitude,
  try_cast(open_dt as date)                       as open_dt,
  try_cast(close_dt as date)                      as close_dt,
  ingestion_ts, src_filename, src_row_hash
from src
where try_cast(store_id as bigint) is not null
