{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_suppliers_parquet
),
normalized as (
  select
    try_cast(supplier_id as bigint) as supplier_id,
    lower(trim(name))               as name,
    upper(trim(country_code))       as country_code,
    lower(trim(supplier_code))      as supplier_code,
    try_cast(lead_time_days as int) as lead_time_days,  
    try_cast(preferred as boolean)  as preferred,
    ingestion_ts, src_filename, src_row_hash
  from src
  where try_cast(supplier_id as bigint) is not null
),
dedup as (
  select *
  from (
    select
      n.*,
      row_number() over (
        partition by supplier_id
        order by ingestion_ts desc, src_row_hash desc
      ) as _rn
    from normalized n
  ) t
  where _rn = 1
)
select 
  supplier_id,
  name,
  country_code,
  supplier_code,
  lead_time_days,
  preferred,
  ingestion_ts,
  src_filename,
  src_row_hash
from dedup
