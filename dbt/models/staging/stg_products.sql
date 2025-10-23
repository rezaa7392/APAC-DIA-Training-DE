{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_products_parquet
),
norm as (
  select
    try_cast(product_id as bigint)           as product_id,
    lower(trim(sku))                         as sku,
    lower(trim(name))                        as name,
    lower(trim(category))                    as category,
    lower(trim(subcategory))                 as subcategory,
    try_cast(current_price as decimal(18,2)) as current_price,
    upper(trim(currency))                    as currency,
    try_cast(is_discontinued as boolean)     as is_discontinued,
    try_cast(introduced_dt as date)          as introduced_dt,
    try_cast(discontinued_dt as date)        as discontinued_dt,
    ingestion_ts, src_filename, src_row_hash
  from src
  where try_cast(product_id as bigint) is not null
),

dedup as (
  select *
  from (
    select
      w.*,
      row_number() over (
        partition by product_id
        order by ingestion_ts desc, src_row_hash desc
      ) as _rn
    from norm w
  ) t
  where _rn = 1
)
select 
    product_id,
    sku,
    name,
    category,
    subcategory,
    current_price,
    currency,
    is_discontinued,
    introduced_dt,
    discontinued_dt,
    ingestion_ts, 
    src_filename, 
    src_row_hash 
from dedup
