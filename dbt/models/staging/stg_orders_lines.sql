{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_orders_lines_parquet
),
norm as (
  select
    try_cast(order_id as bigint)             as order_id_raw,
    try_cast(line_number as int)             as line_number,
    try_cast(product_id as bigint)           as product_id_raw,
    try_cast(qty as int)                     as qty,
    try_cast(unit_price as decimal(18,2))    as unit_price,
    try_cast(line_discount_pct as double)    as line_discount_pct,
    try_cast(tax_pct as double)              as tax_pct,
    ingestion_ts, src_filename, src_row_hash
  from src
  where try_cast(order_id as bigint) is not null
    and try_cast(line_number as int) is not null
),
-- keep only rows whose ORDER exists
with_orders as (
  select n.*
  from norm n
  where exists (
    select 1
    from {{ ref('stg_orders_header') }} oh
    where oh.order_id = n.order_id_raw
  )
),

-- and whose PRODUCT exists
with_products as (
  select w.*
  from with_orders w
  where exists (
    select 1
    from {{ ref('stg_products') }} p
    where p.product_id = w.product_id_raw
  )
),

-- project the final keys from the raw, now that we know they exist
projected as (
  select
    order_id_raw   as order_id,
    line_number,
    product_id_raw as product_id,
    qty, unit_price, line_discount_pct, tax_pct,
    ingestion_ts, src_filename, src_row_hash
  from with_products
),
dedup as (
  select *
  from (
    select
      f.*,
      row_number() over (
        partition by order_id, line_number
        order by ingestion_ts desc, src_row_hash desc
      ) as _rn
    from projected  f
  ) t
  where _rn = 1
)
select 
  order_id,
  line_number,
  product_id,
  qty,
  unit_price,
  line_discount_pct,
  tax_pct,
  ingestion_ts, 
  src_filename,
  src_row_hash
from dedup
