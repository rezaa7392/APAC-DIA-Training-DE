{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_orders_header_parquet
),
norm as (
  select
    try_cast(order_id as bigint)             as order_id,
    try_cast(customer_id as bigint)          as customer_id,
    try_cast(store_id as bigint)             as store_id,
    try_cast(order_ts as timestamp)          as order_ts,
    upper(trim(currency))                    as currency,
    try_cast(order_dt_local as date)         as order_dt_local,
    trim(channel)                            as channel, 
    trim(payment_method)                     as payment_method,
    trim(coupon_code)                        as coupon_code,
    try_cast(shipping_fee as decimal(18,2))  as shipping_fee,
    ingestion_ts, src_filename, src_row_hash
  from src
  where try_cast(order_id as bigint) is not null
),
-- Enforce FK to customers & stores (filter to only matched rows)
with_fk as (
  select
    n.order_id,
    c.customer_id,
    st.store_id,
    n.order_ts, n.currency, n.shipping_fee, n.coupon_code, n.payment_method, n.channel, 
    n.order_dt_local,
    n.ingestion_ts, n.src_filename, n.src_row_hash
  from norm n
  join {{ ref('stg_customers') }} c
    on n.customer_id = c.customer_id
  join {{ ref('stg_stores') }} st
    on n.store_id = st.store_id
),
dedup as (
  select *
  from (
    select
      f.*,
      row_number() over (
        partition by order_id
        order by ingestion_ts desc, src_row_hash desc
      ) as _rn
    from with_fk f
  ) t
  where _rn = 1
)
select 
    order_id,
    customer_id,
    store_id,
    order_ts,
    currency,
    order_dt_local,
    channel, 
    payment_method,
    coupon_code,
    shipping_fee,
    ingestion_ts, 
    src_filename, 
    src_row_hash
from dedup
