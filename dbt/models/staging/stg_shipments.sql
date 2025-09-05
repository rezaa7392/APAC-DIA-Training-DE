{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_shipments_parquet
),
norm as (
  select
    try_cast(shipment_id as bigint)    as shipment_id,
    try_cast(order_id as bigint)       as order_id_raw,
    upper(trim(carrier))               as carrier,
    try_cast(shipped_at as timestamp)  as shipped_at,
    try_cast(delivered_at as timestamp)as delivered_at,
    try_cast(ship_cost as decimal(18,2)) as ship_cost,
    ingestion_ts, src_filename, src_row_hash
  from src
  where try_cast(shipment_id as bigint) is not null
),
-- Enforce FK to orders (YAML has not_null + relationship on order_id)
with_fk as (
  select
    n.shipment_id,
    oh.order_id,
    n.carrier, n.shipped_at, n.delivered_at, n.ship_cost,
    n.ingestion_ts, n.src_filename, n.src_row_hash
  from norm n
  join {{ ref('stg_orders_header') }} oh
    on n.order_id_raw = oh.order_id
)
select * from with_fk
