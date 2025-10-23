{{
  config(
    materialized='table',
    schema='gold',
    contract={'enforced': true}
  )
}}


with
shipments as (
  select * from {{ ref('stg_shipments') }}
),

sales as (
  select
    order_id,
    customer_sk,
    store_sk
  from {{ ref('fact_sales') }}

  group by 1,2,3
),


joined as (
  select
    s.shipment_id,
    s.order_id,
    f.customer_sk,
    f.store_sk,
    cast(s.shipped_at as date) as shipped_date_sk,
    cast(s.delivered_at as date) as delivered_date_sk,

    s.carrier,

    s.ship_cost,
    -- Calculate number of days from shipped to delivered
    date_diff('day', s.shipped_at, s.delivered_at) as delivery_days,

    -- Flag to check if shipment was "on time" (within 5 days)
    case
        when date_diff('day', s.shipped_at, s.delivered_at) <= 5 then true
        else false
    end as on_time_flag,

    s.shipped_at,
    s.delivered_at,

    s.ingestion_ts
  from shipments s
  left join sales f
    on s.order_id = f.order_id
  
)

select * from joined