{{ config(
    materialized = 'incremental',
    unique_key   = ['order_id','line_number'],      
    incremental_strategy = 'delete+insert' ,
    on_schema_change = 'append_new_columns'                  
) }}

with
lines as (
  select
    order_id,
    line_number,
    product_id,
    qty,
    unit_price,
    coalesce(line_discount_pct, 0.0)  as line_discount_pct,
    coalesce(tax_pct, 0.0)            as tax_pct,
    ingestion_ts,
    src_filename,
    src_row_hash
  from {{ ref('stg_orders_lines') }}
  {% if is_incremental() %}
    where ingestion_ts > (select coalesce(max(ingestion_ts), timestamp '1970-01-01') from {{ this }})
  {% endif %}
),

hdr as (
  select
    order_id,
    customer_id,
    store_id,
    order_ts,
    currency,
    order_dt_local
    channel, 
    payment_method,
    coupon_code,
    shipping_fee,
    ingestion_ts, 
    src_filename, 
    src_row_hash
  from {{ ref('stg_orders_header') }}
),

prod as (
  select
    *
  from {{ ref('stg_products') }}
),

xr as (
  select
    date,
    currency,
    rate_to_aud
  from {{ ref('stg_exchange_rates') }}
),

joined as (
  select
    l.order_id,
    l.line_number,
    h.customer_id,
    h.store_id,
    h.order_ts,
    h.currency as order_currency,

    l.product_id,
    p.name             as product_name,
    p.category,
    p.subcategory,

    l.qty,
    l.unit_price,
    cast(round(l.qty * l.unit_price, 2) as decimal(18,2))                       as line_gross_amount,
    cast(round((l.qty * l.unit_price) * (1 - l.line_discount_pct), 2)as decimal(18,2)) as line_net_before_tax,
    cast(round((l.qty * l.unit_price) * (1 - l.line_discount_pct) * (1 + l.tax_pct), 2)as decimal(18,2)) as line_total_amount,

    x.rate_to_aud,
    case when x.rate_to_aud is not null
         then cast(round((l.qty * l.unit_price) * (1 - l.line_discount_pct) * (1 + l.tax_pct) * x.rate_to_aud, 2)as decimal(18,2))
         else null end                                    as line_total_amount_aud,

    l.ingestion_ts,
    l.src_filename,
    l.src_row_hash
  from lines l
  join hdr   h on l.order_id   = h.order_id
  join prod  p on l.product_id = p.product_id
  left join xr x
    on x.currency = h.currency
   and x.date = cast(h.order_ts as date)
)

select * from joined
