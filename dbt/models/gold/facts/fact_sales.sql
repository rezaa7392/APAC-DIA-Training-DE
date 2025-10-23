-- models/gold/facts/fct_sales.sql

{{
    config(
        materialized = 'incremental',
        unique_key   = ['order_id', 'line_number'],
        incremental_strategy = 'delete+insert',
        on_schema_change = 'append_new_columns',
        schema='gold'
    )
}}

--
-- Sources from Silver layer
--
with
lines as (
  select
    *
  from {{ ref('stg_orders_lines') }}
  {% if is_incremental() %}
    where ingestion_ts > (select coalesce(max(ingestion_ts), timestamp '1970-01-01') from {{ this }})
  {% endif %}
),

hdr as (
  select
    *
  from {{ ref('stg_orders_header') }}
),

--
-- Joins to Gold dimensions
--
joined as (
  select
    l.order_id,
    l.line_number,
    -- Degenerate dimension
    l.order_id as order_number,

    -- Foreign Keys to dimensions
    {{ surrogate_key(['h.customer_id']) }} as customer_sk,
    {{ surrogate_key(['h.store_id']) }}    as store_sk,
    
    cast(h.order_ts as date) as date_sk,

    p.product_scd_sk,

    -- Measures
    l.qty,
    l.unit_price,

    -- Calculated Measures (in local currency)
    l.qty * l.unit_price                                                            as gross_amount,
    cast((l.qty * l.unit_price) * l.line_discount_pct  as DECIMAL(18,2))                                  as discount_amount,
    cast((l.qty * l.unit_price) * (1 - l.line_discount_pct)   as DECIMAL(18,2))                            as net_amount_before_tax,
    cast((l.qty * l.unit_price) * (1 - l.line_discount_pct) * l.tax_pct  as DECIMAL(18,2))                 as tax_amount,
    cast((l.qty * l.unit_price) * (1 - l.line_discount_pct) * (1 + l.tax_pct) as DECIMAL(18,2))            as net_amount,

    -- Conversion to AUD
    cast((l.qty * l.unit_price) * (1 - l.line_discount_pct) * (1 + l.tax_pct) * xr.rate_to_aud as DECIMAL(18,2))  as net_amount_aud,
    cast((l.qty * l.unit_price) * (1 - l.line_discount_pct) * (1 + l.tax_pct) as DECIMAL(18,2))                 as line_total,

    -- Additional attributes for context
    h.order_ts,
    h.currency as order_currency,
    h.channel, 
    h.payment_method,
    h.coupon_code,
    h.shipping_fee,
    l.ingestion_ts

  from lines l
  inner join hdr h
    on l.order_id = h.order_id
  left join {{ ref('dim_product_scd') }} p
    on l.product_id = p.product_id
    and p.effective_to_date is null
  left join {{ ref('stg_exchange_rates') }} xr
    on cast(h.order_ts as date) = xr.date and h.currency = xr.currency
)

select * from joined