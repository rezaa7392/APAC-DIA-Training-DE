-- models/gold/views/vw_sales_analysis.sql

{{
  config(
    materialized='view',
    schema='gold'
  )
}}

with sales as (
    select
        *
    from {{ ref('fact_sales') }}
),

customers as (
    select
        *
    from {{ ref('dim_customer') }}
),

products as (
    select
        *
    from {{ ref('dim_product_scd') }}
),

stores as (
    select
        *
    from {{ ref('dim_store') }}
),

dates as (
    select
        *
    from {{ ref('dim_date') }}
)

select
    -- Order and Line Item Details
    s.order_id,
    s.line_number,
    s.channel as order_channel,
    s.payment_method,
    s.shipping_fee,
    s.order_ts as order_timestamp_utc,
    s.unit_price,
    s.order_currency,
    s.discount_amount,
    s.coupon_code,
    s.line_total,
    s.qty as quantity_sold,
    s.gross_amount,
    s.discount_amount,
    s.tax_amount,
    s.net_amount as total_net_amount,
    s.net_amount_aud as total_net_amount_aud,

    -- Customer Details
    c.customer_id,
    c.first_name as customer_first_name,
    c.last_name as customer_last_name,
    c.email as customer_email,
    c.city as customer_city,
    c.state_region as customer_state,
    c.country_code as customer_country,
    c.customer_segment,
    c.customer_age,

    -- Product Details
    p.product_id,
    p.name as product_name,
    p.category as product_category,
    p.subcategory as product_subcategory,
    
    -- Store Details
    st.store_id,
    st.name as store_name,
    st.region as store_region,
    st.channel as store_channel,
    st.state as store_state

    
    
from sales s
left join customers c on s.customer_sk = c.customer_sk
left join products p on s.product_scd_sk = p.product_scd_sk
left join stores st on s.store_sk = st.store_sk
left join dates d on s.date_sk = d.date