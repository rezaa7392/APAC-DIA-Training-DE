{{ config(materialized='view') }}

-- External views pointing at Bronze Parquet/Delta. Adjust path if needed.
{% set lake_root = var('lake_root', 'C:/Users/rahmadi/APAC-DIA-Training-DE/lake/bronze') %}

{% if execute %}
  {% do run_query("create schema if not exists bronze") %}

  -- Parquet views
  {% do run_query("create or replace view bronze_customers_parquet       as select * from read_parquet('" ~ lake_root ~ "/parquet/customers/*.parquet')") %}
  {% do run_query("create or replace view bronze_products_parquet        as select * from read_parquet('" ~ lake_root ~ "/parquet/products/*.parquet')") %}
  {% do run_query("create or replace view bronze_suppliers_parquet       as select * from read_parquet('" ~ lake_root ~ "/parquet/suppliers/*.parquet')") %}
  {% do run_query("create or replace view bronze_stores_parquet          as select * from read_parquet('" ~ lake_root ~ "/parquet/stores/*.parquet')") %}
  {% do run_query("create or replace view bronze_orders_header_parquet   as select * from read_parquet('" ~ lake_root ~ "/parquet/orders_header/**/*.parquet')") %}
  {% do run_query("create or replace view bronze_orders_lines_parquet    as select * from read_parquet('" ~ lake_root ~ "/parquet/orders_lines/**/*.parquet')") %}
  {% do run_query("create or replace view bronze_shipments_parquet       as select * from read_parquet('" ~ lake_root ~ "/parquet/shipments/*.parquet')") %}
  {% do run_query("create or replace view bronze_exchange_rates_parquet  as select * from read_parquet('" ~ lake_root ~ "/parquet/exchange_rates/*.parquet')") %}
  {% do run_query("create or replace view bronze_events_parquet          as select * from read_parquet('" ~ lake_root ~ "/parquet/events/**/*.parquet')") %}
  {% do run_query("create or replace view bronze_sensors_parquet         as select * from read_parquet('" ~ lake_root ~ "/parquet/sensors/**/*.parquet')") %}

  {# Delta #}
  {# 
  {% do run_query("install delta") %}
  {% do run_query("load delta") %}
  {% do run_query("create or replace view bronze_customers_delta       as select * from delta_scan('" ~ lake_root ~ "/delta/customers')") %}
  {% do run_query("create or replace view bronze_products_delta        as select * from delta_scan('" ~ lake_root ~ "/delta/products')") %}
  {% do run_query("create or replace view bronze_suppliers_delta       as select * from delta_scan('" ~ lake_root ~ "/delta/suppliers')") %}
  {% do run_query("create or replace view bronze_stores_delta          as select * from delta_scan('" ~ lake_root ~ "/delta/stores')") %}
  {% do run_query("create or replace view bronze_orders_header_delta   as select * from delta_scan('" ~ lake_root ~ "/delta/orders_header')") %}
  {% do run_query("create or replace view bronze_orders_lines_delta    as select * from delta_scan('" ~ lake_root ~ "/delta/orders_lines')") %}
  {% do run_query("create or replace view bronze_shipments_delta       as select * from delta_scan('" ~ lake_root ~ "/delta/shipments')") %}
  {% do run_query("create or replace view bronze_exchange_rates_delta  as select * from delta_scan('" ~ lake_root ~ "/delta/exchange_rates')") %}
  {% do run_query("create or replace view bronze_events_delta          as select * from delta_scan('" ~ lake_root ~ "/delta/events')") %}
  {% do run_query("create or replace view bronze_sensors_delta         as select * from delta_scan('" ~ lake_root ~ "/delta/sensors')") %}
  #}
{% endif %}

-- The model itself returns a trivial select; the real work happens in run_query() above.
select 1 as noop
