{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_exchange_rates_parquet
),
-- Robust date parsing: dd/mm/YYYY and ISO fallback
norm as (
  select
    try_cast(date as date)                    as date,
    upper(trim(currency))                      as currency,
    try_cast(rate_to_aud as double)            as rate_to_aud,
    ingestion_ts, src_filename, src_row_hash
  from src
),
filtered as (
  select *
  from norm
  where date is not null
    and currency is not null and currency <> ''
)
select * from filtered
