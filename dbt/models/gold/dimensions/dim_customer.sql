{{ 
  config(
    materialized='table',
    schema='gold',
    contract={'enforced': true},
    tags=['gold','dimension']
  ) 
}}

-- Source (Silver staging)
with s as (
  select * from {{ ref('stg_customers') }}
),

-- Apply GDPR masking rules per 05_gold_model.md:
-- - If gdpr_consent = false:
--     * email -> hash
--     * phone -> masked (keep last 2 chars)
--     * address generalized to city level: null street lines & postcode, keep city/state/country
masked as (
  select
    s.customer_id,
    s.natural_key,
    s.first_name,
    s.last_name,

    case 
      when s.gdpr_consent = false then md5(coalesce(s.email, ''))
      else s.email
    end as email,

    case 
      when s.gdpr_consent = false then
        case 
          when s.phone is null then null
          else repeat('X', greatest(length(s.phone) - 2, 0)) || right(s.phone, 2)
        end
      else s.phone
    end as phone,

    -- Generalize address to city level when consent is false
    case when s.gdpr_consent = false then null else s.address_line1 end as address_line1,
    case when s.gdpr_consent = false then null else s.address_line2 end as address_line2,
    s.city,
    s.state_region,
    case when s.gdpr_consent = false then null else s.postcode end as postcode,
    s.country_code,

    s.birth_date,
    s.join_ts,

    -- Derived attributes
    cast(date_diff('day', cast(s.join_ts as date), current_date) as int) as customer_lifetime_days,
    cast(date_diff('year', s.birth_date, current_date) as int)         as customer_age,
    case
      when s.is_vip then 'VIP'
      when date_diff('day', cast(s.join_ts as date), current_date) < 90 then 'New'
      else 'Standard'
    end as customer_segment,

    s.is_vip,
    s.gdpr_consent
  from s
)

select
  {{ surrogate_key(['customer_id']) }} as customer_sk,
  customer_id,
  natural_key,
  first_name,
  last_name,
  email,
  phone,
  address_line1,
  address_line2,
  city,
  state_region,
  postcode,
  country_code,
  birth_date,
  join_ts,
  customer_lifetime_days,
  customer_age,
  customer_segment,
  is_vip,
  gdpr_consent
from masked
