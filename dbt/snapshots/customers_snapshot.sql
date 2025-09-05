{% snapshot customers_snapshot %}
  {{
    config(
      target_schema='snapshot',
      unique_key='customer_id',
      strategy='check',
      check_cols=[
        'customer_id',
        'natural_key',
        'first_name',
        'last_name',
        'email',
        'phone',
        'address_line1',
        'address_line2',
        'city',
        'state_region',
        'postcode',
        'country_code',
        'latitude',
        'longitude',
        'birth_date',
        'join_ts',
        'is_vip',
        'gdpr_consent',
      ]
    )
  }}

  select *
  from {{ ref('stg_customers') }}

{% endsnapshot %}
