# Exercise 5: Gold Dimensional Model

## Objective
Design and implement a star schema in the Gold layer optimized for business intelligence and analytics.

## Your Task
Create dimension and fact tables in `/dbt/models/gold/` following dimensional modeling best practices.

## Requirements

### 1. Dimension Tables

**dim_customer**
- Include all customer attributes
- Mask PII when `gdpr_consent = false`:
  - Replace email with hash
  - Mask phone numbers
  - Generalize addresses to city level
- Add derived attributes:
  - customer_age
  - customer_lifetime_days
  - customer_segment (based on VIP status and join date)

**dim_product_scd**
- Source from products snapshot (SCD Type 2)
- Include current and historical records
- Add effective date ranges
- Include is_current flag
- Calculate price change indicators

**dim_store**
- All store attributes
- Add store_age_days
- Calculate store_size_category
- Include operational status

**dim_supplier**
- Supplier master data
- Add supplier_tier based on lead time
- Include country region mapping

**dim_date**
- Generate comprehensive date dimension
- Include:
  - Fiscal periods
  - Holidays (AU/US/UK)
  - Weekend indicators
  - Quarter boundaries
  - Week numbers (ISO)

### 2. Fact Tables

**fct_sales**
- Grain: One row per order line
- Measures:
  - quantity
  - unit_price
  - line_total
  - discount_amount
  - tax_amount
  - net_amount
- Foreign keys to all relevant dimensions
- Include degenerate dimensions (order_number)

**fct_shipments**
- Grain: One row per shipment
- Measures:
  - shipping_cost
  - delivery_days
  - on_time_flag
- Calculate SLA compliance

**fct_sensor_readings**
- Grain: One row per sensor reading
- Pre-aggregate to hourly averages
- Include anomaly flags for out-of-range values
- Calculate rolling statistics

**fct_ingestion_audit**
- Track pipeline health metrics
- Include:
  - rows_processed
  - rows_rejected
  - processing_time_seconds
  - file_size_bytes

### 3. Business Views

Create user-friendly views that:
- Hide technical columns (surrogate keys, audit fields)
- Use business-friendly naming
- Pre-join commonly used dimensions
- Include calculated KPIs

## Implementation Guidelines

### Star Schema Design Principles
1. **Conformed Dimensions**: Reuse dimensions across facts
2. **Surrogate Keys**: Use for all dimension joins
3. **Grain Statement**: Document clearly for each fact
4. **Additive Measures**: Ensure facts are additive
5. **Sparse Dimensions**: Handle optional relationships

### Sample Model Structure
```sql
-- models/gold/dimensions/dim_customer.sql
WITH customers_cleaned AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
customers_masked AS (
    SELECT
        customer_id,
        CASE 
            WHEN gdpr_consent = false THEN 'MASKED'
            ELSE email
        END AS email,
        -- more masking logic
    FROM customers_cleaned
)
SELECT 
    {{ dbt_utils.surrogate_key(['customer_id']) }} AS customer_sk,
    * 
FROM customers_masked
```

### Performance Optimization
- Create appropriate indexes
- Consider materialized views for complex aggregations
- Partition large facts by date
- Use columnar storage advantages

## Testing Requirements

### Data Quality Tests
```yaml
models:
  - name: fct_sales
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - line_number
    columns:
      - name: customer_sk
        tests:
          - relationships:
              to: ref('dim_customer')
              field: customer_sk
```

### Performance Benchmarks
Document query performance for:
1. Total sales by category last 30 days
2. Customer lifetime value calculation
3. Year-over-year growth by store
4. Product performance with returns rate
5. Sensor anomaly detection rate by store

## Evaluation Criteria

1. **Dimensional Design**: Proper star schema implementation
2. **Data Privacy**: GDPR compliance for PII
3. **Query Performance**: Optimized for BI workloads
4. **Completeness**: All required dimensions and facts
5. **Documentation**: Clear grain statements and relationships
6. **Usability**: Business-friendly naming and structure

## Common Pitfalls to Avoid
- Don't create snowflake schemas unnecessarily
- Avoid many-to-many relationships in facts
- Don't forget to handle slowly changing dimensions
- Remember to create a complete date dimension
- Don't expose technical fields to business users

## What NOT to Do
- Do not denormalize everything into one wide table
- Do not use natural keys for dimension joins
- Do not create facts without clear grain
- Do not forget about data privacy requirements
