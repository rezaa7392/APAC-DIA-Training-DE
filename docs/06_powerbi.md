# Exercise 6: Power BI Dashboard

## Objective
Create an interactive business intelligence dashboard in Power BI that demonstrates your ability to visualize data and deliver business insights.

## Your Task
Build a Power BI report (`/analytics/report.pbix`) connecting to your Gold layer tables through DuckDB.

## Requirements

### 1. Data Connection & Modeling

**Connection Setup:**
- Use DuckDB ODBC driver to connect to `duckdb/warehouse.duckdb`
- Import Gold layer tables (dimensions and facts)
- Configure refresh settings appropriately

**Data Model:**
- Recreate star schema relationships in Power BI
- Set proper cardinality and cross-filter direction
- Mark date table for time intelligence
- Hide technical columns from report view

### 2. DAX Measures

Create a measures table with business KPIs:

**Sales Metrics:**
```dax
Total Sales = SUM(fct_sales[net_amount])
Total Quantity = SUM(fct_sales[quantity])
Average Order Value = DIVIDE([Total Sales], DISTINCTCOUNT(fct_sales[order_id]))
```

**Growth Metrics:**
```dax
Sales YTD = TOTALYTD([Total Sales], dim_date[date])
Sales MTD = TOTALMTD([Total Sales], dim_date[date])
YoY Growth % = 
    VAR CurrentYear = [Total Sales]
    VAR PreviousYear = CALCULATE([Total Sales], SAMEPERIODLASTYEAR(dim_date[date]))
    RETURN DIVIDE(CurrentYear - PreviousYear, PreviousYear)
```

**Operational Metrics:**
```dax
Return Rate = DIVIDE(SUM(fct_returns[quantity]), SUM(fct_sales[quantity]))
Avg Delivery Days = AVERAGE(fct_shipments[delivery_days])
On-Time Delivery % = DIVIDE(
    COUNTROWS(FILTER(fct_shipments, fct_shipments[on_time_flag] = TRUE)),
    COUNTROWS(fct_shipments)
)
```

**Data Quality Metrics:**
```dax
Data Freshness Hours = 
    DATEDIFF(
        MAX(fct_ingestion_audit[processed_at]),
        NOW(),
        HOUR
    )
Rejection Rate = 
    DIVIDE(
        SUM(fct_ingestion_audit[rows_rejected]),
        SUM(fct_ingestion_audit[rows_processed])
    )
```

### 3. Report Pages

**Page 1: Executive Dashboard**
- KPI cards: Total Sales, Orders, Customers, YoY Growth
- Sales trend line chart (with forecast)
- Top 10 products by revenue (bar chart)
- Geographic sales heatmap
- Sales by category (donut chart)
- Include slicers: Date range, Region, Category

**Page 2: Customer Analytics**
- Customer segmentation matrix
- Customer lifetime value distribution
- New vs returning customer trends
- VIP customer contribution analysis
- Customer acquisition funnel
- Churn analysis indicators

**Page 3: Product Performance**
- Product performance scatter plot (Revenue vs Margin)
- Category hierarchy drill-through
- Price change impact analysis
- Inventory turnover metrics
- Product lifecycle stage indicators
- ABC analysis visualization

**Page 4: Operations Monitor**
- Delivery performance gauges
- Shipping cost trends
- Carrier performance comparison
- Sensor anomaly detection alerts
- Store performance heatmap
- Supply chain lead time analysis

**Page 5: Data Quality Dashboard**
- Pipeline health status
- Rejection rates by table (over time)
- Data freshness indicators
- Schema drift detection
- Missing data heatmap
- Processing time trends

### 4. Advanced Features

**Implement at least 3 of the following:**
- Drill-through pages for detailed analysis
- Bookmarks for different user personas
- What-if parameters for scenario analysis
- Custom tooltips with additional context
- Row-level security (RLS) for store managers
- Mobile-optimized layout
- R/Python visuals for advanced analytics

## Design Guidelines

### Visual Best Practices
1. **Consistent Color Scheme**: Use company colors or a professional palette
2. **Clear Hierarchy**: Most important metrics at top/left
3. **Appropriate Chart Types**: Choose visualizations that match the data
4. **Minimal Clutter**: Remove unnecessary gridlines, borders
5. **Responsive Layout**: Test on different screen sizes

### Performance Optimization
- Use aggregation tables where possible
- Limit visual-level filters
- Optimize DAX calculations
- Consider DirectQuery vs Import mode trade-offs
- Implement incremental refresh if needed

### User Experience
- Add clear titles and subtitles
- Include data refresh timestamp
- Provide filter reset buttons
- Add navigation buttons between pages
- Include help tooltips for complex metrics

## Testing Your Report

1. **Data Accuracy**:
   - Cross-check totals with SQL queries
   - Verify calculations match business rules
   - Test filter combinations

2. **Performance**:
   - Page load times < 3 seconds
   - Smooth interactivity
   - Efficient refresh times

3. **Usability**:
   - Test with sample user scenarios
   - Verify all interactions work
   - Check mobile view rendering

## Evaluation Criteria

1. **Completeness**: All required pages and metrics implemented
2. **Accuracy**: Calculations are correct and match source data
3. **Design**: Professional, clean, and intuitive layout
4. **Performance**: Fast loading and responsive interactions
5. **Insights**: Actionable business intelligence delivered
6. **Innovation**: Creative use of Power BI features

## Common Pitfalls to Avoid
- Don't overcrowd visuals with too much information
- Avoid using 3D charts or excessive animations
- Don't forget to format numbers appropriately
- Remember to test cross-filtering behavior
- Don't ignore performance implications of complex DAX

## What NOT to Do
- Do not hardcode values in measures
- Do not create circular dependencies in calculations
- Do not use pie charts for >5 categories
- Do not forget to document your measures
