# Data Engineering Assessment - Post Mortem

## Architecture Decisions

### Why I Chose Option A Over DLT
When I looked at the assessment requirements, I decided to go with **Option A (Traditional Python Script)** instead of the DLT approach. Here's my thinking:

I wanted full control over the data validation process, especially knowing that CSV files can be messy in the real world. The assessment mentioned handling malformed records, and I figured a custom Python script would give me more flexibility to handle edge cases than DLT's more structured approach.

Looking at the dual output requirement (both Parquet and Delta), I thought it would be more straightforward to use PyArrow and deltalake libraries directly rather than trying to configure DLT for multiple outputs.

### Design Patterns I Implemented

**Schema-First Approach**: I decided early on to use the PyArrow schemas from `schemas/schemas.py` as the single source of truth. This way, any type mismatches would be caught immediately rather than causing issues downstream.

**Fail-Fast Philosophy**: Rather than trying to "fix" bad data, I chose to reject invalid records to separate tables with detailed reasons. This preserves data integrity while still giving visibility into what went wrong.

**Comprehensive Audit Trail**: I added three audit columns to every record:
- `ingestion_ts`: When the record was processed
- `src_filename`: Which source file it came from  
- `src_row_hash`: A hash for change detection and lineage

**Idempotency by Design**: The manifest table tracks processed files with MD5 checksums so the pipeline can be safely re-run without creating duplicates.

### How I Structured the Implementation

I ended up building separate handlers for each file format because they all had different quirks:
- `load_csv_generic()` for the CSV files (which turned out to need custom parsing)
- `load_parquet_single()` for the shipments data  
- `load_xlsx_single()` for the exchange rates Excel file
- `load_events()` for the JSONL event data

I implemented Hive-style partitioning logic with `_parse_hive_parts()` and `_append_partition_cols()` functions to handle the date-partitioned order data properly.

For storage, I made sure both `write_parquet_partitioned()` and `write_delta()` functions would write identical data.

## Challenges I Encountered

### CSV Parsing Reality Check
The biggest headache was definitely the CSV files. I initially thought I could just use pandas `read_csv()`, but that failed completely when I hit files with unquoted commas in text fields (like company names).

I had to build a comprehensive validation function `_validate_to_typed_and_rejects()` that:
- Tries to cast each field according to the PyArrow schema
- Separates type errors from null constraint violations
- Handles multiple date formats (because of course the data wasn't consistent)
- Normalizes boolean values from "Yes/No", "1/0", and "True/False" variations
- Strips whitespace that was breaking validations

### Memory Management Lessons
I learned quickly that I couldn't load entire directories of partitioned data into memory at once. The orders_lines table with 22 date partitions would have killed my laptop. So I implemented file-by-file processing throughout the pipeline.

### Learning dbt Along the Way
Coming into this assessment, I had some previous experience with dbt, so the learning curve wasn't too steep. I set up the standard medallion architecture:
- **Staging Layer**: 9 staging models in `models/staging/` to clean and standardize the bronze data
- **Silver Layer**: Business logic transformations in `models/silver/`
- **Gold Layer**: Final aggregations optimized for analytics in `models/gold/`
- **Testing**: Data quality constraints in the `tests/` directory

The dbt approach really grew on me - having dependency management and built-in testing made the transformations much more maintainable than raw SQL scripts would have been.

### What I Actually Delivered
By the end, I had a complete pipeline running:
- 11 bronze tables ingesting all the source data formats
- Clean staging models for each major entity (customers, orders, products, etc.)
- Silver layer with business transformations and data quality rules
- Gold layer aggregations ready for analytics
- A Power BI report connected to the final data warehouse

The whole thing processes data from CSV, Parquet, Excel, and JSONL sources into a queryable warehouse with both Parquet and Delta Lake storage options.

## What I Learned

### Technical Insights
**PyArrow is Powerful but Picky**: The schema enforcement was incredibly valuable for data quality, but getting the type conversions right took some trial and error. Empty strings vs null values, different date formats, boolean variations - all needed special handling.

**Delta Lake's Value Proposition**: Having transaction logs and time travel capabilities made debugging so much easier. When something went wrong, I could query previous versions of tables to understand what changed.

**dbt's Sweet Spot**: Once I got past the Jinja templating learning curve, dbt's dependency management and testing framework made the transformation layer much more professional than maintaining a collection of SQL files.

### Data Engineering Mindset Shifts
This assessment reinforced some key principles I'll carry forward:

**Design for Failure from Day One**: Rather than assuming data will be clean, I built validation and error handling into every step. The reject tables saved me countless debugging hours.

**Idempotency Isn't Optional**: Being able to safely re-run parts of the pipeline without causing duplicates or inconsistencies is essential for reliable operations.

**Observability Must Be Built In**: The audit columns and manifest tracking weren't just nice-to-haves - they were essential for understanding what the pipeline was actually doing.

## Production Recommendations

### What I'd Do Differently Next Time

**Environment Setup**: I'd definitely start with a more robust virtual environment and dependency management strategy. Getting PyArrow and Delta Lake versions to play nicely together took longer than it should have.

**Configuration Management**: All the hardcoded paths and parameters should really be in config files or environment variables for different deployment environments.

**Logging Strategy**: While the current implementation works, I'd add structured logging with correlation IDs to make troubleshooting easier in production.

**Error Handling**: Adding retry logic for transient failures would make the pipeline more robust in real-world scenarios.

### Monitoring I'd Add in Production

**Data Quality Monitoring**:
- Track reject rates over time and alert on unusual spikes  
- Monitor row count changes to catch upstream data issues
- Set up referential integrity checks across related tables

**Pipeline Health**:
- Processing time trends to catch performance degradation
- Disk space and memory usage monitoring
- Schema drift detection for incoming data sources

**Business Impact**:
- Data freshness SLAs for critical business processes
- Revenue data completeness tracking
- Customer data quality scores

## Tools Assessment

### My Experience with Each Technology

**Delta Lake**: I had some previous experience with Delta Lake, so I was already familiar with the transaction log concept and time travel capabilities. It was good to apply this knowledge in a comprehensive pipeline context and see how it integrates with different storage formats.

**dbt**: Building on my existing dbt experience, I was able to focus more on the business logic and testing rather than learning the basics. The dependency management and testing framework continue to prove their value compared to managing raw SQL files.

**DuckDB**: Incredibly fast for analytical workloads and the pandas integration made development smooth. The simple deployment story (just a file!) is perfect for this kind of local development.

**Power BI**: This played to my strengths since I have extensive experience with Power BI. The DuckDB connector worked well, and I was able to leverage my DAX knowledge to create effective visualizations and calculated measures quickly.

### Overall Assessment Thoughts

This assessment did a great job of covering the full data engineering stack - from messy raw data through to business intelligence. The combination of multiple file formats, schema validation requirements, and end-to-end pipeline delivery closely mirrors what I'd expect to encounter in a real data engineering role.

The most valuable part was seeing how decisions made in the bronze layer (partitioning strategy, schema enforcement, audit columns) impact everything downstream. It really reinforced the importance of thinking holistically about data architecture from the start.

**Preference for Real-World Scenarios**: While this local assessment was comprehensive, I'd personally prefer even more real-world scenarios using cloud infrastructure. For instance, leveraging our current Azure Visual Studio subscription to build scenarios with Fabric and Ingenious Framework using DBT would add authentic enterprise complexity - dealing with networking, security, cost optimization, and scalability constraints that you encounter in production environments. The local setup is great for learning concepts, but cloud scenarios would better simulate the challenges of actual data engineering work.

