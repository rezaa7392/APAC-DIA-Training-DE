# Exercise 3: Bronze Ingestion Layer

## Objective
Build a robust ingestion pipeline that reads raw files, validates schemas, handles bad data, and writes to both Parquet and Delta Lake formats in the Bronze layer.

## Your Task
Complete the implementation of `/scripts/load_to_bronze.py` to ingest all data sources into the Bronze layer with proper validation and error handling.

## Requirements

### Core Functionality
1. **Command-line interface** with arguments:
   - `--raw`: Input directory with raw files (default: `data_raw`)
   - `--lake`: Output lake directory (default: `lake`)
   - `--manifest`: DuckDB database for tracking (default: `duckdb/warehouse.duckdb`)
   - `--dry-run`: Validate without writing (optional)

2. **Read and validate** all file formats:
   - CSV files using PyArrow CSV reader
   - Parquet files natively
   - XLSX files (consider pandas or openpyxl)
   - JSONL files line by line
   - Delta files for incremental updates

3. **Schema validation**:
   - Enforce schemas from `/schemas/schemas.py`
   - Separate valid and invalid records
   - Write rejects to `/lake/_rejects/<table>/` with reason codes

4. **Dual output formats**:
   - Parquet: `/lake/bronze/parquet/<table>/`
   - Delta: `/lake/bronze/delta/<table>/`
   - Both formats must contain identical data

5. **Add audit columns**:
   - `ingestion_ts`: Timestamp of ingestion (UTC)
   - `src_filename`: Source file name
   - `src_row_hash`: Hash of original row for lineage

### Advanced Requirements

**Idempotency:**
- Track processed files in DuckDB manifest table
- Support re-running without duplicating data
- Handle partial failures gracefully

**Partitioning Strategy:**
- Orders: Partition by `order_dt`
- Events: Partition by event date
- Sensors: Partition by `store_id` and month
- Other tables: No partitioning needed

**File Management:**
- Coalesce small files to ~100-250MB
- Maintain reasonable number of partitions
- Optimize for query performance

**Special Handling for Returns Table:**
- Implement UPSERT logic for Delta format
- Handle schema evolution (new columns)
- Support soft deletes with tombstone records

## Implementation Hints

**Schema Validation Approach:**
```python
# Example pattern (not complete solution)
try:
    table = csv.read_csv(file_path)
    table = table.cast(target_schema)  # Will fail if incompatible
except:
    # Handle validation errors
    # Write to rejects with reason
```

**Manifest Table Structure:**
```sql
CREATE TABLE IF NOT EXISTS manifest_processed_files (
    src_path TEXT PRIMARY KEY,
    processed_at TIMESTAMP,
    row_count BIGINT,
    reject_count BIGINT,
    status TEXT
)
```

**Delta Lake Operations:**
- Use `write_deltalake` with `mode='append'` for initial loads
- Use `merge` operations for upserts on returns table
- Enable `overwrite_schema=True` for schema evolution

## Testing Your Implementation

```bash
# Dry run first to validate
python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb --dry-run

# Full ingestion
python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb

# Verify outputs
find lake/bronze -name "*.parquet" | wc -l
find lake/_rejects -name "*.csv" | wc -l

# Check manifest in DuckDB
duckdb duckdb/warehouse.duckdb "SELECT * FROM manifest_processed_files LIMIT 5"
```

## Evaluation Criteria

Your ingestion pipeline will be evaluated on:
1. **Correctness**: All valid data is ingested with proper schemas
2. **Error Handling**: Invalid records are properly rejected with reasons
3. **Idempotency**: Re-running doesn't duplicate data
4. **Audit Trail**: Proper tracking of what was processed when
5. **Performance**: Efficient handling of large files
6. **Code Quality**: Modular, maintainable implementation

## Common Pitfalls to Avoid
- Don't load entire files into memory at once
- Remember to handle missing files gracefully
- Ensure timestamp columns maintain timezone info
- Don't forget to create partition directories
- Handle concurrent runs (file locking if needed)

## What NOT to Do
- Do not skip schema validation to "make it work"
- Do not silently drop invalid records
- Do not modify the source schemas to fit your data
- Do not write Bronze data without audit columns

