# Mid-Level Data Engineer Assessment (Local Only)

## Overview
This is a hands-on data engineering assessment where you will build a complete data pipeline for a fictional retail company using local tools only. You will demonstrate your ability to generate synthetic data, implement medallion architecture, perform data transformations, and create business intelligence reports.

## Assessment Structure

This bundle contains:
- `/docs` — Step-by-step exercise instructions (start with `00_overview.md`)
- `/schemas` — PyArrow schema definitions for raw tables (do not modify)
- `/scripts` — Starter templates for your implementation
- `/dbt` — dbt-duckdb project skeleton to extend
- `/lake` — Output directory for Bronze/Silver/Gold layers (create if missing)
- `/duckdb` — Database file location (create if missing)
- `/analytics` — Final Power BI report location (create if missing)

## Prerequisites
- Python 3.10+ installed
- Power BI Desktop (for final visualization task)
- 8GB+ available RAM
- ~5GB disk space for generated data

## Getting Started

1. **Setup Environment**
```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create required directories
mkdir -p lake duckdb analytics data_raw
mkdir -p lake/bronze/parquet lake/bronze/delta lake/_rejects
```

2. **Follow the Documentation**
Start with `/docs/00_overview.md` and proceed through each numbered document in order. Each document covers one part of the assessment:
- `01_raw_schemas.md` - Data specifications
- `02_data_generation.md` - Implement data generator
- `03_bronze_ingestion.md` - Build Bronze layer ingestion
- `04_dbt_silver.md` - Create Silver transformations
- `05_gold_model.md` - Design dimensional model
- `06_powerbi.md` - Build BI dashboard
- `07_reliability_ops.md` - Operational testing

## What You Will Build

You must implement:
1. **Data Generator** (`scripts/generate_data.py`) - Create realistic synthetic data with controlled anomalies
2. **Bronze Ingestion** (`scripts/load_to_bronze.py`) - Validate and load raw data to lake storage
3. **dbt Models** - Transform data through Silver and Gold layers
4. **Power BI Report** - Interactive business intelligence dashboard
5. **Documentation** - Brief post-mortem explaining your approach and decisions

## Evaluation Criteria

Your submission will be evaluated on:
- **Correctness**: Does the pipeline run end-to-end without errors?
- **Data Quality**: Are schemas enforced? Are anomalies handled appropriately?
- **Architecture**: Is the medallion architecture properly implemented?
- **Performance**: Can the solution handle the target data volumes?
- **Code Quality**: Is the code readable, maintainable, and well-structured?
- **Documentation**: Are your design decisions clearly explained?

## Important Notes

- **Do NOT modify** files in `/schemas/` - these are the canonical schema definitions
- **Do NOT hardcode solutions** - Your code should be parameterizable and reusable
- **Do document** any assumptions or design decisions in your post-mortem
- **Do test** at small scale first (use `--scale` parameters) before full runs
- The provided starter code is minimal - you must extend it significantly

## Submission Requirements

Your final submission should include:
1. All implemented code in `/scripts/` and `/dbt/`
2. Generated sample data in `/data_raw/` (small subset for review)
3. Power BI report in `/analytics/report.pbix`
4. Post-mortem document explaining your approach, challenges, and trade-offs
5. Any additional scripts or utilities you created

## Time Expectation

This assessment is designed to take 6-10 hours for a mid-level data engineer. Budget your time across:
- 2-3 hours: Data generation and Bronze ingestion
- 2-3 hours: dbt Silver/Gold transformations
- 1-2 hours: Power BI dashboard
- 1-2 hours: Testing, documentation, and polish

Good luck!
