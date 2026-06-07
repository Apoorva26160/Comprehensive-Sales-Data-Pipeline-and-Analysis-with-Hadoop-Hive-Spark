# Comprehensive Sales Data Pipeline — Hadoop, Hive & Spark

End-to-end big data pipeline for sales analytics using **PySpark** for transformation and **Hive SQL** for querying, designed to run on a Hadoop cluster or locally.

## Architecture

```
Raw Sales CSVs (HDFS / local)
    → PySpark Extract + Schema Validation
    → Transform (revenue, profit, discount tiers, running totals)
    → Parquet Output (partitioned by year/region)
    → Hive External Table
    → Analytical SQL Queries
```

## Pipeline Stages

| Stage | Tool | Output |
|---|---|---|
| Ingest | PySpark | Validated DataFrame |
| Transform | PySpark (Window functions) | Enriched sales data |
| Aggregate | PySpark | Region/category/product summaries |
| Store | Parquet (HDFS-compatible) | Partitioned by year + region |
| Query | Hive SQL | Business insights |

## Hive Analyses

- Monthly revenue trend with profit margins
- Regional performance comparison
- Category & sub-category breakdown
- Discount impact on revenue and profit
- Top 10 customers by lifetime value
- Year-over-year growth by region

## Usage

```bash
pip install -r requirements.txt

# Run PySpark pipeline (add your CSV path)
python pipeline.py

# Connect to Hive and run analytical queries
hive -f hive_queries.sql
```

## Project Structure

```
pipeline.py        # PySpark ETL: Extract → Transform → Load (Parquet)
hive_queries.sql   # Hive SQL: 6 analytical queries over the pipeline output
requirements.txt
```
