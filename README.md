## Spark + Airflow ETL Pipeline
#### Overview

This project demonstrates a production-style ETL pipeline built using:

Apache Spark (PySpark) for scalable data transformation

Apache Airflow for orchestration, retries, and monitoring

Parquet as the optimized output format

The pipeline ingests raw CSV employee data, validates and cleans it, applies business rules, and writes partitioned Parquet output.

#### Architecture
CSV Files
   ->
Spark ETL (Validation + Transformation)
   ->
Parquet Output (Partitioned by load_date)
   ->
Airflow DAG (Scheduling & Monitoring)

#### ETL Logic
-- Extract

-- Reads multiple CSV files from input directory

-- Uses an explicit schema to avoid inference issues

-- Transform

-- Casts data types (age, salary, active)

-- Handles invalid values (NaN, nulls, negative salary)

-- Applies row-level validation rules:

-- name must not be empty

-- age >= 18

-- salary >= 0

-- Calculates bad-row ratio

-- Pipeline fails if invalid rows exceed 25%

-- Load

-- Writes valid rows as partitioned Parquet

-- Writes invalid rows to bad_records/

-- Adds load_date partition column

#### Key Production Concepts Implemented

- Schema enforcement

- Row-level validation

- Threshold-based data quality checks

- Spark caching (persist / unpersist)

- Partitioned Parquet output

- Airflow retries & failure alerts

#### Example Input
name,age,salary,active
Liam,34,72000.50,True
Oliver,15,48000.00,False

#### How to Run Spark Job Manually
spark-submit spark_etl.py input/ processed/

#### How to Run via Airflow

Place DAG file in:

airflow/dags/


Start Airflow:

airflow standalone


Trigger DAG from UI

#### Output

Valid data → processed/load_date=YYYY-MM-DD/

Invalid data → processed/bad_records/

### Why This Project Matters

This pipeline reflects real-world data engineering patterns used in production systems:

Spark for scale

Airflow for orchestration

Strong data quality enforcement

### Author
#### Ayush
(Data Engineering Practice Project)
