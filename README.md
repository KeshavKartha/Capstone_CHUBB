# Population Census & Demographic Insights Analytics System

A simple, end-to-end demo of a census analytics pipeline built with **Databricks (Delta + Unity Catalog)**, **Airflow orchestration**, and **Power BI reporting**.

This repository contains the notebooks, DAG definitions, and supporting artifacts used to ingest synthetic census snapshots, validate and conform the data, produce analytics-ready Gold tables, and surface dashboards for stakeholders.


## What this project delivers

* **Reproducible Pipeline:** Reads raw census parts (`parquet`/`csv`) from a Unity Catalog volume and produces Bronze, Silver, and Gold Delta tables.
* **Data Quality Gates:** Validation checks that strictly gate progress through the pipeline.
* **Entity Resolution:** Deterministic person deduplication, SCD2-style person history, household aggregates, and lineage tracking.
* **Advanced Analytics:** Gold-level KPIs and statistical enrichments (income deciles, Gini coefficients, small-area shrinkage).
* **Orchestration:** Airflow DAGs to manage the full medallion flow and operational tasks.
* **Reporting:** Power BI report files (`.pbix`) that connect to Gold tables for stakeholder dashboards.

---

## Project Layout

### 1. Databricks Notebooks
*Place these notebooks into your workspace.*

* **`register_raw_files_to_registry`**
    Scans the Unity Catalog volume, reads `manifest.json`, computes checksums, performs quick-counts for CSVs, and writes to the `file_registry` Delta table. Produces a registration report JSON in the raw volume.
* **`bronze_ingest`**
    Picks a pending ingestion batch, marks files as *Processing*, reads raw files robustly (handling mixed encodings), coalesces schema variants, stores raw payload JSONs, and writes to `census.bronze.individuals_raw_v1`.
* **`bronze_validation`**
    Validates a Bronze partition (manifest vs. observed, age bounds, required columns). Appends to `validation_reports_v1` and exits with a structured JSON (`validated: true|false`) used for Airflow branching.
* **`silver_conform`**
    Performs region reconciliation, deterministic grouping & canonical record selection, SCD2 upsert into `dim_person`, household aggregation, and lineage tracking.
* **`silver_validation`**
    Runs checks against Silver tables (presence, uniqueness, lineage completeness) and exits with structured JSON.
* **`silver_optimize`**
    Runs Delta `OPTIMIZE`/compaction for Silver tables (defensive z-ordering).
* **`gold_materialize`** (aka `gold_materialize_extended`)
    Builds all Gold-level fact and dimension tables from conformed Silver data.
* **`gold_optimize`**
    (Optional) Compaction for Gold tables.
* **`reset_registry`**
    Helper utility to reset `ingestion_status` on file registry rows for manual retries.

### 2. Airflow DAGs
* **`census_full_medallion_pipeline_v2`**
    The main end-to-end DAG:
    1.  Preflight checks
    2.  Register files
    3.  Bronze Ingest → Bronze Validation
    4.  **Branch:** Re-ingest or Continue
    5.  Silver Conform → Silver Validation → Silver Optimize
    6.  Gold Materialize → Gold Optimize
    7.  Notify Success
* **`ingest_register_raw_files`**
    A utility DAG to run just the registration notebook (for ad-hoc file registration).

### 3. Raw Data & Manifest
*Example Unity Catalog volume structure:*

```text
/Volumes/census/raw/raw_files/
├── raw_part_01.parquet
├── raw_part_02_gender.drift.parquet
├── raw_part_03_admin.parquet
├── raw_part_04_income_int.parquet
├── region_lookup.parquet
├── raw_sample_mixed_encoding.csv
└── manifest.json  <-- Authoritative provenance (parts, seeds, row counts)
