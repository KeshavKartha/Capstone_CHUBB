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
* **`gold_validation`**
    Validates gold tables.
* **`reset_registry`**
    Helper utility to reset `ingestion_status` to Pending on file registry rows for manual retries.

### 2. Airflow DAGs
* **`census_full_pipeline`**
    The main end-to-end DAG:
    1.  Preflight checks
    2.  Register files
    3.  Bronze Ingest → Bronze Validation
    4.  **Branch:** Re-ingest or Continue
    5.  Silver Conform → Silver Validation → Silver Optimize
    6. **Branch:** Retry Silver Conform or continue
    7.  Gold Materialize → Gold Validation
    8. **Branch:** Retry Gold Materialize or continue
    9.  Notify Success 
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
└── manifest.json  
```

## Delta Tables Created

### Bronze Layer
* `census.bronze.file_registry_v1` — File manifest, checksums, and ingestion state.
* `census.bronze.individuals_raw_v1` — Raw ingested rows normalized with ingestion metadata.
* `census.bronze.ingestion_audit_v1` — Ingestion run logs.
* `census.bronze.validation_reports_v1` — Validation logs.

### Silver Layer
* `census.silver.dim_region` — Canonical region lookup.
* `census.silver.dim_person` — Conformed person SCD2 table (includes `is_current` flag).
* `census.silver.dim_person_history` — Appended history rows.
* `census.silver.dim_household` — Household aggregates.
* `census.silver.lineage` — Mapping from Silver entities back to Bronze rows.
* `census.silver.validation_reports_v1` — Silver validation logs.

### Gold Layer
* `census.gold.dim_age_group`
* `census.gold.metric_definitions`
* `census.gold.fact_population_by_region_year`
* `census.gold.indicators_literacy_employment`
* `census.gold.fact_household_summary`
* `census.gold.income_distribution_by_region_year`
* `census.gold.small_area_shrinkage_estimates`
* `census.gold.fact_population_flat_region_year`
* `census.gold.education_distribution_by_region_year`
* `census.gold.education_employment_crosswalk`
* `census.gold.ingestion_audit_v1`

---

## Quick Start

### Prerequisites

1.  **Databricks Workspace** with Unity Catalog enabled.
2.  **Volume Mounted** at `/Volumes/census/raw/raw_files/`.
3.  **Raw Files** and `manifest.json` placed in the volume root.
4.  **Airflow (2.x)** with a `databricks_default` connection (Host + Token) and access to an existing Databricks cluster ID.
5.  **Power BI Desktop** (optional) to view reports.

### Option A: Run via Airflow (Production Mode)

1.  Confirm raw files are present in the volume.
2.  Trigger **`census_full_medallion_pipeline_v2`** from the Airflow UI.
3.  Watch task logs in Airflow. The DAG uses structured JSON pushed by Databricks tasks to determine branching logic.
4.  Upon success, open Power BI and connect to `census.gold` tables.

### Option B: Run Manually (Developer Mode)

1.  **Register:** Run notebook `register_raw_files_to_registry`.
2.  **Ingest:** Run `bronze_ingest` (use widget `ingestion_batch_id` or allow auto-select).
3.  **Validate:** Run `bronze_validation`. Check the output JSON for `validated: true`.
4.  **Process:** If validated, run `silver_conform` → `silver_validation` → `gold_materialize`.

---

## How Validation & Retries Work

This pipeline avoids ambiguous states by making retry logic explicit:

1.  **Structured Output:** Each validation notebook (Bronze/Silver/Gold) runs checks and exits with a small JSON object (via `dbutils.notebook.exit`). This object contains a boolean `validated` flag and the report details.
2.  **Airflow Decision:** The DAG inspects this JSON.
    * If `validated == true`: The pipeline proceeds to the next layer.
    * If `validated == false`: The DAG branches to run the retry node exactly 2 times at most.

This ensures that bad data is caught at each step of the way.
