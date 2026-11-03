# BLS Economic Lakehouse

This project packages a small medallion architecture example on Databricks using BLS productivity data and U.S. population data. It includes raw ingestion code, a Delta Live Tables (DLT) Pipeline, and an AI/BI dashboard definition so the project can be deployed with Databricks Asset Bundles (DABs).

> **Note:** Screenshots of the completed DLT pipeline DAG, AI/BI dashboard, and Gold layer query results are available in the `Screenshots/` directory.

## Included assets

- `src/ingestion/bls_client.py` - downloads BLS productivity files and the U.S. population JSON payload into a Unity Catalog volume.
- `src/pipeline/dlt_pipeline.py` - builds bronze, silver, and gold pipeline tables in Unity Catalog.
- `src/pipeline/sql_alternatives.sql` - reference SQL queries equivalent to the gold-layer logic.
- `databricks.yml` - Databricks Asset Bundle (DABs) definition for the pipeline and dashboard.
- `01_run_ingestion` - notebook used to land the raw source files.

## 🚀 Key Engineering Features

* **Compliant & Idempotent Ingestion:** Appends custom `User-Agent` contact headers to bypass BLS `403 Forbidden` restrictions. Utilizes dynamic regex scraping to handle inconsistent HTML directory paths.
* **Data Quality & Schema Resilience:** 
  * **Header Normalization:** Dynamically trims invisible whitespace from tab-padded BLS headers (e.g., `series_id    \t`).
  * **Identifier Integrity:** Explicitly casts dimensional codes as strings to prevent Spark's schema inference from truncating leading zeros.
  * **Expectations:** Enforces strict pipeline data quality via `@dlt.expect_or_drop` on primary keys.
* **Deterministic Aggregations:** Calculates the best year per series using window functions (`ROW_NUMBER()`) with multi-column sorting to ensure deterministic tie-breaking.

## Data targets

The default bundle variables assume these Unity Catalog targets:

- Catalog: `macroeconomic_data`
- Schema: `raw_data`
- Raw landing volume: `/Volumes/macroeconomic_data/raw_data/bls_raw_landing`

The pipeline publishes these gold tables:

- `macroeconomic_data.raw_data.gold_q1_population_stats`
- `macroeconomic_data.raw_data.gold_q2_best_year_per_series`
- `macroeconomic_data.raw_data.gold_q3_prs30006032_q01_population`

## Prerequisites

You need:

- a Databricks workspace with Unity Catalog enabled
- permission to read and write in catalog `macroeconomic_data` and schema `raw_data`
- permission to read and write `/Volumes/macroeconomic_data/raw_data/bls_raw_landing`
- permission to create and run Delta Live Tables (DLT) pipelines
- permission to create AI/BI dashboards
- Databricks CLI version 0.283.0 or later for recent dashboard bundle features

## Deployment flow

1. Run the ingestion notebook so the raw files exist in the landing volume.
2. Validate the bundle.
3. Deploy the bundle to a target workspace.
4. Run the created pipeline.
5. Open the deployed dashboard and publish it when ready for stakeholders.

## Bundle commands

Run these commands from the repository root after authenticating the Databricks CLI to your workspace.

```sh
databricks bundle validate
databricks bundle deploy -t dev
