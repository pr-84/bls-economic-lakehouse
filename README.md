# BLS Economic Lakehouse

This project packages a small medallion architecture example on Databricks using BLS productivity data and U.S. population data. It includes raw ingestion code, a Lakeflow Spark Declarative Pipeline, and an AI/BI dashboard definition so the project can be deployed with Declarative Automation Bundles.

## Included assets

* `src/ingestion/bls_client.py` - downloads BLS productivity files and the U.S. population JSON payload into a Unity Catalog volume.
* `src/pipeline/dlt_pipeline.py` - builds bronze, silver, and gold pipeline tables in Unity Catalog.
* `src/pipeline/sql_alternatives.sql` - reference SQL queries equivalent to the gold-layer logic.
* `databricks.yml` - Declarative Automation Bundle definition for the pipeline and dashboard.
* `01_run_ingestion` - notebook used to land the raw source files.

## Data targets

The default bundle variables assume these Unity Catalog targets:

* Catalog: `macroeconomic_data`
* Schema: `raw_data`
* Raw landing volume: `/Volumes/macroeconomic_data/raw_data/bls_raw_landing`

The pipeline publishes these gold tables:

* `macroeconomic_data.raw_data.gold_q1_population_stats`
* `macroeconomic_data.raw_data.gold_q2_best_year_per_series`
* `macroeconomic_data.raw_data.gold_q3_prs30006032_q01_population`

## Prerequisites

You need:

* a Databricks workspace with Unity Catalog enabled
* permission to read and write in catalog `macroeconomic_data` and schema `raw_data`
* permission to read and write `/Volumes/macroeconomic_data/raw_data/bls_raw_landing`
* permission to create and run Lakeflow Spark Declarative Pipelines
* permission to create AI/BI dashboards
* Databricks CLI version 0.283.0 or later for recent dashboard bundle features

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
```

After deploy, run the pipeline from the Databricks UI or use the Databricks CLI pipeline commands if desired.

## Notes for other users

* GitHub access to this repository does not automatically grant access to the Databricks workspace, Unity Catalog tables, or the UC volume.
* If you want to deploy to a different catalog or schema, update bundle variables or add additional targets in `databricks.yml`.
* The dashboard definition is embedded in `databricks.yml`, so if you change the dashboard in the UI later, re-export or regenerate the bundled dashboard content before redeploying.
