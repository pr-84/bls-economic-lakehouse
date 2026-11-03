# Thought Process & Methodology

## Architecture & Design Choices
I designed a Medallion Lakehouse architecture using Databricks Unity Catalog and Delta Live Tables (DLT) to ensure a robust, governable, and scalable pipeline.

* **Ingestion:** I built a custom Python OOP client (`BLSDataIngestor`) to handle BLS's strict crawling policies. By passing a custom `User-Agent` header containing contact details, the pipeline bypasses HTTP 403 Forbidden restrictions. The script uses a regex fallback to dynamically parse directory links (handling both relative and absolute paths) and lands the raw data idempotently into a Unity Catalog Volume.
* **Medallion Pipeline (DLT):** 
  * **Bronze:** Ingests the raw payloads and dynamically normalizes tab-padded BLS column headers (e.g., converting `series_id    ` to `series_id`).
  * **Silver:** Enforces data quality via `@dlt.expect_or_drop`. I explicitly cast dimensional codes as strings to prevent Spark's schema inference from stripping leading zeros. I also denormalized the BLS relational mapping (`pr.class`, `pr.measure`, etc.) into a comprehensive human-readable `series_title`.
  * **Gold:** Computes final analytical aggregations.
* **Language Choice:** I utilized PySpark as the primary engine for the DLT pipeline to leverage programmatic capabilities like dynamic header normalization and strict schema enforcement. I provided equivalent Spark SQL queries for the Gold layer to demonstrate fluency across both APIs, as SQL is often the standard for downstream analytics and BI.

## Trade-offs & Production Considerations
If delivering this for a real enterprise client, I would adapt the following for production scale:
* **Data Volume & Orchestration:** The current ingestion script uses standard Python `requests`. For massive scale, I would utilize Databricks Auto Loader (`cloudFiles`) to incrementally stream new files as they land in cloud storage.
* **Schema Drift:** I would enable Auto Loader schema evolution (`cloudFiles.schemaLocation`) rather than defining schemas statically, ensuring downstream pipelines don't break if BLS adds or modifies columns.
* **Cost & Compute:** The pipeline currently runs in `Triggered` mode to optimize costs. For strict SLAs, I would utilize Serverless DLT pipelines to eliminate cluster spin-up time.
* **Access Control:** I applied basic Unity Catalog `GRANT SELECT` statements at the schema level for the Gold tables. In production, I would implement dynamic views with Row-Level Security (RLS) to restrict sensitive macroeconomic data by region or department.

## AI Usage
I integrated AI heavily as a collaborative engineering partner throughout this workflow. My approach was to first create the GitHub repository, establish the directory structure, and scaffold the necessary files (`bls_client.py`, `dlt_pipeline.py`, `sql_alternatives.sql`) with basic boilerplate code. 

Once the repository was synced to my Databricks workspace, I utilized **Databricks Genie**
