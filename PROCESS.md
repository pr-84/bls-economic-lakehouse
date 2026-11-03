# Thought Process & Methodology

## Architecture
I designed a Medallion Lakehouse architecture using Databricks Unity Catalog and Delta Live Tables (DLT). 
* **Ingestion:** I built a custom Python OOP client (`BLSDataIngestor`) to handle BLS's strict crawling policies (custom User-Agent headers) and deployed it as a Databricks task. It lands raw data idempotently into a Unity Catalog Volume.
* **Medallion Pipeline (DLT):** 
  * **Bronze:** Ingests raw data and dynamically normalizes tab-padded BLS column headers (e.g., `series_id    `).
  * **Silver:** Enforces data quality via `@dlt.expect_or_drop`, preserves leading zeros by typing code columns as strings, and denormalizes the BLS relational mapping (`pr.class`, `pr.measure`, etc.) into a human-readable `series_title`.
  * **Gold:** Computes final analytical aggregations.
* **Language Choice:** I utilized PySpark as the primary engine for the DLT pipeline to leverage programmatic header normalization and strict schema enforcement. I provided equivalent Spark SQL queries for the Gold layer to demonstrate fluency across both APIs, as SQL is often preferred by downstream analysts.

## Trade-offs & Production Considerations
If delivering this for a real enterprise client, I would adapt the following:
* **Data Volume & Orchestration:** The current ingestion script uses standard Python `requests`. For massive scale, I would utilize PySpark distributed HTTP requests (UDFs) or Databricks Auto Loader for streaming ingestion.
* **Schema Drift:** I would enable Auto Loader schema evolution (`cloudFiles.schemaLocation`) rather than inferring schemas dynamically, ensuring downstream pipelines don't break if BLS adds new columns.
* **Cost & Compute:** The pipeline runs in `Triggered` mode to optimize costs, but for strict SLAs, Serverless DLT pipelines would be utilized to eliminate cluster spin-up time.
* **Access Control:** I applied basic Unity Catalog `GRANT SELECT` statements at the schema level. In production, I would implement dynamic views with Row-Level Security (RLS) to restrict sensitive macro data by region or department.

## AI Usage
I integrated AI heavily as a collaborative engineering partner throughout this workflow:
* **Drafting:** Used Gemini to scaffold the initial OOP ingestion class and DLT PySpark boilerplate. 
* **Debugging & Validation:** Databricks Genie was instrumental in diagnosing a critical data quality bug. When the raw BLS tab-delimited files failed downstream joins, Genie identified that the raw column headers contained hidden padding. I validated Genie's suggested Python list-comprehension fix (`[c.strip() for c in df.columns]`) and integrated it into the Bronze layer. Genie also highlighted that Window functions without secondary sort keys lead to non-deterministic tie-breaking in Gold Q2, which I corrected.

## Retrospective
The hardest part to get right was the BLS directory ingestion and string-key integrity. The BLS web server dynamically returns both relative and absolute HTML paths depending on the directory, which required implementing a robust regex fallback to scrape the file links correctly. Additionally, ensuring Spark did not aggressively infer numeric types on BLS dimension codes (which would strip leading zeros) required strict manual schema casting in the Silver layer.
