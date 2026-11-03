import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VOLUME_PATH = "/Volumes/macroeconomic_data/raw_data/bls_raw_landing"

# ==============================================================================
# BRONZE LAYER - Raw Ingestion
# ==============================================================================

@dlt.table(
    name="bronze_bls_data",
    comment="Raw BLS productivity time-series data ingested from Volume",
    table_properties={"quality": "bronze"}
)
def bronze_bls_data():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/pr.data.1.AllData")
    )

@dlt.table(
    name="bronze_bls_series",
    comment="Raw BLS series metadata mapping series_id to title descriptions",
    table_properties={"quality": "bronze"}
)
def bronze_bls_series():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/pr.series")
    )

@dlt.table(
    name="bronze_population",
    comment="Raw US Population JSON payload ingested from Volume",
    table_properties={"quality": "bronze"}
)
def bronze_population():
    return (
        spark.read.format("json")
        .option("multiLine", "true")
        .load(f"{VOLUME_PATH}/us_population.json")
        .select(F.explode(F.col("data")).alias("record"))
        .select("record.*")
    )

# ==============================================================================
# SILVER LAYER - Cleaned, Typed & Quality Checked
# ==============================================================================

@dlt.table(
    name="silver_bls_data",
    comment="Cleaned BLS time-series data with stripped spaces and enforced types",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND length(series_id) > 0")
@dlt.expect_or_drop("valid_value", "value IS NOT NULL")
def silver_bls_data():
    return (
        dlt.read("bronze_bls_data")
        .select(
            F.trim(F.col("series_id")).alias("series_id"),
            F.col("year").cast("int").alias("year"),
            F.trim(F.col("period")).alias("period"),
            F.col("value").cast("double").alias("value"),
            F.trim(F.col("footnote_codes")).alias("footnote_codes")
        )
    )

@dlt.table(
    name="silver_bls_series",
    comment="Cleaned BLS series metadata mapping with human-readable titles",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL")
def silver_bls_series():
    return (
        dlt.read("bronze_bls_series")
        .select(
            F.trim(F.col("series_id")).alias("series_id"),
            F.trim(F.col("series_title")).alias("series_title"),
            F.trim(F.col("sector_code")).alias("sector_code"),
            F.trim(F.col("measure_code")).alias("measure_code")
        )
    )

@dlt.table(
    name="silver_population",
    comment="Cleaned US Population metrics with validated year and integer population",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_year", "year IS NOT NULL")
@dlt.expect_or_drop("valid_population", "population > 0")
def silver_population():
    return (
        dlt.read("bronze_population")
        .select(
            F.col("Year").cast("int").alias("year"),
            F.col("Nation").alias("nation"),
            F.col("Population").cast("long").alias("population")
        )
    )

# ==============================================================================
# GOLD LAYER - Analytical Marts Answering Questions 1, 2 & 3
# ==============================================================================

@dlt.table(
    name="gold_q1_population_stats",
    comment="Question 1: Mean and StdDev of US Population across 2013-2018 inclusive",
    table_properties={"quality": "gold"}
)
def gold_q1_population_stats():
    return (
        dlt.read("silver_population")
        .filter((F.col("year") >= 2013) & (F.col("year") <= 2018))
        .agg(
            F.avg("population").alias("mean_population"),
            F.stddev("population").alias("stddev_population"),
            F.count("population").alias("year_count")
        )
    )

@dlt.table(
    name="gold_q2_best_year_per_series",
    comment="Question 2: Best year per BLS series_id based on sum of quarterly values with human-readable labels",
    table_properties={"quality": "gold"}
)
def gold_q2_best_year_per_series():
    quarterly_data = (
        dlt.read("silver_bls_data")
        .filter(F.col("period").startswith("Q") & (F.col("period") != "Q55"))
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("annual_sum"))
    )

    window_spec = Window.partitionBy("series_id").orderBy(F.col("annual_sum").desc())
    
    ranked_years = (
        quarterly_data
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    return (
        ranked_years
        .join(dlt.read("silver_bls_series"), on="series_id", how="left")
        .select(
            "series_id",
            "series_title",
            F.col("year").alias("best_year"),
            "annual_sum"
        )
    )

@dlt.table(
    name="gold_q3_prs30006032_q01_population",
    comment="Question 3: PRS30006032 Q01 values joined with annual US Population",
    table_properties={"quality": "gold"}
)
def gold_q3_prs30006032_q01_population():
    bls_target = (
        dlt.read("silver_bls_data")
        .filter((F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01"))
        .select("series_id", "year", "period", "value")
    )

    return (
        bls_target
        .join(dlt.read("silver_population"), on="year", how="left")
        .select(
            "year",
            "series_id",
            "period",
            "value",
            "population"
        )
        .orderBy("year")
    )
