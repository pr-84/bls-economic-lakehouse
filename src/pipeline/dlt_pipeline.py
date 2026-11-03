import re

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VOLUME_PATH = "/Volumes/macroeconomic_data/raw_data/bls_raw_landing"


def _normalize_column_names(df):
    normalized_columns = [
        re.sub(r"_+", "_", column_name.strip().lower().replace(" ", "_")).strip("_")
        for column_name in df.columns
    ]
    return df.toDF(*normalized_columns)



def _read_bls_tsv(file_name: str):
    return _normalize_column_names(
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "false")
        .load(f"{VOLUME_PATH}/{file_name}")
    )


# ==============================================================================
# BRONZE LAYER - Raw Ingestion
# ==============================================================================

@dlt.table(
    name="bronze_bls_data",
    comment="Raw BLS productivity time-series data ingested from a Unity Catalog volume",
    table_properties={"quality": "bronze"}
)
def bronze_bls_data():
    return _read_bls_tsv("pr.data.1.AllData")


@dlt.table(
    name="bronze_bls_series",
    comment="Raw BLS series metadata file",
    table_properties={"quality": "bronze"}
)
def bronze_bls_series():
    return _read_bls_tsv("pr.series")


@dlt.table(
    name="bronze_bls_class",
    comment="Raw BLS class mapping file",
    table_properties={"quality": "bronze"}
)
def bronze_bls_class():
    return _read_bls_tsv("pr.class")


@dlt.table(
    name="bronze_bls_measure",
    comment="Raw BLS measure mapping file",
    table_properties={"quality": "bronze"}
)
def bronze_bls_measure():
    return _read_bls_tsv("pr.measure")


@dlt.table(
    name="bronze_bls_sector",
    comment="Raw BLS sector mapping file",
    table_properties={"quality": "bronze"}
)
def bronze_bls_sector():
    return _read_bls_tsv("pr.sector")


@dlt.table(
    name="bronze_bls_duration",
    comment="Raw BLS duration mapping file",
    table_properties={"quality": "bronze"}
)
def bronze_bls_duration():
    return _read_bls_tsv("pr.duration")


@dlt.table(
    name="bronze_population",
    comment="Raw US population JSON payload ingested from a Unity Catalog volume",
    table_properties={"quality": "bronze"}
)
def bronze_population():
    return _normalize_column_names(
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
    comment="Cleaned BLS time-series data with normalized column names, trimmed keys, and typed values",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND length(series_id) > 0")
@dlt.expect_or_drop("valid_period", "period IS NOT NULL AND length(period) > 0")
@dlt.expect_or_drop("valid_year", "year IS NOT NULL")
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
    comment="Cleaned BLS series metadata with a derived human-readable series title",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND length(series_id) > 0")
def silver_bls_series():
    series_df = (
        dlt.read("bronze_bls_series")
        .select(
            F.trim(F.col("series_id")).alias("series_id"),
            F.trim(F.col("sector_code")).alias("sector_code"),
            F.trim(F.col("class_code")).alias("class_code"),
            F.trim(F.col("measure_code")).alias("measure_code"),
            F.trim(F.col("duration_code")).alias("duration_code"),
            F.trim(F.col("seasonal")).alias("seasonal_code"),
            F.trim(F.col("base_year")).alias("base_year"),
            F.trim(F.col("footnote_codes")).alias("footnote_codes"),
            F.col("begin_year").cast("int").alias("begin_year"),
            F.trim(F.col("begin_period")).alias("begin_period"),
            F.col("end_year").cast("int").alias("end_year"),
            F.trim(F.col("end_period")).alias("end_period")
        )
    )

    class_df = dlt.read("bronze_bls_class").select(
        F.trim(F.col("class_code")).alias("class_code"),
        F.trim(F.col("class_text")).alias("class_text")
    )

    measure_df = dlt.read("bronze_bls_measure").select(
        F.trim(F.col("measure_code")).alias("measure_code"),
        F.trim(F.col("measure_text")).alias("measure_text")
    )

    sector_df = dlt.read("bronze_bls_sector").select(
        F.trim(F.col("sector_code")).alias("sector_code"),
        F.trim(F.col("sector_name")).alias("sector_name")
    )

    duration_df = dlt.read("bronze_bls_duration").select(
        F.trim(F.col("duration_code")).alias("duration_code"),
        F.trim(F.col("duration_text")).alias("duration_text")
    )

    return (
        series_df
        .join(sector_df, on="sector_code", how="left")
        .join(class_df, on="class_code", how="left")
        .join(measure_df, on="measure_code", how="left")
        .join(duration_df, on="duration_code", how="left")
        .withColumn(
            "series_title",
            F.concat_ws(" | ", "sector_name", "class_text", "measure_text", "duration_text")
        )
        .select(
            "series_id",
            "series_title",
            "sector_code",
            "sector_name",
            "class_code",
            "class_text",
            "measure_code",
            "measure_text",
            "duration_code",
            "duration_text",
            "seasonal_code",
            "base_year",
            "footnote_codes",
            "begin_year",
            "begin_period",
            "end_year",
            "end_period"
        )
    )


@dlt.table(
    name="silver_population",
    comment="Cleaned US population metrics with validated year, nation, and integer population",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_year", "year IS NOT NULL")
@dlt.expect_or_drop("valid_nation", "nation IS NOT NULL AND length(nation) > 0")
@dlt.expect_or_drop("valid_population", "population > 0")
def silver_population():
    return (
        dlt.read("bronze_population")
        .select(
            F.col("year").cast("int").alias("year"),
            F.trim(F.col("nation")).alias("nation"),
            F.col("population").cast("long").alias("population")
        )
    )


# ==============================================================================
# GOLD LAYER - Analytical Marts Answering Questions 1, 2 & 3
# ==============================================================================

@dlt.table(
    name="gold_q1_population_stats",
    comment="Question 1: Mean and standard deviation of US population across 2013-2018 inclusive",
    table_properties={"quality": "gold"}
)
def gold_q1_population_stats():
    return (
        dlt.read("silver_population")
        .filter(
            (F.col("nation") == "United States")
            & (F.col("year") >= 2013)
            & (F.col("year") <= 2018)
        )
        .agg(
            F.avg("population").alias("mean_population"),
            F.stddev("population").alias("stddev_population"),
            F.count("population").alias("year_count")
        )
    )


@dlt.table(
    name="gold_q2_best_year_per_series",
    comment="Question 2: Best year per BLS series based on the annual sum of quarterly values",
    table_properties={"quality": "gold"}
)
def gold_q2_best_year_per_series():
    quarterly_data = (
        dlt.read("silver_bls_data")
        .filter(F.col("period").startswith("Q") & (F.col("period") != "Q55"))
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("annual_sum"))
    )

    window_spec = Window.partitionBy("series_id").orderBy(
        F.col("annual_sum").desc(),
        F.col("year").desc()
    )

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
    comment="Question 3: PRS30006032 Q01 values joined with annual US population",
    table_properties={"quality": "gold"}
)
def gold_q3_prs30006032_q01_population():
    bls_target = (
        dlt.read("silver_bls_data")
        .filter((F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01"))
        .select("series_id", "year", "period", "value")
    )

    population_df = (
        dlt.read("silver_population")
        .filter(F.col("nation") == "United States")
        .select("year", "population")
    )

    return (
        bls_target
        .join(population_df, on="year", how="left")
        .select(
            "year",
            "series_id",
            "period",
            "value",
            "population"
        )
        .orderBy("year")
    )
