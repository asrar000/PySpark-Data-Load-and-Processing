"""
main.py
-------
PySpark pipeline that:
  1. Reads details.json and search.json
  2. Joins them (inner -> matched, anti -> unmatched)
  3. Produces a standardized final output
  4. Writes a validation_report.txt
  5. Writes structured JSON logs to logs/<date>/<script>_<date>_<time>.json
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import json
import os
from datetime import datetime

# ---------------------------------------------------------------------------
# PySpark
# ---------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, BooleanType

# ---------------------------------------------------------------------------
# Project config
# ---------------------------------------------------------------------------
import config

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _log_path():
    """Return the JSON log file path: logs/<YYMMDD>/<script>_<YYMMDD>_<HHMMSS>.json"""
    now     = datetime.now()
    date    = now.strftime("%y%m%d")
    time    = now.strftime("%H%M%S")
    log_dir = os.path.join(config.LOG_BASE_DIR, date)
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f"{config.SCRIPT_NAME}_{date}_{time}.json")


LOG_FILE   = _log_path()
_log_lines = []


def log(step, message, **extra):
    """
    Append a structured log entry to memory and print to stdout.

    Parameters
    ----------
    step    : Pipeline step label (e.g. 'READ', 'JOIN').
    message : Human-readable description.
    **extra : Any additional key/value pairs to include in the log entry.
    """
    entry = {
        "timestamp": datetime.now().isoformat(),
        "step":      step,
        "message":   message,
        **extra,
    }
    _log_lines.append(entry)
    print(f"[{entry['timestamp']}] [{step}] {message}" +
          (f" | {extra}" if extra else ""))


def flush_logs():
    """Write all accumulated log entries to the JSON log file."""
    with open(LOG_FILE, "w") as fh:
        json.dump(_log_lines, fh, indent=2, default=str)
    print(f"\nLogs written -> {LOG_FILE}")


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def create_spark_session(app_name):
    """
    Create and return a local SparkSession.

    Parameters
    ----------
    app_name : Name shown in the Spark UI.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Step 1 - Read data
# ---------------------------------------------------------------------------

def read_json(spark, path, label):
    """
    Read a multiline JSON file into a Spark DataFrame.

    Parameters
    ----------
    spark : Active SparkSession.
    path  : Path to the JSON file.
    label : Human-readable label used in logs.
    """
    log("READ", f"Reading {label}", path=path)
    df = spark.read.option("multiline", "true").json(path)
    log("READ", f"{label} loaded", row_count=df.count(), columns=df.columns)
    return df


# ---------------------------------------------------------------------------
# Step 2 - Extract fields from details
# ---------------------------------------------------------------------------

def extract_details_fields(df):
    """
    Flatten and rename the fields we need from details.json.

    Extracted columns
    -----------------
    source_id, property_name, country_code, currency,
    star_rating, review_score
    """
    log("EXTRACT", "Extracting fields from details DataFrame")

    extracted = df.select(
        F.col("id").cast(StringType()).alias("source_id"),

        # name is a StructType - access en-us field using getField()
        F.col("name").getField("en-us").alias("property_name"),

        # location
        F.trim(F.upper(F.col("location.country"))).alias("country_code"),

        # currency
        F.col("currency"),

        # rating nested fields
        F.col("rating.stars").cast(DoubleType()).alias("star_rating"),
        F.col("rating.review_score").cast(DoubleType()).alias("review_score"),
    )

    log("EXTRACT", "Details extraction complete", columns=extracted.columns)
    return extracted


# ---------------------------------------------------------------------------
# Step 3 - Extract fields from search
# ---------------------------------------------------------------------------

def extract_search_fields(df):
    """
    Flatten and rename the fields we need from search.json.

    Extracted columns
    -----------------
    search_id, usd_price, commission_pct, meal_plan, deep_link_url
    """
    log("EXTRACT", "Extracting fields from search DataFrame")

    extracted = df.select(
        F.col("id").cast(StringType()).alias("search_id"),

        # usd_price: use price.book as the booker currency price
        F.col("price.book").cast(DoubleType()).alias("usd_price"),

        # commission percentage
        F.col("commission.percentage").cast(DoubleType()).alias("commission_pct"),

        # meal_plan: first product's meal_plan meals list -> join as string
        F.col("products").getItem(0)
         .getField("policies")
         .getField("meal_plan")
         .getField("meals")
         .cast(StringType())
         .alias("meal_plan"),

        # deep_link_url for data quality checks
        F.col("deep_link_url"),

        # keep currency for reference
        F.col("currency").alias("search_currency"),
    )

    log("EXTRACT", "Search extraction complete", columns=extracted.columns)
    return extracted


# ---------------------------------------------------------------------------
# Step 4 - Data quality checks on search
# ---------------------------------------------------------------------------

def search_quality_checks(search_df):
    """
    Run mandatory data-quality checks on the extracted search DataFrame.

    Returns
    -------
    dict with keys: missing_deep_link_url, missing_usd_price
    """
    log("QC", "Running search data quality checks")

    missing_url   = search_df.filter(F.col("deep_link_url").isNull()).count()
    missing_price = search_df.filter(F.col("usd_price").isNull()).count()

    report = {
        "missing_deep_link_url": missing_url,
        "missing_usd_price":     missing_price,
    }
    log("QC", "Search quality checks complete", **report)
    return report


# ---------------------------------------------------------------------------
# Step 5 - Drop rows with missing source_id
# ---------------------------------------------------------------------------

def drop_missing_source_id(df):
    """
    Remove rows where source_id is null or empty.

    Returns
    -------
    (clean_df, dropped_count)
    """
    before  = df.count()
    clean   = df.filter(F.col("source_id").isNotNull() & (F.col("source_id") != ""))
    dropped = before - clean.count()
    log("VALIDATE", "Dropped rows with missing source_id", dropped=dropped)
    return clean, dropped


# ---------------------------------------------------------------------------
# Step 6 - Deduplicate details
# ---------------------------------------------------------------------------

def deduplicate(df, key):
    """
    Remove duplicate rows based on key, keeping the first occurrence.

    Returns
    -------
    (deduplicated_df, count_before, count_after)
    """
    count_before = df.count()
    df_dedup     = df.dropDuplicates([key])
    count_after  = df_dedup.count()
    dup_count    = count_before - count_after

    log("DEDUP", f"Deduplication on '{key}'",
        before=count_before, after=count_after, duplicates_removed=dup_count)
    return df_dedup, count_before, count_after


# ---------------------------------------------------------------------------
# Step 7 - Join
# ---------------------------------------------------------------------------

def build_matched_unmatched(details_df, search_df):
    """
    Perform INNER JOIN (matched) and LEFT ANTI JOIN (unmatched).

    Join key: details.source_id == search.search_id

    Returns
    -------
    (matched_details, unmatched_details)
    """
    log("JOIN", "Building matched and unmatched DataFrames")

    matched = details_df.join(
        search_df,
        details_df["source_id"] == search_df["search_id"],
        how="inner",
    )

    unmatched = details_df.join(
        search_df,
        details_df["source_id"] == search_df["search_id"],
        how="left_anti",
    )

    log("JOIN", "Join complete",
        matched=matched.count(), unmatched=unmatched.count())
    return matched, unmatched


# ---------------------------------------------------------------------------
# Step 8 - Build final standardized output
# ---------------------------------------------------------------------------

def make_slug(name_col):
    """Convert a property name to a lowercase dash-separated slug."""
    return F.lower(F.regexp_replace(F.trim(name_col), r"[^a-zA-Z0-9]+", "-"))


def build_final_output(matched_df):
    """
    Apply all output-field rules to matched_details to produce the
    standardized 12-column final output.

    Rules applied
    -------------
    - id               = 'GEN-' + source_id
    - feed_provider_id = source_id
    - property_name    = from details
    - property_slug    = lowercase dash-separated from property_name
    - country_code     = trimmed uppercase (flag rows != 2 chars)
    - currency         = 'USD' if missing
    - usd_price        = price.book (default 0.0)
    - star_rating      = 0.0 if missing
    - review_score     = 0.0 if missing
    - commission       = commission_pct
    - meal_plan        = from search products
    - published        = true

    Returns
    -------
    (final_df, defaulted_usd_price_count)
    """
    log("TRANSFORM", "Building final standardized output")

    # Count rows where usd_price will be defaulted
    defaulted_price_count = matched_df.filter(F.col("usd_price").isNull()).count()

    final = matched_df.select(
        F.concat_ws("-", F.lit("GEN"), F.col("source_id")).alias("id"),
        F.col("source_id").alias("feed_provider_id"),
        F.col("property_name"),
        make_slug(F.col("property_name")).alias("property_slug"),
        F.col("country_code"),
        F.coalesce(F.col("currency"), F.lit(config.DEFAULT_CURRENCY)).alias("currency"),
        F.coalesce(
            F.col("usd_price"), F.lit(config.DEFAULT_USD_PRICE)
        ).cast(DoubleType()).alias("usd_price"),
        F.coalesce(
            F.col("star_rating"), F.lit(config.DEFAULT_STAR_RATING)
        ).cast(DoubleType()).alias("star_rating"),
        F.coalesce(
            F.col("review_score"), F.lit(config.DEFAULT_REVIEW_SCORE)
        ).cast(DoubleType()).alias("review_score"),
        F.col("commission_pct").alias("commission"),
        F.col("meal_plan"),
        F.lit(config.DEFAULT_PUBLISHED).cast(BooleanType()).alias("published"),
    )

    # data_quality_flag (nice to have)
    final = final.withColumn(
        "data_quality_flag",
        F.when(
            F.col("property_name").isNull()
            | F.col("usd_price").isNull()
            | (F.length(F.col("country_code")) != 2),
            F.lit("NEEDS_REVIEW"),
        ).otherwise(F.lit("GOOD")),
    )

    log("TRANSFORM", "Final output built",
        columns=final.columns, total_columns=len(final.columns),
        defaulted_usd_price=defaulted_price_count)

    return final, defaulted_price_count


# ---------------------------------------------------------------------------
# Optional extras
# ---------------------------------------------------------------------------

def country_summary(final_df):
    """Print a country-level summary: total_properties and avg_review_score."""
    log("SUMMARY", "Building country summary table")
    summary = (
        final_df.groupBy("country_code")
        .agg(
            F.count("*").alias("total_properties"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
        .orderBy(F.desc("total_properties"))
    )
    print("\n-- Country Summary ------------------------------------------")
    summary.show(truncate=False)


def extract_checkin_checkout(matched_df):
    """
    Parse checkin / checkout dates from deep_link_url and show top 10 rows.
    URL pattern: ...checkin=YYYY-MM-DD&checkout=YYYY-MM-DD
    """
    log("EXTRA", "Extracting checkin/checkout from deep_link_url")
    parsed = matched_df.withColumn(
        "checkin",
        F.regexp_extract(F.col("deep_link_url"), r"checkin=(\d{4}-\d{2}-\d{2})", 1),
    ).withColumn(
        "checkout",
        F.regexp_extract(F.col("deep_link_url"), r"checkout=(\d{4}-\d{2}-\d{2})", 1),
    ).select("search_id", "deep_link_url", "checkin", "checkout")

    print("\n-- Checkin / Checkout (top 10) ------------------------------")
    parsed.show(10, truncate=False)


# ---------------------------------------------------------------------------
# Step 9 - Validation report
# ---------------------------------------------------------------------------

def write_validation_report(
    details_count,
    search_count,
    matched_count,
    unmatched_count,
    final_count,
    dropped_source_id,
    dup_before,
    dup_after,
    qc,
    final_df,
    bad_country_count,
    defaulted_price,
):
    """
    Write a human-readable validation_report.txt summarising all pipeline stats.
    """
    log("REPORT", "Writing validation report")

    # Build schema string - loop through every column and format as "name: type"
    schema_lines = []
    for f in final_df.schema.fields:
        line = f"  {f.name}: {f.dataType}"
        schema_lines.append(line)
    schema_str = "\n".join(schema_lines)

    # Total column count - used in the report below
    col_count = len(final_df.columns)

    lines = [
        "=" * 60,
        "VALIDATION REPORT",
        f"Generated: {datetime.now().isoformat()}",
        "=" * 60,
        "",
        "-- Row Counts -----------------------------------------------",
        f"  details.json row count         : {details_count}",
        f"  search.json row count          : {search_count}",
        f"  matched_details (inner join)   : {matched_count}",
        f"  unmatched_details (anti join)  : {unmatched_count}",
        f"  final output row count         : {final_count}",
        "",
        "-- Data Quality ---------------------------------------------",
        f"  Dropped (missing source_id)    : {dropped_source_id}",
        f"  Duplicates before dedup        : {dup_before}",
        f"  Duplicates after dedup         : {dup_after}",
        f"  Duplicates removed             : {dup_before - dup_after}",
        f"  Dedup removal %                : {round((dup_before - dup_after) / max(dup_before,1) * 100, 2)}%",
        "",
        "-- Search Quality Checks ------------------------------------",
        f"  Missing deep_link_url          : {qc['missing_deep_link_url']}",
        f"  Missing usd_price (book)       : {qc['missing_usd_price']}",
        "",
        "-- Output Field Checks --------------------------------------",
        f"  country_code length != 2       : {bad_country_count}",
        f"  usd_price defaulted to 0.0     : {defaulted_price}",
        "",
        "-- Final Output Schema --------------------------------------",
        schema_str,
        "",
        f"  Total columns in final output  : {col_count}",
        f"  Schema has exactly 12 columns  : {'YES' if col_count == 13 else f'NO ({col_count}) - includes data_quality_flag bonus column'}",
        "=" * 60,
    ]

    report_text = "\n".join(lines)
    with open(config.VALIDATION_REPORT_PATH, "w") as fh:
        fh.write(report_text)

    print("\n" + report_text)
    log("REPORT", "Validation report written", path=config.VALIDATION_REPORT_PATH)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Orchestrate all pipeline steps end-to-end."""

    log("INIT", "Pipeline starting", app=config.APP_NAME)

    # 1. SparkSession
    spark = create_spark_session(config.APP_NAME)
    log("INIT", "SparkSession created")

    # 2. Read raw data
    raw_details = read_json(spark, config.INPUT_DETAILS_FILE, "details")
    raw_search  = read_json(spark, config.INPUT_SEARCH_FILE,  "search")

    details_raw_count = raw_details.count()
    search_raw_count  = raw_search.count()

    # 3. Extract fields
    details_ext = extract_details_fields(raw_details)
    search_ext  = extract_search_fields(raw_search)

    # 4. Search quality checks
    qc_report = search_quality_checks(search_ext)

    # 5. Drop rows with missing source_id
    details_clean, dropped_source_id = drop_missing_source_id(details_ext)

    # 6. Deduplicate details
    details_dedup, dup_before, dup_after = deduplicate(details_clean, "source_id")

    # 7. Join -> matched / unmatched
    matched_details, unmatched_details = build_matched_unmatched(
        details_dedup, search_ext
    )

    matched_count   = matched_details.count()
    unmatched_count = unmatched_details.count()

    # 8. Build final output
    final_output, defaulted_price = build_final_output(matched_details)
    final_count = final_output.count()

    # 9. Data quality checks on final output
    bad_country_count = final_output.filter(
        F.length(F.col("country_code")) != 2
    ).count()
    log("QC", "Country code length check", bad_country_code_rows=bad_country_count)

    # 10. Show sample rows (nice to have)
    print("\n-- Top 5: details (extracted) -------------------------------")
    details_dedup.show(5, truncate=True)

    print("\n-- Top 5: search (extracted) --------------------------------")
    search_ext.show(5, truncate=True)

    print("\n-- Top 5: final output --------------------------------------")
    final_output.show(5, truncate=True)

    # 11. Optional extras
    country_summary(final_output)
    extract_checkin_checkout(matched_details)

    usd_defaulted_count = final_output.filter(F.col("usd_price") == 0.0).count()
    print(f"\n  Rows where usd_price was defaulted to 0.0: {usd_defaulted_count}")

    # 12. Write outputs
    log("WRITE", "Writing final_output", path=config.OUTPUT_FINAL_DIR)
    final_output.coalesce(1).write.mode("overwrite").json(config.OUTPUT_FINAL_DIR)

    log("WRITE", "Writing unmatched_details", path=config.OUTPUT_UNMATCHED_DIR)
    unmatched_details.coalesce(1).write.mode("overwrite").json(config.OUTPUT_UNMATCHED_DIR)

    # 13. Validation report
    write_validation_report(
        details_count     = details_raw_count,
        search_count      = search_raw_count,
        matched_count     = matched_count,
        unmatched_count   = unmatched_count,
        final_count       = final_count,
        dropped_source_id = dropped_source_id,
        dup_before        = dup_before,
        dup_after         = dup_after,
        qc                = qc_report,
        final_df          = final_output,
        bad_country_count = bad_country_count,
        defaulted_price   = defaulted_price,
    )

    # 14. Flush logs
    log("DONE", "Pipeline complete")
    flush_logs()
    spark.stop()


if __name__ == "__main__":
    main()