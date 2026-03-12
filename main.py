"""
main.py
-------
Entry point for the PySpark property search pipeline.

This file contains only orchestration logic. All business logic
lives in the utils/ package and logger.py.

Pipeline steps
--------------
  1.  Read property.json and search.json
  2.  Extract required fields from details
  3.  Extract required fields from search
  4.  Run data quality checks on search
  5.  Drop rows with missing source_id
  6.  Deduplicate details on source_id
  7.  Inner join  -> matched_details
      Anti join   -> unmatched_details
  8.  Build standardized final output
  9.  Write final_output and unmatched_details to disk
  10. Write validation_report.txt
  11. Write structured JSON logs
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Project config
# ---------------------------------------------------------------------------
import config

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
from logger import log, flush_logs

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------
from utils.spark_session import create_spark_session
from utils.readers       import read_json
from utils.transforms    import extract_details_fields, extract_search_fields, build_final_output
from utils.cleaning      import drop_missing_source_id, deduplicate
from utils.joins         import build_matched_unmatched
from utils.quality       import search_quality_checks, write_validation_report
from utils.extras        import country_summary, extract_checkin_checkout


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

    # 10. Show sample rows
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