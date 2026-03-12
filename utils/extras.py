"""
utils/extras.py
---------------
Optional bonus features for the PySpark property pipeline.
These functions print additional insights but do not affect
the core pipeline output.
"""

from pyspark.sql import functions as F

from logger import log


def country_summary(final_df):
    """
    Print a country-level summary showing total properties
    and average review score per country, ordered by count descending.

    Parameters
    ----------
    final_df : Final standardized output DataFrame.
    """
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
    Parse checkin and checkout dates from deep_link_url and print top 10 rows.

    Expected URL pattern:
        booking://hotel/<id>?...checkin=YYYY-MM-DD&checkout=YYYY-MM-DD

    Parameters
    ----------
    matched_df : Matched DataFrame containing deep_link_url and search_id.
    """
    log("EXTRA", "Extracting checkin/checkout from deep_link_url")
    parsed = (
        matched_df
        .withColumn(
            "checkin",
            F.regexp_extract(F.col("deep_link_url"), r"checkin=(\d{4}-\d{2}-\d{2})", 1),
        )
        .withColumn(
            "checkout",
            F.regexp_extract(F.col("deep_link_url"), r"checkout=(\d{4}-\d{2}-\d{2})", 1),
        )
        .select("search_id", "deep_link_url", "checkin", "checkout")
    )
    print("\n-- Checkin / Checkout (top 10) ------------------------------")
    parsed.show(10, truncate=False)