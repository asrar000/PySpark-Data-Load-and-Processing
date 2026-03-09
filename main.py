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
from logging import log
import os
import re
from datetime import datetime

# ---------------------------------------------------------------------------
# PySpark
# ---------------------------------------------------------------------------
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, BooleanType

# ---------------------------------------------------------------------------
# Project config
# ---------------------------------------------------------------------------
import config

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


# ══════════════════════════════════════════════════════════════════════════════
# Step 1 – Read data
# ══════════════════════════════════════════════════════════════════════════════

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
# Main
# ------------------------------------------------------------------------
def main():
    """Orchestrate all pipeline steps end-to-end."""

    log("INIT", "Pipeline starting", app=config.APP_NAME)

    # ── 1. SparkSession ───────────────────────────────────────────────────────
    spark = create_spark_session(config.APP_NAME)
    log("INIT", "SparkSession created")

    # ── 2. Read raw data ──────────────────────────────────────────────────────
    raw_details = read_json(spark, config.INPUT_DETAILS_FILE, "details")
    raw_search  = read_json(spark, config.INPUT_SEARCH_FILE,  "search")

    details_raw_count = raw_details.count()
    search_raw_count  = raw_search.count()
