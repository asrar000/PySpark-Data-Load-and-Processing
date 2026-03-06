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


# ---------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------
def main():
    spark = create_spark_session(config.APP_NAME)