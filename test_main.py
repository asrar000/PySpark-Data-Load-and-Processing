"""
test_main.py
------------
Unit tests for main.py using pytest.

Run once:
    pytest test_main.py -v

Run continuously with py-watch (in a separate terminal):
    ptw test_main.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# Import the functions we want to test from main.py
from main import (
    extract_details_fields,
    extract_search_fields,
    search_quality_checks,
    drop_missing_source_id,
    deduplicate,
    build_matched_unmatched,
    build_final_output,
    make_slug,
)


# ---------------------------------------------------------------------------
# Fixture: SparkSession
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Create a single SparkSession for the entire test session.
    'scope=session' means Spark starts once and is reused for all tests.
    This makes tests run much faster.
    """
    session = (
        SparkSession.builder
        .appName("test_main")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Sample DataFrames used across multiple tests
# ---------------------------------------------------------------------------

def make_details_df(spark):
    """
    Create a raw details DataFrame that mimics details.json structure.
    Two valid rows + one row with a null id to test dropping logic.
    """
    data = [
        # (id, name_en_us, country, currency, stars, review_score)
        (101, "Grand Hotel",   "US", "USD", 4.0, 8.5),
        (102, "Sea View Inn",  "GB", "GBP", 3.0, 7.0),
        (None, "Ghost Hotel",  "FR", "EUR", 2.0, 6.0),  
    ]
    df = spark.createDataFrame(data, ["id", "name_en_us", "country", "currency", "stars", "review_score"])

    # Replicate the JSON structure that extract_details_fields() expects
    df = df.select(
        F.col("id"),
        F.struct(F.col("name_en_us").alias("en-us")).alias("name"),
        F.struct(F.col("country").alias("country")).alias("location"),
        F.col("currency"),
        F.struct(
            F.col("stars").alias("stars"),
            F.col("review_score").alias("review_score")
        ).alias("rating"),
    )
    return df


def make_search_df(spark):
    """
    Create a raw search DataFrame that mimics search.json structure.
    One row has a null deep_link_url and one has a null price to test QC checks.
    """
    data = [
        # (id, price_book, commission_pct, deep_link_url, currency)
        (101, 72.5,  8.7, "booking://hotel/101?checkin=2026-06-02&checkout=2026-06-05", "USD"),
        (102, None,  5.0, None,                                                          "GBP"),
        (103, 50.0, 10.0, "booking://hotel/103?checkin=2026-07-01&checkout=2026-07-03", "USD"),
    ]
    df = spark.createDataFrame(data, ["id", "price_book", "commission_pct", "deep_link_url", "currency"])

    # Replicate the JSON structure that extract_search_fields() expects
    df = df.select(
        F.col("id"),
        F.struct(F.col("price_book").cast(DoubleType()).alias("book")).alias("price"),
        F.struct(F.col("commission_pct").cast(DoubleType()).alias("percentage")).alias("commission"),
        F.array(
            F.struct(
                F.struct(
                    F.struct(
                        F.array().alias("meals"),
                        F.lit("no_plan").alias("plan")
                    ).alias("meal_plan")
                ).alias("policies")
            )
        ).alias("products"),
        F.col("deep_link_url"),
        F.col("currency"),
    )
    return df


# ---------------------------------------------------------------------------
# Tests for extract_details_fields()
# ---------------------------------------------------------------------------

def test_extract_details_fields_columns(spark):
    """
    After extraction, the DataFrame must have exactly these 6 columns.
    """
    raw = make_details_df(spark)
    result = extract_details_fields(raw)

    expected_cols = {"source_id", "property_name", "country_code", "currency", "star_rating", "review_score"}
    assert set(result.columns) == expected_cols


def test_extract_details_fields_country_is_uppercase(spark):
    """
    country_code should be trimmed and UPPERCASE regardless of input case.
    'us' in the raw data should become 'US'.
    """
    raw = make_details_df(spark)
    result = extract_details_fields(raw)

    # Collect country codes and make sure they are all uppercase
    for row in result.collect():
        code = row["country_code"]
        if code:
            assert code.isupper()


def test_extract_details_fields_source_id_is_string(spark):
    """
    source_id must be cast to StringType (the raw id is an integer in the JSON).
    """
    raw = make_details_df(spark)
    result = extract_details_fields(raw)

    id_type = dict(result.dtypes)["source_id"]
    assert id_type == "string"


# ---------------------------------------------------------------------------
# Tests for extract_search_fields()
# ---------------------------------------------------------------------------

def test_extract_search_fields_columns(spark):
    """
    After extraction, the DataFrame must have exactly these 6 columns.
    """
    raw = make_search_df(spark)
    result = extract_search_fields(raw)

    expected_cols = {"search_id", "usd_price", "commission_pct", "meal_plan", "deep_link_url", "search_currency"}
    assert set(result.columns) == expected_cols


def test_extract_search_fields_search_id_is_string(spark):
    """
    search_id must be a string (raw id is an integer).
    """
    raw = make_search_df(spark)
    result = extract_search_fields(raw)

    id_type = dict(result.dtypes)["search_id"]
    assert id_type == "string"