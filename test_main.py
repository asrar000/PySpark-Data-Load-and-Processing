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
from pyspark.sql.types import DoubleType

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


# ---------------------------------------------------------------------------
# Tests for search_quality_checks()
# ---------------------------------------------------------------------------

def test_search_quality_checks_missing_deep_link(spark):
    """
    One row in our sample has a null deep_link_url, so the count should be 1.
    """
    raw = make_search_df(spark)
    search_ext = extract_search_fields(raw)
    report = search_quality_checks(search_ext)

    assert report["missing_deep_link_url"] == 1


def test_search_quality_checks_missing_usd_price(spark):
    """
    One row in our sample has a null price, so missing_usd_price should be 1.
    """
    raw = make_search_df(spark)
    search_ext = extract_search_fields(raw)
    report = search_quality_checks(search_ext)

    assert report["missing_usd_price"] == 1


def test_search_quality_checks_returns_dict(spark):
    """
    The function must always return a dict with the two expected keys.
    """
    raw = make_search_df(spark)
    search_ext = extract_search_fields(raw)
    report = search_quality_checks(search_ext)

    assert isinstance(report, dict)
    assert "missing_deep_link_url" in report
    assert "missing_usd_price" in report


# ---------------------------------------------------------------------------
# Tests for drop_missing_source_id()
# ---------------------------------------------------------------------------

def test_drop_missing_source_id_removes_null_rows(spark):
    """
    Our details sample has 3 rows; 1 has a null id.
    After extraction + drop, we should have 2 rows.
    """
    raw = make_details_df(spark)
    extracted = extract_details_fields(raw)
    clean, _ = drop_missing_source_id(extracted)

    assert clean.count() == 2


def test_drop_missing_source_id_dropped_count_is_correct(spark):
    """
    The returned dropped count should equal 1 (we have 1 null source_id row).
    """
    raw = make_details_df(spark)
    extracted = extract_details_fields(raw)
    _, dropped = drop_missing_source_id(extracted)

    assert dropped == 1


def test_drop_missing_source_id_no_nulls_in_result(spark):
    """
    After the function runs, no row in the result should have a null source_id.
    """
    raw = make_details_df(spark)
    extracted = extract_details_fields(raw)
    clean, _ = drop_missing_source_id(extracted)

    null_count = clean.filter(F.col("source_id").isNull()).count()
    assert null_count == 0


# ---------------------------------------------------------------------------
# Tests for deduplicate()
# ---------------------------------------------------------------------------

def test_deduplicate_removes_exact_duplicates(spark):
    """
    If we add a duplicate row, dedup should remove it and return count_after < count_before.
    """
    raw = make_details_df(spark)
    extracted = extract_details_fields(raw)
    clean, _ = drop_missing_source_id(extracted)

    first_row = clean.limit(1)
    with_dup = clean.union(first_row)

    deduped, before, after = deduplicate(with_dup, "source_id")

    assert before == 3
    assert after == 2
    assert deduped.count() == 2


def test_deduplicate_no_duplicates_unchanged(spark):
    """
    If there are no duplicates, count_before should equal count_after.
    """
    raw = make_details_df(spark)
    extracted = extract_details_fields(raw)
    clean, _ = drop_missing_source_id(extracted)

    _, before, after = deduplicate(clean, "source_id")

    assert before == after


# ---------------------------------------------------------------------------
# Tests for build_matched_unmatched()
# ---------------------------------------------------------------------------

def test_matched_count_is_correct(spark):
    """
    details has ids 101, 102. search has ids 101, 102, 103.
    Inner join should give 2 matched rows.
    """
    details_ext = extract_details_fields(make_details_df(spark))
    details_clean, _ = drop_missing_source_id(details_ext)
    search_ext = extract_search_fields(make_search_df(spark))

    matched, _ = build_matched_unmatched(details_clean, search_ext)
    assert matched.count() == 2


def test_unmatched_count_is_correct(spark):
    """
    details has ids 101, 102. search has 101, 102, 103.
    Anti-join should give 0 unmatched rows from details side.
    """
    details_ext = extract_details_fields(make_details_df(spark))
    details_clean, _ = drop_missing_source_id(details_ext)
    search_ext = extract_search_fields(make_search_df(spark))

    _, unmatched = build_matched_unmatched(details_clean, search_ext)
    assert unmatched.count() == 0


def test_unmatched_when_details_has_extra_id(spark):
    """
    If we add an extra id (999) to details that does NOT exist in search,
    that row should appear in unmatched.
    """
    details_ext = extract_details_fields(make_details_df(spark))
    details_clean, _ = drop_missing_source_id(details_ext)

    extra = spark.createDataFrame(
        [("999", "New Hotel", "DE", "EUR", 5.0, 9.0)],
        ["source_id", "property_name", "country_code", "currency", "star_rating", "review_score"]
    )
    details_with_extra = details_clean.union(extra)

    search_ext = extract_search_fields(make_search_df(spark))
    _, unmatched = build_matched_unmatched(details_with_extra, search_ext)

    assert unmatched.count() == 1
    assert unmatched.collect()[0]["source_id"] == "999"


# ---------------------------------------------------------------------------
# Tests for make_slug()
# ---------------------------------------------------------------------------

def test_make_slug_lowercase(spark):
    """
    make_slug should produce all lowercase text.
    'Grand Hotel' -> 'grand-hotel'
    """
    df = spark.createDataFrame([("Grand Hotel",)], ["name"])
    result = df.select(make_slug(F.col("name")).alias("slug")).collect()[0]["slug"]
    assert result == "grand-hotel"


def test_make_slug_replaces_spaces_with_dashes(spark):
    """
    Spaces and special characters should become dashes.
    """
    df = spark.createDataFrame([("Sea View Inn!",)], ["name"])
    result = df.select(make_slug(F.col("name")).alias("slug")).collect()[0]["slug"]
    assert "-" in result
    assert " " not in result


# ---------------------------------------------------------------------------
# Tests for build_final_output()
# ---------------------------------------------------------------------------

def test_final_output_has_13_columns(spark):
    """
    Final output must have 13 columns:
    12 required + 1 bonus data_quality_flag column.
    """
    details_ext = extract_details_fields(make_details_df(spark))
    details_clean, _ = drop_missing_source_id(details_ext)
    search_ext = extract_search_fields(make_search_df(spark))
    matched, _ = build_matched_unmatched(details_clean, search_ext)

    final, _ = build_final_output(matched)
    assert len(final.columns) == 13


def test_final_output_id_starts_with_gen(spark):
    """
    The 'id' column must always start with 'GEN-'.
    """
    details_ext = extract_details_fields(make_details_df(spark))
    details_clean, _ = drop_missing_source_id(details_ext)
    search_ext = extract_search_fields(make_search_df(spark))
    matched, _ = build_matched_unmatched(details_clean, search_ext)

    final, _ = build_final_output(matched)
    ids = [row["id"] for row in final.collect()]
    for record_id in ids:
        assert record_id.startswith("GEN-")


def test_final_output_published_is_always_true(spark):
    """
    The 'published' column must be True for every row.
    """
    details_ext = extract_details_fields(make_details_df(spark))
    details_clean, _ = drop_missing_source_id(details_ext)
    search_ext = extract_search_fields(make_search_df(spark))
    matched, _ = build_matched_unmatched(details_clean, search_ext)

    final, _ = build_final_output(matched)
    for row in final.collect():
        assert row["published"] is True


def test_final_output_usd_price_defaults_to_zero(spark):
    """
    When usd_price is null in search, it should be defaulted to 0.0.
    Row with search_id=102 has null price -> should become 0.0.
    """
    details_ext = extract_details_fields(make_details_df(spark))
    details_clean, _ = drop_missing_source_id(details_ext)
    search_ext = extract_search_fields(make_search_df(spark))
    matched, _ = build_matched_unmatched(details_clean, search_ext)

    final, _ = build_final_output(matched)
    for row in final.collect():
        if row["feed_provider_id"] == "102":
            assert row["usd_price"] == 0.0


def test_final_output_currency_defaults_to_usd(spark):
    """
    When currency is null or missing, it should default to 'USD'.
    """
    df = spark.createDataFrame(
        [("201", "Test Hotel", "US", None, 3.0, 7.5)],
        ["source_id", "property_name", "country_code", "currency", "star_rating", "review_score"]
    )
    search_df = spark.createDataFrame(
        [("201", 100.0, 5.0, "USD", None, "booking://hotel/201")],
        ["search_id", "usd_price", "commission_pct", "search_currency", "meal_plan", "deep_link_url"]
    )
    matched = df.join(search_df, df["source_id"] == search_df["search_id"], "inner")

    final, _ = build_final_output(matched)
    currency = final.collect()[0]["currency"]
    assert currency == "USD"


def test_final_output_data_quality_flag_good(spark):
    """
    Rows with all key fields present and valid 2-char country code
    should get a 'GOOD' data_quality_flag.
    """
    details_ext = extract_details_fields(make_details_df(spark))
    details_clean, _ = drop_missing_source_id(details_ext)
    search_ext = extract_search_fields(make_search_df(spark))
    matched, _ = build_matched_unmatched(details_clean, search_ext)

    final, _ = build_final_output(matched)
    for row in final.collect():
        if row["feed_provider_id"] == "101":
            assert row["data_quality_flag"] == "GOOD"