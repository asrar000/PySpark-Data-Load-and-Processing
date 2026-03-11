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
