"""
utils/cleaning.py
-----------------
Data cleaning functions for the PySpark property pipeline.
Handles null removal and deduplication.
"""

from pyspark.sql import functions as F

from logger import log


def drop_missing_source_id(df):
    """
    Remove rows where source_id is null or an empty string.

    Parameters
    ----------
    df : Extracted details DataFrame containing a source_id column.

    Returns
    -------
    (clean_df, dropped_count)
    """
    before  = df.count()
    clean   = df.filter(
        F.col("source_id").isNotNull() & (F.col("source_id") != "")
    )
    dropped = before - clean.count()
    log("VALIDATE", "Dropped rows with missing source_id", dropped=dropped)
    return clean, dropped


def deduplicate(df, key):
    """
    Remove duplicate rows based on the given key column,
    keeping the first occurrence.

    Parameters
    ----------
    df  : DataFrame to deduplicate.
    key : Column name to deduplicate on.

    Returns
    -------
    (deduplicated_df, count_before, count_after)
    """
    count_before = df.count()
    df_dedup     = df.dropDuplicates([key])
    count_after  = df_dedup.count()
    dup_count    = count_before - count_after

    log(
        "DEDUP", f"Deduplication on '{key}'",
        before=count_before,
        after=count_after,
        duplicates_removed=dup_count,
    )
    return df_dedup, count_before, count_after