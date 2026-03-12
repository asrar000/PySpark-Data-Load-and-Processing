"""
utils/joins.py
--------------
Join logic for the PySpark property pipeline.
Produces matched (inner join) and unmatched (left anti join) DataFrames.
"""

from logger import log


def build_matched_unmatched(details_df, search_df):
    """
    Perform an INNER JOIN and a LEFT ANTI JOIN between details and search.

    Join key: details.source_id == search.search_id

    Parameters
    ----------
    details_df : Cleaned and deduplicated details DataFrame.
    search_df  : Extracted search DataFrame.

    Returns
    -------
    (matched_df, unmatched_df)
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

    log(
        "JOIN", "Join complete",
        matched=matched.count(),
        unmatched=unmatched.count(),
    )
    return matched, unmatched