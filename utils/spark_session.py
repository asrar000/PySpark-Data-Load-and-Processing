"""
utils/spark_session.py
----------------------
SparkSession factory for the PySpark property pipeline.
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name):
    """
    Create and return a local SparkSession.

    Parameters
    ----------
    app_name : Name shown in the Spark UI.

    Returns
    -------
    SparkSession
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