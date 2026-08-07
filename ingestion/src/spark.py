"""
Define create_spark_session function that creates or retrieves a Spark session.
"""

from pyspark.sql import SparkSession

def create_spark_session() -> SparkSession:
    #Creates or retrieves a Spark session.
    return (
        SparkSession.builder
        .appName("rocket_retail_ingestion")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.4",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )