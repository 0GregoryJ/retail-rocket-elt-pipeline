"""
Define read_events, read_item_properties, read_category_tree functions that read csvs to DataFrames.
"""

from config import RAW_DATA_DIR
from pyspark.sql import SparkSession, DataFrame
from schemas import EVENTS_SCHEMA, CATEGORY_TREE_SCHEMA, ITEM_PROPERTIES_SCHEMA
from validation import validate_file_exists

    
def read_events(spark: SparkSession) -> DataFrame:
    # Validate
    validate_file_exists(f"{RAW_DATA_DIR}/events.csv")
    # Read events.csv into a DataFrame
    return (
        spark.read
        .option("header", True)
        .option("mode", "FAILFAST")
        .schema(EVENTS_SCHEMA)
        .csv(f"{RAW_DATA_DIR}/events.csv")
    )

def read_category_tree(spark: SparkSession) -> DataFrame:
    # Validate
    validate_file_exists(f"{RAW_DATA_DIR}/category_tree.csv")
    # Read events.csv into a DataFrame
    return (
        spark.read
        .option("header", True)
        .option("mode", "FAILFAST")
        .schema(CATEGORY_TREE_SCHEMA)
        .csv(f"{RAW_DATA_DIR}/category_tree.csv")
    )

def read_item_properties(spark: SparkSession) -> DataFrame:
    # Validate
    validate_file_exists(f"{RAW_DATA_DIR}/item_properties_part1.csv")
    validate_file_exists(f"{RAW_DATA_DIR}/item_properties_part2.csv")
    # Read events.csv into a DataFrame
    return (
        spark.read
        .option("header", True)
        .option("mode", "FAILFAST")
        .schema(ITEM_PROPERTIES_SCHEMA)
        .csv([f"{RAW_DATA_DIR}/item_properties_part1.csv", f"{RAW_DATA_DIR}/item_properties_part2.csv"])
    )