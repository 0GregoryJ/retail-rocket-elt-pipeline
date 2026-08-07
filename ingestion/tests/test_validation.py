"""
Define test functions for ingestion validation functions. 
"""

from pathlib import Path

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from validation import (
    validate_category_tree,
    validate_events,
    validate_file_exists,
    validate_item_properties,
)


EVENTS_SCHEMA = StructType([
    StructField("timestamp", LongType(), True),
    StructField("visitorid", LongType(), True),
    StructField("event", StringType(), True),
    StructField("itemid", LongType(), True),
    StructField("transactionid", LongType(), True),
])

CATEGORY_SCHEMA = StructType([
    StructField("categoryid", LongType(), True),
    StructField("parentid", LongType(), True),
])

PROPERTIES_SCHEMA = StructType([
    StructField("timestamp", LongType(), True),
    StructField("itemid", LongType(), True),
    StructField("property", StringType(), True),
    StructField("value", StringType(), True),
])


def test_existing_file_passes(tmp_path: Path):
    file_path = tmp_path / "events.csv"
    file_path.write_text("test")

    validate_file_exists(str(file_path))


def test_missing_file_fails(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        validate_file_exists(str(tmp_path / "missing.csv"))


def test_valid_events_pass(spark):
    dataframe = spark.createDataFrame(
        [(1433221332117, 100, "view", 200, None)],
        schema=EVENTS_SCHEMA,
    )

    validate_events(dataframe)


def test_null_event_visitor_fails(spark):
    dataframe = spark.createDataFrame(
        [(1433221332117, None, "view", 200, None)],
        schema=EVENTS_SCHEMA,
    )

    with pytest.raises(ValueError, match="visitorid"):
        validate_events(dataframe)


def test_root_category_passes(spark):
    dataframe = spark.createDataFrame(
        [(10, None)],
        schema=CATEGORY_SCHEMA,
    )

    validate_category_tree(dataframe)


def test_valid_item_property_passes(spark):
    dataframe = spark.createDataFrame(
        [(1433221332117, 200, "available", "1")],
        schema=PROPERTIES_SCHEMA,
    )

    validate_item_properties(dataframe)