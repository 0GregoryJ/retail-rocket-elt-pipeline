"""
Define raw data schemas for Spark.
"""

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

EVENTS_SCHEMA = StructType([
    StructField("timestamp", LongType(), nullable=False),
    StructField("visitorid", LongType(), nullable=False),
    StructField("event", StringType(), nullable=False),
    StructField("itemid", LongType(), nullable=True),
    StructField("transactionid", LongType(), nullable=True)
])

CATEGORY_TREE_SCHEMA = StructType([
    StructField("categoryid", LongType(), nullable=False),
    StructField("parentid", LongType(), nullable=True)
])

ITEM_PROPERTIES_SCHEMA = StructType([
    StructField("timestamp", LongType(), nullable=False),
    StructField("itemid", LongType(), nullable=False),
    StructField("property", StringType(), nullable=False),
    StructField("value", StringType(), nullable=True)
])