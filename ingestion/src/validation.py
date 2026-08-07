"""
Define functions validate_events, validate_category_tree, and validate_item_properties to validate data before writing to PostgreSQL.
"""

import os
from pyspark.sql import DataFrame, functions as F

# File exists
def validate_file_exists(
    file_path: str
):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found")

# File has required columns
def validate_required_columns(
    dataframe: DataFrame,
    required_columns: set[str],
) -> None:
    actual_columns = set(dataframe.columns)

    missing_columns = required_columns - actual_columns

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )
    
# File has required not null columns
def validate_required_not_null(
    dataframe: DataFrame,
    required_not_nulls: set[str],
) -> None:
    for column in required_not_nulls:
        null_count = (
            dataframe
            .filter(F.col(column).isNull())
            .limit(1)
            .count()
        )

        if null_count > 0:
            raise ValueError(
                f"Required not nullcolumn '{column}' contains NULL values."
            )

# Validate events
def validate_events(
    dataframe: DataFrame
):
    validate_required_columns(
        dataframe,
        {
            "timestamp",
            "visitorid",
            "event",
            "itemid",
            "transactionid",
        }
    )
    validate_required_not_null(
        dataframe,
        {
            "timestamp",
            "visitorid",
            "event",
        }
    )

# Validate category tree
def validate_category_tree(
    dataframe: DataFrame
):
    validate_required_columns(
        dataframe,
        {
            "categoryid",
            "parentid",
        }
    )
    validate_required_not_null(
        dataframe,
        {"categoryid"}
    )

# Validate item properties
def validate_item_properties(
    dataframe: DataFrame
):
    validate_required_columns(
        dataframe,
        {
            "timestamp",
            "itemid",
            "property",
            "value",
        }
    )
    validate_required_not_null(
        dataframe,
        {
            "timestamp",
            "itemid",
            "property",
        }
    )