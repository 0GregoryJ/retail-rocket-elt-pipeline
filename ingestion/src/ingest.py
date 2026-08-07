"""
Orchestrate ingestion. Read csvs to DataFrames and write to PostgreSQL.
"""

from readers import read_events, read_category_tree, read_item_properties
from writers import ensure_raw_schema_exists, write_to_postgres
from spark import create_spark_session
from validation import validate_events, validate_category_tree, validate_item_properties

def main() -> None:
    # Create Spark session
    spark = create_spark_session()
    try:
        # Read csvs to DataFrames
        events = read_events(spark)
        category_tree = read_category_tree(spark)
        item_properties = read_item_properties(spark)

        # Validate DataFrames
        validate_events(events)
        validate_category_tree(category_tree)
        validate_item_properties(item_properties)
        
        # Write DataFrames to PostgreSQL
        ensure_raw_schema_exists()
        write_to_postgres(events, "events", "overwrite")
        write_to_postgres(category_tree, "category_tree", "overwrite")
        write_to_postgres(item_properties, "item_properties", "overwrite")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()