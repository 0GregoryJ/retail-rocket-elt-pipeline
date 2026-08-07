"""
Define ensure_raw_schema_exists function that creates or checks the raw schema in PostgreSQL,
and write_to_postgres function that writes a Dataframe to a PostgreSQL table in the raw schema.
"""

from pyspark.sql import DataFrame
import psycopg2

from config import (
    JDBC_URL,
    POSTGRES_USER,
    POSTGRES_PW,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)

def ensure_raw_schema_exists() -> None:
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PW,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE SCHEMA IF NOT EXISTS raw;"
            )

        connection.commit()
    finally:
        connection.close()


def write_to_postgres(
    dataframe: DataFrame,
    table_name: str,
    mode: str = "overwrite",
):
    (
        dataframe.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"raw.{table_name}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PW)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "10000")
        .mode(mode)
        .save()
    )
    print(f"Successfully wrote {dataframe.count()} rows to {table_name} in the raw schema.")
    