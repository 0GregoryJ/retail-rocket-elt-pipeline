import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("validation-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield session

    session.stop()