import os
import sys

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    # Make Spark's Python workers use the same interpreter as the test run.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    session = (
        SparkSession.builder.master("local[1]")
        .appName("o2c-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        # Databricks serverless runs with ANSI mode on; test under the same rules.
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()
