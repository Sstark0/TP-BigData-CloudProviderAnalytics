"""Creación de la SparkSession con la configuración común del proyecto."""
import os
import time

from pyspark.sql import SparkSession


def get_spark(app_name: str = "cloud-provider-analytics") -> SparkSession:
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
