"""Creación de la SparkSession con la configuración común del proyecto."""
import os
import sys
import time
import warnings

from pyspark.sql import SparkSession


def _prepare_environment() -> None:
    """Ajustes de entorno previos a arrancar Spark.

    - Zona horaria UTC en el proceso de Python (Spark además la fija en la JVM).
    - PYSPARK_PYTHON: Spark lanza procesos "worker" de Python con el comando `python`. En Windows ese
      comando puede resolver al acceso directo de Microsoft Store en lugar del Python del entorno
      virtual, y los workers no arrancan. Apuntarlo al intérprete actual garantiza que driver y workers usen el mismo Python y las mismas librerías.
    - En Windows, escribir archivos necesita winutils.exe (HADOOP_HOME)

    """

    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    if os.name == "nt" and not os.getenv("HADOOP_HOME"):
        warnings.warn("Windows sin HADOOP_HOME: Spark puede fallar al escribir Parquet. "
                      "Ver README, sección 'Windows'.", RuntimeWarning, stacklevel=3)


def get_spark(app_name: str = "cloud-provider-analytics") -> SparkSession:
    _prepare_environment()
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