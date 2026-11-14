"""
B3a · Silver de maestros: customers_orgs y resources (E2-05).

    Toma un snapshot de cada maestro en Bronze, es decir, la partición ingest_date más reciente -> silver/<maestro>
    Una fila por clave natural, columnas de negocio con nombres y tipos estables, y el linaje de qué carga de Bronze la originó.

    Bronze guarda una foto por carga (D-11) para conservar la historia. 
    Para enriquecer eventos alcanza con el estado actual del maestro.
    Si más adelante hiciera falta el estado a una fecha (SCD tipo 2), la historia sigue en Bronze.

    tags_json llega como texto JSON (ej. '["env:prod", "pii:true"]') o vacío. 
    Se convierte en una lista `tags` S
    e extraen las etiquetas que el negocio usa: `env`, `team`, `costcenter` y `has_pii` 
    (gobierno: marca recursos con datos personales, sin exponer qué datos son).
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.io import replace_table
from src.common.runlog import RunLog

MASTERS = ["customers_orgs", "resources"]


def latest_snapshot(spark: SparkSession, bronze_table) -> DataFrame:
    """Filas de la partición ingest_date más reciente de una tabla de Bronze."""
    df = spark.read.parquet(str(bronze_table))
    last = df.agg(F.max("ingest_date")).first()[0]
    return df.where(F.col("ingest_date") == F.lit(last))


def _lineage(df: DataFrame, run_id: str) -> list:
    return ["source_file", F.col("ingest_date").alias("bronze_ingest_date"),
            F.current_timestamp().alias("silver_processed_ts"), F.lit(run_id).alias("silver_run_id")]


def conform_customers_orgs(df: DataFrame, run_id: str) -> DataFrame:
    trim = ["org_id", "org_name", "industry", "hq_region", "plan_tier", "sales_rep", "lifecycle_stage",
            "marketing_source"]
    return df.select(*[F.trim(c).alias(c) for c in trim], "is_enterprise", "signup_date", "nps_score",
                     *_lineage(df, run_id))


def _tag_value(prefix: str):
    """Valor de la primera etiqueta 'prefix:valor' de la lista `tags` (NULL si no hay)."""
    match = F.filter("tags", lambda t: t.startswith(prefix + ":"))
    return F.element_at(F.split(F.element_at(match, 1), ":", 2), 2)


def conform_resources(df: DataFrame, run_id: str) -> DataFrame:
    with_tags = df.withColumn("tags", F.coalesce(F.from_json("tags_json", "array<string>"),
                                                 F.array().cast("array<string>")))
    return with_tags.select(
        F.trim("resource_id").alias("resource_id"), F.trim("org_id").alias("org_id"), "service", "region",
        "created_at", "state", "tags",
        _tag_value("env").alias("env"), _tag_value("team").alias("team"),
        _tag_value("costcenter").alias("costcenter"),
        F.array_contains("tags", "pii:true").alias("has_pii"),
        *_lineage(with_tags, run_id))


CONFORM = {"customers_orgs": conform_customers_orgs, "resources": conform_resources}


def build_masters(spark: SparkSession, run: RunLog, bronze_root, silver_root) -> dict:
    """Conforma los maestros y devuelve los DataFrames escritos (leídos de Silver)."""
    out = {}
    for name in MASTERS:
        snapshot = latest_snapshot(spark, bronze_root / name)
        run.count(f"{name}.bronze_ultima_foto", snapshot.count())
        written = replace_table(CONFORM[name](snapshot, run.run_id), silver_root / name)
        run.count(f"{name}.silver", written)
        out[name] = spark.read.parquet(str(silver_root / name))
        run.count(f"{name}.claves_repetidas", written - out[name].select(*_key(name)).distinct().count())
        run.add_output(silver_root / name)
    return out


def _key(name: str) -> list:
    return ["org_id"] if name == "customers_orgs" else ["resource_id"]
