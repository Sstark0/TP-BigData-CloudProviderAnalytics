"""
G1 · Mart FinOps org_daily_usage_by_service (E2-07).

    Una fila por (org_id, usage_date, service), con usage_date = día UTC del evento (consigna §6.2 y §7.3).

    Contiene
    - Atributos de la organización para servir sin joins en Cassandra: org_name, plan_tier, industry.
    - Métricas y costos del día: daily_cost_usd (decimal exacto, D-14), requests, cpu_hours, storage_gb_hours,
      genai_tokens, carbon_kg, events, resources_active (recursos distintos con uso ese día).
    - Indicadores de calidad (D-15) para que FinOps vea qué tan confiable es cada cifra: negative_cost_events y
      negative_cost_usd (costos marcados), unit_inferred_events, late_events (recuperados por la reconciliación,
      D-13) y carbon_coverage.
    - Linaje: gold_processed_ts y gold_run_id.


    Sale de silver/usage_events_enriched (E2-05/E2-06). 
    La tabla se recalcula entera en cada ejecución (replace_table). Depende solo de Silver, así que re-ejecutar da el mismo contenido. 
    `fingerprint` es una huella del contenido (claves + costo + requests + eventos) que debe coincidir entre ejecuciones.

    Partición
    usage_date, un archivo por día (60 archivos). 
    Coincide con E1 (`org_daily_usage_by_service/dt=`) y con las consultas por rango de fechas; un día se puede reprocesar o borrar sin tocar los demás.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.io import replace_table
from src.common.runlog import RunLog

PROCESS = "g1_gold_finops"
TABLE = "org_daily_usage_by_service"
KEY = ["org_id", "usage_date", "service"]
COST = "decimal(18,6)"


def org_daily_usage_by_service(enriched: DataFrame, run_id: str) -> DataFrame:
    return (enriched.withColumnRenamed("event_date", "usage_date")
            .groupBy(*KEY)
            .agg(F.first("org_name", ignorenulls=True).alias("org_name"),
                 F.first("plan_tier", ignorenulls=True).alias("plan_tier"),
                 F.first("industry", ignorenulls=True).alias("industry"),
                 F.sum("cost_usd").cast(COST).alias("daily_cost_usd"),
                 F.sum("requests").alias("requests"),
                 F.sum("cpu_hours").alias("cpu_hours"),
                 F.sum("storage_gb_hours").alias("storage_gb_hours"),
                 F.sum("genai_tokens").alias("genai_tokens"),
                 F.round(F.sum("carbon_kg"), 6).alias("carbon_kg"),
                 F.count(F.lit(1)).alias("events"),
                 F.countDistinct("resource_id").alias("resources_active"),
                 F.sum(F.col("cost_negative").cast("int")).alias("negative_cost_events"),
                 F.coalesce(F.sum(F.when(F.col("cost_negative"), F.col("cost_usd"))),
                            F.lit(0).cast(COST)).cast(COST).alias("negative_cost_usd"),
                 F.sum(F.col("unit_inferred").cast("int")).alias("unit_inferred_events"),
                 F.sum((F.col("arrival_path") == "tardio").cast("int")).alias("late_events"),
                 F.round(F.count("carbon_kg") / F.count(F.lit(1)), 4).alias("carbon_coverage"))
            .withColumn("gold_processed_ts", F.current_timestamp())
            .withColumn("gold_run_id", F.lit(run_id)))


def fingerprint(df: DataFrame) -> int:
    """Huella del contenido: igual entre dos ejecuciones si y solo si (con altísima probabilidad) el contenido es igual."""
    h = F.hash(*KEY, F.col("daily_cost_usd").cast("string"), F.col("requests"), F.col("events"))
    return int(df.agg(F.sum(h.cast("long"))).first()[0] or 0)


def _micro_usd(df: DataFrame, column: str) -> int:
    total = df.agg(F.sum(column)).first()[0]
    return int(round(total * 1_000_000)) if total is not None else 0


def reference_queries(gold: DataFrame, org_id: str, days: int = 14, top_n: int = 5) -> dict:
    """Las dos consultas de la consigna (§7.4) calculadas en Spark, para comparar con el resultado de CQL (E2-08).

    1. Costos y requests diarios de una organización por servicio en un rango de fechas (últimos 7 días del dataset).
    2. Top-N servicios por costo acumulado en los últimos `days` días para esa organización.
    El "hoy" es el último usage_date del dataset (31/08/2025), no la fecha real: los datos son históricos.
    """
    last = gold.agg(F.max("usage_date")).first()[0]
    org = gold.where(F.col("org_id") == org_id)
    q1 = (org.where(F.col("usage_date") > F.date_sub(F.lit(last), 7))
          .select("usage_date", "service", "daily_cost_usd", "requests").orderBy("usage_date", "service"))
    q2 = (org.where(F.col("usage_date") > F.date_sub(F.lit(last), days))
          .groupBy("service").agg(F.sum("daily_cost_usd").alias("cost_usd"), F.sum("requests").alias("requests"))
          .orderBy(F.desc("cost_usd")).limit(top_n))

    def rows(df):
        return [{k: (v if isinstance(v, (int, float, type(None))) else str(v)) for k, v in r.asDict().items()}
                for r in df.collect()]

    return {"org_id": org_id, "ultimo_dia_del_dataset": str(last),
            "q1_costos_y_requests_ultimos_7_dias": rows(q1),
            f"q2_top{top_n}_servicios_ultimos_{days}_dias": rows(q2)}


def build_gold_finops(spark: SparkSession, run: RunLog, enriched: DataFrame, features: DataFrame, gold_path) -> DataFrame:
    run.count("silver_eventos", enriched.count())
    run.count("gold_filas", replace_table(org_daily_usage_by_service(enriched, run.run_id), gold_path,
                                          partition_by=["usage_date"]))
    gold = spark.read.parquet(str(gold_path))

    run.count("gold_claves_distintas", gold.select(*KEY).distinct().count())
    run.count("gold_eventos", gold.agg(F.sum("events")).first()[0] or 0)
    run.count("costo_silver_micro_usd", _micro_usd(enriched, "cost_usd"))
    run.count("costo_gold_micro_usd", _micro_usd(gold, "daily_cost_usd"))
    run.count("requests_silver", int(enriched.agg(F.sum("requests")).first()[0] or 0))
    run.count("requests_gold", int(gold.agg(F.sum("requests")).first()[0] or 0))
    # Cruce con las features de Silver: por organización y día, Gold (sumando servicios) = org_daily_features
    by_org_day = gold.groupBy("org_id", "usage_date").agg(F.sum("daily_cost_usd").alias("gold_cost"))
    mismatches = (by_org_day.join(features.withColumnRenamed("event_date", "usage_date"), ["org_id", "usage_date"],
                                  "full_outer")
                  .where(F.coalesce("gold_cost", F.lit(0)) != F.coalesce("daily_cost_usd", F.lit(0))).count())
    run.count("org_dia_distintos_de_features", mismatches)
    run.count("particiones_usage_date", gold.select("usage_date").distinct().count())
    run.count("fingerprint", fingerprint(gold))
    run.add_output(gold_path)

    ok = all([run.check_balance("gold_filas", ["gold_claves_distintas"]),
              run.check_balance("silver_eventos", ["gold_eventos"]),
              run.check_balance("costo_silver_micro_usd", ["costo_gold_micro_usd"]),
              run.check_balance("requests_silver", ["requests_gold"]),
              mismatches == 0])
    if not ok:
        raise RuntimeError("Gold FinOps no concilia con Silver; ver el registro de la ejecución")
    return gold
