"""
B2 · Silver canónico de eventos: reconciliación batch desde Bronze completo (E2-04).

    Recalcula silver/usage_events desde todos los lotes completos de Bronze (los que tienen manifiesto, D-12):
      1. Reglas de rechazo → quarantine/source=usage_events_silver/ (una fila por registro, con motivo).
      2. Deduplicación por event_id (D-06): se queda la primera llegada (menor batch_id, luego ingest_ts).
      3. Marca de camino: 
        3.1. `arrival_path = 'a_tiempo'` si el evento salió por el stream (S2)
        3.2. `'tardio'` si el watermark lo descartó y lo recupera esta reconciliación.
      4. Escribe Silver particionado por event_date (fecha del evento, UTC), reemplazando la tabla completa.

    D-04: el histórico canónico no sale de la ventana de streaming, sino de Bronze completo. 
    Con 43.200 eventos, recalcular es más simple y más seguro que actualizar particiones sueltas.
    El resultado depende solo de Bronze, así que re-ejecutar da exactamente lo mismo.

    RULES es la lista de reglas de calidad de Silver. 
        E2-04 activa solo la que la deduplicación necesita (event_id nulo)
        E2-06 agrega las demás a esta misma lista.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.io import replace_table
from src.common.runlog import RunLog

PROCESS = "b2_silver_events"
SILVER_SOURCE = "usage_events_silver"

# (rule_id, condición SQL que hace FALLAR la regla, motivo legible). Se evalúan en orden; la primera que falla manda.
RULES = [
    ("event_id_null", "event_id IS NULL", "event_id nulo: no se puede deduplicar ni rastrear el evento"),
]

SILVER_COLUMNS = ["event_id", "event_ts", "org_id", "resource_id", "service", "region", "metric", "value", "unit",
                  "cost_usd_increment", "schema_version", "carbon_kg", "genai_tokens"]


def apply_rules(bronze: DataFrame) -> DataFrame:
    """Agrega `_rule` y `_reason` con la primera regla que falla (NULL si pasa todas)."""
    rule, reason = F.lit(None).cast("string"), F.lit(None).cast("string")
    for rule_id, fails, text in reversed(RULES):
        rule = F.when(F.expr(fails), F.lit(rule_id)).otherwise(rule)
        reason = F.when(F.expr(fails), F.lit(text)).otherwise(reason)
    return bronze.withColumn("_rule", rule).withColumn("_reason", reason)


def to_quarantine(rejected: DataFrame, run_id: str) -> DataFrame:
    return rejected.select(
        F.lit("usage_events").alias("source"), F.col("_rule").alias("rule_id"), F.col("_reason").alias("reason"),
        F.coalesce("event_id", F.lit("∅")).alias("record_key"),
        F.to_json(F.struct(*SILVER_COLUMNS)).alias("raw_record"),
        "source_file", F.col("batch_id").alias("bronze_batch_id"),
        F.current_timestamp().alias("silver_processed_ts"), F.lit(run_id).alias("run_id"),
        F.col("ingest_date").alias("dt"))


def deduplicate(valid: DataFrame) -> DataFrame:
    """Un evento por event_id: la primera llegada a Bronze. Agrega `_copies` (cuántas veces llegó)."""
    by_id = Window.partitionBy("event_id")
    first = by_id.orderBy("batch_id", "ingest_ts", "source_file")
    return (valid.withColumn("_rank", F.row_number().over(first))
                 .withColumn("_copies", F.count(F.lit(1)).over(by_id))
                 .where("_rank = 1").drop("_rank"))


def to_silver(unique: DataFrame, live_ids: DataFrame, run_id: str) -> DataFrame:
    flagged = unique.join(live_ids.withColumn("_live", F.lit(True)), "event_id", "left")
    return flagged.select(
        *SILVER_COLUMNS,
        F.when(F.col("_live"), "a_tiempo").otherwise("tardio").alias("arrival_path"),
        "source_file", "arrival_ts", F.col("ingest_ts").alias("bronze_ingest_ts"),
        F.col("batch_id").alias("bronze_batch_id"), F.col("_copies").alias("copies_in_bronze"),
        F.current_timestamp().alias("silver_processed_ts"), F.lit(run_id).alias("silver_run_id"),
        "event_date")


def reconcile(spark: SparkSession, run: RunLog, bronze: DataFrame, live_path, silver_path, quarantine_path) -> dict:
    """Recalcula Silver y su quarantine desde `bronze` (solo lotes completos). Devuelve los conteos."""
    bronze = apply_rules(bronze).cache()
    try:
        live_ids = spark.read.parquet(str(live_path)).select("event_id").distinct()
    except Exception:                       # el stream todavía no escribió nada: no hay eventos a tiempo
        live_ids = spark.createDataFrame([], "event_id string")

    unique = deduplicate(bronze.where(F.col("_rule").isNull())).cache()
    silver = to_silver(unique, live_ids, run.run_id)
    quarantine = to_quarantine(bronze.where(F.col("_rule").isNotNull()), run.run_id)

    run.count("bronze_eventos", bronze.count())
    run.count("silver_eventos", replace_table(silver, silver_path, partition_by=["event_date"]))
    run.count("quarantine_eventos", replace_table(quarantine, quarantine_path, partition_by=["dt"]))
    for rule_id, _, _ in RULES:
        run.count(f"quarantine.{rule_id}", bronze.where(F.col("_rule") == rule_id).count())
    run.count("duplicados_event_id", unique.agg(F.sum(F.col("_copies") - 1)).first()[0] or 0)

    written = spark.read.parquet(str(silver_path))
    run.count("silver_a_tiempo", written.where("arrival_path = 'a_tiempo'").count())
    run.count("silver_tardios_recuperados", written.where("arrival_path = 'tardio'").count())
    run.count("silver_event_id_repetidos", written.count() - written.select("event_id").distinct().count())
    run.count("silver_particiones_event_date", written.select("event_date").distinct().count())
    run.add_output(silver_path)
    bronze.unpersist()
    unique.unpersist()

    ok = (run.check_balance("bronze_eventos", ["silver_eventos", "quarantine_eventos", "duplicados_event_id"])
          and run.check_balance("silver_eventos", ["silver_a_tiempo", "silver_tardios_recuperados"])
          and run.data["counts"]["silver_event_id_repetidos"] == 0)
    if not ok:
        raise RuntimeError("Silver de eventos no concilia con Bronze; ver el registro de la ejecución")
    return run.data["counts"]
