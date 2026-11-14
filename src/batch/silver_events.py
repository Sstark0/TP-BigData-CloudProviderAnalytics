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

    Reglas de calidad (E2-06, D-15): REJECT_RULES sacan el evento de Silver; FIX_RULES lo conservan corregido
    o marcado. Todas dejan una fila con motivo y acción en quarantine/source=usage_events_silver/, que funciona
    como registro de calidad: los rechazos reales son las filas con action = 'rechazado' (o 'descartado').
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.io import replace_table
from src.common.runlog import RunLog

PROCESS = "b2_silver_events"
SILVER_SOURCE = "usage_events_silver"

# Unidad que corresponde a cada métrica (perfil de E1: ningún evento con unidad trae otra combinación)
EXPECTED_UNIT = {"requests": "count", "cpu_hours": "hours", "storage_gb_hours": "gb_hours"}
COST_TOLERANCE_USD = -0.01
_METRICS = ", ".join(f"'{m}'" for m in EXPECTED_UNIT)
_EXPECTED = "CASE metric " + " ".join(f"WHEN '{m}' THEN '{u}'" for m, u in EXPECTED_UNIT.items()) + " END"

# Reglas de calidad de Silver (E2-06). Cada regla termina en una ACCIÓN, que queda registrada en quarantine:
#   rechazado   el evento sale de Silver (no se puede usar)
#   descartado  copia repetida de un event_id: se conserva la primera llegada y las demás salen
#   corregido   el evento sigue en Silver con el dato reparado (y se registra qué se reparó)
#   marcado     el evento sigue en Silver tal cual, con una bandera para que Gold y FinOps lo vean
# (rule_id, condición SQL que hace FALLAR la regla, motivo legible). Se evalúan en orden; la primera que falla manda.
REJECT_RULES = [
    ("event_id_null", "event_id IS NULL", "event_id nulo: no se puede deduplicar ni rastrear el evento"),
    ("unit_mismatch", f"unit IS NOT NULL AND metric IN ({_METRICS}) AND unit <> {_EXPECTED}",
     "la unidad no corresponde a la métrica"),
    ("unit_missing_uninferable", f"unit IS NULL AND value IS NOT NULL AND (metric IS NULL OR metric NOT IN ({_METRICS}))",
     "unidad vacía y la métrica no permite inferirla"),
]
# (rule_id, acción, condición SQL, motivo). Se aplican a los eventos únicos; un evento puede cumplir varias.
FIX_RULES = [
    ("unit_missing", "corregido", f"unit IS NULL AND value IS NOT NULL AND metric IN ({_METRICS})",
     "unidad vacía: se completa a partir de la métrica"),
    ("cost_below_tolerance", "marcado", f"cost_usd_increment < {COST_TOLERANCE_USD}",
     f"costo menor a {COST_TOLERANCE_USD} USD: se conserva y se marca para revisión"),
]
RULES = REJECT_RULES + [(r, c, t) for r, _, c, t in FIX_RULES] + [
    ("event_id_duplicate", "", "copia repetida de un event_id: se conserva la primera llegada")]

SILVER_COLUMNS = ["event_id", "event_ts", "org_id", "resource_id", "service", "region", "metric", "value", "unit",
                  "cost_usd_increment", "schema_version", "carbon_kg", "genai_tokens"]


def apply_rules(bronze: DataFrame) -> DataFrame:
    """Agrega `_rule` y `_reason` con la primera regla de RECHAZO que falla (NULL si pasa todas)."""
    rule, reason = F.lit(None).cast("string"), F.lit(None).cast("string")
    for rule_id, fails, text in reversed(REJECT_RULES):
        rule = F.when(F.expr(fails), F.lit(rule_id)).otherwise(rule)
        reason = F.when(F.expr(fails), F.lit(text)).otherwise(reason)
    return bronze.withColumn("_rule", rule).withColumn("_reason", reason)


def to_quarantine(df: DataFrame, action: str, run_id: str) -> DataFrame:
    """Una fila del registro de calidad por (evento, regla). `df` trae `_rule` y `_reason`."""
    return df.select(
        F.lit("usage_events").alias("source"), F.col("_rule").alias("rule_id"), F.lit(action).alias("action"),
        F.col("_reason").alias("reason"), F.coalesce("event_id", F.lit("∅")).alias("record_key"),
        F.to_json(F.struct(*SILVER_COLUMNS)).alias("raw_record"),
        "source_file", F.col("batch_id").alias("bronze_batch_id"),
        F.current_timestamp().alias("silver_processed_ts"), F.lit(run_id).alias("run_id"),
        F.col("ingest_date").alias("dt"))


def deduplicate(valid: DataFrame) -> DataFrame:
    """Numera las copias de cada event_id por orden de llegada (`_rank` = 1 es la que queda) y cuenta `_copies`."""
    by_id = Window.partitionBy("event_id")
    first = by_id.orderBy("batch_id", "ingest_ts", "source_file")
    return (valid.withColumn("_rank", F.row_number().over(first))
                 .withColumn("_copies", F.count(F.lit(1)).over(by_id)))


def apply_fixes(unique: DataFrame) -> DataFrame:
    """Corrige y marca los eventos únicos. Agrega `_fixes`: lista de (regla, acción, motivo) que cumplió."""
    hits = [F.when(F.expr(cond), F.struct(F.lit(r).alias("rule"), F.lit(a).alias("action"), F.lit(t).alias("reason")))
            for r, a, cond, t in FIX_RULES]
    return (unique.withColumn("_fixes", F.filter(F.array(*hits), lambda h: h.isNotNull()))
                  .withColumn("unit_inferred", F.expr(FIX_RULES[0][2]))
                  .withColumn("cost_negative", F.expr(FIX_RULES[1][2]))
                  .withColumn("_raw_unit", F.col("unit"))
                  .withColumn("unit", F.when(F.col("unit_inferred"), F.expr(_EXPECTED)).otherwise(F.col("unit"))))


def to_silver(unique: DataFrame, live_ids: DataFrame, run_id: str) -> DataFrame:
    flagged = unique.join(live_ids.withColumn("_live", F.lit(True)), "event_id", "left")
    return flagged.select(
        *SILVER_COLUMNS, "unit_inferred", "cost_negative",
        F.when(F.col("_live"), "a_tiempo").otherwise("tardio").alias("arrival_path"),
        "source_file", "arrival_ts", F.col("ingest_ts").alias("bronze_ingest_ts"),
        F.col("batch_id").alias("bronze_batch_id"), F.col("_copies").alias("copies_in_bronze"),
        F.current_timestamp().alias("silver_processed_ts"), F.lit(run_id).alias("silver_run_id"),
        "event_date")


def reconcile(spark: SparkSession, run: RunLog, bronze: DataFrame, live_path, silver_path, quarantine_path) -> dict:
    """Recalcula Silver y su registro de calidad desde `bronze` (solo lotes completos). Devuelve los conteos."""
    bronze = apply_rules(bronze).cache()
    try:
        live_ids = spark.read.parquet(str(live_path)).select("event_id").distinct()
    except Exception:                       # el stream todavía no escribió nada: no hay eventos a tiempo
        live_ids = spark.createDataFrame([], "event_id string")

    ranked = deduplicate(bronze.where(F.col("_rule").isNull()))
    unique = apply_fixes(ranked.where("_rank = 1")).cache()
    duplicates = (ranked.where("_rank > 1").withColumn("_rule", F.lit("event_id_duplicate"))
                        .withColumn("_reason", F.lit(RULES[-1][2])))
    fixes = (unique.withColumn("_fix", F.explode("_fixes"))
                   .withColumn("_rule", F.col("_fix.rule")).withColumn("_reason", F.col("_fix.reason"))
                   .withColumn("unit", F.col("_raw_unit")))             # en el registro va el dato original
    quality_log = (to_quarantine(bronze.where(F.col("_rule").isNotNull()), "rechazado", run.run_id)
                   .unionByName(to_quarantine(duplicates, "descartado", run.run_id))
                   .unionByName(to_quarantine(fixes.where("_fix.action = 'corregido'"), "corregido", run.run_id))
                   .unionByName(to_quarantine(fixes.where("_fix.action = 'marcado'"), "marcado", run.run_id)))

    run.count("bronze_eventos", bronze.count())
    run.count("silver_eventos", replace_table(to_silver(unique, live_ids, run.run_id), silver_path,
                                              partition_by=["event_date"]))
    run.count("quarantine_registros", replace_table(quality_log, quarantine_path, partition_by=["dt"]))
    log = spark.read.schema(quality_log.schema).parquet(str(quarantine_path))   # esquema explícito: puede estar vacío
    by_action = {r["action"]: r["count"] for r in log.groupBy("action").count().collect()}
    run.count("rechazados", by_action.get("rechazado", 0))
    run.count("duplicados_event_id", by_action.get("descartado", 0))
    run.count("corregidos", by_action.get("corregido", 0))
    run.count("marcados", by_action.get("marcado", 0))
    by_rule = {r["rule_id"]: r["count"] for r in log.groupBy("rule_id").count().collect()}
    for rule_id, _, _ in RULES:
        run.count(f"quarantine.{rule_id}", by_rule.get(rule_id, 0))

    written = spark.read.parquet(str(silver_path))
    run.count("silver_a_tiempo", written.where("arrival_path = 'a_tiempo'").count())
    run.count("silver_tardios_recuperados", written.where("arrival_path = 'tardio'").count())
    run.count("silver_event_id_repetidos", written.count() - written.select("event_id").distinct().count())
    run.count("silver_unit_inferida", written.where("unit_inferred").count())
    run.count("silver_costo_negativo_marcado", written.where("cost_negative").count())
    run.count("silver_unit_nula_con_value", written.where("unit IS NULL AND value IS NOT NULL").count())
    run.count("silver_particiones_event_date", written.select("event_date").distinct().count())
    run.add_output(silver_path)
    run.add_output(quarantine_path)
    bronze.unpersist()
    unique.unpersist()

    ok = all([
        run.check_balance("bronze_eventos", ["silver_eventos", "rechazados", "duplicados_event_id"]),
        run.check_balance("silver_eventos", ["silver_a_tiempo", "silver_tardios_recuperados"]),
        run.check_balance("quarantine_registros", ["rechazados", "duplicados_event_id", "corregidos", "marcados"]),
        run.check_balance("corregidos", ["silver_unit_inferida"]),
        run.check_balance("marcados", ["silver_costo_negativo_marcado"]),
        run.data["counts"]["silver_event_id_repetidos"] == 0,
        run.data["counts"]["silver_unit_nula_con_value"] == 0,
    ])
    if not ok:
        raise RuntimeError("Silver de eventos no concilia con Bronze; ver el registro de la ejecución")
    return run.data["counts"]