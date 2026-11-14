"""
B1 · Ingesta batch Landing → Bronze (E2-02).

Para cada fuente CSV:

  1. Lee el archivo con el lector común (texto, dialecto RFC 4180, encabezado validado).
  2. Agrega las columnas técnicas (source_file, arrival_ts, ingest_ts, run_id).
  3. Aplica reglas en orden; la primera que falla manda el registro a quarantine con su motivo:
       malformed_row  fila con más o menos columnas que el encabezado
       key_null       falta alguna columna de la clave natural
       cast_error     un valor presente no se puede convertir a su tipo (D-05)
       key_conflict   la misma clave aparece con contenidos distintos: no se sabe cuál es la buena
  4. Deduplica: las copias exactas de una fila se descartan y se cuentan (no son errores).
  5. Escribe Bronze tipado en <bronze>/<fuente>/ingest_date=AAAA-MM-DD y quarantine en
     <quarantine>/source=<fuente>/dt=AAAA-MM-DD, reemplazando solo la partición del día (idempotente).
  6. Concilia: leídas = aceptadas + quarantine + duplicados exactos.
"""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common import config
from src.common.io import CORRUPT, add_technical_columns, read_landing_csv, replace_partition
from src.common.runlog import RunLog
from src.common.schemas import BRONZE_TYPES, CSV_COLUMNS, NATURAL_KEYS

TECHNICAL = ["source_file", "arrival_ts", "ingest_ts", "run_id"]
RULES = {
    "malformed_row": "la fila no tiene la cantidad de columnas del encabezado",
    "key_null": "falta una columna de la clave natural",
    "cast_error": "valor no convertible al tipo de Bronze",
    "key_conflict": "la clave natural se repite con contenidos distintos",
}


def _cast_errors(columns, types):
    """Lista de columnas cuyo valor original existe pero no se pudo convertir."""
    checks = [F.when(F.col(c).isNotNull() & F.col(c).cast(t).isNull(), F.lit(c)) for c, t in types.items()]
    return F.filter(F.array(*checks), lambda x: x.isNotNull()) if checks else F.array().cast("array<string>")


def classify(raw: DataFrame, source: str) -> DataFrame:
    """Agrega `_rule` (primera regla que falla, o NULL si el registro es válido) y `_detail`."""
    columns, keys, types = CSV_COLUMNS[source], NATURAL_KEYS[source], BRONZE_TYPES.get(source, {})
    key_null = F.lit(False)
    for k in keys:
        key_null = key_null | F.col(k).isNull()
    df = raw.withColumn("_cast_errors", _cast_errors(columns, types))

    # key_conflict: misma clave con contenido distinto (se compara solo el contenido del archivo)
    content = F.sha2(F.to_json(F.struct(*columns)), 256)
    by_key = Window.partitionBy(*keys)
    df = df.withColumn("_variants", F.size(F.collect_set(content).over(by_key)))

    rule = (F.when(F.col(CORRUPT).isNotNull(), "malformed_row")
            .when(key_null, "key_null")
            .when(F.size("_cast_errors") > 0, "cast_error")
            .when(F.col("_variants") > 1, "key_conflict"))
    detail = (F.when(rule == "cast_error", F.concat(F.lit("columnas: "), F.array_join("_cast_errors", ", ")))
              .when(rule == "key_conflict", F.concat(F.lit("versiones distintas: "), F.col("_variants").cast("string"))))
    return df.withColumn("_rule", rule).withColumn("_detail", detail)


def to_quarantine(df: DataFrame, source: str) -> DataFrame:
    """Formato de quarantine: un registro rechazado con su regla, motivo, origen y la fila original."""
    keys, columns = NATURAL_KEYS[source], CSV_COLUMNS[source]
    reason = F.concat_ws(" · ", F.element_at(F.create_map(*[x for r, d in RULES.items() for x in (F.lit(r), F.lit(d))]),
                                             F.col("_rule")), F.col("_detail"))
    return df.select(
        F.lit(source).alias("source"),
        F.col("_rule").alias("rule_id"),
        reason.alias("reason"),
        F.concat_ws("|", *[F.coalesce(F.col(k), F.lit("∅")) for k in keys]).alias("record_key"),
        F.coalesce(F.col(CORRUPT), F.to_json(F.struct(*columns))).alias("raw_record"),
        *TECHNICAL,
    )


def to_bronze(df: DataFrame, source: str) -> DataFrame:
    """Filas válidas, deduplicadas, con cada columna en su tipo de Bronze y las columnas técnicas."""
    columns, types = CSV_COLUMNS[source], BRONZE_TYPES.get(source, {})
    typed = [F.col(c).cast(types[c]).alias(c) if c in types else F.col(c) for c in columns]
    return df.select(*typed, *TECHNICAL)


def load_source(spark: SparkSession, source: str, run: RunLog, landing=None, bronze_root=None,
                quarantine_root=None) -> dict:
    """Carga una fuente completa y devuelve sus conteos. Los parámetros de ruta permiten probar sin el lago real."""
    landing = Path(landing or config.LANDING)
    bronze_dir = Path(bronze_root or config.BRONZE) / source
    quarantine_dir = Path(quarantine_root or config.QUARANTINE) / f"source={source}"
    run.add_inputs([landing / f"{source}.csv"])

    raw = add_technical_columns(read_landing_csv(spark, source, landing=landing), run.run_id).cache()
    read_rows = raw.count()
    classified = classify(raw, source).cache()

    rejected = classified.where(F.col("_rule").isNotNull())
    valid = classified.where(F.col("_rule").isNull())
    deduped = valid.dropDuplicates(CSV_COLUMNS[source])         # copias exactas: misma fila, mismo contenido

    ingest_date = raw.select(F.date_format(F.to_date("ingest_ts"), "yyyy-MM-dd")).first()[0]
    replace_partition(to_bronze(deduped, source).withColumn("ingest_date", F.lit(ingest_date)),
                      bronze_dir, "ingest_date", ingest_date)
    replace_partition(to_quarantine(rejected, source).withColumn("dt", F.lit(ingest_date)),
                      quarantine_dir, "dt", ingest_date)

    counts = {
        "leidas": read_rows,
        "aceptadas": deduped.count(),
        "quarantine": rejected.count(),
        "duplicados_exactos": valid.count() - deduped.count(),
    }
    for rule_id in RULES:
        counts[f"quarantine.{rule_id}"] = rejected.where(F.col("_rule") == rule_id).count()
    written = spark.read.parquet(str(bronze_dir / f"ingest_date={ingest_date}")).count()
    counts["escritas_en_bronze"] = written

    for name, value in counts.items():
        run.count(name, value)
    run.param("ingest_date", ingest_date)
    run.add_output(bronze_dir)
    run.add_output(quarantine_dir)
    balanced = run.check_balance("leidas", ["aceptadas", "quarantine", "duplicados_exactos"])
    written_ok = run.check_balance("escritas_en_bronze", ["aceptadas"])
    raw.unpersist()
    classified.unpersist()
    if not (balanced and written_ok):
        raise RuntimeError(f"{source}: la conciliación no cierra (ver {run.path})")
    return counts
