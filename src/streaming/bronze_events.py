"""
S1 · Ingesta streaming de eventos Landing → Bronze (E2-03).

Qué hace
    Lee usage_events_stream/*.jsonl con Structured Streaming y esquema explícito (superset v1/v2, D-05),
    tipa los campos, agrega linaje y escribe Bronze SIN filtro temporal: todo evento que se pueda leer
    queda en Bronze, llegue tarde o no (D-04). El watermark y la deduplicación se aplican en Silver (E2-04).

Cómo es idempotente (D-04 y D-12)
    Cada micro-lote se escribe con foreachBatch en su propia carpeta:
        bronze/usage_events/ingest_date=AAAA-MM-DD/batch_id=N/
    y recién al terminar se registra en un manifiesto (_runs/s1_bronze_events/batches/batch_id=N.json).
    - Si Spark re-ejecuta un lote (por ejemplo, tras un corte antes de confirmar el checkpoint), ese lote
      sobrescribe su propia carpeta: no se duplican filas.
    - Un lote sin manifiesto es un lote incompleto: los lectores lo ignoran (read_bronze_events).
    - El checkpoint recuerda qué archivos ya se leyeron: re-arrancar la consulta no vuelve a leerlos.

Qué va a quarantine (quarantine/source=usage_events/dt=AAAA-MM-DD/batch_id=N/)
    Solo errores estructurales, que impiden ubicar o interpretar el evento:
      malformed_json     la línea no es un JSON válido
      timestamp_invalid  falta el timestamp o no se puede interpretar
      cast_error         value, cost_usd_increment, carbon_kg o genai_tokens presente pero no numérico
    Las reglas de calidad de negocio (event_id nulo o duplicado, costo negativo, unit faltante) se aplican
    en Silver (E2-06): Bronze conserva el dato tal como llegó.
"""
import json
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import config
from src.common.io import CORRUPT
from src.common.runlog import get_logger, utc_now
from src.common.schemas import EVENTS_RAW_SCHEMA

QUERY_ID = "s1_bronze_events"
TABLE = "usage_events"
NUMERIC = {"value": "double", "cost_usd_increment": "double", "carbon_kg": "double", "genai_tokens": "long"}
RAW_FIELDS = [f.name for f in EVENTS_RAW_SCHEMA.fields if f.name != CORRUPT]
RULES = {
    "malformed_json": "la línea no es un JSON válido",
    "timestamp_invalid": "falta el timestamp o no se puede interpretar",
    "cast_error": "valor numérico no convertible",
}
log = get_logger(QUERY_ID)


class Paths:
    """Rutas del proceso. Se pueden cambiar todas juntas para las pruebas."""

    def __init__(self, landing=None, root=None):
        root = Path(root) if root else config.DATALAKE_ROOT
        self.landing = Path(landing) if landing else config.EVENTS_DIR
        self.bronze = root / "bronze" / TABLE
        self.quarantine = root / "quarantine" / "source=usage_events"
        self.checkpoint = root / "_checkpoints" / QUERY_ID
        self.manifest = root / "_runs" / QUERY_ID / "batches"


def read_stream(spark: SparkSession, paths: Paths, max_files: int) -> DataFrame:
    """Fuente streaming de archivos JSONL con esquema explícito.

    maxFilesPerTrigger limita cuántos archivos nuevos entra en cada micro-lote. Spark toma los archivos en
    orden de fecha de modificación y recuerda en el checkpoint cuáles ya procesó.
    """
    return (spark.readStream
            .schema(EVENTS_RAW_SCHEMA)
            .option("maxFilesPerTrigger", max_files)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT)
            .json(str(paths.landing))
            .withColumn("source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
            .withColumn("arrival_ts", F.col("_metadata.file_modification_time")))


def classify(df: DataFrame) -> DataFrame:
    """Agrega event_ts, los numéricos tipados y `_rule` (primera regla que falla, o NULL)."""
    event_ts = F.to_timestamp("timestamp")
    bad_casts = F.filter(F.array(*[F.when(F.col(c).isNotNull() & F.col(c).cast(t).isNull(), F.lit(c))
                                   for c, t in NUMERIC.items()]), lambda x: x.isNotNull())
    rule = (F.when(F.col(CORRUPT).isNotNull(), "malformed_json")
            .when(event_ts.isNull(), "timestamp_invalid")
            .when(F.size(bad_casts) > 0, "cast_error"))
    detail = F.when(rule == "cast_error", F.concat(F.lit("columnas: "), F.array_join(bad_casts, ", ")))
    return (df.withColumn("event_ts", event_ts)
              .withColumn("_rule", rule)
              .withColumn("_detail", detail))


def to_bronze(df: DataFrame, batch_id: int, run_id: str, ingest_date: str) -> DataFrame:
    typed = [F.col(c).cast(NUMERIC[c]).alias(c) if c in NUMERIC else F.col(c)
             for c in RAW_FIELDS if c != "timestamp"]
    return df.select(
        *typed[:1], "event_ts", F.to_date("event_ts").alias("event_date"), *typed[1:],
        "source_file", "arrival_ts",
        F.current_timestamp().alias("ingest_ts"), F.lit(run_id).alias("run_id"),
        F.lit(batch_id).cast("long").alias("batch_id"), F.lit(ingest_date).cast("date").alias("ingest_date"))


def to_quarantine(df: DataFrame, batch_id: int, run_id: str) -> DataFrame:
    reason = F.concat_ws(" · ", F.element_at(F.create_map(*[x for r, d in RULES.items() for x in (F.lit(r), F.lit(d))]),
                                             F.col("_rule")), F.col("_detail"))
    return df.select(
        F.lit("usage_events").alias("source"), F.col("_rule").alias("rule_id"), reason.alias("reason"),
        F.coalesce(F.col("event_id"), F.lit("∅")).alias("record_key"),
        F.coalesce(F.col(CORRUPT), F.to_json(F.struct(*RAW_FIELDS))).alias("raw_record"),
        "source_file", "arrival_ts", F.current_timestamp().alias("ingest_ts"), F.lit(run_id).alias("run_id"),
        F.lit(batch_id).cast("long").alias("batch_id"))


def _write_batch_dir(df: DataFrame, target: Path) -> int:
    """Escribe un lote en su carpeta, reemplazando lo que hubiera de ese mismo lote.

    Borra el batch_id en TODAS las fechas, no solo en la de hoy: si Spark re-ejecuta el lote otro día,
    la copia anterior quedaría en otra carpeta ingest_date y las filas se duplicarían.
    """
    for old in target.parent.parent.glob(f"*/{target.name}"):
        shutil.rmtree(old)
        if not any(old.parent.iterdir()):        # no dejar carpetas de fecha vacías
            old.parent.rmdir()
    rows = df.count()
    if rows:
        df.coalesce(1).write.mode("overwrite").parquet(str(target))
    return rows


def make_batch_writer(paths: Paths, run_id: str):
    """Función que Spark llama en cada micro-lote (foreachBatch)."""

    def process_batch(batch: DataFrame, batch_id: int) -> None:
        ingest_date = utc_now()[:10]
        batch = classify(batch).cache()
        files = sorted(r[0] for r in batch.select("source_file").distinct().collect())
        bronze_dir = paths.bronze / f"ingest_date={ingest_date}" / f"batch_id={batch_id}"
        quarantine_dir = paths.quarantine / f"dt={ingest_date}" / f"batch_id={batch_id}"

        read = batch.count()
        accepted = _write_batch_dir(to_bronze(batch.where(F.col("_rule").isNull()), batch_id, run_id, ingest_date)
                                    .drop("ingest_date", "batch_id"), bronze_dir)
        rejected = _write_batch_dir(to_quarantine(batch.where(F.col("_rule").isNotNull()), batch_id, run_id)
                                    .drop("batch_id"), quarantine_dir)
        by_rule = {r: batch.where(F.col("_rule") == r).count() for r in RULES}
        batch.unpersist()
        if read != accepted + rejected:
            raise RuntimeError(f"lote {batch_id}: {read} leídos ≠ {accepted} Bronze + {rejected} quarantine")

        # El manifiesto se escribe AL FINAL: si existe, el lote está completo.
        paths.manifest.mkdir(parents=True, exist_ok=True)
        (paths.manifest / f"batch_id={batch_id}.json").write_text(json.dumps({
            "batch_id": batch_id, "run_id": run_id, "completed_utc": utc_now(), "ingest_date": ingest_date,
            "files": files, "read": read, "bronze": accepted, "quarantine": rejected, "quarantine_by_rule": by_rule,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("lote %s: %s archivo(s), %s leídos, %s a Bronze, %s a quarantine",
                 batch_id, len(files), read, accepted, rejected)

    return process_batch


def run_query(spark: SparkSession, paths: Paths, run_id: str, max_files: int = 1, continuous: bool = False,
              interval: str = "1 minute"):
    """Arranca la consulta y espera a que termine.

    continuous=False → Trigger.AvailableNow: procesa todo lo pendiente, en lotes de `max_files`, y se detiene.
                       Es lo que se usa en las demostraciones y en las pruebas.
    continuous=True  → un micro-lote cada `interval`, hasta que se detenga a mano (Ctrl+C).
    """
    writer = (read_stream(spark, paths, max_files).writeStream
              .foreachBatch(make_batch_writer(paths, run_id))
              .option("checkpointLocation", str(paths.checkpoint))
              .queryName(QUERY_ID))
    writer = writer.trigger(processingTime=interval) if continuous else writer.trigger(availableNow=True)
    query = writer.start()
    query.awaitTermination()
    return query


def completed_batches(paths: Paths) -> list:
    """Manifiestos de los lotes completos."""
    if not paths.manifest.exists():
        return []
    return sorted((json.loads(p.read_text(encoding="utf-8")) for p in paths.manifest.glob("batch_id=*.json")),
                  key=lambda m: m["batch_id"])


def read_bronze_events(spark: SparkSession, paths: Paths = None) -> DataFrame:
    """Bronze de eventos, solo con los lotes completos (los que tienen manifiesto)."""
    paths = paths or Paths()
    done = [m["batch_id"] for m in completed_batches(paths)]
    df = spark.read.parquet(str(paths.bronze))
    return df.where(F.col("batch_id").isin(done))
