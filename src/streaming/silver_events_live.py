"""
S2 · Camino operativo (streaming) de Silver de eventos: watermark + deduplicación (E2-04).

    Lee Bronze de eventos como stream (un archivo Parquet de Bronze por micro-lote, en el orden en que llegaron),
    descarta duplicados de event_id con `dropDuplicatesWithinWatermark` y escribe lo que llega a tiempo en
    silver/usage_events_live.
    
    max event_ts - watermark = retraso tolerado (24 h por defecto, EVENTS_WATERMARK)

    Un evento con event_ts anterior al watermark se considera tardío:
        Spark lo descarta del stream y libera el estado de deduplicación de las claves viejas.
        La memoria no crece sin límite, a cambio de perder los eventos que llegan demasiado tarde.

    Spark aplica el watermark calculado en el lote N recién a partir del lote N+2 (un lote de desfase).

    Este camino NO es el histórico
    
        Cada archivo del dataset mezcla casi 60 días de eventos. Después del primer archivo el watermark ya está
        cerca del 30/08, y casi todo lo que viene después cae como tardío. 
        Los tardíos no se pierden: siguen en Bronze y la reconciliación batch (src/batch/silver_events.py) los recupera.

    El sink de archivos Parquet de Spark es exactly-once: registra cada lote confirmado en `_spark_metadata/`
    Si un lote se re-ejecuta, no lo vuelve a agregar.
    Los lectores deben leer la carpeta con spark.read.parquet (que respeta ese registro), no archivo por archivo.

"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.runlog import get_logger
from src.common.schemas import BRONZE_EVENTS_SCHEMA

QUERY_ID = "s2_silver_events_live"
log = get_logger(QUERY_ID)


def live_events(bronze_stream: DataFrame, watermark: str) -> DataFrame:
    """Eventos a tiempo y sin duplicados. Los event_id nulos no entran: no se pueden deduplicar."""
    return (bronze_stream
            .where(F.col("event_id").isNotNull())
            .withWatermark("event_ts", watermark)
            .dropDuplicatesWithinWatermark(["event_id"])
            .select("event_id", "event_ts", "event_date", "org_id", "resource_id", "service", "region", "metric",
                    "value", "unit", "cost_usd_increment", "schema_version", "carbon_kg", "genai_tokens",
                    "source_file", "arrival_ts", F.col("batch_id").alias("bronze_batch_id"),
                    F.current_timestamp().alias("silver_processed_ts"))
            .coalesce(1))          # un archivo por micro-lote


def _count(spark: SparkSession, path) -> int:
    try:
        return spark.read.parquet(str(path)).count()
    except Exception:              # todavía no existe
        return 0


def run_live_query(spark: SparkSession, bronze_path, output_path, checkpoint, watermark: str = "24 hours",
                   max_files: int = 1) -> dict:
    """Procesa todo lo pendiente de Bronze (Trigger.AvailableNow) y devuelve las métricas de esta ejecución."""
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "10000")   # guardar el progreso de cada lote
    before = _count(spark, output_path)
    source = (spark.readStream.schema(BRONZE_EVENTS_SCHEMA)
              .option("maxFilesPerTrigger", max_files)
              .parquet(str(bronze_path)))
    query = (live_events(source, watermark).writeStream
             .format("parquet")
             .option("path", str(output_path))
             .option("checkpointLocation", str(checkpoint))
             .queryName(QUERY_ID)
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()

    progress = [p for p in query.recentProgress if p["numInputRows"] or p.get("stateOperators")]
    ops = [p["stateOperators"][0] for p in progress if p.get("stateOperators")]
    metrics = {
        "microlotes": sum(1 for p in progress if p["numInputRows"]),
        "leidos": sum(p["numInputRows"] for p in progress),
        "emitidos": _count(spark, output_path) - before,       # el sink de archivos no informa filas escritas
        "descartados_por_watermark": sum(o.get("numRowsDroppedByWatermark", 0) for o in ops),
        "duplicados_descartados": sum(o.get("customMetrics", {}).get("numDroppedDuplicateRows", 0) for o in ops),
        "ultimo_watermark": progress[-1]["eventTime"].get("watermark") if progress else None,
    }
    log.info("stream Silver: %(microlotes)s lotes, %(leidos)s leídos, %(emitidos)s a tiempo, %(descartados_por_watermark)s tardíos "
             "descartados por el watermark, %(duplicados_descartados)s duplicados", metrics)
    return metrics
