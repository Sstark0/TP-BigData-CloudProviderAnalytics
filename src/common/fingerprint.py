"""
Huella de contenido de una tabla (E2-10, D-20).

Se requiere demostrar idempotencia. Dos tablas pueden tener la misma cantidad de filas con contenido distinto. 

La huella resume todo el contenido en un número

    huella = Σ xxhash64(fila)   sobre las columnas de negocio de cada fila

- No depende del orden de las filas ni de cómo Spark las reparta en archivos (suma conmutativa).
- Cambia si cambia cualquier valor, si aparece o desaparece una fila, o si una fila se duplica.
- Ignora las columnas volátiles: las que registran cuándo y en qué ejecución se procesó la fila (hora de
  proceso, run_id, fecha de carga). Esas cambian legítimamente en cada corrida sin que cambie el dato.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VOLATILE = {"ingest_ts", "run_id", "ingest_date", "bronze_ingest_date", "bronze_ingest_ts", "silver_processed_ts",
            "silver_run_id", "gold_processed_ts", "gold_run_id", "loaded_at"}


def stable_columns(df: DataFrame) -> list:
    return sorted(c for c in df.columns if c not in VOLATILE)


def content_fingerprint(df: DataFrame) -> dict:
    """Filas y huella del contenido estable de `df`."""
    cols = stable_columns(df)
    row = df.agg(F.count(F.lit(1)).alias("rows"),
                 F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("hash")).first()
    return {"filas": int(row["rows"]), "huella": str(row["hash"] or 0)}
