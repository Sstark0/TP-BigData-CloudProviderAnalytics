"""
Lectura y escritura comunes.

- read_landing_csv: único lector de CSV del proyecto (dialecto RFC 4180, encabezado validado, todo como texto).
- add_technical_columns: columnas de linaje que lleva toda fila desde Bronze.
- write_parquet: escritura con control de cantidad de archivos.
"""
import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import LANDING
from src.common.schemas import CSV_COLUMNS, all_string_schema

CORRUPT = "_corrupt_record"


class SchemaMismatchError(ValueError):
    """El encabezado del CSV no coincide con el esquema esperado."""


def validate_csv_header(path, expected_columns) -> None:
    """Compara el encabezado real con el esperado, en nombre y en orden.

    Con esquema explícito Spark asigna las columnas por POSICIÓN
    Archivo con las columnas en otro orden = Datos bajo el nombre equivocado sin error (D-10) 

    """
    with open(path, newline="", encoding="utf-8") as f:
        header = next(csv.reader(f), [])
    if header != list(expected_columns):
        raise SchemaMismatchError(
            f"{Path(path).name}: encabezado {header} distinto del esperado {list(expected_columns)}")


def read_landing_csv(spark: SparkSession, source: str, landing=None) -> DataFrame:
    """Lee <landing>/<source>.csv con todas las columnas como texto.

    - escape='"': los CSV duplican las comillas internas (RFC 4180); sin esto `tags_json` se corta (D-10).
    - Todo como texto, el tipado se hace después, de forma controlada
    - Se manda a quarantine lo que no se pueda convertir en lugar de perderlo como NULL (D-05).
    - PERMISSIVE + _corrupt_record: una fila con más o menos columnas no rompe la carga; queda marcada.
    """
    if source not in CSV_COLUMNS:
        raise ValueError(f"Fuente desconocida: {source!r}. Fuentes: {sorted(CSV_COLUMNS)}")
    path = Path(landing or LANDING) / f"{source}.csv"
    columns = CSV_COLUMNS[source]
    validate_csv_header(path, columns)
    schema = all_string_schema(columns + [CORRUPT])
    return (spark.read
            .option("header", True)
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT)
            .schema(schema)
            .csv(str(path)))


def add_technical_columns(df: DataFrame, run_id: str) -> DataFrame:
    """Agrega las columnas de linaje del contrato del lago:

    source_file  archivo de origen           arrival_ts  fecha de modificación del archivo (cuándo llegó)
    ingest_ts    momento de la carga (UTC)   run_id      ejecución que lo cargó (enlaza con _runs/)
    Usa la columna oculta `_metadata` que Spark expone para las fuentes de archivos.
    """
    return (df
            .withColumn("source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
            .withColumn("arrival_ts", F.col("_metadata.file_modification_time"))
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id)))


def write_parquet(df: DataFrame, path, mode: str = "overwrite", partition_by=None, files: int = 1) -> None:
    """Escribe Parquet controlando cuántos archivos salen.

    Sin partición: `files` archivos (1 por defecto; las tablas chicas no necesitan más).
    Con partición: un archivo por valor de partición, para no multiplicar archivos pequeños.
    """
    if partition_by:
        cols = [partition_by] if isinstance(partition_by, str) else list(partition_by)
        df.repartition(*cols).write.mode(mode).partitionBy(*cols).parquet(str(path))
    else:
        df.coalesce(files).write.mode(mode).parquet(str(path))
