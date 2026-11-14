"""S1 · Ingesta streaming de eventos a Bronze (E2-03).

Uso (desde la raíz del repositorio, con el dataset en datalake/landing/):
    python scripts/run_bronze_streaming.py                 # procesa lo pendiente (AvailableNow), 1 archivo por lote
    python scripts/run_bronze_streaming.py --max-files 10  # 10 archivos por micro-lote
    python scripts/run_bronze_streaming.py --reset         # borra Bronze de eventos, quarantine y checkpoint, y re-ingesta
    python scripts/run_bronze_streaming.py --continuous    # un micro-lote por minuto hasta Ctrl+C

Al terminar concilia Landing contra Bronze + quarantine y deja el resumen en evidence/entrega2/e2_03_bronze_streaming.json.
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.common.config import EVIDENCE_DIR, MAX_FILES_PER_TRIGGER
from src.common.runlog import RunLog, describe_output
from src.common.schemas import EVENTS_RAW_SCHEMA
from src.common.spark import get_spark
from src.streaming.bronze_events import QUERY_ID, Paths, completed_batches, read_bronze_events, run_query


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-files", type=int, default=MAX_FILES_PER_TRIGGER)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()

    paths = Paths()
    if args.reset:
        for p in (paths.bronze, paths.quarantine, paths.checkpoint, paths.manifest):
            shutil.rmtree(p, ignore_errors=True)

    spark = get_spark("s1-bronze-streaming")
    with RunLog(QUERY_ID) as run:
        run.param("max_files_per_trigger", args.max_files)
        run.param("trigger", "processingTime" if args.continuous else "availableNow")
        files = sorted(paths.landing.glob("*.jsonl"))
        run.add_inputs(files)
        run_query(spark, paths, run.run_id, max_files=args.max_files, continuous=args.continuous)

        # Conciliación sobre todo lo ingerido (todas las ejecuciones): Landing = Bronze + quarantine
        batches = completed_batches(paths)
        run.count("microlotes_esta_ejecucion", sum(m["run_id"] == run.run_id for m in batches))
        landing_rows = spark.read.schema(EVENTS_RAW_SCHEMA).json(str(paths.landing)).count()
        bronze = read_bronze_events(spark, paths)
        bronze_rows = bronze.count()
        quarantine_rows = sum(m["quarantine"] for m in batches)
        run.count("landing_eventos", landing_rows)
        run.count("bronze_eventos", bronze_rows)
        run.count("quarantine_eventos", quarantine_rows)
        run.count("lotes_completos", len(batches))
        run.count("archivos_ingeridos", len({f for m in batches for f in m["files"]}))
        run.count("archivos_en_landing", len(files))
        # Ningún archivo se cargó dos veces: (archivo, lote) es único por archivo
        repeated = bronze.groupBy("source_file").agg(F.countDistinct("batch_id").alias("n")).where("n > 1").count()
        run.count("archivos_en_mas_de_un_lote", repeated)
        run.add_output(paths.bronze)
        ok = (run.check_balance("landing_eventos", ["bronze_eventos", "quarantine_eventos"])
              and run.check_balance("archivos_en_landing", ["archivos_ingeridos"])
              and repeated == 0)

    summary = {k: run.data[k] for k in ("run_id", "params", "counts", "balances", "outputs")}
    summary["lotes"] = [{k: m[k] for k in ("batch_id", "files", "read", "bronze", "quarantine")} for m in batches]
    summary["bronze_tabla"] = describe_output(paths.bronze)
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / "e2_03_bronze_streaming.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
