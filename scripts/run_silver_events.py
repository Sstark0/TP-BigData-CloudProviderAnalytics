"""Silver de eventos (E2-04): camino streaming (S2) + reconciliación batch (B2).

Requiere Bronze de eventos (python scripts/run_bronze_streaming.py).

    python scripts/run_silver_events.py                        # procesa lo pendiente y reconcilia
    python scripts/run_silver_events.py --reset                # borra Silver, su quarantine y el checkpoint de S2
    python scripts/run_silver_events.py --watermark "2 days"   # otro retraso tolerado (default: EVENTS_WATERMARK)

Resumen
    - evidence/entrega2/e2_04_silver_events.json (corrida que procesó lotes nuevos)
    - evidence/entrega2/e2_04_silver_events_rerun.json (re-ejecución sin lotes nuevos)

"""
import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.batch.silver_events import PROCESS, SILVER_SOURCE, reconcile
from src.common.config import EVENTS_WATERMARK, EVIDENCE_DIR, MAX_FILES_PER_TRIGGER, checkpoint_path, \
    quarantine_path, table_path
from src.common.runlog import RunLog, describe_output
from src.common.spark import get_spark
from src.streaming.bronze_events import Paths, completed_batches, read_bronze_events
from src.streaming.silver_events_live import QUERY_ID, run_live_query


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--watermark", default=EVENTS_WATERMARK)
    parser.add_argument("--max-files", type=int, default=MAX_FILES_PER_TRIGGER)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    bronze = Paths()
    live, silver = table_path("silver", "usage_events_live"), table_path("silver", "usage_events")
    quarantine, checkpoint = quarantine_path(SILVER_SOURCE), checkpoint_path(QUERY_ID)
    if not completed_batches(bronze):
        print("No hay lotes en Bronze de eventos: ejecutá antes scripts/run_bronze_streaming.py")
        return 1
    if args.reset:
        for p in (live, silver, quarantine, checkpoint):
            shutil.rmtree(p, ignore_errors=True)

    spark = get_spark("silver-events")
    with RunLog(PROCESS) as run:
        run.param("watermark", args.watermark)
        run.param("max_files_per_trigger", args.max_files)
        stream = run_live_query(spark, bronze.bronze, live, checkpoint, args.watermark, args.max_files)
        for name in ("microlotes", "leidos", "emitidos", "descartados_por_watermark", "duplicados_descartados"):
            run.count(f"stream.{name}", stream[name])
        # Lo que el stream leyó en esta ejecución: salió a tiempo, lo descartó el watermark, o era duplicado
        run.check_balance("stream.leidos", ["stream.emitidos", "stream.descartados_por_watermark",
                                            "stream.duplicados_descartados"])
        run.param("stream.ultimo_watermark", stream["ultimo_watermark"])
        counts = reconcile(spark, run, read_bronze_events(spark, bronze), live, silver, quarantine)

    sample = (spark.read.parquet(str(silver)).where("arrival_path = 'tardio'")
              .select("event_id", "event_ts", "source_file", "bronze_batch_id", "arrival_path")
              .orderBy("bronze_batch_id", "event_ts").limit(3))
    summary = {k: run.data[k] for k in ("run_id", "params", "counts", "balances", "outputs")}
    summary["muestra_tardios"] = [{k: str(v) for k, v in r.asDict().items()} for r in sample.collect()]
    summary["silver_live"] = describe_output(live)
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    # Si el stream procesó lotes, es la corrida principal; si no, es una re-ejecución 
    name = "e2_04_silver_events.json" if stream["microlotes"] else "e2_04_silver_events_rerun.json"
    (out / name).write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    spark.stop()
    return 0 if all(b["ok"] for b in run.data["balances"]) else 1


if __name__ == "__main__":
    sys.exit(main())
