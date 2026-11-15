"""Idempotencia (E2-10): re-ejecutar el pipeline no cambia Bronze, Silver, Gold ni Cassandra.

Requiere que el pipeline ya se haya ejecutado al menos una vez (python scripts/run_pipeline.py).

Uso (desde la raíz del repositorio, con el venv activo):
    set -a; source ~/.astra/astra.env; set +a      # para incluir Cassandra (si no, se omite y queda registrado)
    python scripts/check_idempotency.py --operador "Nombre"

Pasos
    1. ANTES: filas y huella de contenido de cada tabla del lago, lotes de los dos streams y, si hay
       credenciales, filas y costo total de cada tabla de Cassandra.
    2. Ejecuta el pipeline completo otra vez, SIN --clean (como una re-ejecución real).
    3. DESPUÉS: lo mismo que en 1.
    4. Compara métrica por métrica. Si algo cambió, termina con error.
Deja el resultado en evidence/entrega2/e2_10_idempotency.json y el registro de la re-ejecución del pipeline en
evidence/entrega2/e2_10_pipeline_rerun.json.
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.common.config import BRONZE, CHECKPOINTS, EVIDENCE_DIR, GOLD, QUARANTINE, REPO_ROOT, SILVER
from src.common.fingerprint import content_fingerprint
from src.common.runlog import utc_now
from src.common.schemas import CSV_COLUMNS
from src.common.spark import get_spark
from src.streaming.bronze_events import Paths, completed_batches, read_bronze_events

SILVER_TABLES = ["usage_events", "usage_events_live", "usage_events_enriched", "org_daily_features",
                 "customers_orgs", "resources"]
GOLD_TABLES = ["org_daily_usage_by_service", "top_services_by_org_14d", "cost_anomaly_mart"]
CASSANDRA = {"org_daily_usage_by_service": "daily_cost_usd", "top_services_by_org_14d": "cost_usd"}


def lake_snapshot() -> dict:
    """Filas y huella de cada tabla del lago, más los lotes de cada stream."""
    spark = get_spark("idempotency-snapshot")
    snap = {}
    for source in CSV_COLUMNS:                                  # Bronze batch: última foto de cada maestro
        df = spark.read.parquet(str(BRONZE / source))
        last = df.agg(F.max("ingest_date")).first()[0]
        snap[f"bronze.{source}"] = content_fingerprint(df.where(F.col("ingest_date") == F.lit(last)))
    snap["bronze.usage_events"] = content_fingerprint(read_bronze_events(spark))
    for table in SILVER_TABLES:
        snap[f"silver.{table}"] = content_fingerprint(spark.read.parquet(str(SILVER / table)))
    log = spark.read.parquet(str(QUARANTINE / "source=usage_events_silver"))
    snap["silver.registro_calidad"] = content_fingerprint(log)
    for r in log.groupBy("action").count().collect():
        snap[f"silver.registro_calidad.{r['action']}"] = r["count"]
    for table in GOLD_TABLES:
        snap[f"gold.{table}"] = content_fingerprint(spark.read.parquet(str(GOLD / table)))
    snap["stream.s1.lotes_completos"] = len(completed_batches(Paths()))
    # Spark conserva solo los ~100 últimos registros de lotes en el checkpoint: se compara el número del último
    # lote confirmado (si no cambia, el stream no procesó nada nuevo), no la cantidad de archivos.
    commits = CHECKPOINTS / "s2_silver_events_live" / "commits"
    ids = [int(p.name) for p in commits.iterdir() if p.name.isdigit()] if commits.exists() else []
    snap["stream.s2.ultimo_lote_confirmado"] = max(ids) if ids else None
    orgs = [r.org_id for r in spark.read.parquet(str(GOLD / GOLD_TABLES[0])).select("org_id").distinct().collect()]
    spark.stop()
    snap.update(cassandra_snapshot(orgs))
    return snap


def cassandra_snapshot(orgs: list) -> dict:
    if not (os.getenv("ASTRA_DB_APPLICATION_TOKEN") and os.getenv("ASTRA_SECURE_BUNDLE_PATH")):
        return {}
    from src.serving.cassandra import AstraSettings, partition_totals
    settings = AstraSettings().check()
    session = settings()
    try:
        out = {}
        for table, measure in CASSANDRA.items():
            totals = partition_totals(session, table, orgs, measure, settings.keyspace)
            out[f"cassandra.{table}"] = {"filas": sum(n for n, _ in totals.values()),
                                         "costo_usd": str(sum((t for _, t in totals.values() if t is not None)))}
        return out
    finally:
        session.cluster.shutdown()


def bronze_snapshots() -> dict:
    """Fotos de cada maestro en Bronze (informativo). Una re-ejecución otro día agrega una foto: es lo diseñado
    en D-11 (historia por carga), no una duplicación; Silver toma la última y no cambia."""
    spark = get_spark("idempotency-snapshots")
    out = {s: spark.read.parquet(str(BRONZE / s)).select("ingest_date").distinct().count() for s in CSV_COLUMNS}
    spark.stop()
    return out


def compare(before: dict, after: dict) -> list:
    return [{"metrica": k, "antes": before.get(k), "despues": after.get(k), "igual": before.get(k) == after.get(k)}
            for k in sorted(set(before) | set(after))]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--operador", default=os.getenv("USER", "sin informar"))
    args = parser.parse_args()
    if not (GOLD / GOLD_TABLES[-1]).exists():
        print("Falta el lago completo: ejecutá antes  python scripts/run_pipeline.py")
        return 1

    record = {"operador": args.operador, "inicio_utc": utc_now(),
              "cassandra_incluida": bool(os.getenv("ASTRA_DB_APPLICATION_TOKEN"))}
    print("1/3 · conteos y huellas ANTES ...", flush=True)
    before = lake_snapshot()
    print("2/3 · re-ejecutando el pipeline completo (sin --clean) ...", flush=True)
    rerun = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "run_pipeline.py"), "--operador",
                            args.operador, "--evidencia", "e2_10_pipeline_rerun.json"], cwd=REPO_ROOT)
    record["pipeline_rerun"] = {"codigo_salida": rerun.returncode, "registro": "evidence/entrega2/e2_10_pipeline_rerun.json"}
    print("3/3 · conteos y huellas DESPUÉS ...", flush=True)
    after = lake_snapshot()

    rows = compare(before, after)
    record["informativo_fotos_bronze_por_maestro"] = bronze_snapshots()
    record.update(fin_utc=utc_now(), comparacion=rows,
                  metricas=len(rows), iguales=sum(r["igual"] for r in rows),
                  idempotente=rerun.returncode == 0 and all(r["igual"] for r in rows))
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / "e2_10_idempotency.json").write_text(json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8")

    width = max(len(r["metrica"]) for r in rows)
    for r in rows:
        show = lambda v: v["filas"] if isinstance(v, dict) else v          # noqa: E731
        print(f"{'OK ' if r['igual'] else 'DIF'} {r['metrica']:<{width}}  {show(r['antes'])} → {show(r['despues'])}")
    print(f"\n{record['iguales']}/{record['metricas']} métricas iguales · idempotente: {record['idempotente']}")
    return 0 if record["idempotente"] else 1


if __name__ == "__main__":
    sys.exit(main())
