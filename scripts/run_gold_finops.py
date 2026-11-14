"""Gold FinOps: mart org_daily_usage_by_service (E2-07).

Requiere Silver enriquecido (scripts/run_silver_enrichment.py).

Uso (desde la raíz del repositorio):
    python scripts/run_gold_finops.py
    python scripts/run_gold_finops.py --org org_xxxxxxxx    # organización para las consultas de referencia

Deja esquema, conciliaciones, archivos, muestras y las consultas de referencia (para comparar con CQL en E2-08) en evidence/entrega2/e2_07_gold_finops.json.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.common.config import EVIDENCE_DIR, GOLD, SILVER
from src.common.runlog import RunLog, describe_output, latest_run
from src.common.spark import get_spark
from src.gold.finops import PROCESS, TABLE, build_gold_finops, reference_queries


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--org", help="org_id para las consultas de referencia (por defecto, la de mayor costo)")
    args = parser.parse_args()
    if not (SILVER / "usage_events_enriched").exists():
        print("Falta Silver enriquecido: ejecutá antes scripts/run_silver_enrichment.py")
        return 1

    previous = latest_run(PROCESS)
    spark = get_spark("gold-finops")
    with RunLog(PROCESS) as run:
        enriched = spark.read.parquet(str(SILVER / "usage_events_enriched"))
        features = spark.read.parquet(str(SILVER / "org_daily_features"))
        gold = build_gold_finops(spark, run, enriched, features, GOLD / TABLE)

    org = args.org or (gold.groupBy("org_id").agg(F.sum("daily_cost_usd").alias("c"))
                       .orderBy(F.desc("c")).first()["org_id"])
    sample = (gold.where(F.col("org_id") == org).orderBy(F.desc("usage_date"), "service").limit(4)
              .drop("gold_processed_ts", "gold_run_id"))
    summary = {k: run.data[k] for k in ("run_id", "counts", "balances")}
    summary["re_ejecucion"] = (
        {"run_id_anterior": previous["run_id"],
         "fingerprint_anterior": previous["counts"].get("fingerprint"),
         "mismo_contenido": previous["counts"].get("fingerprint") == run.data["counts"]["fingerprint"]}
        if previous else "primera ejecución: volvé a correr el script para comparar la huella")
    summary["tabla"] = describe_output(GOLD / TABLE)
    summary["esquema"] = [f"{f.name}: {f.dataType.simpleString()}" for f in gold.schema]
    summary["muestra"] = [{k: (v if isinstance(v, (int, float, type(None))) else str(v)) for k, v in r.asDict().items()}
                          for r in sample.collect()]
    summary["consultas_de_referencia"] = reference_queries(gold, org)
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / "e2_07_gold_finops.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
