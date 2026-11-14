"""Silver de maestros, eventos enriquecidos y features diarias (E2-05).

Requiere Bronze de maestros (scripts/run_bronze_batch.py) y Silver de eventos (scripts/run_silver_events.py).

Uso (desde la raíz del repositorio):
    python scripts/run_silver_enrichment.py

Deja esquemas, conteos, conciliaciones y muestras en evidence/entrega2/e2_05_silver_enrichment.json.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.batch.silver_features import build_features
from src.batch.silver_masters import build_masters
from src.common.config import BRONZE, EVIDENCE_DIR, SILVER
from src.common.runlog import RunLog
from src.common.spark import get_spark

PROCESS = "b3_silver_enrichment"


def rows(df, n=3):
    return [{k: (v if isinstance(v, (int, float, bool, list, type(None))) else str(v)) for k, v in r.asDict().items()}
            for r in df.limit(n).collect()]


def main() -> int:
    if not (SILVER / "usage_events").exists():
        print("Falta Silver de eventos: ejecutá antes scripts/run_silver_events.py")
        return 1
    spark = get_spark("silver-enrichment")
    with RunLog(PROCESS) as run:
        masters = build_masters(spark, run, BRONZE, SILVER)
        events = spark.read.parquet(str(SILVER / "usage_events"))
        out = build_features(spark, run, events, masters["customers_orgs"], masters["resources"], SILVER)

    features = out["features"]
    summary = {k: run.data[k] for k in ("run_id", "counts", "balances", "outputs")}
    summary["esquemas"] = {name: [f"{f.name}: {f.dataType.simpleString()}" for f in df.schema]
                           for name, df in [("customers_orgs", masters["customers_orgs"]),
                                            ("resources", masters["resources"]),
                                            ("usage_events_enriched", out["enriched"]),
                                            ("org_daily_features", features)]}
    summary["muestras"] = {
        "resources": rows(masters["resources"].where("size(tags) > 0")
                          .select("resource_id", "org_id", "service", "tags", "env", "team", "has_pii")),
        "usage_events_enriched": rows(out["enriched"].where("metric = 'requests' AND schema_version = 2")
                                      .select("event_id", "event_ts", "org_id", "org_name", "plan_tier", "service",
                                              "metric", "requests", "cost_usd", "genai_tokens", "carbon_kg",
                                              "resource_env").orderBy("event_ts")),
        "org_daily_features_mayor_costo": rows(features.orderBy(F.desc("daily_cost_usd"))
                                               .select("org_id", "org_name", "event_date", "daily_cost_usd",
                                                       "requests", "genai_tokens", "carbon_kg", "carbon_coverage",
                                                       "events")),
    }
    summary["features_resumen"] = rows(features.select(
        F.count("*").alias("filas_org_dia"), F.countDistinct("org_id").alias("orgs"),
        F.countDistinct("event_date").alias("dias"),
        F.round(F.sum("daily_cost_usd"), 2).alias("costo_total_usd"),
        F.sum(F.col("requests").isNull().cast("int")).alias("org_dia_sin_requests"),
        F.sum(F.col("genai_tokens").isNull().cast("int")).alias("org_dia_sin_genai_tokens"),
        F.round(F.avg("carbon_coverage"), 4).alias("carbon_coverage_promedio")), 1)[0]
    out_dir = EVIDENCE_DIR / "entrega2"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "e2_05_silver_enrichment.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False),
                                                          encoding="utf-8")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
