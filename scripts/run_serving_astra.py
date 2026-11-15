"""Serving FinOps en AstraDB (E2-08): crea las tablas, carga Gold desde Spark, limpia, verifica y consulta.

Requiere Gold (python scripts/run_gold_finops.py), cassandra-driver y las credenciales de Astra en el entorno:
    set -a; source ~/.astra/astra.env; set +a
    python scripts/run_serving_astra.py

Pasos
    1. Aplica cql/01_schema.cql (CREATE TABLE IF NOT EXISTS: se puede repetir).
    2. Cuenta lo que ya hay en Cassandra (antes).
    3. Carga las dos tablas de Gold con foreachPartition (upsert por clave primaria).
    4. Borra las filas que ya no están en Gold.
    5. Cuenta otra vez (después) y compara, por organización, filas y costo contra Gold.
    6. Ejecuta las dos consultas de cql/02_queries.cql y las compara con las calculadas en Spark (E2-07).
Deja todo en evidence/entrega2/e2_08_serving_astra.json (sin credenciales).
"""
import argparse
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.common.config import EVIDENCE_DIR, GOLD, REPO_ROOT
from src.common.runlog import RunLog
from src.common.spark import get_spark
from src.gold.finops import TABLE, TOP_TABLE, reference_queries
from src.serving.cassandra import AstraSettings, apply_schema, load_from_spark, partition_totals, prune_stale, \
    run_queries

PROCESS = "v1_serving_astra"
SCHEMA, QUERIES = REPO_ROOT / "cql" / "01_schema.cql", REPO_ROOT / "cql" / "02_queries.cql"
MEASURE = {TABLE: "daily_cost_usd", TOP_TABLE: "cost_usd"}
QUERY_ORG = "org_53lc58dr"          # la misma organización que cql/02_queries.cql


def gold_totals(gold, measure):
    return {r.org_id: (int(r.n), r.total) for r in
            gold.groupBy("org_id").agg(F.count("*").alias("n"), F.sum(measure).alias("total")).collect()}


def compare(cassandra: dict, gold: dict) -> list:
    """Organizaciones en que Cassandra y Gold no coinciden en filas o en costo."""
    diffs = []
    for org in sorted(set(cassandra) | set(gold)):
        c, g = cassandra.get(org, (0, Decimal(0))), gold.get(org, (0, Decimal(0)))
        if c[0] != g[0] or Decimal(str(c[1] or 0)) != Decimal(str(g[1] or 0)):
            diffs.append({"org_id": org, "cassandra": [c[0], str(c[1])], "gold": [g[0], str(g[1])]})
    return diffs


def same_rows(cql_rows, spark_rows, fields) -> bool:
    def norm(rows):
        return sorted(tuple(str(Decimal(str(r[f]))) if f in ("daily_cost_usd", "cost_usd") else str(r[f])
                            for f in fields) for r in rows)
    return norm(cql_rows) == norm(spark_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--partitions", type=int, default=4, help="particiones de Spark que escriben en paralelo")
    args = parser.parse_args()
    settings = AstraSettings().check()
    if not (GOLD / TABLE).exists() or not (GOLD / TOP_TABLE).exists():
        print("Falta Gold: ejecutá antes scripts/run_gold_finops.py")
        return 1

    spark = get_spark("serving-astra")
    session = settings()
    with RunLog(PROCESS) as run:
        run.param("keyspace", settings.keyspace)
        run.param("region", os.getenv("ASTRA_DB_REGION", "sin informar"))
        run.count("sentencias_esquema", apply_schema(session, SCHEMA, settings.keyspace))
        tables = {TABLE: spark.read.parquet(str(GOLD / TABLE)), TOP_TABLE: spark.read.parquet(str(GOLD / TOP_TABLE))}
        verification = {}
        for name, gold in tables.items():
            orgs = [r.org_id for r in gold.select("org_id").distinct().collect()]
            before = partition_totals(session, name, orgs, MEASURE[name], settings.keyspace)
            run.count(f"{name}.cassandra_antes", sum(n for n, _ in before.values()))
            run.count(f"{name}.gold_filas", gold.count())
            run.count(f"{name}.escritas", load_from_spark(gold, name, settings, settings.keyspace, args.partitions))
            run.count(f"{name}.obsoletas_borradas", prune_stale(session, gold, name, settings.keyspace))
            after = partition_totals(session, name, orgs, MEASURE[name], settings.keyspace)
            run.count(f"{name}.cassandra_despues", sum(n for n, _ in after.values()))
            diffs = compare(after, gold_totals(gold, MEASURE[name]))
            run.count(f"{name}.organizaciones_distintas_de_gold", len(diffs))
            run.check_balance(f"{name}.gold_filas", [f"{name}.cassandra_despues"])
            verification[name] = {"organizaciones": len(orgs), "diferencias": diffs[:5]}

        results = run_queries(session, QUERIES, settings.keyspace)
        reference = reference_queries(tables[TABLE], QUERY_ORG)
        checks = [same_rows(results[0]["filas"], reference["q1_costos_y_requests_ultimos_7_dias"],
                            ["usage_date", "service", "daily_cost_usd", "requests"]),
                  same_rows(results[1]["filas"], reference["q2_top5_servicios_ultimos_14_dias"],
                            ["service", "cost_usd", "requests"])]
        run.count("consultas_iguales_a_spark", sum(checks))
        ok = all(b["ok"] for b in run.data["balances"]) and all(checks) and \
            not any(v["diferencias"] for v in verification.values())
        if not ok:
            raise RuntimeError("Cassandra no coincide con Gold; ver el registro de la ejecución")

    summary = {k: run.data[k] for k in ("run_id", "params", "counts", "balances")}
    summary["verificacion_por_organizacion"] = verification
    summary["esquema_cql"] = str(SCHEMA.relative_to(REPO_ROOT))
    summary["consultas"] = [{**r, "igual_a_spark": c} for r, c in zip(results, checks)]
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / "e2_08_serving_astra.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    for r, c in zip(results, checks):
        print(f"\n{r['cql']}\n  {len(r['filas'])} filas · igual a Spark: {c}")
        for row in r["filas"][:6]:
            print("  ", row)
    session.cluster.shutdown()
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
