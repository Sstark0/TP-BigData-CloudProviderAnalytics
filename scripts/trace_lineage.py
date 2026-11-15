"""Linaje de una fila de Gold hasta Landing (E2-11): de un costo del mart a los archivos originales.

Uso (desde la raíz del repositorio, con el lago ya construido):
    python scripts/trace_lineage.py                                   # ejemplo: eventos a tiempo, tardíos y corregidos
    python scripts/trace_lineage.py --org org_53lc58dr --fecha 2025-08-31 --servicio analytics

Recorre, en orden inverso al pipeline, y verifica en cada paso que los números cierran:
    1. Gold      la fila de org_daily_usage_by_service y la ejecución que la escribió (gold_run_id → _runs)
    2. Silver    los eventos enriquecidos que suman esa fila (mismo costo y cantidad de eventos)
    3. Silver    esos eventos en el histórico canónico: camino de llegada y lotes de Bronze de origen
    4. Bronze    los manifiestos de esos lotes: qué archivo trajo cada uno y en qué ejecución de S1
    5. Landing   el SHA-256 de cada archivo, recalculado ahora y comparado con el registrado al ingerirlo
Deja el recorrido en evidence/entrega2/e2_11_linaje_ejemplo.json.
"""
import argparse
import hashlib
import json
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.common.config import EVENTS_DIR, EVIDENCE_DIR, GOLD, RUNS, SILVER
from src.common.spark import get_spark
from src.streaming.bronze_events import Paths


def run_log(process: str, run_id: str):
    path = RUNS / process / f"{run_id}.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--org", default="org_53lc58dr")
    parser.add_argument("--fecha", default="2025-08-21")
    parser.add_argument("--servicio", default="database")
    args = parser.parse_args()
    spark = get_spark("trace-lineage")
    key = (F.col("org_id") == args.org) & (F.col("service") == args.servicio)
    steps, ok = {}, True

    # 1. Gold
    gold = spark.read.parquet(str(GOLD / "org_daily_usage_by_service")).where(key & (F.col("usage_date") == args.fecha)).collect()
    if len(gold) != 1:
        print(f"No hay una fila de Gold para {args.org} · {args.fecha} · {args.servicio}")
        return 1
    g = gold[0]
    g_log = run_log("g1_gold_finops", g.gold_run_id)
    steps["1_gold"] = {"tabla": "gold/org_daily_usage_by_service", "daily_cost_usd": str(g.daily_cost_usd),
                       "events": g.events, "gold_run_id": g.gold_run_id,
                       "registro_de_la_ejecucion": f"_runs/g1_gold_finops/{g.gold_run_id}.json",
                       "ejecucion_ok": bool(g_log and g_log["status"] == "ok")}
    ok &= steps["1_gold"]["ejecucion_ok"]

    # 2. Silver enriquecido: los eventos que suman la fila de Gold
    enriched = spark.read.parquet(str(SILVER / "usage_events_enriched")).where(key & (F.col("event_date") == args.fecha))
    agg = enriched.agg(F.count("*").alias("n"), F.sum("cost_usd").alias("cost")).first()
    same = agg["n"] == g.events and Decimal(agg["cost"]) == Decimal(g.daily_cost_usd)
    steps["2_silver_enriquecido"] = {"tabla": "silver/usage_events_enriched", "eventos": agg["n"],
                                     "costo_usd": str(agg["cost"]), "coincide_con_gold": same}
    ok &= same

    # 3. Silver canónico: camino de llegada y lotes de Bronze de origen
    ids = [r.event_id for r in enriched.select("event_id").collect()]
    canon = spark.read.parquet(str(SILVER / "usage_events")).where(F.col("event_id").isin(ids))
    batches = sorted({r.bronze_batch_id for r in canon.select("bronze_batch_id").distinct().collect()})
    steps["3_silver_canonico"] = {
        "tabla": "silver/usage_events", "eventos": canon.count(),
        "por_camino": {r.arrival_path: r["count"] for r in canon.groupBy("arrival_path").count().collect()},
        "corregidos_o_marcados": canon.where("unit_inferred OR cost_negative").count(),
        "lotes_de_bronze": batches,
        "silver_run_ids": sorted({r.silver_run_id for r in canon.select("silver_run_id").distinct().collect()})}
    ok &= steps["3_silver_canonico"]["eventos"] == len(ids)
    spark.stop()

    # 4. Bronze: manifiesto de cada lote → archivo de Landing y ejecución de S1
    manifests = Paths().manifest
    files = {}
    for b in batches:
        m = json.loads((manifests / f"batch_id={b}.json").read_text(encoding="utf-8"))
        for f in m["files"]:
            files[f] = {"lote": b, "s1_run_id": m["run_id"], "lote_completo": True}
    steps["4_bronze"] = {"manifiestos": f"_runs/s1_bronze_events/batches/batch_id=<lote>.json",
                         "lotes": len(batches), "archivos": len(files)}

    # 5. Landing: integridad de cada archivo contra el SHA-256 registrado al ingerirlo
    for name, info in files.items():
        s1 = run_log("s1_bronze_events", info["s1_run_id"]) or {}
        recorded = next((i["sha256"] for i in s1.get("inputs", []) if i["file"] == name), None)
        current = sha256(EVENTS_DIR / name) if (EVENTS_DIR / name).exists() else None
        info.update(sha256_registrado=recorded, sha256_actual=current, intacto=recorded is not None and recorded == current)
    steps["5_landing"] = {"carpeta": "landing/usage_events_stream/", "archivos": files,
                          "todos_intactos": all(i["intacto"] for i in files.values())}
    ok &= steps["5_landing"]["todos_intactos"]

    result = {"fila": {"org_id": args.org, "usage_date": args.fecha, "service": args.servicio},
              "recorrido": steps, "linaje_completo_y_consistente": bool(ok)}
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / "e2_11_linaje_ejemplo.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Gold      {args.org} · {args.fecha} · {args.servicio}: USD {g.daily_cost_usd}, {g.events} eventos "
          f"(ejecución {g.gold_run_id})")
    print(f"Silver    {agg['n']} eventos enriquecidos, USD {agg['cost']} · coincide con Gold: {same}")
    print(f"          caminos {steps['3_silver_canonico']['por_camino']} · lotes de Bronze {batches}")
    print(f"Bronze    {len(files)} archivo(s) de Landing según los manifiestos")
    for name, i in files.items():
        print(f"Landing   {name}: lote {i['lote']}, SHA-256 {'intacto' if i['intacto'] else 'DISTINTO o sin registro'}")
    print(f"\nlinaje completo y consistente: {bool(ok)}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
