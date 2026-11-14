"""Reporte de las reglas de calidad de Silver de eventos (E2-06).

Lee lo que ya dejó scripts/run_silver_events.py (Silver y su registro de calidad); no recalcula nada.

Uso (desde la raíz del repositorio):
    python scripts/run_silver_events.py      # aplica las reglas
    python scripts/report_quality.py         # conteos por regla y acción, impacto en costo y muestras

Deja el resultado en evidence/entrega2/e2_06_quality_rules.json.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.batch.silver_events import COST_TOLERANCE_USD, EXPECTED_UNIT, FIX_RULES, PROCESS, REJECT_RULES, \
    SILVER_SOURCE
from src.common.config import EVIDENCE_DIR, quarantine_path, table_path
from src.common.runlog import latest_run
from src.common.spark import get_spark


def main() -> int:
    run = latest_run(PROCESS)
    if run is None:
        print("No hay una ejecución correcta de Silver de eventos: ejecutá antes scripts/run_silver_events.py")
        return 1
    spark = get_spark("quality-report")
    log = spark.read.parquet(str(quarantine_path(SILVER_SOURCE)))
    silver = spark.read.parquet(str(table_path("silver", "usage_events")))

    by_rule = (log.groupBy("rule_id", "action").count().orderBy("action", "rule_id").collect())
    rules = [{"rule_id": r, "accion": "rechazado", "condicion_falla": c, "motivo": t} for r, c, t in REJECT_RULES]
    rules += [{"rule_id": r, "accion": a, "condicion_falla": c, "motivo": t} for r, a, c, t in FIX_RULES]
    rules.append({"rule_id": "event_id_duplicate", "accion": "descartado",
                  "condicion_falla": "misma event_id ya llegó antes", "motivo": "se conserva la primera llegada"})
    counts = {f"{r['rule_id']}/{r['action']}": r["count"] for r in by_rule}
    for rule in rules:
        rule["registros_en_el_dataset"] = counts.get(f"{rule['rule_id']}/{rule['accion']}", 0)

    def cost(df):
        return round(float(df.agg(F.sum("cost_usd_increment")).first()[0] or 0), 2)

    def samples(rule_id, n=2):
        return [r.asDict() for r in log.where(F.col("rule_id") == rule_id)
                .select("rule_id", "action", "reason", "record_key", "raw_record", "source_file")
                .orderBy("record_key").limit(n).collect()]

    summary = {
        "silver_run_id": run["run_id"],
        "parametros": {"unidad_esperada_por_metrica": EXPECTED_UNIT, "tolerancia_costo_usd": COST_TOLERANCE_USD},
        "reglas": rules,
        "balances_de_la_ejecucion": run.get("balances", []),
        "impacto": {
            "silver_eventos": silver.count(),
            "silver_costo_total_usd": cost(silver),
            "unidad_inferida_eventos": silver.where("unit_inferred").count(),
            "unidad_inferida_costo_usd_conservado": cost(silver.where("unit_inferred")),
            "unidad_inferida_por_metrica": {r["metric"]: r["count"] for r in
                                            silver.where("unit_inferred").groupBy("metric").count().collect()},
            "costo_negativo_marcado_eventos": silver.where("cost_negative").count(),
            "costo_negativo_marcado_usd": cost(silver.where("cost_negative")),
            "costo_negativo_dentro_de_tolerancia_eventos":
                silver.where(f"cost_usd_increment < 0 AND NOT cost_negative").count(),
        },
        "muestras": {r["rule_id"]: samples(r["rule_id"]) for r in rules if r["registros_en_el_dataset"]},
    }
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / "e2_06_quality_rules.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False, default=str),
                                                  encoding="utf-8")
    for rule in rules:
        print(f"{rule['rule_id']:<26} {rule['accion']:<11} {rule['registros_en_el_dataset']:>6}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
