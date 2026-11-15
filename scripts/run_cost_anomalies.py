"""Anomalías de costo: cost_anomaly_mart (E2-09).

Requiere Gold FinOps (scripts/run_gold_finops.py) y Silver enriquecido (para la referencia de spikes de E1).

Uso (desde la raíz del repositorio):
    python scripts/run_cost_anomalies.py

Deja en evidence/entrega2/e2_09_cost_anomalies.json: parámetros, distribución de flags, recall frente a los
spikes conocidos de E1, sensibilidad al umbral, comparación con el z-score clásico, los casos de mayor exceso y
las limitaciones.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.common.config import EVIDENCE_DIR, GOLD, SILVER
from src.common.runlog import RunLog, describe_output
from src.common.spark import get_spark
from src.gold import anomalies as A
from src.gold.finops import TABLE as FINOPS


def counts(df, *cols):
    return {" / ".join(str(r[c]) for c in cols): r["count"] for r in df.groupBy(*cols).count().orderBy(*cols).collect()}


def plain(row):
    return {k: (v if isinstance(v, (int, float, bool, type(None))) else str(v)) for k, v in row.asDict().items()}


def main() -> int:
    if not (GOLD / FINOPS).exists():
        print("Falta el mart FinOps: ejecutá antes scripts/run_gold_finops.py")
        return 1
    spark = get_spark("cost-anomalies")
    with RunLog(A.PROCESS) as run:
        for name in ("WINDOW_DAYS", "MIN_HISTORY", "Z_THRESHOLD", "MIN_EXCESS_USD", "COLD_START_PERCENTILE"):
            run.param(name.lower(), getattr(A, name))
        gold = spark.read.parquet(str(GOLD / FINOPS))
        mart = A.build_anomalies(spark, run, gold, GOLD / A.TABLE)

    known = A.known_spikes(spark.read.parquet(str(SILVER / "usage_events_enriched")))
    hits = known.join(mart, A.KEY, "left")
    scored = mart.where("score_status = 'puntuado'")

    def recall(flagged):
        return known.join(flagged.select(*A.KEY), A.KEY).count()

    sensitivity = []
    for z in (3.0, 3.5, 5.0):
        for excess in (0.0, 5.0):
            flagged = scored.where(f"robust_z > {z} AND excess_cost_usd >= {excess}")
            sensitivity.append({"z_umbral": z, "exceso_min_usd": excess, "flags_mad": flagged.count(),
                                "spikes_conocidos_detectados": recall(flagged)})

    summary = {k: run.data[k] for k in ("run_id", "params", "counts", "balances")}
    summary["tabla"] = describe_output(GOLD / A.TABLE)
    summary["distribucion"] = {
        "por_tipo_y_severidad": counts(mart.where("is_anomaly"), "anomaly_type", "severity"),
        "spikes_por_servicio": counts(mart.where("is_spike"), "service"),
        "spikes_por_deteccion": counts(mart.where("is_spike"), "detection"),
        # Con pocos datos (por ejemplo, la muestra de data/sample) puede no haber ningún día puntuado
        "tasa_spike_sobre_puntuadas": (round(scored.where("detection = 'mad'").count() / n_scored, 4)
                                       if (n_scored := scored.count()) else None),
        "robust_z_percentiles_50_90_95_99_999": scored.select(
            F.expr("percentile(robust_z, array(0.5, 0.9, 0.95, 0.99, 0.999))")).first()[0],
    }
    summary["evaluacion"] = {
        "referencia": "spikes de E1: eventos con costo > 10 × p99 del costo por evento (una fila por org, día y servicio)",
        "spikes_conocidos": known.count(),
        "detectados": hits.where("is_spike").count(),
        "detectados_por_metodo": counts(hits.where("is_spike"), "detection"),
        "no_detectados": [plain(r) for r in hits.where("NOT is_spike OR is_spike IS NULL")
                          .select(*A.KEY, "daily_cost_usd", "robust_z", "history_days").collect()],
        "sensibilidad_al_umbral": sensitivity,
        "comparacion_z_clasico": {
            "z_clasico_mayor_3": scored.where("z_classic > 3").count(),
            "robust_z_spike_mad": scored.where("detection = 'mad'").count(),
            "ambos": scored.where("z_classic > 3 AND detection = 'mad'").count(),
            "spikes_conocidos_con_z_clasico": recall(scored.where("z_classic > 3")),
            "spikes_conocidos_con_robust_z": recall(scored.where("detection = 'mad'")),
        },
    }
    summary["mayor_exceso"] = [plain(r) for r in mart.where("is_spike")
                               .orderBy(F.desc(F.coalesce("excess_cost_usd", F.col("daily_cost_usd") - F.col("service_p99_usd"))))
                               .select(*A.KEY, "org_name", "daily_cost_usd", "baseline_median_usd", "baseline_mad_usd",
                                       "excess_cost_usd", "robust_z", "detection", "severity").limit(5).collect()]
    summary["limitaciones"] = [
        "No hay etiquetas reales de anomalía: la precisión no se puede medir. El recall se mide contra los 34 spikes "
        "evidentes encontrados en E1, que son los casos fáciles; anomalías sutiles no tienen referencia.",
        "Las series tienen días sin uso (no son ceros): la línea base usa solo los días con uso de los últimos 28.",
        "Arranque en frío: con menos de 7 días de historia se usa el p99 del servicio de todo el período, que mira hacia "
        "adelante. En operación real se usaría el p99 del período anterior.",
        "Solo se marcan subas de costo. Las bajas (y los créditos) se ven por el flag de costo negativo de D-15.",
        "Umbrales (3,5 y USD 5) fijados con este dataset; deben revisarse con FinOps cuando haya casos confirmados.",
    ]
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / "e2_09_cost_anomalies.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary["evaluacion"]["sensibilidad_al_umbral"], ensure_ascii=False))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
