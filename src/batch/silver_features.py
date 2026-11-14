"""
B3b · Eventos enriquecidos y features diarias (E2-05).

Eventos enriquecidos (silver/usage_events_enriched, partición event_date)
    Cada evento de Silver (E2-04) con: 
    - Los atributos de su organización (customers_orgs) y de su recurso (resources)
    - Una columna por medida: `requests`, `cpu_hours`, `storage_gb_hours` toman `value` solo cuando la métrica del evento es esa, así nunca se suman horas con requests (diseño E1, §6).
    - Los joins son LEFT: un evento sin maestro no se pierde, queda con `org_found` / `resource_found` en falso.

Features diarias (silver/org_daily_features, una fila por organización y día UTC del evento)
    daily_cost_usd    suma de cost_usd_increment, en decimal(18,6) para que la suma sea exacta y concilie
    requests          suma de value de los eventos metric = requests (NULL si ese día no hubo ninguno válido)
    cpu_hours, storage_gb_hours  ídem con su métrica
    genai_tokens, carbon_kg      solo existen en eventos schema v2; NULL si ese día no hubo ninguno
    carbon_coverage   fracción de eventos del día con carbon_kg: indica cuán completo es carbon_kg ese día
    events            cantidad de eventos
    Los costos negativos se suman tal cual; E2-06 los marca con su regla de calidad.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.io import replace_table
from src.common.runlog import RunLog

COST = "decimal(18,6)"
MEASURES = ["requests", "cpu_hours", "storage_gb_hours"]
ORG_ATTRS = ["org_name", "industry", "plan_tier", "is_enterprise", "hq_region", "lifecycle_stage"]
RESOURCE_ATTRS = {"state": "resource_state", "env": "resource_env", "team": "resource_team",
                  "costcenter": "resource_costcenter", "has_pii": "resource_has_pii"}


def enrich(events: DataFrame, orgs: DataFrame, resources: DataFrame) -> DataFrame:
    org = orgs.select("org_id", *ORG_ATTRS, F.lit(True).alias("org_found"))
    res = resources.select("resource_id", *[F.col(c).alias(a) for c, a in RESOURCE_ATTRS.items()],
                           F.lit(True).alias("resource_found"))
    joined = (events.join(F.broadcast(org), "org_id", "left")
                    .join(F.broadcast(res), "resource_id", "left"))
    return joined.select(
        "event_id", "event_ts", "org_id", "resource_id", "service", "region", "metric", "value", "unit",
        "unit_inferred", "cost_negative", "schema_version",
        F.col("cost_usd_increment").cast(COST).alias("cost_usd"),
        *[F.when(F.col("metric") == m, F.col("value")).alias(m) for m in MEASURES],
        "genai_tokens", "carbon_kg",
        *ORG_ATTRS, *RESOURCE_ATTRS.values(),
        F.coalesce("org_found", F.lit(False)).alias("org_found"),
        F.coalesce("resource_found", F.lit(False)).alias("resource_found"),
        "arrival_path", "source_file", "event_date")


def daily_features(enriched: DataFrame) -> DataFrame:
    return (enriched.groupBy("org_id", "event_date")
            .agg(F.sum("cost_usd").alias("daily_cost_usd"),
                 *[F.sum(m).alias(m) for m in MEASURES],
                 F.count("requests").alias("requests_events"),
                 F.sum("genai_tokens").alias("genai_tokens"),
                 F.round(F.sum("carbon_kg"), 6).alias("carbon_kg"),
                 F.round(F.count("carbon_kg") / F.count(F.lit(1)), 4).alias("carbon_coverage"),
                 F.count(F.lit(1)).alias("events"),
                 F.sum(F.col("cost_negative").cast("int")).alias("negative_cost_events"),
                 *[F.first(a).alias(a) for a in ("org_name", "plan_tier", "industry")])
            .withColumn("silver_processed_ts", F.current_timestamp()))


def _micro_usd(df: DataFrame, column: str) -> int:
    """Suma de un costo en micro-USD (entero), para comparar sumas sin errores de redondeo."""
    total = df.agg(F.sum(column)).first()[0]
    return int(round(total * 1_000_000)) if total is not None else 0


def build_features(spark: SparkSession, run: RunLog, events: DataFrame, orgs: DataFrame, resources: DataFrame,
                   silver_root) -> dict:
    enriched_path, features_path = silver_root / "usage_events_enriched", silver_root / "org_daily_features"
    run.count("silver_eventos", events.count())
    run.count("enriquecidos", replace_table(enrich(events, orgs, resources), enriched_path,
                                            partition_by=["event_date"]))
    enriched = spark.read.parquet(str(enriched_path))
    run.count("enriquecidos.sin_org", enriched.where("NOT org_found").count())
    run.count("enriquecidos.sin_recurso", enriched.where("NOT resource_found").count())
    run.count("features_org_dia", replace_table(daily_features(enriched), features_path))
    features = spark.read.parquet(str(features_path))

    # Conciliaciones: ningún evento se pierde ni se duplica en el join, y las features suman lo mismo que Silver
    run.count("costo_silver_micro_usd", _micro_usd(events.select(F.col("cost_usd_increment").cast(COST).alias("c")), "c"))
    run.count("costo_features_micro_usd", _micro_usd(features, "daily_cost_usd"))
    run.count("eventos_en_features", features.agg(F.sum("events")).first()[0] or 0)
    run.count("requests_silver", int(events.where("metric = 'requests'").agg(F.sum("value")).first()[0] or 0))
    run.count("requests_features", int(features.agg(F.sum("requests")).first()[0] or 0))
    run.add_output(enriched_path)
    run.add_output(features_path)
    checks = [run.check_balance("silver_eventos", ["enriquecidos"]),
              run.check_balance("silver_eventos", ["eventos_en_features"]),
              run.check_balance("costo_silver_micro_usd", ["costo_features_micro_usd"]),
              run.check_balance("requests_silver", ["requests_features"])]
    if not all(checks):
        raise RuntimeError("Las features no concilian con Silver; ver el registro de la ejecución")
    return {"enriched": enriched, "features": features}
