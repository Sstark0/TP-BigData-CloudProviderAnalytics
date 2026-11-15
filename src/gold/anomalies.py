"""
G3 · Anomalías de costo: cost_anomaly_mart (E2-09, D-18).

Se presta particular atención a la siguiente pregunta de negocio
    ¿Qué días una organización gastó en un servicio mucho más de lo que venía gastando?
    (cuánto se esperaba, cuánto de más, qué tan raro es).

Grano
    org_id, usage_date, service: el mismo del mart org_daily_usage_by_service, del que sale.

Método: z-score robusto con MAD sobre una línea base móvil
    Para cada fila se mira la historia de la misma serie (organización + servicio) en los `window_days` días
    anteriores (sin incluir el día evaluado, así no hay "mirar el futuro" y el día raro no contamina su propia
    referencia):
        mediana   = costo típico de la serie
        MAD       = mediana de |costo − mediana|: cuánto se aparta un día típico
        robust_z  = 0,6745 × (costo − mediana) / MAD
    La mediana y la MAD no se mueven por unos pocos días extremos; la media y el desvío estándar sí (un spike
    infla el desvío y esconde al siguiente). 0,6745 hace que robust_z sea comparable con un z-score normal.
    Si la MAD es 0 (días casi iguales) se usa 1,2533 × desvío absoluto medio, la variante habitual.

Flag
    spike           robust_z > z_threshold (3,5, Iglewicz y Hoaglin) Y exceso ≥ min_excess_usd (materialidad:
                    un salto de USD 0,05 a USD 0,40 es estadísticamente raro pero no le importa a FinOps)
    costo_negativo  el día tiene costos marcados como negativos por la regla de calidad (D-15)

Arranque en frío: percentil del servicio
    Con menos de `min_history` días de historia la mediana y la MAD no son confiables (score_status =
    'historia_insuficiente'). Para no dejar esos días sin vigilancia, se marca spike si el costo supera el
    percentil `cold_start_percentile` (p99) del costo diario de ese servicio entre todas las organizaciones, y el
    exceso sobre ese percentil supera la materialidad. Es una referencia de todo el período (mira hacia adelante):
    es la limitación aceptada para los primeros días de cada serie.

También se calcula el z-score clásico (media y desvío) solo para comparar métodos en la evidencia.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.io import replace_table
from src.common.runlog import RunLog

PROCESS = "g3_cost_anomalies"
TABLE = "cost_anomaly_mart"
SERIES = ["org_id", "service"]
KEY = ["org_id", "usage_date", "service"]
WINDOW_DAYS, MIN_HISTORY, Z_THRESHOLD, MIN_EXCESS_USD, COLD_START_PERCENTILE = 28, 7, 3.5, 5.0, 0.99
MAD_TO_SIGMA = 0.6745           # 1 / 1,4826: la MAD de una normal equivale a 0,6745 desvíos
MEAN_AD_TO_SIGMA = 1.2533       # sqrt(pi/2): desvío absoluto medio → desvío, para cuando la MAD es 0


def baseline(gold: DataFrame, window_days: int = WINDOW_DAYS) -> DataFrame:
    """Mediana, MAD, media, desvío y días de historia de cada fila, con los `window_days` días anteriores."""
    cur = gold.select(*KEY, F.col("daily_cost_usd").cast("double").alias("cost"))
    hist = cur.select(*SERIES, F.col("usage_date").alias("h_date"), F.col("cost").alias("h_cost"))
    pairs = cur.join(hist, SERIES).where((F.col("h_date") < F.col("usage_date"))
                                         & (F.col("h_date") >= F.date_sub("usage_date", window_days)))
    stats = pairs.groupBy(*KEY).agg(F.median("h_cost").alias("baseline_median_usd"),
                                    F.avg("h_cost").alias("baseline_mean_usd"),
                                    F.stddev_samp("h_cost").alias("baseline_std_usd"),
                                    F.count("*").alias("history_days"))
    spread = (pairs.join(stats, KEY)
              .groupBy(*KEY).agg(F.median(F.abs(F.col("h_cost") - F.col("baseline_median_usd"))).alias("mad"),
                                 F.avg(F.abs(F.col("h_cost") - F.col("baseline_median_usd"))).alias("mean_ad")))
    return cur.join(stats, KEY, "left").join(spread, KEY, "left")


def score(gold: DataFrame, run_id: str, window_days: int = WINDOW_DAYS, min_history: int = MIN_HISTORY,
          z_threshold: float = Z_THRESHOLD, min_excess_usd: float = MIN_EXCESS_USD,
          cold_start_percentile: float = COLD_START_PERCENTILE) -> DataFrame:
    b = baseline(gold, window_days)
    service_pct = gold.groupBy("service").agg(
        F.expr(f"percentile(cast(daily_cost_usd as double), {cold_start_percentile})").alias("service_p99_usd"))
    enough = F.coalesce(F.col("history_days"), F.lit(0)) >= min_history
    scale = (F.when(F.col("mad") > 0, F.col("mad") / MAD_TO_SIGMA)
             .when(F.col("mean_ad") > 0, F.col("mean_ad") * MEAN_AD_TO_SIGMA))
    excess = F.col("cost") - F.col("baseline_median_usd")
    robust_z = F.when(enough & scale.isNotNull(), excess / scale)
    z_classic = F.when(enough & (F.col("baseline_std_usd") > 0),
                       (F.col("cost") - F.col("baseline_mean_usd")) / F.col("baseline_std_usd"))
    scored = (b.withColumn("excess_cost_usd", F.when(enough, excess))
              .withColumn("robust_z", F.round(robust_z, 3))
              .withColumn("z_classic", F.round(z_classic, 3)))
    context = gold.select(*KEY, "org_name", "plan_tier", "daily_cost_usd", "requests", "events",
                          "negative_cost_events", "negative_cost_usd")
    mad_spike = (F.col("robust_z") > z_threshold) & (F.col("excess_cost_usd") >= min_excess_usd)
    cold_spike = ((F.col("score_status") == "historia_insuficiente")
                  & (F.col("daily_cost_usd") - F.col("service_p99_usd") >= min_excess_usd))
    negative = F.col("negative_cost_events") > 0
    return (context.join(scored.drop("cost"), KEY).join(F.broadcast(service_pct), "service")
            .withColumn("score_status", F.when(F.col("history_days") >= min_history, "puntuado")
                        .otherwise("historia_insuficiente"))
            .withColumn("detection", F.when(F.coalesce(mad_spike, F.lit(False)), "mad")
                        .when(cold_spike, "percentil_servicio"))
            .withColumn("is_spike", F.col("detection").isNotNull())
            .withColumn("has_negative_cost", negative)
            .withColumn("is_anomaly", F.col("is_spike") | F.col("has_negative_cost"))
            .withColumn("anomaly_type", F.when(F.col("is_spike") & negative, "spike+costo_negativo")
                        .when(F.col("is_spike"), "spike").when(negative, "costo_negativo"))
            .withColumn("severity", F.when(F.col("is_spike") & ((F.col("robust_z") >= 2 * z_threshold)
                                                                | (F.col("detection") == "percentil_servicio")), "alta")
                        .when(F.col("is_spike"), "media").when(negative, "revisar"))
            .withColumn("service_p99_usd", F.round("service_p99_usd", 6))
            .withColumn("method", F.lit(f"robust_z_mad_{window_days}d"))
            .withColumn("z_threshold", F.lit(z_threshold))
            .withColumn("min_excess_usd", F.lit(min_excess_usd))
            .withColumn("baseline_median_usd", F.round("baseline_median_usd", 6))
            .withColumn("baseline_mad_usd", F.round("mad", 6))
            .withColumn("excess_cost_usd", F.round("excess_cost_usd", 6))
            .drop("mad", "mean_ad", "baseline_mean_usd", "baseline_std_usd")
            .withColumn("gold_processed_ts", F.current_timestamp())
            .withColumn("gold_run_id", F.lit(run_id)))


def known_spikes(enriched: DataFrame, factor: float = 10.0) -> DataFrame:
    """Referencia de E1: eventos con costo > factor × p99 del costo por evento. Una fila por (org, día, servicio)."""
    p99 = enriched.agg(F.expr("percentile(cast(cost_usd as double), 0.99)")).first()[0]
    return (enriched.where(F.col("cost_usd").cast("double") > factor * p99)
            .select("org_id", F.col("event_date").alias("usage_date"), "service").distinct())


def build_anomalies(spark: SparkSession, run: RunLog, gold: DataFrame, path, **params) -> DataFrame:
    run.count("gold_filas", gold.count())
    run.count("anomaly_filas", replace_table(score(gold, run.run_id, **params), path, partition_by=["usage_date"]))
    mart = spark.read.parquet(str(path))
    run.count("puntuadas", mart.where("score_status = 'puntuado'").count())
    run.count("historia_insuficiente", mart.where("score_status = 'historia_insuficiente'").count())
    run.count("flag_spike", mart.where("is_spike").count())
    run.count("flag_spike.mad", mart.where("detection = 'mad'").count())
    run.count("flag_spike.percentil_servicio", mart.where("detection = 'percentil_servicio'").count())
    run.count("flag_costo_negativo", mart.where("has_negative_cost").count())
    run.count("flag_cualquiera", mart.where("is_anomaly").count())
    run.count("claves_distintas", mart.select(*KEY).distinct().count())
    run.add_output(path)
    if not (run.check_balance("gold_filas", ["anomaly_filas"])
            and run.check_balance("anomaly_filas", ["claves_distintas"])
            and run.check_balance("anomaly_filas", ["puntuadas", "historia_insuficiente"])):
        raise RuntimeError("cost_anomaly_mart no concilia con el mart FinOps; ver el registro de la ejecución")
    return mart
