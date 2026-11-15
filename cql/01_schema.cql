-- Serving FinOps en Cassandra/AstraDB (E2-08, D-17).
-- El keyspace cloud_analytics se crea desde el portal de Astra: AstraDB no acepta CREATE KEYSPACE por CQL.
-- Se puede ejecutar más de una vez (IF NOT EXISTS).

-- Consulta 1: costos y requests diarios de una organización por servicio en un rango de fechas.
--   Partición = org_id: todas las filas que pide la consulta están en una sola partición.
--   Clustering = usage_date DESC, service: el rango de fechas se resuelve con una lectura secuencial y los días
--   más recientes salen primero. Una fila por (org_id, usage_date, service): el grano del mart de Gold.
CREATE TABLE IF NOT EXISTS cloud_analytics.org_daily_usage_by_service (
    org_id               text,
    usage_date           date,
    service              text,
    org_name             text,
    plan_tier            text,
    industry             text,
    daily_cost_usd       decimal,
    requests             double,
    cpu_hours            double,
    storage_gb_hours     double,
    genai_tokens         bigint,
    carbon_kg            double,
    events               int,
    resources_active     int,
    negative_cost_events int,
    negative_cost_usd    decimal,
    unit_inferred_events int,
    late_events          int,
    carbon_coverage      double,
    gold_run_id          text,
    loaded_at            timestamp,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- Consulta 2: Top-N servicios por costo acumulado en los últimos 14 días para una organización.
--   Cassandra solo ordena por columnas de clustering y no agrupa por service en la tabla anterior, así que el
--   ranking llega calculado desde Spark (gold/top_services_by_org_14d) y cost_rank es columna de clustering:
--   la consulta es una lectura directa con LIMIT N, sin agregar ni ordenar en el cliente.
CREATE TABLE IF NOT EXISTS cloud_analytics.top_services_by_org_14d (
    org_id           text,
    window_end_date  date,
    cost_rank        int,
    service          text,
    cost_usd         decimal,
    requests         double,
    days_with_usage  int,
    window_days      int,
    window_complete  boolean,
    gold_run_id      text,
    loaded_at        timestamp,
    PRIMARY KEY ((org_id), window_end_date, cost_rank)
) WITH CLUSTERING ORDER BY (window_end_date DESC, cost_rank ASC);
