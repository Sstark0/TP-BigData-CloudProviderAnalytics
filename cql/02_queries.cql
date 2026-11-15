-- Las dos consultas mínimas de la entrega 2. Ejecutar en el CQL Console de Astra.
-- La organización es la de mayor costo del dataset y el "hoy" es el último día con datos (31/08/2025),
-- igual que en las consultas de referencia de evidence/entrega2/e2_07_gold_finops.json.

-- Consulta 1: costos y requests diarios por servicio de una organización, últimos 7 días.
SELECT usage_date, service, daily_cost_usd, requests
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = 'org_53lc58dr'
  AND usage_date >= '2025-08-25' AND usage_date <= '2025-08-31';

-- Consulta 2: top 5 servicios por costo acumulado en los 14 días que terminan el 31/08/2025.
SELECT cost_rank, service, cost_usd, requests, days_with_usage
FROM cloud_analytics.top_services_by_org_14d
WHERE org_id = 'org_53lc58dr' AND window_end_date = '2025-08-31'
LIMIT 5;
