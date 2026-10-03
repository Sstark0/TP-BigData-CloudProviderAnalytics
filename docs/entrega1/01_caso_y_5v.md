# 1. Interpretación del caso y justificación Big Data

> Entrega 1 · Documento de diseño · Sección 1 · v0.2 (2026-09-30)
> Evidencia: `notebooks/01_exploracion_fuentes.ipynb` y `evidence/entrega1/perfil_fuentes.json`.
> Los valores marcados como **(supuesto)** son hipótesis de diseño, no mediciones.

## 1.1 El problema

Somos el área de datos de un proveedor de nube. Los datos de uso, facturación, clientes y soporte
llegan **crudos, en formatos distintos y con errores**: números como texto, nulos, costos negativos, un cambio
de esquema a mitad del período y una facturación con monedas inconsistentes. Con esos datos, las áreas de
negocio no pueden responder con confianza cuánto consume y cuánto paga cada cliente, ni cómo se lo atiende.

**Objetivo:** construir una plataforma que **ingiera** esas fuentes (en lote y en flujo continuo), las
**limpie y conforme** en un Data Lake por capas y **publique** tablas listas para consultar, con la
calidad, trazabilidad y reproducibilidad necesarias para que los números sean confiables.

## 1.2 Usuarios y sus preguntas

| Usuario | Qué necesita decidir | Preguntas principales | Consulta obligatoria / mart |
|---|---|---|---|
| **FinOps** (finanzas de la nube) | Dónde se va el gasto, qué clientes y servicios generan ingresos, qué costos son anómalos | ¿Cuánto costó y cuántos requests hubo por organización, servicio y día? ¿Cuáles son los servicios más caros de una organización en los últimos 14 días? ¿Cuál es la facturación mensual con créditos e impuestos, normalizada a USD? ¿Qué costos son anómalos? | Q1, Q2, Q4 · `org_daily_usage_by_service`, `revenue_by_org_month`, `cost_anomaly_mart` |
| **Soporte** | Si se cumple lo prometido a los clientes | ¿Cómo evolucionan los tickets críticos y la tasa de incumplimiento de SLA por día en los últimos 30 días? ¿Cuál es el CSAT promedio? | Q3 · `tickets_by_org_date` |
| **Producto / Usage** | Qué servicios se usan y cómo crece la IA generativa | ¿Cuántos tokens GenAI y qué costo estimado por día? ¿Cuánto carbono se emite? | Q5 · `genai_tokens_by_org_date` |
| **Equipo de datos** (nosotros, operadores) | Si la plataforma es confiable | ¿Llegó todo? ¿Qué se rechazó y por qué? ¿Se puede reprocesar sin duplicar? | Quarantine, métricas de calidad, linaje |

Las consultas Q1 a Q5 son las cinco obligatorias de la consigna (sección 7.4) y se responden desde Cassandra/AstraDB.

> **Nota de terminología.** Usamos *revenue* solo para nombrar el mart y la consulta tal como los define la consigna.
> Para el dato en sí hablamos de **facturación mensual normalizada**, porque la regla que la obtiene a partir de
> `billing_monthly` (moneda, tasa de cambio, créditos) sigue abierta (D-08, D-09).

## 1.3 Objetivos medibles (criterios de éxito)

Los objetivos se **definen** en esta entrega y se **verifican** en las siguientes, cuando exista el pipeline.

| # | Objetivo | Indicador y meta | Cómo se verifica | Se verifica en |
|---|---|---|---|---|
| O1 | **Completitud de la ingesta** | 100 % de los registros de Landing quedan clasificados como aceptados, rechazados o duplicados (eventos: 43.200) | Balance de aceptados/rechazados/duplicados, por fuente y corrida | Entrega 2 |
| O2 | **Unicidad e idempotencia** | 0 `event_id` duplicados en Bronze/Silver, también después de re-ejecutar el pipeline | Conteo antes/después de una segunda ejecución | Entrega 2 |
| O3 | **Calidad controlada** | ≥ 3 reglas de calidad activas; cada registro rechazado o marcado tiene su motivo; % por regla reportado | Tabla de quarantine y métricas por regla | Entrega 2 |
| O4 | **Frescura** | Eventos visibles en Silver en ≤ 5 minutos desde que llega el archivo **(supuesto)**; maestros y facturación actualizados con frecuencia diaria / mensual | Diferencia entre `silver_processed_ts` (disponibilidad durable en Silver) y `arrival_ts` (detección del archivo) | Entrega 2 |
| O5 | **Consistencia entre capas** | El costo total en Gold coincide con el de Silver para el mismo período (igualdad a precisión definida; tolerancia de redondeo declarada, salvo registros en quarantine) | Conciliación de sumas Silver vs. Gold | Entrega 2 |
| O6 | **Consultas de negocio** | Las 5 consultas obligatorias se resuelven desde los marts Gold y el modelo de serving propuesto. Las metas de desempeño se fijan al diseñar Cassandra | CQL + captura de resultados | Final (2 consultas en la entrega 2) |
| O7 | **Reproducibilidad** | El pipeline completo corre desde un entorno limpio siguiendo el Quickstart | Ejecución por otro integrante del equipo | Entrega 2 y final |

## 1.4 Justificación Big Data: las 5V aplicadas al caso

Las V se usan como **marco para decidir**, no como lista para completar: para cada una indicamos la evidencia,
su peso en este caso y la decisión de diseño que provoca. Siguiendo el criterio docente de la clase 1, señalamos
cuáles son **dominantes**; eso no implica que las demás sean irrelevantes, sino que cada una aporta de forma distinta.

### Volumen — peso en la muestra: **bajo**

- **Medido:** 43.200 eventos en 60 días (exactamente 720 por día), 120 archivos JSONL, ≈ 12,3 MB, más 7 CSV
  con 307.906 bytes (≈ 301 KiB). Cada evento ocupa ≈ 300 bytes.
- **Lectura honesta:** el volumen de la muestra **no constituye por sí mismo una justificación de Big Data**.
- **Por qué igual se diseña para escalar:** el volumen de eventos crece en forma lineal con la cantidad de recursos
  monitoreados y con la frecuencia de medición (≈ 300 bytes por evento, según la muestra). La arquitectura usa
  procesamiento distribuido (Spark, Parquet particionado) para que ese crecimiento se absorba agregando máquinas sin
  cambiar el código: el contrato de transformaciones se conserva; despliegue y recursos se configuran por entorno.
- **Decisión:** escalamiento horizontal por diseño y tratamiento del problema de **archivos pequeños**
  (120 archivos de ~105 KB) compactando al escribir Bronze.

### Velocidad — peso: **alto (dominante)**

- **Origen del requerimiento:** la consigna pide métricas operativas de uso y costo **near real-time**, mientras que
  maestros y facturación se actualizan en lotes diarios o mensuales. La necesidad de velocidad proviene de ese
  **requerimiento funcional**.
- **Cómo lo refleja el dataset:** los eventos llegan fragmentados en 120 archivos JSONL que simulan una fuente de
  llegada incremental. Además, cada archivo contiene eventos de casi todo el período, lo que permite estudiar
  explícitamente el problema de **eventos fuera de orden** y la elección de watermark (simulación en la celda del
  watermark del notebook). Ese análisis es **evidencia experimental sobre el comportamiento temporal del dataset**, no una
  medición de latencia productiva.
- **Decisión:** dos ritmos distintos → **streaming** para eventos y **batch** para maestros y facturación
  (patrón híbrido, D-03). El streaming preserva todos los eventos en Bronze; dedupe y publicación incremental en Silver son operaciones con estado o control de lotes. Los agregados operativos se actualizan por micro-lote; el batch diario reconcilia histórico (D-04).

### Variedad — peso: **medio-alto**

- **Medido:** 8 fuentes en 2 formatos (CSV estructurado y JSONL semiestructurado), 3 dominios de negocio
  (FinOps, Soporte, Producto), 6 servicios × 3 métricas, 7 regiones, 3 monedas y **dos versiones de esquema**
  (v2 agrega `carbon_kg` y `genai_tokens` desde el 2025-07-18).
- **Decisión:** esquemas explícitos por fuente; un **esquema superset** para leer v1 y v2 con el mismo lector;
  conformación en Silver (tipos, unidades, monedas) antes de cruzar fuentes.

### Veracidad — peso: **alto (dominante)**

- **Medido** (entre otros):

  | Problema | Magnitud |
  |---|---|
  | `value` nulo / llegó como texto | 877 / 1.309 eventos |
  | `unit` nulo | 2.075 eventos (4,8 %) |
  | Costos negativos (< −0,01) | 211 eventos |
  | Spikes de costo (> 10 × p99) | 34 eventos |
  | Eventos anteriores a la creación de su recurso | 7.371 (17 %) |
  | Facturas USD con tasa ≠ 1 / montos ARS en escala USD | 160 / 51 facturas; la facturación normalizada varía 22 % según el criterio |
  | CSAT fuera de 1–5 / login anterior al alta | 40 tickets / 232 usuarios |

- **Decisión:** capa Bronze con tipos explícitos y columnas técnicas (`ingest_ts`, `source_file`), reglas de
  calidad con **quarantine**, **flags** de anomalía en lugar de borrar, y decisiones documentadas para cada
  interpretación ambigua (D-05, D-08, D-09).

### Valor — peso: **alto**

- **Medido:** ≈ 147.400 USD de costo de uso en 60 días, concentrado en `compute` y `genai` (62 %);
  facturación de 3 meses entre ≈ 164.000 y ≈ 211.000 USD según el criterio de normalización (D-08);
  1.000 tickets con 56 críticos y 95 SLA incumplidos.
- **Decisión:** los datos solo generan valor si llegan a quien decide → **marts Gold por dominio** con un grano
  definido y una **capa de serving en Cassandra** modelada a partir de las consultas (query-first).

### Síntesis

| V | Peso en el caso | Qué la hace relevante | Componente que provoca |
|---|---|---|---|
| Volumen | Bajo en la muestra | Crecimiento lineal con recursos y frecuencia; archivos pequeños | Spark + Parquet particionado + compactación |
| **Velocidad** | **Alto** | Requerimiento near real-time; llegada incremental y desordenada | Structured Streaming + batch (híbrido) |
| Variedad | Medio-alto | 2 formatos, 8 fuentes, esquema v1/v2 | Esquemas explícitos + conformación en Silver |
| **Veracidad** | **Alto** | Nulos, tipos ambiguos, anomalías, FX inconsistente | Reglas de calidad, quarantine, flags, linaje |
| Valor | Alto | Preguntas concretas de 3 dominios | Marts Gold + serving query-first |

**Conclusión.** El caso presenta una combinación de factores que justifican el enfoque Big Data. En la muestra
provista el volumen es reducido y no constituye por sí solo un desafío de escala. En cambio, la necesidad de
procesar eventos de uso de manera incremental, la variedad de fuentes y la evolución de esquemas, y especialmente
los problemas de calidad y consistencia, justifican una arquitectura distribuida y un proceso explícito de
conformación. El valor se materializa en los marts de FinOps, Soporte y Producto/Usage.
