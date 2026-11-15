# Catálogo de datos (diccionario preliminar)

**Generado:** 2026-11-15T10:20:17Z con `python scripts/build_catalog.py` · **No editar a mano:** las descripciones están en `src/common/catalog.py` y las cifras salen del lago.

Clasificación: **interna** (operativo, sin datos personales ni financieros por cliente) · **confidencial** (información comercial o financiera de clientes) · **personal** (identifica a una persona). Responsables: roles de `docs/entrega2/plan_trabajo.md` §1. Gobierno: [`gobierno.md`](gobierno.md).

## Resumen

| Tabla | Capa | Grano | Filas | Partición | Clasificación | Responsable |
|---|---|---|---:|---|---|---|
| [`bronze/customers_orgs`](#bronzecustomers_orgs) | Bronze | Organización | 80 | ingest_date | confidencial | Batch y calidad |
| [`bronze/users`](#bronzeusers) | Bronze | Usuario | 800 | ingest_date | personal | Batch y calidad |
| [`bronze/resources`](#bronzeresources) | Bronze | Recurso | 400 | ingest_date | interna | Batch y calidad |
| [`bronze/support_tickets`](#bronzesupport_tickets) | Bronze | Ticket | 1.000 | ingest_date | confidencial | Batch y calidad |
| [`bronze/marketing_touches`](#bronzemarketing_touches) | Bronze | Contacto | 1.500 | ingest_date | interna | Batch y calidad |
| [`bronze/nps_surveys`](#bronzenps_surveys) | Bronze | Encuesta | 92 | ingest_date | confidencial | Batch y calidad |
| [`bronze/billing_monthly`](#bronzebilling_monthly) | Bronze | Factura | 240 | ingest_date | confidencial | Batch y calidad |
| [`bronze/usage_events`](#bronzeusage_events) | Bronze | Evento (tal como llegó) | 43.200 | ingest_date / batch_id | confidencial | Streaming |
| [`silver/usage_events_live`](#silverusage_events_live) | Silver | Evento a tiempo | 1.428 | — | confidencial | Streaming |
| [`silver/usage_events`](#silverusage_events) | Silver | Evento único | 43.200 | event_date | confidencial | Streaming |
| [`quarantine/source=usage_events_silver`](#quarantinesourceusage_events_silver) | Silver | Evento × regla | 2.249 | dt | confidencial | Batch y calidad |
| [`silver/customers_orgs`](#silvercustomers_orgs) | Silver | Organización | 80 | — | confidencial | Batch y calidad |
| [`silver/resources`](#silverresources) | Silver | Recurso | 400 | — | interna | Batch y calidad |
| [`silver/usage_events_enriched`](#silverusage_events_enriched) | Silver | Evento único | 43.200 | event_date | confidencial | Batch y calidad |
| [`silver/org_daily_features`](#silverorg_daily_features) | Silver | Organización × día | 4.580 | — | confidencial | Batch y calidad |
| [`gold/org_daily_usage_by_service`](#goldorg_daily_usage_by_service) | Gold | Organización × día × servicio | 11.050 | usage_date | confidencial | Gold y serving |
| [`gold/top_services_by_org_14d`](#goldtop_services_by_org_14d) | Gold | Organización × fin de ventana × posición | 15.599 | — | confidencial | Gold y serving |
| [`gold/cost_anomaly_mart`](#goldcost_anomaly_mart) | Gold | Organización × día × servicio | 11.050 | usage_date | confidencial | Batch y calidad |
| [`cassandra/cloud_analytics.org_daily_usage_by_service`](#cassandracloud_analyticsorg_daily_usage_by_service) | Serving | Organización × día × servicio | — | — | confidencial | Gold y serving |
| [`cassandra/cloud_analytics.top_services_by_org_14d`](#cassandracloud_analyticstop_services_by_org_14d) | Serving | Organización × fin de ventana × posición | — | — | confidencial | Gold y serving |

## bronze/customers_orgs

Clientes: plan, industria, región y ciclo de vida; tipada, una foto por carga.

- **Grano:** Organización · **Clave:** `org_id` · **Decisión:** D-11
- **Proceso:** B1 · run_bronze_batch.py · **Origen:** `landing/customers_orgs.csv`
- **Clasificación:** confidencial · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 80 filas · 1 archivo · 7 KB · partición: ingest_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `org_id` | string | Identificador de la organización cliente |  |
| `org_name` | string | Nombre de la organización |  |
| `industry` | string | Industria de la organización |  |
| `hq_region` | string | Región de la casa central de la organización (por ejemplo us-east, eu-west, sa-east) |  |
| `plan_tier` | string | Plan contratado (free, standard, pro, enterprise) |  |
| `is_enterprise` | boolean | La organización es cliente enterprise |  |
| `signup_date` | date | Fecha de alta de la organización |  |
| `sales_rep` | string | Código del representante comercial (seudónimo, no identifica a la persona) | seudónimo |
| `lifecycle_stage` | string | Etapa del ciclo de vida del cliente (lead, prospect, active, at_risk, churned) |  |
| `marketing_source` | string | Canal por el que llegó el cliente |  |
| `nps_score` | double | Puntaje NPS (0 a 10) |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `ingest_ts` | timestamp | Momento en que la fila se escribió en Bronze | linaje |
| `run_id` | string | Ejecución que escribió la fila; su registro está en datalake/_runs/<proceso>/<run_id>.json | linaje |
| `ingest_date` | date | Día de carga en Bronze (partición): una foto por día de carga | linaje |

## bronze/users

Usuarios de cada organización; tipada, una foto por carga.

- **Grano:** Usuario · **Clave:** `user_id` · **Decisión:** D-11
- **Proceso:** B1 · run_bronze_batch.py · **Origen:** `landing/users.csv`
- **Clasificación:** personal · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 800 filas · 1 archivo · 24 KB · partición: ingest_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `user_id` | string | Identificador del usuario |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `email` | string | Correo del usuario (dato personal; no sale de Bronze) | personal |
| `role` | string | Rol del usuario en su organización (admin, analyst, developer, devops, data_engineer, ml_engineer) |  |
| `active` | boolean | El usuario está activo |  |
| `created_at` | date | Fecha de creación |  |
| `last_login` | date | Fecha del último ingreso |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `ingest_ts` | timestamp | Momento en que la fila se escribió en Bronze | linaje |
| `run_id` | string | Ejecución que escribió la fila; su registro está en datalake/_runs/<proceso>/<run_id>.json | linaje |
| `ingest_date` | date | Día de carga en Bronze (partición): una foto por día de carga | linaje |

## bronze/resources

Recursos de nube de cada organización, con etiquetas; tipada, una foto por carga.

- **Grano:** Recurso · **Clave:** `resource_id` · **Decisión:** D-11
- **Proceso:** B1 · run_bronze_batch.py · **Origen:** `landing/resources.csv`
- **Clasificación:** interna · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 400 filas · 1 archivo · 12 KB · partición: ingest_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `resource_id` | string | Identificador del recurso de nube |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `service` | string | Servicio de nube (compute, storage, database, networking, analytics, genai) |  |
| `region` | string | Región de nube del recurso o del evento |  |
| `created_at` | date | Fecha de creación |  |
| `state` | string | Estado del recurso (running, stopped, terminated) |  |
| `tags_json` | string | Etiquetas del recurso como texto JSON, tal como llegan | puede indicar PII |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `ingest_ts` | timestamp | Momento en que la fila se escribió en Bronze | linaje |
| `run_id` | string | Ejecución que escribió la fila; su registro está en datalake/_runs/<proceso>/<run_id>.json | linaje |
| `ingest_date` | date | Día de carga en Bronze (partición): una foto por día de carga | linaje |

## bronze/support_tickets

Tickets de soporte con severidad, SLA y CSAT; tipada, una foto por carga.

- **Grano:** Ticket · **Clave:** `ticket_id` · **Decisión:** D-11
- **Proceso:** B1 · run_bronze_batch.py · **Origen:** `landing/support_tickets.csv`
- **Clasificación:** confidencial · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 1.000 filas · 1 archivo · 19 KB · partición: ingest_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `ticket_id` | string | Identificador del ticket de soporte |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `category` | string | Categoría del ticket |  |
| `severity` | string | alta, media o revisar (en anomalías); severidad del ticket (en soporte) |  |
| `created_at` | date | Fecha de creación |  |
| `resolved_at` | date | Fecha de resolución del ticket |  |
| `csat` | double | Satisfacción del cliente con la resolución |  |
| `sla_breached` | boolean | El ticket incumplió el SLA |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `ingest_ts` | timestamp | Momento en que la fila se escribió en Bronze | linaje |
| `run_id` | string | Ejecución que escribió la fila; su registro está en datalake/_runs/<proceso>/<run_id>.json | linaje |
| `ingest_date` | date | Día de carga en Bronze (partición): una foto por día de carga | linaje |

## bronze/marketing_touches

Contactos de marketing por campaña y canal; tipada, una foto por carga.

- **Grano:** Contacto · **Clave:** `touch_id` · **Decisión:** D-11
- **Proceso:** B1 · run_bronze_batch.py · **Origen:** `landing/marketing_touches.csv`
- **Clasificación:** interna · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 1.500 filas · 1 archivo · 23 KB · partición: ingest_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `touch_id` | string | Identificador del contacto de marketing |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `campaign` | string | Campaña de marketing |  |
| `channel` | string | Canal del contacto |  |
| `timestamp` | date | Fecha del contacto de marketing |  |
| `clicked` | boolean | El contacto hizo clic |  |
| `converted` | boolean | El contacto convirtió |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `ingest_ts` | timestamp | Momento en que la fila se escribió en Bronze | linaje |
| `run_id` | string | Ejecución que escribió la fila; su registro está en datalake/_runs/<proceso>/<run_id>.json | linaje |
| `ingest_date` | date | Día de carga en Bronze (partición): una foto por día de carga | linaje |

## bronze/nps_surveys

Encuestas NPS con comentario libre; tipada, una foto por carga.

- **Grano:** Encuesta · **Clave:** `org_id, survey_date` · **Decisión:** D-11
- **Proceso:** B1 · run_bronze_batch.py · **Origen:** `landing/nps_surveys.csv`
- **Clasificación:** confidencial · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 92 filas · 1 archivo · 4 KB · partición: ingest_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `org_id` | string | Identificador de la organización cliente |  |
| `survey_date` | date | Fecha de la encuesta NPS |  |
| `nps_score` | double | Puntaje NPS (0 a 10) |  |
| `comment` | string | Comentario libre de la encuesta (texto libre: revisar antes de publicar) | texto libre |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `ingest_ts` | timestamp | Momento en que la fila se escribió en Bronze | linaje |
| `run_id` | string | Ejecución que escribió la fila; su registro está en datalake/_runs/<proceso>/<run_id>.json | linaje |
| `ingest_date` | date | Día de carga en Bronze (partición): una foto por día de carga | linaje |

## bronze/billing_monthly

Facturación mensual en moneda original; tipada, una foto por carga.

- **Grano:** Factura · **Clave:** `invoice_id` · **Decisión:** D-11
- **Proceso:** B1 · run_bronze_batch.py · **Origen:** `landing/billing_monthly.csv`
- **Clasificación:** confidencial · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 240 filas · 1 archivo · 11 KB · partición: ingest_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `invoice_id` | string | Identificador de la factura |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `month` | date | Mes facturado (primer día del mes) |  |
| `subtotal` | decimal(12,2) | Subtotal facturado, en la moneda de la factura |  |
| `credits` | decimal(12,2) | Créditos aplicados, en la moneda de la factura |  |
| `taxes` | decimal(12,2) | Impuestos, en la moneda de la factura |  |
| `currency` | string | Moneda de la factura (USD, ARS, EUR) |  |
| `exchange_rate_to_usd` | decimal(12,6) | Tasa de cambio a USD informada (inconsistente: ver D-08) |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `ingest_ts` | timestamp | Momento en que la fila se escribió en Bronze | linaje |
| `run_id` | string | Ejecución que escribió la fila; su registro está en datalake/_runs/<proceso>/<run_id>.json | linaje |
| `ingest_date` | date | Día de carga en Bronze (partición): una foto por día de carga | linaje |

## bronze/usage_events

Eventos de uso tipados, sin filtro temporal; un micro-lote por carpeta, con manifiesto.

- **Grano:** Evento (tal como llegó) · **Clave:** `event_id` · **Decisión:** D-12
- **Proceso:** S1 · run_bronze_streaming.py · **Origen:** `landing/usage_events_stream/*.jsonl`
- **Clasificación:** confidencial · **Responsable:** Streaming (Agustín Ezequiel Pagani)
- **Física:** 43.200 filas · 120 archivos · 2.9 MB · partición: ingest_date / batch_id

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `event_id` | string | Identificador único del evento de uso (clave de deduplicación, D-06) |  |
| `event_ts` | timestamp | Momento del evento, en UTC |  |
| `event_date` | date | Día UTC del evento (partición en Silver) |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `resource_id` | string | Identificador del recurso de nube |  |
| `service` | string | Servicio de nube (compute, storage, database, networking, analytics, genai) |  |
| `region` | string | Región de nube del recurso o del evento |  |
| `metric` | string | Métrica medida: requests, cpu_hours o storage_gb_hours |  |
| `value` | double | Valor medido, en la unidad de la métrica (NULL si no se informó) |  |
| `unit` | string | Unidad del valor; en Silver, completada desde la métrica si faltaba (D-15) |  |
| `cost_usd_increment` | double | Costo del evento en USD (puede ser negativo: ajustes y créditos) |  |
| `schema_version` | int | Versión del esquema del evento (1 o 2; v2 agrega carbono y tokens) |  |
| `carbon_kg` | double | Emisiones estimadas en kg de CO2 (solo eventos v2) |  |
| `genai_tokens` | bigint | Tokens de IA generativa consumidos (solo eventos v2) |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `ingest_ts` | timestamp | Momento en que la fila se escribió en Bronze | linaje |
| `run_id` | string | Ejecución que escribió la fila; su registro está en datalake/_runs/<proceso>/<run_id>.json | linaje |
| `ingest_date` | date | Día de carga en Bronze (partición): una foto por día de carga | linaje |
| `batch_id` | int | Micro-lote de streaming que trajo el evento (partición de Bronze de eventos) | linaje |

## silver/usage_events_live

Vista reciente: eventos que llegaron dentro del watermark de 24 h, sin duplicados.

- **Grano:** Evento a tiempo · **Clave:** `event_id` · **Decisión:** D-13
- **Proceso:** S2 · run_silver_events.py · **Origen:** `bronze/usage_events`
- **Clasificación:** confidencial · **Responsable:** Streaming (Agustín Ezequiel Pagani)
- **Física:** 1.428 filas · 120 archivos · 726 KB · partición: —

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `event_id` | string | Identificador único del evento de uso (clave de deduplicación, D-06) |  |
| `event_ts` | timestamp | Momento del evento, en UTC |  |
| `event_date` | date | Día UTC del evento (partición en Silver) |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `resource_id` | string | Identificador del recurso de nube |  |
| `service` | string | Servicio de nube (compute, storage, database, networking, analytics, genai) |  |
| `region` | string | Región de nube del recurso o del evento |  |
| `metric` | string | Métrica medida: requests, cpu_hours o storage_gb_hours |  |
| `value` | double | Valor medido, en la unidad de la métrica (NULL si no se informó) |  |
| `unit` | string | Unidad del valor; en Silver, completada desde la métrica si faltaba (D-15) |  |
| `cost_usd_increment` | double | Costo del evento en USD (puede ser negativo: ajustes y créditos) |  |
| `schema_version` | int | Versión del esquema del evento (1 o 2; v2 agrega carbono y tokens) |  |
| `carbon_kg` | double | Emisiones estimadas en kg de CO2 (solo eventos v2) |  |
| `genai_tokens` | bigint | Tokens de IA generativa consumidos (solo eventos v2) |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `bronze_batch_id` | int | Micro-lote de Bronze del que salió la fila | linaje |
| `silver_processed_ts` | timestamp | Momento en que la fila se escribió en Silver | linaje |

## silver/usage_events

Histórico canónico de eventos: deduplicado, con reglas de calidad y tardíos recuperados.

- **Grano:** Evento único · **Clave:** `event_id` · **Decisión:** D-13, D-15
- **Proceso:** B2 · run_silver_events.py · **Origen:** `bronze/usage_events`, `silver/usage_events_live`
- **Clasificación:** confidencial · **Responsable:** Streaming (Agustín Ezequiel Pagani)
- **Física:** 43.200 filas · 60 archivos · 2.5 MB · partición: event_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `event_id` | string | Identificador único del evento de uso (clave de deduplicación, D-06) |  |
| `event_ts` | timestamp | Momento del evento, en UTC |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `resource_id` | string | Identificador del recurso de nube |  |
| `service` | string | Servicio de nube (compute, storage, database, networking, analytics, genai) |  |
| `region` | string | Región de nube del recurso o del evento |  |
| `metric` | string | Métrica medida: requests, cpu_hours o storage_gb_hours |  |
| `value` | double | Valor medido, en la unidad de la métrica (NULL si no se informó) |  |
| `unit` | string | Unidad del valor; en Silver, completada desde la métrica si faltaba (D-15) |  |
| `cost_usd_increment` | double | Costo del evento en USD (puede ser negativo: ajustes y créditos) |  |
| `schema_version` | int | Versión del esquema del evento (1 o 2; v2 agrega carbono y tokens) |  |
| `carbon_kg` | double | Emisiones estimadas en kg de CO2 (solo eventos v2) |  |
| `genai_tokens` | bigint | Tokens de IA generativa consumidos (solo eventos v2) |  |
| `unit_inferred` | boolean | La unidad se completó a partir de la métrica (regla unit_missing, D-15) |  |
| `cost_negative` | boolean | Costo menor a −0,01 USD, marcado para revisión (regla cost_below_tolerance, D-15) |  |
| `arrival_path` | string | Camino por el que entró a Silver: a_tiempo (stream S2) o tardio (reconciliación B2) |  |
| `copies_in_bronze` | bigint | Cuántas veces llegó el mismo event_id a Bronze |  |
| `event_date` | date | Día UTC del evento (partición en Silver) |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `arrival_ts` | timestamp | Fecha de modificación del archivo en Landing: cuándo llegó el dato | linaje |
| `bronze_ingest_ts` | timestamp | Momento en que el evento se escribió en Bronze | linaje |
| `bronze_batch_id` | int | Micro-lote de Bronze del que salió la fila | linaje |
| `silver_processed_ts` | timestamp | Momento en que la fila se escribió en Silver | linaje |
| `silver_run_id` | string | Ejecución de Silver que escribió la fila | linaje |

## quarantine/source=usage_events_silver

Registro de calidad: cada regla aplicada, con acción, motivo y registro original.

- **Grano:** Evento × regla · **Clave:** `record_key, rule_id` · **Decisión:** D-15
- **Proceso:** B2 · run_silver_events.py · **Origen:** `bronze/usage_events`
- **Clasificación:** confidencial · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 2.249 filas · 1 archivo · 197 KB · partición: dt

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `source` | string | Fuente del registro |  |
| `rule_id` | string | Regla de calidad que aplicó |  |
| `action` | string | Acción de la regla: rechazado, descartado, corregido o marcado |  |
| `reason` | string | Motivo legible |  |
| `record_key` | string | Clave del registro (event_id, o ∅ si no tiene) |  |
| `raw_record` | string | Registro original, como llegó, en JSON |  |
| `dt` | date | Día de carga en Bronze del registro (partición) |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `bronze_batch_id` | int | Micro-lote de Bronze del que salió la fila | linaje |
| `silver_processed_ts` | timestamp | Momento en que la fila se escribió en Silver | linaje |
| `run_id` | string | Ejecución que escribió la fila; su registro está en datalake/_runs/<proceso>/<run_id>.json | linaje |

## silver/customers_orgs

Última foto del maestro de organizaciones.

- **Grano:** Organización · **Clave:** `org_id` · **Decisión:** D-14
- **Proceso:** B3a · run_silver_enrichment.py · **Origen:** `bronze/customers_orgs`
- **Clasificación:** confidencial · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 80 filas · 1 archivo · 7 KB · partición: —

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `org_id` | string | Identificador de la organización cliente |  |
| `org_name` | string | Nombre de la organización |  |
| `industry` | string | Industria de la organización |  |
| `hq_region` | string | Región de la casa central de la organización (por ejemplo us-east, eu-west, sa-east) |  |
| `plan_tier` | string | Plan contratado (free, standard, pro, enterprise) |  |
| `sales_rep` | string | Código del representante comercial (seudónimo, no identifica a la persona) | seudónimo |
| `lifecycle_stage` | string | Etapa del ciclo de vida del cliente (lead, prospect, active, at_risk, churned) |  |
| `marketing_source` | string | Canal por el que llegó el cliente |  |
| `is_enterprise` | boolean | La organización es cliente enterprise |  |
| `signup_date` | date | Fecha de alta de la organización |  |
| `nps_score` | double | Puntaje NPS (0 a 10) |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `bronze_ingest_date` | date | Foto de Bronze de la que salió la fila | linaje |
| `silver_processed_ts` | timestamp | Momento en que la fila se escribió en Silver | linaje |
| `silver_run_id` | string | Ejecución de Silver que escribió la fila | linaje |

## silver/resources

Última foto del maestro de recursos, con etiquetas como columnas.

- **Grano:** Recurso · **Clave:** `resource_id` · **Decisión:** D-14
- **Proceso:** B3a · run_silver_enrichment.py · **Origen:** `bronze/resources`
- **Clasificación:** interna · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 400 filas · 1 archivo · 12 KB · partición: —

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `resource_id` | string | Identificador del recurso de nube |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `service` | string | Servicio de nube (compute, storage, database, networking, analytics, genai) |  |
| `region` | string | Región de nube del recurso o del evento |  |
| `created_at` | date | Fecha de creación |  |
| `state` | string | Estado del recurso (running, stopped, terminated) |  |
| `tags` | array<string> | Etiquetas del recurso como lista (por ejemplo env:prod, pii:true) |  |
| `env` | string | Entorno del recurso, de la etiqueta env: |  |
| `team` | string | Equipo dueño del recurso, de la etiqueta team: |  |
| `costcenter` | string | Centro de costo, de la etiqueta costcenter: |  |
| `has_pii` | boolean | El recurso está etiquetado pii:true (procesa datos personales) | indicador de PII |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |
| `bronze_ingest_date` | date | Foto de Bronze de la que salió la fila | linaje |
| `silver_processed_ts` | timestamp | Momento en que la fila se escribió en Silver | linaje |
| `silver_run_id` | string | Ejecución de Silver que escribió la fila | linaje |

## silver/usage_events_enriched

Eventos con atributos de la organización y del recurso, y una columna por medida.

- **Grano:** Evento único · **Clave:** `event_id` · **Decisión:** D-14
- **Proceso:** B3b · run_silver_enrichment.py · **Origen:** `silver/usage_events`, `silver/customers_orgs`, `silver/resources`
- **Clasificación:** confidencial · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 43.200 filas · 60 archivos · 2.8 MB · partición: event_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `event_id` | string | Identificador único del evento de uso (clave de deduplicación, D-06) |  |
| `event_ts` | timestamp | Momento del evento, en UTC |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `resource_id` | string | Identificador del recurso de nube |  |
| `service` | string | Servicio de nube (compute, storage, database, networking, analytics, genai) |  |
| `region` | string | Región de nube del recurso o del evento |  |
| `metric` | string | Métrica medida: requests, cpu_hours o storage_gb_hours |  |
| `value` | double | Valor medido, en la unidad de la métrica (NULL si no se informó) |  |
| `unit` | string | Unidad del valor; en Silver, completada desde la métrica si faltaba (D-15) |  |
| `unit_inferred` | boolean | La unidad se completó a partir de la métrica (regla unit_missing, D-15) |  |
| `cost_negative` | boolean | Costo menor a −0,01 USD, marcado para revisión (regla cost_below_tolerance, D-15) |  |
| `schema_version` | int | Versión del esquema del evento (1 o 2; v2 agrega carbono y tokens) |  |
| `cost_usd` | decimal(18,6) | Costo en USD, decimal exacto |  |
| `requests` | double | Requests (solo de eventos con métrica requests) |  |
| `cpu_hours` | double | Horas de CPU (solo de eventos con métrica cpu_hours) |  |
| `storage_gb_hours` | double | GB-hora de almacenamiento (solo de eventos con métrica storage_gb_hours) |  |
| `genai_tokens` | bigint | Tokens de IA generativa consumidos (solo eventos v2) |  |
| `carbon_kg` | double | Emisiones estimadas en kg de CO2 (solo eventos v2) |  |
| `org_name` | string | Nombre de la organización |  |
| `industry` | string | Industria de la organización |  |
| `plan_tier` | string | Plan contratado (free, standard, pro, enterprise) |  |
| `is_enterprise` | boolean | La organización es cliente enterprise |  |
| `hq_region` | string | Región de la casa central de la organización (por ejemplo us-east, eu-west, sa-east) |  |
| `lifecycle_stage` | string | Etapa del ciclo de vida del cliente (lead, prospect, active, at_risk, churned) |  |
| `resource_state` | string | Estado del recurso del evento |  |
| `resource_env` | string | Entorno del recurso del evento |  |
| `resource_team` | string | Equipo dueño del recurso del evento |  |
| `resource_costcenter` | string | Centro de costo del recurso del evento |  |
| `resource_has_pii` | boolean | El recurso del evento procesa datos personales | indicador de PII |
| `org_found` | boolean | La organización del evento existe en el maestro |  |
| `resource_found` | boolean | El recurso del evento existe en el maestro |  |
| `arrival_path` | string | Camino por el que entró a Silver: a_tiempo (stream S2) o tardio (reconciliación B2) |  |
| `event_date` | date | Día UTC del evento (partición en Silver) |  |
| `source_file` | string | Archivo de Landing del que vino la fila (linaje hasta el origen) | linaje |

## silver/org_daily_features

Features diarias: costo, requests, CPU, almacenamiento, tokens y carbono.

- **Grano:** Organización × día · **Clave:** `org_id, event_date` · **Decisión:** D-14
- **Proceso:** B3b · run_silver_enrichment.py · **Origen:** `silver/usage_events_enriched`
- **Clasificación:** confidencial · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 4.580 filas · 1 archivo · 137 KB · partición: —

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `org_id` | string | Identificador de la organización cliente |  |
| `event_date` | date | Día UTC del evento (partición en Silver) |  |
| `daily_cost_usd` | decimal(28,6) | Costo del día en USD, decimal exacto |  |
| `requests` | double | Requests (solo de eventos con métrica requests) |  |
| `cpu_hours` | double | Horas de CPU (solo de eventos con métrica cpu_hours) |  |
| `storage_gb_hours` | double | GB-hora de almacenamiento (solo de eventos con métrica storage_gb_hours) |  |
| `requests_events` | bigint | Eventos de requests con valor informado ese día |  |
| `genai_tokens` | bigint | Tokens de IA generativa consumidos (solo eventos v2) |  |
| `carbon_kg` | double | Emisiones estimadas en kg de CO2 (solo eventos v2) |  |
| `carbon_coverage` | double | Fracción de eventos del día con carbon_kg medido |  |
| `events` | bigint | Cantidad de eventos |  |
| `negative_cost_events` | bigint | Eventos con costo negativo marcado |  |
| `org_name` | string | Nombre de la organización |  |
| `plan_tier` | string | Plan contratado (free, standard, pro, enterprise) |  |
| `industry` | string | Industria de la organización |  |
| `silver_processed_ts` | timestamp | Momento en que la fila se escribió en Silver | linaje |

## gold/org_daily_usage_by_service

Mart FinOps: uso, requests, costo diario e indicadores de calidad.

- **Grano:** Organización × día × servicio · **Clave:** `org_id, usage_date, service` · **Decisión:** D-16
- **Proceso:** G1 · run_gold_finops.py · **Origen:** `silver/usage_events_enriched`
- **Clasificación:** confidencial · **Responsable:** Gold y serving (Tomas Lautaro Reymundo)
- **Física:** 11.050 filas · 60 archivos · 809 KB · partición: usage_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `org_id` | string | Identificador de la organización cliente |  |
| `service` | string | Servicio de nube (compute, storage, database, networking, analytics, genai) |  |
| `org_name` | string | Nombre de la organización |  |
| `plan_tier` | string | Plan contratado (free, standard, pro, enterprise) |  |
| `industry` | string | Industria de la organización |  |
| `daily_cost_usd` | decimal(18,6) | Costo del día en USD, decimal exacto |  |
| `requests` | double | Requests (solo de eventos con métrica requests) |  |
| `cpu_hours` | double | Horas de CPU (solo de eventos con métrica cpu_hours) |  |
| `storage_gb_hours` | double | GB-hora de almacenamiento (solo de eventos con métrica storage_gb_hours) |  |
| `genai_tokens` | bigint | Tokens de IA generativa consumidos (solo eventos v2) |  |
| `carbon_kg` | double | Emisiones estimadas en kg de CO2 (solo eventos v2) |  |
| `events` | bigint | Cantidad de eventos |  |
| `resources_active` | bigint | Recursos distintos con uso ese día |  |
| `negative_cost_events` | bigint | Eventos con costo negativo marcado |  |
| `negative_cost_usd` | decimal(18,6) | Suma de los costos negativos marcados |  |
| `unit_inferred_events` | bigint | Eventos con unidad inferida |  |
| `late_events` | bigint | Eventos recuperados por la reconciliación (llegaron fuera del watermark) |  |
| `carbon_coverage` | double | Fracción de eventos del día con carbon_kg medido |  |
| `usage_date` | date | Día UTC de uso (partición) |  |
| `gold_processed_ts` | timestamp | Momento en que la fila se escribió en Gold | linaje |
| `gold_run_id` | string | Ejecución de Gold que escribió la fila | linaje |

## gold/top_services_by_org_14d

Ranking de servicios por costo en ventanas móviles de 14 días.

- **Grano:** Organización × fin de ventana × posición · **Clave:** `org_id, window_end_date, cost_rank` · **Decisión:** D-17
- **Proceso:** G2 · run_gold_finops.py · **Origen:** `gold/org_daily_usage_by_service`
- **Clasificación:** confidencial · **Responsable:** Gold y serving (Tomas Lautaro Reymundo)
- **Física:** 15.599 filas · 1 archivo · 140 KB · partición: —

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `org_id` | string | Identificador de la organización cliente |  |
| `window_end_date` | date | Último día de la ventana de 14 días |  |
| `service` | string | Servicio de nube (compute, storage, database, networking, analytics, genai) |  |
| `cost_usd` | decimal(18,6) | Costo en USD, decimal exacto |  |
| `requests` | double | Requests (solo de eventos con métrica requests) |  |
| `days_with_usage` | bigint | Días de la ventana con uso del servicio |  |
| `cost_rank` | int | Posición del servicio por costo en la ventana (1 = mayor costo) |  |
| `window_days` | int | Largo de la ventana en días |  |
| `window_complete` | boolean | La ventana tiene sus 14 días dentro del período con datos |  |
| `gold_processed_ts` | timestamp | Momento en que la fila se escribió en Gold | linaje |
| `gold_run_id` | string | Ejecución de Gold que escribió la fila | linaje |

## gold/cost_anomaly_mart

Score y flags de anomalía de costo, con contexto y método.

- **Grano:** Organización × día × servicio · **Clave:** `org_id, usage_date, service` · **Decisión:** D-18
- **Proceso:** G3 · run_cost_anomalies.py · **Origen:** `gold/org_daily_usage_by_service`
- **Clasificación:** confidencial · **Responsable:** Batch y calidad (Stephanie Amairany Castillo Avendaño)
- **Física:** 11.050 filas · 60 archivos · 1.1 MB · partición: usage_date

| Columna | Tipo | Descripción | Nota |
|---|---|---|---|
| `service` | string | Servicio de nube (compute, storage, database, networking, analytics, genai) |  |
| `org_id` | string | Identificador de la organización cliente |  |
| `org_name` | string | Nombre de la organización |  |
| `plan_tier` | string | Plan contratado (free, standard, pro, enterprise) |  |
| `daily_cost_usd` | decimal(18,6) | Costo del día en USD, decimal exacto |  |
| `requests` | double | Requests (solo de eventos con métrica requests) |  |
| `events` | bigint | Cantidad de eventos |  |
| `negative_cost_events` | bigint | Eventos con costo negativo marcado |  |
| `negative_cost_usd` | decimal(18,6) | Suma de los costos negativos marcados |  |
| `baseline_median_usd` | double | Mediana del costo de la serie en los 28 días anteriores |  |
| `history_days` | bigint | Días con uso en la línea base |  |
| `excess_cost_usd` | double | Costo del día menos la mediana de la línea base |  |
| `robust_z` | double | z-score robusto: 0,6745 × exceso / MAD (D-18) |  |
| `z_classic` | double | z-score clásico (media y desvío), solo para comparar métodos |  |
| `service_p99_usd` | double | p99 del costo diario del servicio (respaldo para arranque en frío) |  |
| `score_status` | string | puntuado, o historia_insuficiente si hay menos de 7 días de historia |  |
| `detection` | string | Cómo se detectó el spike: mad o percentil_servicio |  |
| `is_spike` | boolean | El día es un spike de costo |  |
| `has_negative_cost` | boolean | El día tiene costos negativos marcados |  |
| `is_anomaly` | boolean | El día tiene algún flag (spike o costo negativo) |  |
| `anomaly_type` | string | spike, costo_negativo o spike+costo_negativo |  |
| `severity` | string | alta, media o revisar (en anomalías); severidad del ticket (en soporte) |  |
| `method` | string | Método de detección y largo de la línea base |  |
| `z_threshold` | double | Umbral de z robusto usado |  |
| `min_excess_usd` | double | Exceso mínimo en USD para marcar (materialidad) |  |
| `baseline_mad_usd` | double | MAD (desvío típico robusto) de esos 28 días |  |
| `usage_date` | date | Día UTC de uso (partición) |  |
| `gold_processed_ts` | timestamp | Momento en que la fila se escribió en Gold | linaje |
| `gold_run_id` | string | Ejecución de Gold que escribió la fila | linaje |

## cassandra/cloud_analytics.org_daily_usage_by_service

Consulta 1: costos y requests diarios por organización en un rango de fechas.

- **Grano:** Organización × día × servicio · **Clave:** `PRIMARY KEY ((org_id), usage_date, service)` · **Decisión:** D-17
- **Proceso:** V1 · run_serving_astra.py · **Origen:** `gold/org_daily_usage_by_service`
- **Clasificación:** confidencial · **Responsable:** Gold y serving (Tomas Lautaro Reymundo)
- **Columnas:** las mismas que la tabla de Gold de origen, más `loaded_at` (esquema en `cql/01_schema.cql`).

## cassandra/cloud_analytics.top_services_by_org_14d

Consulta 2: Top-N servicios por costo en los últimos 14 días.

- **Grano:** Organización × fin de ventana × posición · **Clave:** `PRIMARY KEY ((org_id), window_end_date, cost_rank)` · **Decisión:** D-17
- **Proceso:** V1 · run_serving_astra.py · **Origen:** `gold/top_services_by_org_14d`
- **Clasificación:** confidencial · **Responsable:** Gold y serving (Tomas Lautaro Reymundo)
- **Columnas:** las mismas que la tabla de Gold de origen, más `loaded_at` (esquema en `cql/01_schema.cql`).
