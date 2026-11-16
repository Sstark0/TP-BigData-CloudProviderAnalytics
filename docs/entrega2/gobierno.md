# Gobierno preliminar

**Versión:** 1.0 · **Fecha:** 15/11/2026 · **Entrega:** 2 · **Paquete:** E2-11 · **Decisión:** D-21

La consigna pide "definir controles iniciales, metadatos, linaje, responsabilidades, acceso y evidencias" (§6.2),
con gobierno, calidad, seguridad, metadatos y observabilidad como **capacidades transversales**. Este documento
reúne lo que **ya está implementado** y dónde está la evidencia de cada cosa. Lo que falta está en la §7 y pasa
al backlog.

## 1. Controles

Cada control actúa en un punto del pipeline y evita un riesgo concreto. Si un control falla, el proceso termina
con error y no deja una salida a medias (D-19).

| Control | Dónde | Qué evita | Evidencia |
|---|---|---|---|
| Landing inmutable | Todos los procesos solo leen `landing/`; `--clean` se niega a borrarla | Perder o alterar el dato original | D-02, `tests/test_pipeline.py` |
| Integridad de Landing | SHA-256 de cada archivo de entrada en el registro de cada ejecución | Procesar un archivo modificado sin notarlo | `_runs/*/<run_id>.json` (`inputs`), `e2_11_linaje_ejemplo.json` |
| Esquema explícito y validación de encabezados | Lectura de CSV (B1) y JSONL (S1) | Columnas corridas o tipos inferidos mal | D-05, D-10, `tests/test_common.py` |
| Quarantine estructural en Bronze | B1: fila mal formada, clave nula, error de tipo, conflicto de clave. S1: JSON roto, timestamp ilegible, número inválido | Que un registro ilegible contamine Bronze | D-11, D-12, `e2_02`, `e2_03` |
| Reglas de calidad en Silver | B2: rechazar, descartar, corregir o marcar, con motivo por evento | Duplicados, unidades faltantes, costos anómalos sin aviso | D-15, `e2_06_quality_rules.json` |
| Conciliación en cada proceso | Balances del tipo entrada = salida + rechazos; si no cierran, el proceso falla | Pérdida o duplicación silenciosa de filas | Registro de cada ejecución (`balances`), `e2_02` a `e2_09` |
| Conciliación entre capas | Costo y requests de Gold = Silver al micro-dólar; Cassandra = Gold por organización | Que el mart o el serving no cuadren con su origen | D-16, D-17, `e2_07`, `e2_08` |
| Idempotencia verificada | Huella de contenido por tabla antes y después de re-ejecutar | Que una re-ejecución duplique o cambie datos | D-20, `e2_10_idempotency.json` (24/24) |
| Corte ante fallas | `run_pipeline.py` se detiene si una etapa falla o tiene un balance abierto | Construir capas sobre datos viejos o incompletos | D-19, `e2_12_pipeline_run.json` |
| Pruebas automáticas | 75 pruebas, con casos armados a mano para cada regla | Regresiones al cambiar el código | `python -m unittest discover -s tests` |

## 2. Metadatos

**Columnas técnicas.** Todas las capas llevan columnas que dicen de dónde vino cada fila y cuándo y en qué
ejecución se procesó:
- Bronze: `source_file`, `arrival_ts`, `ingest_ts`, `run_id` y `batch_id` en eventos.
- Silver: `bronze_batch_id`, `silver_processed_ts` y `silver_run_id`.
- Gold: `gold_processed_ts` y `gold_run_id`.
- Cassandra: `loaded_at`.

**Registro de cada ejecución.** Cada proceso escribe `datalake/_runs/<proceso>/<run_id>.json` con:
- estado, inicio, fin y duración;
- archivos de entrada con tamaño y SHA-256;
- parámetros y conteos;
- balances, con su resultado;
- salidas, con ruta, archivos y bytes;
- el mensaje de error, si falló.

`run_pipeline.py` agrega un registro por corrida completa, con el entorno, el operador y el resultado de cada etapa.

**Catálogo de datos.** [`catalogo_datos.md`](catalogo_datos.md) es el diccionario preliminar: las 20 tablas (18 en
Parquet y 2 en Cassandra) con grano, clave, partición, filas, archivos, tamaño, origen, clasificación y
responsable, y la descripción de sus 301 columnas.
- Se **genera** con `python scripts/build_catalog.py` cruzando las descripciones de `src/common/catalog.py` con los Parquet reales.
- Si aparece una columna sin describir, el script la marca y termina con error. Así el diccionario no puede quedar desactualizado sin que se note.

## 3. Linaje

**A nivel tabla.** Qué tabla sale de cuáles y por qué proceso: el diagrama
[`arquitectura_v2.png`](../diagramas/arquitectura_v2.png) y la columna *Origen* de cada tabla en el catálogo.

**A nivel fila.** Cualquier fila de Gold se puede rastrear hasta los archivos de Landing de donde salió:

```
gold/org_daily_usage_by_service ── gold_run_id ──▶ _runs/g1_gold_finops/<run_id>.json
        │ (org_id, usage_date, service)
        ▼
silver/usage_events_enriched ── event_id ──▶ silver/usage_events ── arrival_path, bronze_batch_id
        │                                                     │
        │                                                     ▼
        │                         _runs/s1_bronze_events/batches/batch_id=<n>.json  (archivo, ejecución de S1)
        │                                                     │
        ▼                                                     ▼
quarantine/source=usage_events_silver            landing/usage_events_stream/<archivo>.jsonl
(qué se corrigió o marcó y el dato original)     (SHA-256 registrado al ingerirlo)
```

`python scripts/trace_lineage.py` hace ese recorrido y verifica cada paso.
- **Ejemplo:** la fila `org_53lc58dr · 2025-08-21 · database` (USD 24,48) sale de 6 eventos: 5 tardíos recuperados, 1 a tiempo y 2 con correcciones de calidad.
- Los 6 eventos vienen de 6 archivos de Landing distintos.
- El costo coincide en cada capa y los 6 archivos tienen el mismo SHA-256 que cuando se ingirieron.
- Evidencia: [`e2_11_linaje_ejemplo.json`](../../evidence/entrega2/e2_11_linaje_ejemplo.json).

## 4. Responsables

Cada capa y cada dominio tienen un rol responsable y otro rol que revisa (plan de trabajo, §1). El responsable
verifica los criterios de cierre de sus tablas, revisa los cambios antes de integrarlos y responde por sus datos. La asignación
nominal es la de [`plan_trabajo.md`](plan_trabajo.md) §1, confirmada por el equipo.

| Ámbito | Tablas | Responsable | Revisa |
|---|---|---|---|
| Ingesta y Bronze de maestros | `bronze/<7 maestros>` | Batch y calidad | Plataforma y gobierno |
| Eventos: streaming y Silver de eventos | `bronze/usage_events`, `silver/usage_events_live`, `silver/usage_events` | Streaming | Batch y calidad |
| Calidad y quarantine | `quarantine/…`, reglas de D-15 | Batch y calidad | Plataforma y gobierno |
| Maestros conformados y features | `silver/customers_orgs`, `resources`, `usage_events_enriched`, `org_daily_features` | Batch y calidad | Plataforma y gobierno |
| Gold FinOps y serving | `gold/org_daily_usage_by_service`, `top_services_by_org_14d`, tablas de Cassandra | Gold y serving | Streaming |
| Analítica | `gold/cost_anomaly_mart` | Batch y calidad | Plataforma y gobierno |
| Plataforma: orquestación, registros, pruebas, gobierno | `_runs/`, `run_pipeline.py`, este documento | Plataforma y gobierno | Líder técnico |
| Arquitectura y decisiones | `DECISIONS.md`, diagramas | Líder técnico | Gold y serving |

## 5. Acceso y seguridad

**Clasificación.** Cada tabla tiene una de tres clases, detallada en el catálogo:

| Clase | Qué significa | Tablas |
|---|---|---|
| personal | Identifica a una persona | `bronze/users` (columna `email`) |
| confidencial | Información comercial o financiera de clientes | Facturación, tickets, NPS, organizaciones, todos los eventos con costo, Gold y Cassandra |
| interna | Operativa, sin datos personales ni financieros por cliente | `resources`, `marketing_touches` |

**Datos personales y minimización.**
- **`email`** es el único dato personal directo. Se conserva en Bronze, porque Landing es inmutable y Bronze guarda el dato tal como llegó, pero **ningún proceso lo lee**: ninguna tabla de Silver, Gold ni Cassandra deriva de `bronze/users`. Una prueba lo verifica (`tests/test_catalog.py`).
- **`sales_rep`** es un código seudónimo (`rep_a`…).
- **`nps_surveys.comment`** es texto libre, marcado para revisar antes de publicarlo.
- **`resources.has_pii`** indica qué recursos del cliente procesan datos personales, sin exponer cuáles.

**Quién escribe en cada zona.** Cada tabla tiene un único proceso que la escribe (D-04), y la escritura es siempre
por reemplazo controlado: partición del día, carpeta del lote o tabla completa. Nadie escribe en Landing. El
streaming escribe solo a través de su checkpoint.

**Repositorio público: qué no se versiona.**
- Los datos: `datalake/**` está en `.gitignore`, salvo los `.gitkeep`.
- El dataset: se obtiene aparte, según `data/README.md`.
- Las credenciales: `.env` y `*secure-connect*.zip` están en `.gitignore`.

**AstraDB.**
- El token es de alcance limitado a la base `cloud-provider-analytics`, con un rol de administrador de esa base solamente.
- El token y el Secure Connect Bundle viven en `~/.astra/`, con permisos `600`, y se cargan como variables de entorno.
- Ningún archivo del repo ni de la evidencia los contiene. Antes de cada commit se revisa con `git diff --cached | grep -cE "AstraCS:[A-Za-z]+:[0-9a-f]{20,}"`, que debe dar 0.
- Si un token se filtra, se revoca en el portal y se genera otro.

## 6. Observabilidad

| Qué se observa | Cómo | Dónde |
|---|---|---|
| Resultado de cada proceso | Estado, duración, conteos, balances y error | `_runs/<proceso>/<run_id>.json` |
| Resultado de cada corrida completa | Etapas, duración, entorno, pruebas | `_runs/pipeline/<run_id>/`, `evidence/entrega2/e2_12_pipeline_run.json` |
| Salida completa de cada etapa | Log por etapa | `_runs/pipeline/<run_id>/<etapa>.log` |
| Streaming | Manifiesto por lote; métricas de Spark: leídos, emitidos, descartados por watermark, duplicados, último watermark | `_runs/s1_bronze_events/batches/`, registro de `b2_silver_events` |
| Calidad | Registro por evento y conteo por regla y acción | `quarantine/source=usage_events_silver`, `e2_06_quality_rules.json` |
| Anomalías de costo | Flags por organización, día y servicio, con severidad | `gold/cost_anomaly_mart`, `e2_09_cost_anomalies.json` |

## 7. Limitaciones y pendientes (pasan al backlog)

- **Alertas:** hoy un fallo se ve en la salida y en el registro, pero nadie recibe un aviso. Lo mismo con las anomalías de severidad alta.
- **Rol de solo lectura en AstraDB:** quien consulta usa el mismo token que la carga. Lo correcto es un token de lectura para el consumo y otro de escritura para el pipeline, con fecha de expiración.
- **Enmascaramiento de `email` en Bronze:** hoy se conserva tal como llega. Si alguna vez se necesitara en Silver, debería pasar con hash o seudónimo.
- **Retención:** no hay una política escrita de cuánto tiempo se guardan las fotos de Bronze y los registros de `_runs/`. Además, AstraDB Free borra una base hibernada más de 30 días.
- **Latencia del streaming:** no se mide el tiempo entre la llegada de un archivo y su disponibilidad en Silver (objetivo O4 de E1).
