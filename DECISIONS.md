# Registro de decisiones técnicas

Formato: cada decisión tiene **contexto → decisión → alternativas descartadas → consecuencias**.
Estado: `Aceptada` · `Propuesta` (a validar con el equipo/docente; entrega reprogramada al 05/10, hora pendiente) · `Abierta` (sin resolver todavía).

| ID | Decisión | Estado | Fecha |
|---|---|---|---|
| D-01 | Entorno de ejecución: PySpark 3.5 local (validado en WSL2) / Colab; Docker diferido | Aceptada | 2026-09-25 |
| D-02 | Landing inmutable; lectura con esquema explícito "defensivo" | Aceptada | 2026-09-25 |
| D-03 | Patrón arquitectónico híbrido | Aceptada · validada en E2 | 2026-11-15 |
| D-04 | Replay, late data e idempotencia | Aceptada · validada en E2 | 2026-11-15 |
| D-05 | Numéricos ambiguos leídos como texto y casteados con fallback | Aceptada | 2026-09-25 |
| D-06 | Deduplicación de eventos por `event_id` | Aceptada | 2026-09-25 |
| D-07 | Configuración de Spark para dataset chico | Aceptada | 2026-09-25 |
| D-08 | Normalización del revenue a USD (moneda y tasa de cambio) | Abierta · inconsistencia intencional (docente) | 2026-09-28 |
| D-09 | Subtotales negativos e impuesto positivo; `credits` nulo = 0 (supuesto) | Propuesta | 2026-09-28 |
| D-10 | Dialecto CSV: `escape = '"'` y validación de encabezados | Aceptada | 2026-09-30 |
| D-11 | Bronze batch: partición por `ingest_date`, reglas de rechazo e idempotencia | Aceptada | 2026-11-14 |
| D-12 | Bronze streaming: partición por `ingest_date`/`batch_id`, manifiesto de lotes y quarantine estructural | Aceptada | 2026-11-14 |
| D-13 | Silver de eventos: stream con watermark para lo reciente + reconciliación batch como histórico | Aceptada | 2026-11-14 |
| D-14 | Silver de maestros (última foto), enriquecimiento con LEFT JOIN y features diarias por organización | Aceptada | 2026-11-14 |
| D-15 | Reglas de calidad de Silver: rechazar, descartar, corregir o marcar, con registro por evento | Aceptada | 2026-11-14 |
| D-16 | Gold FinOps `org_daily_usage_by_service`: grano, partición por `usage_date`, indicadores de calidad y huella de contenido | Aceptada | 2026-11-14 |
| D-17 | Serving en AstraDB: una tabla por consulta, claves Cassandra, carga con driver desde Spark y limpieza de obsoletas | Aceptada | 2026-11-14 |
| D-18 | Anomalías de costo: z-score robusto (MAD) sobre línea base móvil, percentil para arranque en frío, umbrales | Aceptada | 2026-11-15 |
| D-19 | Orquestación: `run_pipeline.py` por etapas, corte ante fallas, registro de ejecución y limpieza segura | Aceptada | 2026-11-15 |
| D-20 | Idempotencia: comparación antes/después con conteos y huella de contenido por tabla | Aceptada | 2026-11-15 |
| D-21 | Gobierno preliminar: catálogo como código, clasificación en tres clases, linaje verificable y minimización de datos personales | Aceptada | 2026-11-15 |
---

## D-01 · Entorno de ejecución
- **Contexto.** La consigna exige ejecutar en Google Colab o en un entorno equivalente validado. El serving es AstraDB (servicio gestionado), así que no hay Cassandra local que levantar.
- **Decisión.** PySpark 3.5.3 (misma línea que Colab), `local[*]`. Rutas por variables de entorno (`config/.env.example`).
- **Descartado.** Clúster Hadoop/YARN en Docker: costo alto de montaje y ningún requisito lo pide. `docker-compose` con Jupyter + PySpark queda como mejora de reproducibilidad a evaluar para la 2.ª entrega (`infra/README.md`).
- **Consecuencias.** HDFS y YARN se usan como marco conceptual (bloques, archivos pequeños, localidad, recursos), no como infraestructura.
- **Actualización (E2, 15/11/2026).** En Windows nativo, Spark no puede escribir Parquet sin `winutils.exe`, y los workers de Python pueden resolver `python` al alias de Microsoft Store. El entorno validado pasa a ser **Linux o WSL2 (Ubuntu) con Python 3.11 y Java 17**: el pipeline completo corrió ahí (`evidence/entrega2/e2_12_pipeline_run.json`). Colab sigue sin validar para el pipeline; Docker sigue diferido.

## D-02 · Landing inmutable
- **Contexto.** "Los archivos de Landing no deben modificarse" (consigna 3.2).
- **Decisión.** Ningún proceso escribe en `datalake/landing/`. Toda corrección ocurre al promover a Bronze/Silver, y los registros rechazados van a `quarantine/` con el motivo.
- **Consecuencias.** Reprocesar = volver a leer Landing (habilita re-stream/backfill; ver D-03).

## D-03 · Patrón arquitectónico (propuesta)
- **Contexto.** Maestros chicos y de baja frecuencia (80 orgs, 240 facturas en 3 meses) vs. eventos de uso en flujo continuo (720/día en la muestra). La consigna exige como piso streaming de eventos + batch de maestros/facturación.
- **Decisión propuesta.** Híbrido: **eventos con ingesta Structured Streaming**, publicación incremental y reconciliación batch diaria del histórico + **batch para maestros y facturación**. Es híbrido con un único contrato de transformación reutilizado; no se presenta como Kappa pura.
- **Descartado.** *Lambda clásica* (batch layer + speed layer sobre los mismos eventos): duplica la lógica sin ganancia, porque Landing ya permite recalcular. *Kappa pura*: tratar CSV mensuales como streams es artificial.
- **Detalle.** Se desarrolla en el paso 4 (arquitectura v1).
- **Validación (E2, 15/11/2026).** Implementado como se propuso, con un ajuste: los eventos entran por streaming (S1) y desde Bronze se separan en una vista reciente por streaming con watermark (S2) y el histórico canónico por reconciliación batch (B2). Gold sale solo del histórico, no de micro-lotes provisionales. Ver D-12, D-13 y [`docs/entrega2/arquitectura_v2.md`](docs/entrega2/arquitectura_v2.md).

## D-04 · Replay, late data e idempotencia (propuesta v1)
- **Evidencia.** La simulación con un archivo por micro-lote y watermark 24 h identifica 42.118 eventos atrasados; no demuestra el descarte real de Spark.
- **Decisión.** Captura completa hacia Bronze sin filtro por watermark. Dedupe exacto por event_id en publicación Silver. Para el prototipo futuro en Parquet: staging por run_id/batch_id, manifiesto de lotes completados, reemplazo de particiones afectadas y recuperación de corrida incompleta; escritor único. foreachBatch por sí solo no ofrece idempotencia y Parquet no posee merge transaccional nativo.
- **Operación propuesta.** Vista operacional por micro-lote con ventana UTC de 5 minutos y watermark 24 h, identificada como provisional; late data se conserva en Bronze y se incorpora mediante reconciliación batch diaria. El histórico canónico no se deriva exclusivamente de la ventana.
- **Replay didáctico.** Mantener Landing intacto. Una copia derivada para demo puede ordenarse por event_ts fuera de Landing, con manifiesto del origen. Ensayar también archivos originales mezclados y comparar sumas completas. No sustituir el original.
- **Trade-off.** Reescribir particiones pequeñas simplifica el prototipo, pero no escala ni es atómico en todos los almacenamientos. En producción evaluar formato transaccional; no incorporarlo ahora como implementación.
- **Estado.** Diseño propuesto; validar en entrega 2 watermark, checkpoint, dedupe, recuperación y latencia. Véase documento integrado.
- **Validación (E2, 15/11/2026).**
  - **Watermark real de Spark**: con watermark de 24 h y un archivo por micro-lote descarta 41.761 eventos, contra 42.118 estimados en la simulación de E1; la diferencia es el desfase de un lote con que Spark aplica el watermark. La reconciliación los recupera a todos (D-13).
  - **Staging, manifiesto y escritor único**: implementados en S1. Cada lote es una carpeta `batch_id`, el manifiesto se escribe al final y un lote sin manifiesto no se lee (D-12).
  - **Recuperación de corrida incompleta e idempotencia**: probadas con lotes re-ejecutados, en el mismo día y en otro, y con una re-ejecución completa del pipeline que no cambia ninguna tabla (D-20).
  - **Replay didáctico ordenado**: no se implementó; queda en el backlog.

## D-05 · Numéricos ambiguos
- **Contexto.** El 3 % de `value` llega como texto (`"95.0"`). Declararlo `DoubleType` en la lectura lo convertiría en NULL en silencio.
- **Decisión.** Leer como `StringType` en Landing y castear con `cast("double")` en Bronze; si el cast falla y el origen no era nulo → quarantine con motivo `cast_error`. Hoy el 100 % de los textos es casteable.

## D-06 · Clave de deduplicación de eventos
- **Contexto.** 0 `event_id` duplicados en la muestra, pero el streaming con reintentos es *at-least-once*. Hay 14 colisiones de clave contextual: eventos con la misma (org, recurso, minuto, métrica) y **valores distintos**, que no son duplicados.
- **Decisión.** Deduplicar por `event_id`. La clave de negocio no identifica un evento (colisiona con mediciones legítimas).

## D-07 · Configuración de Spark
- **Decisión.** `spark.sql.shuffle.partitions = 8` (el default 200 genera cientos de tareas y archivos vacíos con 13 MB) y zona horaria UTC en sesión, driver y Python.

## D-08 · Normalización del revenue a USD (abierta)
- **Contexto.** La consulta final n.º 4 pide *revenue mensual con créditos e impuestos, normalizado a USD*. `billing_monthly` trae 3 monedas (160 USD, 51 ARS, 29 EUR) y `exchange_rate_to_usd`. Evidencia: notebook, celdas de normalización a USD.
- **Hallazgos.**
  1. Las facturas en USD traen tasas entre 0,85 y 1,12 (deberían ser 1).
  2. Los subtotales en ARS tienen la misma magnitud que los de USD (mediana 816 vs. 655). Aplicando la tasa (~0,0015), las facturas en ARS pasan de 50.293 a **76 USD**.
  3. 50 de las 80 organizaciones cambian de moneda entre meses, algo inverosímil en un cliente real.
  4. La relación subtotal / costo de uso medido es similar en las tres monedas (mediana 0,73–1,08), lo que sugiere que los montos ya están en una escala equivalente a USD.
- **Impacto.** Revenue total jun–ago: **A** (tasa tal cual) 164.185 · **B** (USD forzado a 1) 164.294 · **C** (sin aplicar tasa) 211.294 USD. A es un 22,3 % menor que C.
- **Opciones.** (A) aplicar la tasa provista; (B) forzar 1 solo para USD; (C) tratar los montos como USD y registrar moneda y tasa como atributos con flags de inconsistencia.
- **Propuesta.** Conservar en Silver monto, moneda y tasa originales (sin perder el dato de origen) y calcular `revenue_usd = subtotal − credits + taxes` con la opción **C**, marcando `flag_fx_usd_distinto_de_1` y `flag_moneda_inconsistente`. Si el docente indica aplicar la tasa, el cambio es una sola expresión porque los datos originales se conservan.
- **Estado.** Consultado con el docente (2026-09-28): la inconsistencia es **intencional** y para la entrega 1 corresponde documentar el problema y las posibles soluciones. Se mantienen las tres opciones con su impacto; la **C** es la preferida por la evidencia. Se resuelve antes de construir el mart `revenue_by_org_month` (entrega 2).

## D-09 · Subtotales negativos y créditos nulos (propuesta corregida)
- **Medido.** 13 subtotales negativos y 137 credits nulos. Las 13 facturas negativas tienen taxes positivo: taxes/subtotal ≈ -0,21. Las otras 227 tienen ratio ≈ +0,21. La regla observada es taxes ≈ 0,21 × abs(subtotal), redondeado.
- **Decisión propuesta.** Conservar subtotal y taxes originales; marcar flag_subtotal_negativo y flag_signo_impuesto. No afirmar que sean notas de crédito válidas ni corregir signos automáticamente.
- **Supuesto.** credits NULL → 0 sólo para las comparaciones exploratorias y un cálculo provisional, con flag_credits_imputado. La interpretación contable y el efecto en revenue siguen abiertos antes del mart de entrega 2.
- **Consecuencia.** Se corrige la explicación anterior sin alterar datos ni cifras del perfil. FX, signo del impuesto y ausencia de créditos se resuelven como contratos explícitos.

## D-10 · Dialecto CSV
- **Contexto.** Los CSV siguen RFC 4180: los campos con comas van entre comillas y las comillas internas se duplican (`""`). Spark usa por defecto la barra invertida como escape: con esa configuración `tags_json` se cortaba en la primera coma interna (45 recursos con `pii:true` detectados en lugar de 85).
- **Decisión.** Leer todos los CSV con `option("escape", '"')`. En Bronze, además, `enforceSchema = false` para que Spark valide que el encabezado coincida con el esquema explícito (con esquema explícito las columnas se asignan por posición).
- **Consecuencias.** Se corrigieron los conteos de PII en el perfil. Queda como regla del lector común de CSV.

## D-11 · Bronze batch: partición, rechazos e idempotencia (E2-02)
- **Contexto.** LA consigna misma plantea el requerimiento de ingestar al menos tres maestros "a Parquet particionado con tipificación explícita, `ingest_ts`, `source_file` y deduplicación cuando aplique". El contrato del lago de la entrega 1 (diseño v1.1, §5) proponía maestros en Bronze sin partición.
- **Decisión.**
  - **Partición por `ingest_date`** (fecha de la carga, UTC) en las 7 fuentes CSV: cada carga es un *snapshot* en su propia carpeta. Cumple el requerimiento anterior y conserva además la historia de los maestros. Se aparta del contrato de E1, que se actualiza en la arquitectura de la entrega 2.
  - **Reglas de rechazo**, en este orden; la primera que falla manda el registro a `quarantine/source=<fuente>/dt=<fecha>` con `rule_id`, motivo, clave y fila original: `malformed_row`, `key_null`, `cast_error` (un valor presente que no convierte a su tipo, D-05) y `key_conflict` (misma clave natural con contenidos distintos: no se elige una versión al azar).
  - **Deduplicación**: las copias exactas de una fila se descartan y se cuentan; no son errores.
  - **Idempotencia**: cada ejecución reemplaza solo la partición del día, en Bronze y en quarantine. Re-ejecutar el mismo día da los mismos conteos.
  - **Conciliación**: cada carga verifica `leídas = aceptadas + quarantine + duplicados exactos` y `escritas = aceptadas`; si no cierra, el proceso falla.
- **Descartado.** Partición por `month` en `billing_monthly`: rompería la regla única por fuente y no aporta en Bronze, donde se conserva el grano de origen. *Sobrescribir toda la tabla*: perdería los *snapshots* anteriores.
- **Consecuencias.** En el dataset las 4.112 filas de las 7 fuentes pasan a Bronze sin rechazos (los CSV no traen errores de formato); las reglas están cubiertas por pruebas con casos construidos (`tests/test_bronze_batch.py`). Parquet no es transaccional: si una carga se corta entre el borrado y la escritura, la partición queda vacía hasta la próxima ejecución.

## D-12 · Bronze streaming de eventos: partición, lotes e idempotencia (E2-03)
- **Contexto.** D-04 fijó captura completa hacia Bronze (sin filtro por watermark), staging por `batch_id`, manifiesto de lotes completados y escritor único. El contrato del lago de la entrega 1 (diseño v1.1, §5) proponía Bronze de eventos en `events/dt=AAAA-MM-DD`, con `dt` = fecha del evento. Al implementarlo se midió que cada archivo JSONL trae eventos de muchas fechas: los 120 archivos tocan 7.180 combinaciones archivo × fecha de evento.
- **Decisión.**
  - **Lectura** con Structured Streaming sobre `usage_events_stream/`, esquema explícito (superset v1/v2, D-05), `maxFilesPerTrigger` configurable (1 por defecto, para que cada archivo sea un micro-lote) y checkpoint en `_checkpoints/s1_bronze_events`.
  - **Escritura** con `foreachBatch` en `bronze/usage_events/ingest_date=<fecha de carga>/batch_id=<n>/`. Se agregan `event_ts`, `event_date` y los numéricos tipados, además del linaje (`source_file`, `arrival_ts`, `ingest_ts`, `run_id`, `batch_id`).
  - **Idempotencia.** Un lote re-ejecutado reemplaza su propia carpeta (en cualquier fecha de carga), así que no duplica filas. El manifiesto `_runs/s1_bronze_events/batches/batch_id=<n>.json` se escribe al final; los lectores solo leen lotes con manifiesto. El checkpoint evita volver a leer archivos ya procesados.
  - **Quarantine estructural** en `quarantine/source=usage_events/dt=<fecha de carga>/batch_id=<n>/`: `malformed_json`, `timestamp_invalid`, `cast_error`. Las reglas de negocio (event_id nulo o repetido, costo negativo, `value` nulo) se aplican en Silver (E2-04 y E2-06), porque Bronze conserva el dato como llegó.
  - **Conciliación** por lote (`leídos = Bronze + quarantine`) y global (`eventos en Landing = Bronze + quarantine`; cada archivo de Landing en exactamente un lote).
- **Descartado.** *Partición por fecha del evento en Bronze*: con un archivo por micro-lote escribiría unos 7.180 archivos chicos en vez de 120, y un lote re-ejecutado tocaría hasta 60 particiones, con lo que reemplazarlo sin duplicar sería mucho más difícil. La fecha del evento se usa como partición en Silver, donde el dedup y el watermark ya agruparon los datos. *Escribir con el sink de archivos de Spark* (`writeStream.parquet`): no permite quarantine ni manifiesto propio por lote.
- **Consecuencias.** Se aparta del contrato de E1 (`events/dt=`), que se actualiza en la arquitectura v2 (E2-13). Los 43.200 eventos pasan a Bronze en 120 lotes sin rechazos estructurales; una segunda ejecución no agrega lotes. Las reglas y la re-ejecución de lotes están cubiertas por `tests/test_bronze_streaming.py`. Igual que en D-11, Parquet no es transaccional: un lote cortado a mitad de escritura queda sin manifiesto y se ignora hasta que Spark lo re-ejecute.

## D-13 · Silver de eventos: dos caminos, watermark medido y reconciliación (E2-04)
- **Contexto.** Si bien D-04 propuso un camino operativo con watermark de 24 h y un histórico canónico que no dependa de la ventana, se requiere también medir cuánto descarta Spark de verdad: la simulación de la entrega 1 estimaba 42.118 tardíos.
- **Decisión.**
  - **S2 · camino streaming** (`src/streaming/silver_events_live.py`): lee Bronze como stream, un archivo por micro-lote en orden de llegada, con `withWatermark("event_ts", EVENTS_WATERMARK)` y `dropDuplicatesWithinWatermark("event_id")`. Escribe lo que llega a tiempo en `silver/usage_events_live` con el sink de archivos de Spark, que es exactly-once por `_spark_metadata`. Checkpoint en `_checkpoints/s2_silver_events_live`.
  - **B2 · reconciliación batch** (`src/batch/silver_events.py`): recalcula `silver/usage_events` desde todos los lotes completos de Bronze. Manda a quarantine (`quarantine/source=usage_events_silver/dt=<ingest_date de Bronze>`) lo que no cumple las reglas, deduplica por `event_id` conservando la primera llegada y marca `arrival_path` (`a_tiempo` si salió por S2, `tardio` si la recuperó la reconciliación). Particiona por `event_date`, la fecha del evento en UTC.
  - **Reemplazo de la tabla completa** en cada ejecución: escribe en una carpeta temporal y la cambia de lugar al final (`replace_table`). Los lectores nunca ven una tabla a medio escribir.
  - **Reglas**: E2-04 activa solo `event_id_null`, la que la deduplicación necesita; E2-06 agrega las demás a la misma lista `RULES`.
- **Medición.** Con watermark de 24 h y un archivo por micro-lote, Spark deja pasar **1.439** eventos y descarta **41.761** por tardíos (43.200 = 1.439 + 41.761 + 0 duplicados). La reconciliación los recupera a todos: Silver tiene 43.200 eventos únicos en 60 particiones. Spark aplica el watermark con un micro-lote de desfase: los dos primeros lotes pasan completos.
- **Descartado.** *Silver solo desde el stream*: perdería el 97 % de los eventos con este orden de llegada. *Watermark muy amplio (por ejemplo, 60 días)*: el stream dejaría pasar todo, pero tendría que guardar en memoria el estado de deduplicación de todos los `event_id` durante 60 días, que es lo que el watermark busca evitar. *Actualizar solo las particiones afectadas*: requiere saber qué fechas trae cada lote (hasta 60 por archivo); con este volumen recalcular todo es más simple y el resultado depende solo de Bronze.
- **Consecuencias.** Quien necesite el histórico (Gold, Cassandra, features) lee `silver/usage_events`; `usage_events_live` es solo la vista reciente. Las conciliaciones (`Bronze = Silver + quarantine + duplicados`, `Silver = a_tiempo + tardíos`, `leídos por el stream = a tiempo + descartados + duplicados`) quedan en `evidence/entrega2/e2_04_silver_events.json`. Recalcular todo no escala a volúmenes grandes; en producción se reemplazaría por un formato transaccional con `MERGE` (D-04).

## D-14 · Silver de maestros, enriquecimiento y features (E2-05)
- **Contexto.** En atención a cumplir los requisitos de conformar al menos un maestro en Silver, enriquecer los eventos con joins y calcular tres features (`daily_cost_usd`, `requests` y `genai_tokens` o `carbon_kg`). Bronze guarda una foto por carga de cada maestro (D-11). En el dataset, todos los `org_id` y `resource_id` de los eventos existen en sus maestros y coinciden en servicio y región.
- **Decisión.**
  - **Maestros en Silver**: `customers_orgs` y `resources`, tomados de la **última foto** de Bronze (partición `ingest_date` más reciente), con una fila por clave y linaje (`bronze_ingest_date`, `source_file`). En `resources`, `tags_json` se convierte en la lista `tags` y se extraen `env`, `team`, `costcenter` y `has_pii`.
  - **Eventos enriquecidos** (`silver/usage_events_enriched`, partición `event_date`): eventos de Silver con atributos de la organización y del recurso por **LEFT JOIN**, más `org_found` / `resource_found`. Un evento sin maestro no se pierde: queda marcado.
  - **Una columna por medida**: `requests`, `cpu_hours` y `storage_gb_hours` toman `value` solo cuando la métrica coincide, así nunca se suman horas con requests (diseño E1, §6).
  - **Features diarias** (`silver/org_daily_features`, una fila por organización y día UTC): `daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens`, `carbon_kg`, `carbon_coverage` y `events`. Una medida sin observaciones ese día queda en NULL, no en 0.
  - **Costo en `decimal(18,6)`**: los costos tienen como máximo 4 decimales; sumarlos como decimal es exacto, y el total de las features concilia al micro-dólar con Silver. Gold (E2-07) usa la misma convención.
- **Descartado.** *Historia de maestros (SCD tipo 2)*: hoy hay una sola foto por maestro; la historia queda en Bronze si se la necesita. *INNER JOIN*: perdería en silencio los eventos de organizaciones o recursos que todavía no estén en el maestro. *Features por organización, día y servicio*: ese grano es el del mart de Gold (E2-07); en Silver las features quedan por organización y día, que es lo que usan la detección de anomalías (E2-09) y el análisis por cliente.
- **Consecuencias.** 80 organizaciones y 400 recursos conformados; 43.200 eventos enriquecidos sin huérfanos; 4.580 filas organización × día; costo total USD 147.433,98, igual que en Silver. `genai_tokens` y `carbon_kg` solo existen en eventos v2: `carbon_coverage` (0,75 en promedio) dice qué parte del día está medida. Los costos negativos se suman tal cual hasta que E2-06 los marque.

## D-15 · Reglas de calidad de Silver de eventos (E2-06)
- **Contexto.** Dado que se solcitan tres reglas activas con quarantine y motivo por registro: `event_id` no nulo y único, costo ≥ −0,01 con bandera y `unit` no nula si hay `value`. En el dataset, 2.038 eventos tienen `value` pero no `unit` (USD 7.106 de costo, en las 80 organizaciones y los 60 días), y su métrica indica sin ambigüedad cuál es la unidad. Ningún evento con unidad la trae distinta de la que corresponde a su métrica. Si esos eventos fueran a quarantine, Silver y Gold perderían el 4,8 % del costo.
- **Decisión.** Cada regla termina en una **acción**, y todas dejan una fila con regla, acción, motivo y registro original en `quarantine/source=usage_events_silver/dt=<ingest_date de Bronze>`, que funciona como registro de calidad:

  | Regla | Falla si | Acción |
  |---|---|---|
  | `event_id_null` | `event_id` es nulo | rechazado: sale de Silver |
  | `event_id_duplicate` | el `event_id` ya llegó antes | descartado: queda la primera llegada (D-06) |
  | `unit_mismatch` | la unidad no corresponde a la métrica | rechazado |
  | `unit_missing_uninferable` | sin unidad, con `value` y con métrica desconocida | rechazado |
  | `unit_missing` | sin unidad, con `value` y con métrica conocida | corregido: `unit` se completa (`requests`→`count`, `cpu_hours`→`hours`, `storage_gb_hours`→`gb_hours`) y `unit_inferred = true` |
  | `cost_below_tolerance` | `cost_usd_increment` < −0,01 USD | marcado: el evento sigue tal cual, con `cost_negative = true` |

  - El registro guarda el dato **original** (por ejemplo, sin unidad), y Silver el corregido.
  - Conciliaciones: `Bronze = Silver + rechazados + descartados`; `registros de calidad = rechazados + descartados + corregidos + marcados`; los corregidos y marcados coinciden con las banderas de Silver.
  - Las banderas `unit_inferred` y `cost_negative` pasan a los eventos enriquecidos, y las features diarias suman `negative_cost_events`.
- **Descartado.** *Quarantine para los eventos sin unidad (criterio original del plan)*: perdería USD 7.106 por un dato que se reconstruye sin ambigüedad. *Corregir sin dejar registro*: no quedaría constancia de qué se cambió ni por qué. *Rechazar los costos negativos*: pueden ser créditos o ajustes legítimos (ver D-09 para facturación), y sacarlos alteraría el costo; se conservan y se marcan hasta que el negocio defina su semántica.
- **Consecuencias.** En el dataset: 2.038 corregidos, 211 marcados (USD −917,60), 5 negativos dentro de la tolerancia y 0 rechazados o descartados. Las reglas de rechazo están cubiertas por casos construidos en `tests/test_quality_rules.py`. Cambia el criterio de cierre de E2-06 en `plan_trabajo.md` ("quarantine" pasa a "registro de calidad con acción"). Quien busque solo los rechazos reales filtra `action IN ('rechazado', 'descartado')`.

## D-16 · Mart FinOps `org_daily_usage_by_service` (E2-07)
- **Contexto.** Derivado de que se solicita el mart FinOps con grano `org_id, usage_date, service` y "métricas de uso, requests y costos diarios", preparado para servirse en Cassandra (§7.4: costos y requests por organización y servicio en un rango de fechas; Top-N servicios por costo en los últimos 14 días). El contrato de E1 lo ubicaba en `gold/org_daily_usage_by_service/dt=...`, con el objetivo O5: costo de Silver igual al de Gold al mismo corte.
- **Decisión.**
  - **Fuente**: `silver/usage_events_enriched`. Se recalcula entera en cada ejecución (`replace_table`), igual que Silver (D-13).
  - **Columnas**:
    - Claves: `org_id`, `usage_date` (día UTC del evento) y `service`.
    - Atributos de la organización (`org_name`, `plan_tier`, `industry`), desnormalizados para que Cassandra responda sin joins.
    - Medidas: `daily_cost_usd` en `decimal(18,6)`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens`, `carbon_kg`, `events` y `resources_active`.
    - Indicadores de calidad: `negative_cost_events`, `negative_cost_usd`, `unit_inferred_events`, `late_events` y `carbon_coverage`.
    - Linaje: `gold_processed_ts` y `gold_run_id`.
  - **Partición por `usage_date`**, un archivo por día: 60 archivos, unos 0,8 MB en total. Es el `dt` del contrato de E1 con el nombre de la consigna.
  - **Los costos negativos se suman** al costo diario y se informan aparte (`negative_cost_usd`). FinOps ve el costo real y también cuánto viene de ajustes marcados.
  - **Conciliación**:
    - Una fila por clave.
    - Eventos, costo (al micro-dólar) y requests de Gold iguales a Silver.
    - Por organización y día, Gold (sumando servicios) igual a `silver/org_daily_features`.
    - Si algo no cierra, la ejecución falla.
  - **Huella de contenido** (`fingerprint`): suma de hashes de clave, costo, requests y eventos. Debe repetirse entre ejecuciones; la evidencia compara la de la corrida anterior.
  - **Consultas de referencia**: las dos de §7.4 se calculan también en Spark y quedan en la evidencia, para comparar con el resultado de CQL en E2-08. El "hoy" de las consultas es el último día del dataset (31/08/2025), porque los datos son históricos.
- **Descartado.** *Partición por servicio*: las consultas filtran por organización y rango de fechas, y una partición por servicio no evita leer días que no se piden. *Sin partición*: más simple con este volumen, pero se aparta del contrato y obliga a reescribir todo para corregir un día. *Excluir los costos negativos marcados*: cambiaría el total respecto de Silver y ocultaría los ajustes.
- **Consecuencias.** 11.050 filas para 80 organizaciones, 60 días y 6 servicios. Costo USD 147.433,98 y 2.294.079 requests, iguales a Silver. La re-ejecución da la misma huella. El modelo de Cassandra (E2-08) parte de este grano; las claves de partición y clustering se deciden allí según las dos consultas.

## D-17 · Serving en Cassandra/AstraDB (E2-08)
- **Contexto.** La consigna (§6.2) pide un keyspace, una tabla query-first para el mart cargada desde Spark y dos consultas CQL con resultados. §7.4 fija las consultas: (1) costos y requests diarios por organización y servicio en un rango de fechas; (2) Top-N servicios por costo acumulado en los últimos 14 días para una organización. El checklist pide registrar las claves de Cassandra (§6.3). La base es AstraDB Serverless (non-vector), keyspace `cloud_analytics`, AWS us-east-2; AstraDB no acepta `CREATE KEYSPACE` por CQL, así que el keyspace se crea desde el portal.
- **Decisión: una tabla por consulta** (`cql/01_schema.cql`).

  | Tabla | Clave de partición | Clustering | Responde |
  |---|---|---|---|
  | `org_daily_usage_by_service` | `org_id` | `usage_date DESC, service ASC` | Consulta 1: `WHERE org_id = ? AND usage_date >= ? AND usage_date <= ?` |
  | `top_services_by_org_14d` | `org_id` | `window_end_date DESC, cost_rank ASC` | Consulta 2: `WHERE org_id = ? AND window_end_date = ? LIMIT N` |

  - **Partición por `org_id`**: las dos consultas piden una sola organización, y cada consulta lee una sola partición. Hoy cada partición tiene como máximo 360 filas por tabla (60 días × 6 servicios); con un año serían unas 2.200, muy por debajo de los límites recomendados para una partición de Cassandra.
  - **Ranking precalculado**: Cassandra solo ordena por columnas de clustering y no puede agrupar por `service` en la primera tabla. El ranking se calcula en Spark (`gold/top_services_by_org_14d`, ventanas móviles de 14 días para cada día de fin) y `cost_rank` es columna de clustering. La consulta es una lectura directa, sin agregar ni ordenar en el cliente.
  - **Carga**: driver de Python (`cassandra-driver` 3.29.2) dentro de `foreachPartition`. Cada partición de Spark abre su sesión con el Secure Connect Bundle y escribe con sentencias preparadas en paralelo. Es la opción "foreachBatch/driver" de la consigna (§4.4) y evita instalar los JARs del conector de Spark.
  - **Idempotencia**: el INSERT de Cassandra es un upsert por clave primaria, así que re-cargar no duplica. Las filas cuya clave ya no está en Gold se borran (`prune_stale`, organización por organización). Las claves se normalizan antes de comparar, porque el driver devuelve las fechas como `cassandra.util.Date`, que es igual a `datetime.date` pero con otro hash: sin normalizar, la limpieza borraría filas válidas.
  - **Verificación**: antes y después de cargar se cuentan filas y se suma el costo por organización con `COUNT(*)` y `SUM` dentro de cada partición; deben coincidir con Gold. Las dos consultas se ejecutan desde `cql/02_queries.cql` y se comparan con las calculadas en Spark (E2-07).
  - **Credenciales**: token y bundle se leen de variables de entorno (`ASTRA_DB_APPLICATION_TOKEN`, `ASTRA_SECURE_BUNDLE_PATH`) y se guardan fuera del repositorio. La evidencia no las incluye.
- **Descartado.**
  - *Conector de Spark para Cassandra*: necesita descargar y configurar JARs para Spark 3.5 en cada entorno, y la consigna admite el driver.
  - *Una sola tabla y agregar en el cliente para el Top-N*: no es query-first, porque obliga a traer 14 días × 6 servicios y ordenar fuera de Cassandra.
  - *Partición por `(org_id, mes)`*: limita el tamaño de partición en horizontes largos, pero una consulta de 14 días podría cruzar dos meses y necesitar dos lecturas. Con este volumen no hace falta; queda como mejora si el histórico crece.
  - *`ALLOW FILTERING`*: hace que Cassandra recorra datos que la clave no restringe; ninguna consulta lo necesita.
- **Consecuencias.** Cassandra queda con 11.050 filas en el mart y 15.599 en el ranking, iguales a Gold. Una segunda carga no cambia los conteos. Las cinco consultas de la entrega final (§7.4) requerirán más tablas con el mismo criterio de una tabla por consulta. Los valores nulos se escriben como nulos (tombstones); con este volumen no afecta el rendimiento, y garantiza que una re-carga refleje un valor que pasó a nulo.

## D-18 · Anomalías de costo: `cost_anomaly_mart` (E2-09)
- **Contexto.** La consigna pide flags o scores de anomalía "con métodos justificados, por ejemplo z-score, MAD o percentiles" (§6.2), en un mart `cost_anomaly_mart` con grano `org_id, date, service` y "score o flag de anomalía y contexto del costo" (§7.3), con métricas y limitaciones (§7.1). El checklist pide registrar los umbrales (§6.3). E1 encontró 34 eventos con costo mayor a 10 × p99, cada uno en una combinación distinta de organización, día y servicio. Las series (organización × servicio) tienen una mediana de 39 días con uso.
- **Decisión.**
  - **Fuente y grano**: `gold/org_daily_usage_by_service`, una fila por fila del mart (11.050), con partición por `usage_date`.
  - **Método principal: z-score robusto con MAD.** Para cada día se toma la misma serie en los **28 días anteriores** (sin el día evaluado: no mira el futuro y el día raro no contamina su propia referencia). `robust_z = 0,6745 × (costo − mediana) / MAD`. Si la MAD es 0, se usa 1,2533 × desvío absoluto medio.
  - **Umbrales**:

    | Parámetro | Valor | Por qué |
    |---|---|---|
    | `z_threshold` | 3,5 | Valor de referencia de Iglewicz y Hoaglin para z robusto |
    | `min_excess_usd` | USD 5 | Materialidad: un salto de centavos puede ser estadísticamente raro sin importarle a FinOps |
    | `window_days` | 28 | Cuatro semanas: incluye ciclos semanales y deja historia suficiente en 60 días de datos |
    | `min_history` | 7 | Con menos días, la mediana y la MAD no son estables |
    | `cold_start_percentile` | p99 del servicio | Respaldo para los días sin historia suficiente |

  - **Arranque en frío**: con menos de 7 días de historia se marca spike si el costo supera el p99 del costo diario del servicio entre todas las organizaciones, por al menos USD 5.
  - **Tipos**: `spike` (por `mad` o por `percentil_servicio`), `costo_negativo` (el día tiene costos marcados por D-15) o ambos. **Severidad**: `alta` si `robust_z` ≥ 7 o si se detectó por percentil, `media` para el resto de los spikes y `revisar` para costos negativos.
  - **Contexto en cada fila**: costo del día, mediana y MAD de la línea base, exceso en USD, `robust_z`, `z_classic`, días de historia, método y umbrales usados.
- **Evaluación** (`evidence/entrega2/e2_09_cost_anomalies.json`).
  - Spikes: 222 (204 por MAD, el 2,2 % de los 9.216 días puntuados, y 18 por percentil). Además, 208 días con costos negativos.
  - **Recall frente a los 34 spikes conocidos de E1: 34/34** (25 por MAD y 9 por percentil, todos en los primeros 11 días del dataset).
  - Sensibilidad: con z entre 3 y 5 y exceso entre 0 y 5 USD, los flags por MAD van de 107 a 330 y el recall se mantiene en 25/25.
  - El z-score clásico (media y desvío, z > 3) marca 132 días y detecta 24 de los 25: un spike previo infla el desvío y esconde al siguiente. La MAD no tiene ese problema; `tests/test_cost_anomalies.py` lo reproduce.
- **Descartado.** *z-score clásico*: se enmascara con los propios spikes. *Percentiles por serie*: con unos 28 días de historia, un p99 por serie es casi el máximo, inestable. *Modelos de ML (Isolation Forest, pronóstico)*: sin etiquetas no hay cómo validarlos mejor que un método estadístico explicable, y FinOps necesita entender por qué se marca cada día. *Línea base con todo el período*: miraría el futuro y el propio spike entraría en su referencia.
- **Limitaciones.** Sin etiquetas reales no se puede medir la precisión; el recall se mide contra spikes evidentes. Los días sin uso no son ceros: la línea base usa solo días con uso. El respaldo por percentil usa todo el período (en operación se usaría el período anterior). Solo se marcan subas: las bajas y los créditos se ven por el flag de costo negativo. Los umbrales deben revisarse con FinOps cuando haya casos confirmados. El mart no se sirve todavía en Cassandra; queda para la entrega final, con una tabla por consulta (D-17).

## D-19 · Orquestación, manejo de errores y limpieza (E2-12)
- **Contexto.** El pipeline tiene nueve etapas que dependen unas de otras: cada una lee lo que dejó la anterior. En E2-07 se ejecutó Gold sobre un enriquecimiento desactualizado porque se había saltado una etapa. La consigna pide documentar la puesta en marcha, logs, manejo de errores, pruebas y "mecanismos de limpieza/reinicio" (§6.2 y §7.1). El plan pide que otra persona del equipo ejecute el pipeline desde un entorno limpio siguiendo el README.
- **Decisión.**
  - **`scripts/run_pipeline.py`** ejecuta las etapas en orden, **cada una en un proceso aparte** con su propia sesión de Spark. Es el mismo script que se corre a mano, así que no hay dos formas de ejecutar el pipeline.
  - **Corte ante fallas**: si una etapa sale con error, o si su registro no existe o tiene un balance que no cierra, el pipeline se detiene y dice en qué etapa y en qué log mirar. No sigue sobre datos viejos.
  - **Registro de la ejecución** (`evidence/entrega2/e2_12_pipeline_run.json`): operador, entorno (Python, Java, PySpark, driver de Cassandra, sistema), resultado de las pruebas y, por etapa, duración, estado y los conteos y balances del registro de cada proceso. La salida completa de cada etapa va a `datalake/_runs/pipeline/<run_id>/`.
  - **La evidencia de cada paquete no se reescribe en cada corrida**: las etapas escriben su evidencia en la carpeta de la ejecución, salvo que se pida `--actualizar-evidencias`.
  - **`--clean`** vacía `bronze/`, `silver/`, `gold/`, `quarantine/` y `_checkpoints/` (conserva sus `.gitkeep`) y se niega a borrar una carpeta que contenga `landing/`. Landing es inmutable (D-02) y `_runs/` conserva el historial. Cassandra no necesita limpieza: la carga reemplaza por clave y borra lo que ya no está en Gold (D-17).
  - **AstraDB opcional**: sin credenciales, la etapa de serving se omite y queda registrado. El resto del pipeline se puede reproducir sin cuenta en Astra.
- **Descartado.** *Orquestador externo (Airflow, Prefect, Make)*: agrega una instalación y un servicio más para nueve pasos lineales; queda como mejora para la entrega final si se agendan ejecuciones. *Todas las etapas en una sola sesión de Spark*: más rápido, pero un fallo en una etapa podría dejar estado en memoria que afecte a la siguiente, y ya no sería el mismo script que se corre a mano.
- **Consecuencias.** Una corrida completa desde cero (`--clean --tests`) tarda unos 8 minutos y reproduce exactamente el mismo Gold: la huella de contenido es `144780998816` en dos máquinas distintas. El mismo script sirve para demostrar la idempotencia en E2-10, corriéndolo dos veces y comparando los registros.

## D-20 · Demostración de idempotencia (E2-10)
- **Contexto.** La consigna pide "demostrar re-ejecución sin duplicados mediante conteos antes/después, checkpoints, claves naturales o upserts" (§6.2), y el plan, que re-ejecutar el pipeline no cambie los conteos de Silver, Gold ni Cassandra. Cada capa ya es idempotente por su propio mecanismo:
  - Bronze batch reemplaza la partición del día (D-11).
  - Los streams retoman desde su checkpoint, y cada lote reemplaza su carpeta (D-12, D-13).
  - Silver y Gold se recalculan enteros desde la capa anterior (D-13, D-16).
  - Cassandra hace upsert por clave primaria y borra lo que ya no está en Gold (D-17).
  
  Faltaba demostrarlo de punta a punta, con una sola evidencia.
- **Decisión.** `scripts/check_idempotency.py` toma una foto del estado, re-ejecuta el pipeline completo **sin `--clean`** (como una re-ejecución real) y vuelve a tomar la foto. Compara:
  - **Por tabla del lago** (Bronze, Silver, registro de calidad y Gold): filas y **huella de contenido**, la suma de `xxhash64` de cada fila sobre sus columnas de negocio (`src/common/fingerprint.py`). La huella no depende del orden ni de los archivos, y cambia si cambia un valor o si una fila aparece, desaparece o se duplica. Contar filas solo no alcanza: la misma cantidad puede esconder contenido distinto.
  - **Columnas volátiles excluidas de la huella**: hora de proceso, `run_id` y fecha de carga (`ingest_ts`, `run_id`, `ingest_date`, `*_processed_ts`, `*_run_id`, `loaded_at`). Cambian legítimamente en cada corrida sin que cambie el dato.
  - **Streams**: lotes completos de S1 (manifiestos) y número del último lote confirmado de S2. Spark conserva solo los ~100 últimos registros de lotes, por eso se compara el último número y no la cantidad de archivos.
  - **Cassandra** (si hay credenciales): filas y costo total por tabla, sumando las particiones de cada organización.
  - Si una métrica difiere, o si la re-ejecución falla, el chequeo termina con error.
- **Descartado.** *Comparar solo conteos*: no detecta un valor cambiado ni un reemplazo de filas. *Comparar los archivos Parquet byte a byte*: Spark puede nombrar y repartir los archivos distinto en cada corrida sin que cambie el contenido. *Contar las fotos de Bronze*: una re-ejecución otro día agrega una foto de cada maestro, por diseño (D-11). Queda como dato informativo; lo que se compara es el contenido de la última foto, que Silver usa.
- **Consecuencias.** En la prueba local, la re-ejecución tarda unos 4 minutos (contra 8 desde cero), porque los streams no reprocesan nada. Da 22 de 22 métricas del lago iguales, con las mismas huellas. La evidencia oficial (`evidence/entrega2/e2_10_idempotency.json`) incluye además Cassandra.

## D-21 · Gobierno preliminar: catálogo, clasificación y linaje (E2-11)
- **Contexto.** La consigna pide "definir controles iniciales, metadatos, linaje, responsabilidades, acceso y evidencias" (§6.2), y en la final un diccionario de datos (§7.1). La rúbrica le asigna un 10 % a "Gobierno y calidad": responsables, controles, linaje, seguridad y observabilidad. Casi todos los controles ya existían (D-11 a D-20), pero dispersos. `users.email` es el único dato personal directo del dataset (sintético, `@example.com`), `sales_rep` es un seudónimo y `nps_surveys.comment` es texto libre.
- **Decisión.**
  - **Documento único** ([`docs/entrega2/gobierno.md`](docs/entrega2/gobierno.md)) con controles, metadatos, linaje, responsables, acceso, observabilidad y pendientes. Cada afirmación apunta a su evidencia.
  - **Catálogo como código.** Las descripciones viven en `src/common/catalog.py`, y `scripts/build_catalog.py` las cruza con los Parquet reales para generar [`catalogo_datos.md`](docs/entrega2/catalogo_datos.md): 20 tablas y 301 columnas. Una columna sin describir hace fallar el script, y las pruebas verifican que los esquemas declarados en el código estén descritos.
  - **Clasificación en tres clases**: interna, confidencial y personal, por tabla, más notas por columna (personal, seudónimo, texto libre, indicador de PII).
  - **Linaje verificable a nivel fila.** `scripts/trace_lineage.py` recorre una fila de Gold hasta los archivos de Landing por las columnas de linaje y los manifiestos de lote. Verifica que el costo coincida en cada capa y que el SHA-256 de cada archivo sea el registrado al ingerirlo.
  - **Minimización.** `email` queda en Bronze, porque Landing es inmutable, pero ningún proceso lo propaga. Una prueba verifica que ninguna tabla derive de `bronze/users`.
  - **Responsables por ámbito** según los roles del plan de trabajo, con un rol revisor distinto.
- **Descartado.** *Catálogo escrito a mano*: se desactualiza sin aviso con el primer cambio de esquema. *Herramienta de catálogo externa (DataHub, OpenMetadata)*: requiere un servicio más que montar para 20 tablas; queda como mejora si el proyecto crece. *Hashear `email` ya en Bronze*: rompería la regla de que Bronze conserva el dato tal como llegó; como no se propaga, el riesgo se controla por minimización.
- **Consecuencias.** El diccionario de datos de la final parte de este catálogo. Los pendientes de gobierno pasan al backlog (E2-14): alertas, token de solo lectura para el consumo, política de retención, medición de la frescura O4 y confirmación de responsables.
