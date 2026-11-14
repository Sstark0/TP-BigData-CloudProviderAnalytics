# Registro de decisiones técnicas

Formato: cada decisión tiene **contexto → decisión → alternativas descartadas → consecuencias**.
Estado: `Aceptada` · `Propuesta` (a validar con el equipo/docente; entrega reprogramada al 05/10, hora pendiente) · `Abierta` (sin resolver todavía).

| ID | Decisión | Estado | Fecha |
|---|---|---|---|
| D-01 | Entorno de ejecución: PySpark 3.5 en Colab / local; Docker diferido | Aceptada | 2026-09-25 |
| D-02 | Landing inmutable; lectura con esquema explícito "defensivo" | Aceptada | 2026-09-25 |
| D-03 | Patrón arquitectónico híbrido | Propuesta | 2026-09-25 |
| D-04 | Replay, late data e idempotencia | Propuesta | 2026-09-25 |
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
---

## D-01 · Entorno de ejecución
- **Contexto.** La consigna exige ejecutar en Google Colab o en un entorno equivalente validado. El serving es AstraDB (servicio gestionado), así que no hay Cassandra local que levantar.
- **Decisión.** PySpark 3.5.3 (misma línea que Colab), `local[*]`. Rutas por variables de entorno (`config/.env.example`).
- **Descartado.** Clúster Hadoop/YARN en Docker: costo alto de montaje y ningún requisito lo pide. `docker-compose` con Jupyter + PySpark queda como mejora de reproducibilidad a evaluar para la 2.ª entrega (`infra/README.md`).
- **Consecuencias.** HDFS y YARN se usan como marco conceptual (bloques, archivos pequeños, localidad, recursos), no como infraestructura.

## D-02 · Landing inmutable
- **Contexto.** "Los archivos de Landing no deben modificarse" (consigna 3.2).
- **Decisión.** Ningún proceso escribe en `datalake/landing/`. Toda corrección ocurre al promover a Bronze/Silver, y los registros rechazados van a `quarantine/` con el motivo.
- **Consecuencias.** Reprocesar = volver a leer Landing (habilita re-stream/backfill; ver D-03).

## D-03 · Patrón arquitectónico (propuesta)
- **Contexto.** Maestros chicos y de baja frecuencia (80 orgs, 240 facturas en 3 meses) vs. eventos de uso en flujo continuo (720/día en la muestra). La consigna exige como piso streaming de eventos + batch de maestros/facturación.
- **Decisión propuesta.** Híbrido: **eventos con ingesta Structured Streaming**, publicación incremental y reconciliación batch diaria del histórico + **batch para maestros y facturación**. Es híbrido con un único contrato de transformación reutilizado; no se presenta como Kappa pura.
- **Descartado.** *Lambda clásica* (batch layer + speed layer sobre los mismos eventos): duplica la lógica sin ganancia, porque Landing ya permite recalcular. *Kappa pura*: tratar CSV mensuales como streams es artificial.
- **Detalle.** Se desarrolla en el paso 4 (arquitectura v1).

## D-04 · Replay, late data e idempotencia (propuesta v1)
- **Evidencia.** La simulación con un archivo por micro-lote y watermark 24 h identifica 42.118 eventos atrasados; no demuestra el descarte real de Spark.
- **Decisión.** Captura completa hacia Bronze sin filtro por watermark. Dedupe exacto por event_id en publicación Silver. Para el prototipo futuro en Parquet: staging por run_id/batch_id, manifiesto de lotes completados, reemplazo de particiones afectadas y recuperación de corrida incompleta; escritor único. foreachBatch por sí solo no ofrece idempotencia y Parquet no posee merge transaccional nativo.
- **Operación propuesta.** Vista operacional por micro-lote con ventana UTC de 5 minutos y watermark 24 h, identificada como provisional; late data se conserva en Bronze y se incorpora mediante reconciliación batch diaria. El histórico canónico no se deriva exclusivamente de la ventana.
- **Replay didáctico.** Mantener Landing intacto. Una copia derivada para demo puede ordenarse por event_ts fuera de Landing, con manifiesto del origen. Ensayar también archivos originales mezclados y comparar sumas completas. No sustituir el original.
- **Trade-off.** Reescribir particiones pequeñas simplifica el prototipo, pero no escala ni es atómico en todos los almacenamientos. En producción evaluar formato transaccional; no incorporarlo ahora como implementación.
- **Estado.** Diseño propuesto; validar en entrega 2 watermark, checkpoint, dedupe, recuperación y latencia. Véase documento integrado.

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