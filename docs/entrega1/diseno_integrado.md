# Cloud Provider Analytics · Diseño de primera entrega

**v1.0 · 02/10/2026 · ITBA Big Data 2C 2026.** Primera entrega reprogramada al **05/10**, hora a confirmar. Base: commit 2a167696d23c95594dac297daf43a53eb7839bf0. Esta revisión conserva caso, inventario y decisiones previas y completa su diseño. Arquitectura y contratos son **propuestos**; exploración es ejecutable. No se afirma implementación de batch/streaming/serving.

## 1. Problema, usuarios y éxito

El proveedor necesita conformar uso, clientes, facturación y soporte para decisiones confiables. FinOps pregunta por costos diarios por organización/servicio, servicios más caros y facturación mensual; Soporte por tickets críticos, SLA y CSAT; Producto por uso, carbono y tokens GenAI. Operadores necesitan completitud, rechazos y reprocesamiento.

| Objetivo | Meta propuesta | Evidencia futura |
|---|---|---|
| O1 Completitud | 100% de registros clasificados como aceptados, rechazados o duplicados | balance por fuente/run_id |
| O2 Idempotencia | 0 event_id repetidos tras replay | comparación antes/después |
| O3 Calidad | 3 o más reglas con motivo y conteo | quarantine/flags |
| O4 Frescura | Silver durable ≤5 min desde arrival_ts (supuesto) | silver_processed_ts − arrival_ts |
| O5 Conciliación | costo Silver = Gold en igual corte, con tolerancia de redondeo declarada | sumas/control de nulos |
| O6 Negocio | cinco consultas finales; dos en entrega 2 | CQL/resultados |
| O7 Reproducibilidad | exploración desde entorno limpio ahora; pipeline en entrega 2 | notebook ejecutado/versiones |

## 2. Evidencia de datos y 5V

| Fuente | Filas | Campos | Grano / clave | Llegada supuesta |
|---|---:|---:|---|---|
| customers_orgs | 80 | 11 | organización / org_id | snapshot diario |
| users | 800 | 7 | usuario / user_id | snapshot diario |
| resources | 400 | 7 | recurso / resource_id | snapshot diario |
| support_tickets | 1000 | 8 | ticket / ticket_id | lote diario |
| marketing_touches | 1500 | 7 | interacción / touch_id | lote diario |
| nps_surveys | 92 | 4 | encuesta / org_id+survey_date | lote diario |
| billing_monthly | 240 | 8 | factura org/mes / invoice_id | mensual |
| usage_events_stream | 43200 | 13 superset | medición / event_id | incremental |

Medido: 120 JSONL, 12.927.515 bytes, 60 días, 720 eventos/día; siete CSV suman 307.906 bytes. El paquete v1 contiene esquema v1 (10.800) y v2 (32.400). Desde 18/07/2025 v2 agrega carbon_kg; genai_tokens aparece en GenAI. Ausencia v1 se conserva NULL, no cero. Campos y tipos completos: inventario original y src/common/schemas.py.

Calidad medida: 877 value nulos, 1309 como texto casteable, 2075 unit nulos; 216 costos negativos; 7371 eventos anteriores al alta del recurso. 0 event_id duplicados/huérfanos y 14 colisiones contextuales legítimas. 85 recursos con pii:true; 232 logins anteriores al alta; 40 CSAT fuera de escala. FX y signos de impuestos requieren interpretación explícita: 13 subtotales negativos con impuestos positivos; no confirmar nota de crédito. Muestra didáctica, sin evidencia de throughput productivo.

| V | Evidencia / supuesto | Decisión |
|---|---|---|
| Volumen | bajo medido; escala futura supuesta | Parquet, partición gruesa y compactación |
| Velocidad | near-real-time requerido, no medido | captura streaming y publicación incremental |
| Variedad | 8 fuentes, CSV/JSONL, v1/v2 | esquemas explícitos y Silver |
| Veracidad | nulos, tipos y FX ambiguos | reglas, flags y quarantine |
| Valor | tres dominios y consultas concretas | marts query-first y serving |

Ejemplo de escala **hipotético**: 100.000 recursos × 1440 eventos/día × 300 bytes ≈43,2 GB/día crudos; 90 días ≈3,9 TB antes de compresión. Cambiar frecuencia/recursos cambia el cálculo linealmente. No describe la muestra ni dimensiona un cluster definitivo.

## 3. Arquitectura y patrón

![Arquitectura propuesta v1](arquitectura.svg)

Fuente editable: arquitectura.mmd. **Híbrido**: CSV batch y JSONL Structured Streaming; eventos ingresan una vez a Bronze, transformaciones compartidas conforman Silver; micro-lotes publican métricas operativas provisionales y batch diario reconcilia histórico. Kappa pura complica CSV mensuales; Lambda con lógica duplicada no aporta al prototipo. La combinación conserva ambos ritmos exigidos por 4.3.

Flujo batch: CSV → lectura PySpark con header/escape y esquema defensivo → Bronze tipificado → Silver normalizado y unido a dimensiones → Gold por dominio → carga futura Cassandra/AstraDB → consultas. Dimensiones pequeñas: snapshot; SCD futura sólo si la necesidad temporal lo justifica.

Flujo streaming: JSONL → esquema superset, checkpoint por query y source_file → Bronze sin filtro temporal → Silver dedupe event_id/casts/enriquecimiento → ventanas operativas 5 minutos con watermark propuesto 24 h → Gold provisional/serving. Eventos tardíos permanecen en Bronze y entran al Gold histórico por batch de reconciliación. Checkpoint de fuente no reemplaza idempotencia del destino.

Cada archivo contiene casi 60 días: simulación un archivo/lote con watermark24h identifica 42.118 atrasados. No usar sólo la salida de ventana como histórico canónico. Replay ordenado derivado y replay original mezclado se compararán en entrega2, sin cambiar Landing.

**Idempotencia propuesta para Parquet del prototipo:** escritor único, staging run_id/batch_id, manifest de lotes completados, dedupe event_id, reemplazo de particiones afectadas y protocolo de recuperación. foreachBatch no es garantía por sí mismo; Parquet carece de merge transaccional. Las limitaciones de atomicidad/concurrencia quedan para validar; formato transaccional es alternativa futura.

Gobierno transversal: responsable de fuente, catálogo de schemas/reglas, linaje fuente→tabla→mart, clasificación PII, mínimo acceso, métricas filas/rechazos/latencia y registro de corridas. No emails ni tags personales en marts; secretos futuros mediante entorno, nunca versionados.

## 4. Contrato del Data Lake

| Zona | Grano / formato | Partición / naming | Promoción |
|---|---|---|---|
| Landing | original CSV/JSONL inmutable | nombres del docente | validar manifest/checksum, sólo leer |
| Bronze | fila origen / Parquet tipificado | events/dt=YYYY-MM-DD; maestros sin partición | cast controlado, metadata, balance |
| Silver | evento único o dimensión / Parquet | usage_events/dt=...; batch pequeño sin partición | claves, joins, nulos, flags |
| Gold | org/día/servicio o org/mes / Parquet | org_daily_usage_by_service/dt=...; revenue/month=... | conciliación, grano y controles |
| Quarantine | registro rechazado / Parquet | source=.../dt=...; rule en columna | revisión y replay con nueva regla |
| Checkpoints | estado de query Spark | _checkpoints/query_id/ | exclusivo por query/version |

Rutas relativas al DATALAKE_ROOT. snake_case; fechas UTC; particiones por tiempo, nunca por event_id/org_id en esta muestra. Servicio será columna; añadir partición service sólo con evidencia de tamaño/filtros. Maestros sin particionar evita archivos mínimos. Compactar por fecha en publicación/reconciliación; target futuro 128–256 MiB por archivo es supuesto, sin forzarlo sobre 13 MB.

Retención **propuesta**: prototipo conserva Landing, Bronze, Silver, Gold y manifests hasta fin de evaluación; no borrado automático. Escenario productivo a aprobar: Landing/Bronze90d, Silver180d, Gold24meses, quarantine30d desde resolución (retener pendientes), checkpoints mientras query activa +30d después de retiro. Verificar PII/acceso y requerimientos contables antes de adoptar. No es política legal ni implementación.

Metadata mínima: source_file, ingest_ts UTC, arrival_ts, run_id/batch_id, schema_version en eventos, silver_processed_ts; catálogo por tabla con dueño propuesto, clave, grano, tipos, versión de regla y linaje. Manifest incluye archivos/hash, conteos entrada/aceptados/rechazados/duplicados y salida durable. Rechazos llevan rule_id/reason y origen; flags conservan anomalías no invalidantes.

Reglas: cast inválido → quarantine; claves ausentes → quarantine; unit nulo → imputar por metric y flag; value nulo → flag, sin sumar como consumo conocido; costos negativos → flag, preservar; v1 campos nuevos NULL; org/resource sin dimensión → quarantine o pendiente de dimensión explícito. Sólo promover si balance y claves cumplen. FX/credits/subtotal se conserva crudo, cálculo provisional etiquetado y decisión abierta antes del mart.

## 5. MapReduce conceptual

Referencia batch: costo y requests por organización/fecha/servicio desde Silver conforme.

1. **Map:** validar fecha/clave; emitir K=(org_id, date_UTC(event_ts), service), V=(cost_usd_increment, requests si metric=requests y value válido, count_requests_known, event_count). No sumar cpu_hours y storage_gb_hours a requests.
2. **Combiner opcional:** sumar valores por K dentro de partición; conservar contador de requests conocidos, sin transformar NULL en consumo medido cero.
3. **Shuffle:** reunir misma K; clave temporal acota grano y reparto.
4. **Reduce:** sumar costo, requests conocidos y contadores; si no hay request conocido, requests=NULL. Añadir flags/calidad.
5. **Salida:** org_daily_usage_by_service en Gold, una fila/K; dedupe previo garantiza que replay no suma dos veces. Conciliar costo con Silver para mismo corte y registros válidos.

Equivalente PySpark propuesto: select/casts/filter → dedupe event_id → groupBy(org_id, date, service).agg(sum(cost), sum(when(metric=requests,value)), count(...)) → validación y write Parquet por dt. No se requiere Hadoop/MapReduce ejecutado en esta etapa.

## 6. Matriz requisito–componente–evidencia

| Requisito / objetivo | V | Componente propuesto | Evidencia actual / futura |
|---|---|---|---|
| problema/usuarios/éxito, O6 | Valor | marts FinOps/Soporte/Producto | caso; CQL futuro |
| inventario y tipos | Variedad | schemas + perfil | notebook y perfil ejecutado |
| batch maestros/facturas | Variedad | lector PySpark/Bronze | diseño; job entrega2 |
| captura incremental, O4 | Velocidad | Structured Streaming/checkpoint | flujo; latencia futura |
| v1/v2 | Variedad | superset/Silver | versiones medidas |
| calidad, O1/O3 | Veracidad | quarantine/flags/manifests | conteos; balance futuro |
| replay/dedupe, O2 | Veracidad | event_id + protocolo lotes | claves medidas; rerun futuro |
| particiones/retención | Volumen | Parquet/catalogo | contrato; tamaños futuros |
| agregación, O5 | Valor | batch/MapReduce conceptual | sección5; conciliación futura |
| consumo | Valor | Cassandra/AstraDB query-first | arquitectura; carga futura |
| PII/linaje | Veracidad | acceso/metadata | perfil; controles futuros |
| entorno limpio, O7 | todas | Quickstart + runner | cierre con corrida real |
| riesgo/esfuerzo | todas | plan y decisiones | sección7 |

## 7. Plan, roles y riesgos

Asignaciones **propuestas por rol**, sin inventar acuerdos entre integrantes. Los cinco integrantes del README deben confirmar responsable y reemplazo para arquitectura, datos, procesamiento, ingeniería y revisión/defensa.

| Trabajo de cierre | Rol propuesto | Esfuerzo | Evidencia / fecha |
|---|---|---:|---|
| diseño/matriz revisados por equipo | arquitectura + revisión | 2h | aprobación interna, 03/10 |
| contratos de datos/riesgos | datos | 2h | revisión FX/signos, 03/10 |
| ejecución cruzada Quickstart | ingeniería | 2h | log entorno limpio, 04/10 |
| defensa de flujos/MapReduce | procesamiento + defensa | 2h | checklist, 04/10 |
| confirmar hora/canal y enviar | representante a definir | 1h | envío humano, 05/10 |

Recursos: Colab o Python+Java17/PySpark3.5.3, almacenamiento para ZIP/Parquet y editor. Sin cluster ni cuenta Astra nuevos para primera entrega. 9h-persona de revisión/cierre estimadas; implementación entrega2 se planifica tras feedback.

| Riesgo | Prob./impacto cualitativos | Mitigación / dueño propuesto |
|---|---|---|
| desorden y pérdida por watermark | alta/alto | Bronze completo + reconciliar; procesamiento |
| FX, impuestos, credits ambiguos | alta/alto | conservar originales, contrato provisional; datos |
| PII expuesta | media/alto | acceso/enmascaramiento sin PII en Gold; gobierno/datos |
| CSV dialecto/schema drift | media/alto | escape, encabezado y columnas desconocidas; ingeniería |
| small files y costos | alta/medio | partición gruesa/compactar; arquitectura |
| fallo entorno/Drive/demo | media/alto | runner/config y evidencia alternativa; ingeniería |
| contabilidad idempotente no atómica | media/alto | escritor único, staging/manifest/recuperación; procesamiento |
| hora nueva no confirmada | media/alto | confirmar corte con docente; representante |

Decisiones abiertas: normalización FX (A/B/C); semántica de negativos y credits; watermark definitivo y atomicidad del destino. Se documentan alternativas sin declarar feedback nuevo. Las referencias a confirmación docente de D-08 pertenecen al aporte previo y no fueron verificadas por esta preparación.

## 8. Alcance y verificación

Primera entrega: diseño conciso, perfil, README y evidencia. Segunda: jobs batch/streaming, Silver, features, quarantine, Gold, Cassandra y dos consultas. Final: completar dominios, consultas, analítica y defensa. No se implementan aquí las etapas posteriores.

La evidencia efectiva se registra en evidence/entrega1/CIERRE.md; un objetivo propuesto no equivale a prueba aprobada. Checklist de sección9.1 cubierto por secciones1–7 y archivos de evidencia; recepción por docente, hora de corte y aprobación del equipo pendientes.
