# Plan de trabajo del proyecto

> Versión 2.0 · Reemplaza al plan inicial de la entrega 1 (`docs/entrega1/plan_inicial.md`, v1.1).


## 1. Asignación nominal

Cada rol tiene una persona responsable y cada paquete tiene un revisor de otro rol, para que ningún componente
dependa de una sola persona.

| Rol | Responsable | Se ocupa de | Revisa a |
|---|---|---|---|
| Líder técnico | **Antonio Uriel Serrano Jaimes** | Arquitectura, integración, revisión, documentación, entrega | Gold y serving |
| Streaming | **Agustín Ezequiel Pagani** | Eventos: Landing → Bronze → Silver, watermark, checkpoints, late data | Batch y calidad |
| Batch y calidad | **Stephanie Amairany Castillo Avendaño** | Maestros y facturación a Bronze y Silver; reglas de calidad y quarantine; analítica | Plataforma y gobierno |
| Gold y serving | **Tomas Lautaro Reymundo** | Mart FinOps, AstraDB, consultas CQL | Streaming |
| Plataforma y gobierno | **Gianluca Giannine Lizarraga** | Base común, configuración, logs, pruebas, idempotencia, gobierno, README | Líder técnico |


## 2. Decisiones organizativas

| Tema | Decisión |
|---|---|
| Ramas | `main` es lo que se evalúa. El trabajo va en la rama `entrega2`; cada paquete, en una rama corta `e2/<paquete>` que se integra a `entrega2` |
| Revisión | Cada paquete lo revisa el revisor asignado (§1) antes de integrarse |
| Corte | `entrega2` se integra a `main` el **16/11 antes de las 17:00**. Lo que no esté integrado a esa hora pasa al backlog |
| Cambios posteriores al corte | Commits con prefijo `fix(e2-feedback)`, como pide la regla de corte de la consigna |
| Representante de la entrega | **Antonio Uriel Serrano Jaimes**: verifica el commit entregado, el canal y guarda la constancia |
| Canal y formato | Correo al docente con enlace al repositorio |
| Seguimiento | Revisión corta diaria hasta el 16/11 y semanal hasta el 07/12; se actualiza la tabla de §6 |
| Credenciales | Token de AstraDB solo en `.env` local, nunca en el repositorio, que es público |

## 3. Paquetes de la entrega 2

Alcance: el mínimo de la consigna 6.2. Las horas son la estimación por paquete; el **criterio de cierre** es lo que
tiene que ser verdadero para darlo por terminado.

| ID | Paquete | Rol | Horas | Depende de | Criterio de cierre | Evidencia | Fecha |
|---|---|---|---:|---|---|---|---|
| E2-01 | Base común: rutas por zona, registro de ejecuciones, lector CSV | Plataforma y gobierno | 5 | — | Los demás paquetes importan rutas y lector desde `src/common`; cada ejecución deja un JSON con conteos | `src/common/`, un registro de ejecución de ejemplo | 13/11 |
| E2-02 | Batch a Bronze: al menos tres maestros | Batch y calidad | 5 | E2-01 | `customers_orgs`, `users` y `billing_monthly` en Parquet particionado, con tipos explícitos, `ingest_ts` y `source_file`; filas Landing = Bronze + quarantine | Conteos y rutas en `evidence/entrega2/` | 14/11 |
| E2-03 | Streaming a Bronze | Streaming | 7 | E2-01 | Structured Streaming lee los 120 JSONL con esquema explícito y checkpoint; 43.200 eventos contabilizados entre Bronze y quarantine | Log de la consulta y conteos | 14/11 |
| E2-04 | Silver de eventos: watermark, deduplicación, late data | Streaming | 9 | E2-03 | `event_id` únicos en Silver; los eventos fuera del watermark se recuperan; Bronze = Silver + quarantine + duplicados | Conteos por camino (a tiempo y tardíos) | 15/11 |
| E2-05 | Silver de un maestro, join de enriquecimiento y tres features | Batch y calidad | 5 | E2-02, E2-04 | Al menos un maestro conformado; eventos enriquecidos; `daily_cost_usd`, `requests` y `genai_tokens` o `carbon_kg` calculadas | Muestra de filas y esquema | 15/11 |
| E2-06 | Calidad: tres reglas y quarantine | Batch y calidad | 5 | E2-03, E2-04 | Activas: `event_id` no nulo y único, costo ≥ −0,01 con flag, `unit` no nula si hay `value`; registro de calidad con acción y motivo por registro | Conteo por regla y muestras de quarantine | 15/11 |
| E2-07 | Gold: `org_daily_usage_by_service` | Gold y serving | 5 | E2-05 | Mart con grano org × día × servicio; Σ costo Gold = Σ costo Silver | Conciliación de sumas | 15/11 |
| E2-08 | Serving en AstraDB | Gold y serving | 8 | E2-07 | Keyspace y tabla *query-first* creados; mart cargado desde Spark; dos consultas CQL devuelven resultados | Scripts CQL y capturas | 16/11 |
| E2-09 | Analítica: anomalías de costo, o alternativa acordada con su plan de cierre | Batch y calidad | 5 | E2-07 | Score o flag por org × día × servicio con método y umbral documentados | Distribución de flags | 16/11 |
| E2-10 | Idempotencia | Plataforma y gobierno | 3 | E2-07 | Re-ejecutar el pipeline no cambia los conteos de Silver, Gold ni Cassandra | Conteos antes y después | 16/11 |
| E2-11 | Gobierno preliminar | Plataforma y gobierno | 3 | E2-06 | Documento con controles, metadatos, linaje, responsables y acceso | `docs/entrega2/` | 16/11 |
| E2-12 | Pruebas, logs y Quickstart | Plataforma y gobierno | 6 | E2-07 | Las pruebas pasan; otra persona del equipo ejecuta el pipeline desde un entorno limpio siguiendo el README | Salida de pruebas y registro de esa ejecución | 16/11 |
| E2-13 | Diagrama actualizado y decisiones (particiones, claves Cassandra, umbrales) | Líder técnico | 4 | E2-08 | El diagrama no muestra nada que no esté implementado; decisiones registradas | Diagrama v2, `DECISIONS.md` | 16/11 |
| E2-14 | Backlog final priorizado | Líder técnico | 2 | — | Lo que falta, clasificado en obligatorio, deseable y fuera de alcance | `docs/entrega2/backlog.md` | 16/11 |
| E2-15 | Integración, revisión y corte | Líder técnico | 4 | Todos | `entrega2` integrada a `main` antes de las 17:00 del 16/11 y commit registrado | Enlace al commit | 16/11 |
| | **Subtotal entrega 2** | | **76** | | | | |

## 4. Paquetes de la entrega final (17/11 → 07/12)

| ID | Paquete | Rol | Horas | Criterio de cierre | Evidencia | Fecha |
|---|---|---|---:|---|---|---|
| F-01 | Correcciones del feedback de la entrega 2 | Líder técnico | 6 | Plan de correcciones de E2 con todas las filas cerradas | `docs/final/` | 23/11 |
| F-02 | Marts restantes: tickets, revenue, GenAI y anomalías | Gold y serving | 14 | Los cinco marts de la consigna 7.3 en Gold con su conciliación | Conteos y sumas | 27/11 |
| F-03 | Serving de Q3 a Q5 | Streaming | 8 | Las cinco consultas obligatorias responden desde AstraDB | CQL y capturas | 30/11 |
| F-04 | Analítica o ML completa, con métricas | Batch y calidad | 10 | Componente integrado al pipeline y evaluado | Métricas | 30/11 |
| F-05 | Gobierno: diccionario de datos, linaje y PII | Plataforma y gobierno | 8 | Diccionario de todas las tablas; sin PII en Gold ni en serving | `docs/final/` | 30/11 |
| F-06 | Pruebas ampliadas y evidencias de rendimiento | Plataforma y gobierno | 8 | Pruebas de transformaciones y reglas; tamaños y tiempos registrados | Salida de pruebas | 02/12 |
| F-07 | Ejecución de punta a punta con un solo comando | Plataforma y gobierno | 6 | Entorno limpio → Cassandra poblada siguiendo el README | Registro de la ejecución | 02/12 |
| F-08 | Integración y revisión continua | Líder técnico | 8 | `main` siempre ejecutable | Historial de commits | 07/12 |
| F-09 | Presentación, video y defensa | Todos (3 h cada uno) | 15 | Ensayo completo hecho; todos pueden explicar su componente | Material de la defensa | 05/12 |
| | **Subtotal final** | | **83** | | | |

## 5. Esfuerzo total y capacidad

| Tramo | Horas estimadas | Con 20 % de contingencia | Horas por persona |
|---|---:|---:|---|
| Entrega 2 | 76 | 91 | ≈ 18 h |
| Final | 83 | 100 | ≈ 20 h |
| **Proyecto restante** | **159** | **191** | **≈ 38 h** |



## 6. Seguimiento

Se actualiza en cada revisión. Estado: Pendiente · En curso · En revisión · Hecho.

| ID | Responsable | Estado | Fecha de cierre |
|---|---|---|---|
| E2-01 | Gianluca | Hecho | 14/11 |
| E2-02 | Stephanie | Hecho | 14/11 |
| E2-03 | Agustín | Hecho | 14/11 |
| E2-04 | Agustín | Hecho | 14/11 |
| E2-05 | Stephanie | Hecho | 14/11 |
| E2-06 | Stephanie | Hecho | 14/11 |
| E2-07 | Tomas | Hecho | 14/11 |
| E2-08 | Tomas | Hecho | 15/11 |
| E2-09 | Stephanie | Hecho | 15/11 |
| E2-10 | Gianluca | Hecho | 15/11 |
| E2-11 | Gianluca | Hecho | 15/11 |
| E2-12 | Gianluca | Hecho | 15/11 |
| E2-13 | Antonio | Hecho | 15/11 |
| E2-14 | Antonio | Hecho | 15/11 |
| E2-15 | Antonio | En curso | |

## 7. Riesgos de la ejecución

| Riesgo | Probabilidad / impacto | Mitigación | Responsable |
|---|---|---|---|
| No conectar Spark con AstraDB a tiempo | Media / alto | Crear la base y probar la conexión **el 13/11**, antes de tener Gold; alternativa: carga con el driver de Python | Gold y serving |
| Tiempo insuficiente para el alcance de E2 | Alta / alto | Orden por dependencias; recorte definido en §5; corte a las 17:00 | Líder técnico |
| Credenciales expuestas en un repo público | Media / alto | `.env` ignorado; revisión antes de integrar | Plataforma y gobierno |
| Integración de último momento | Media / alto | Ramas chicas integradas a `entrega2` desde el primer día; nada se integra sin revisor | Líder técnico |
| Streaming inestable en el entorno de ejecución | Media / medio | `Trigger.AvailableNow` para las demostraciones; checkpoints persistentes | Streaming |
