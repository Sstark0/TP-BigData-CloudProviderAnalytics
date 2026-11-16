# Backlog final priorizado

**Versión:** 1.0 · **Fecha:** 15/11/2026 · **Entrega:** 2 · **Paquete:** E2-14 · **Próximo hito:** entrega final, lunes 07/12/2026 21:30

La consigna pide "priorizar el trabajo restante para el MVP mediante categorías obligatorio, deseable y fuera de
alcance" (§6.2). También fija una regla de **congelamiento de alcance** (§6.6): "las funcionalidades opcionales no
deben poner en riesgo la integración end-to-end, la reproducibilidad ni los requisitos mínimos de la instancia final".

**Cómo se usa este backlog.**
- Identificadores: **B-** obligatorio, **BD-** deseable, **BF-** fuera de alcance, **C-** cierre de la entrega 2.
- Cada ítem tiene una categoría, un rol responsable (roles de [`plan_trabajo.md`](plan_trabajo.md) §1), una estimación, sus dependencias y un criterio de aceptación verificable.
- Los ítems están agrupados por los paquetes F-01 a F-09 del plan de trabajo (§4).
- **Prioridad:** P1 bloquea un requisito de la final; P2 lo completa o lo mejora; P3 solo si sobra tiempo.
- **Regla de congelamiento:** desde el **viernes 04/12** solo se integran correcciones. Un ítem deseable que no esté integrado y probado ese día queda afuera.

## 0. Antes del corte del 16/11 (cierre de la entrega 2)

No son trabajo de la final, pero si no se hacen, la entrega 2 queda incompleta o inconsistente.

| ID | Ítem | Rol | Criterio de aceptación |
|---|---|---|---|
| C-2 | Actualizar el seguimiento: E2-01 a E2-14 con estado real, horas y fecha de cierre | Líder técnico | `plan_trabajo.md` §6 sin filas "Pendiente" que estén hechas (C-05) |
| C-3 | Cerrar P-01 a P-04 del plan de correcciones con su evidencia | Líder técnico | P-01 (D-04 validada), P-02 (diagrama v2), P-03 (D-12 a D-18) y P-04 (ver BD-7) en Hecho; P-05 queda abierta con fecha (B-01) |
| C-4 | Revisar E2-03 a E2-14 e integrar `entrega2` en `main` antes de las 17:00 | Todos · Líder técnico | Commit de `main` enviado al docente (E2-15) |
| C-5 | Ejecución del README por otra persona del equipo (criterio de E2-12) | Plataforma y gobierno | `e2_12_pipeline_run_<nombre>.json` en el repo. Si no llega al corte, pasa a la final como B-10 |

## 1. Obligatorio para la final

| ID | Ítem | Fuente | Paquete | Rol | Horas | Depende de | Prioridad | Criterio de aceptación |
|---|---|---|---|---|---:|---|---|---|
| B-01 | Resolver la conversión a USD y la semántica de signos de la facturación | D-08, D-09, P-05 | F-02 | Batch y calidad | 3 | — | P1 | D-08 y D-09 en Aceptada, con la opción elegida y el impacto en revenue |
| B-02 | Silver de `billing_monthly`: montos originales, `revenue_usd` y flags de FX, moneda y signo | §7.3, D-08 | F-02 | Batch y calidad | 4 | B-01 | P1 | Silver con conciliación contra Bronze y flags con conteo |
| B-03 | Gold `revenue_by_org_month` (org × mes): revenue en USD, créditos, impuestos y FX aplicado | §7.3 | F-02 | Gold y serving | 4 | B-02 | P1 | Mart con conciliación contra Silver |
| B-04 | Silver de `support_tickets` y Gold `tickets_by_org_date` (org × día × severidad): conteos, tasa de SLA breach, CSAT promedio | §7.3 | F-02 | Gold y serving | 5 | — | P1 | Mart con conciliación contra Bronze |
| B-05 | Gold `genai_tokens_by_org_date` (org × día): tokens y costo estimado | §7.3 | F-02 | Gold y serving | 3 | — | P1 | Mart que concilia tokens y costo con Silver |
| B-06 | Cassandra: tablas query-first y consultas 3 a 5 (tickets críticos y SLA en 30 días; revenue mensual en USD; tokens y costo por día) | §7.4 | F-03 | Streaming | 8 | B-03, B-04, B-05 | P1 | Para cada consulta: CQL, justificación del modelo, captura y comparación con Spark, como en E2-08 |
| B-07 | Integrar los marts nuevos a `run_pipeline.py`, `check_idempotency.py` y al catálogo | §7.2, §7.6 | F-07 | Plataforma y gobierno | 4 | B-03 a B-06 | P1 | Pipeline completo con las 5 consultas; idempotencia sin diferencias; catálogo sin columnas sin describir |
| B-08 | Pruebas de los marts y reglas nuevas | §7.2 | F-06 | Plataforma y gobierno | 4 | B-02 a B-05 | P1 | Una prueba por regla y por cálculo de cada mart |
| B-09 | Modo demo con la muestra de `data/sample/` y plan alternativo si falla AstraDB o la conexión | §8.2, §8.4 | F-07 | Plataforma y gobierno | 3 | — | P1 | Opción `--muestra` en `run_pipeline.py` con un lago aparte. Ya se probó el 15/11: la muestra corre de Bronze a Gold en unos 2,5 minutos, pero hay que hacerlo a mano y `landing_check` exige los conteos del dataset completo. Evidencia de respaldo documentada |
| B-10 | README validado desde un entorno limpio por otra persona del equipo | §7.6, §9.3 | F-07 | Plataforma y gobierno | 2 | B-07 | P1 | Registro de esa ejecución en `evidence/final/` |
| B-11 | Documentación técnica final: arquitectura final, diccionario completo, modelo de serving, calidad, gobierno, linaje, decisiones y limitaciones | §7.5 | F-05 | Plataforma y gobierno · Líder técnico | 6 | B-07 | P1 | Documentos de `docs/entrega2/` llevados a `docs/final/` con las tablas nuevas; diagrama v3 sin nada aspiracional |
| B-12 | Trazabilidad y evolución de esquema: matriz requisito → componente → evidencia actualizada y tratamiento de eventos v1/v2 documentado | §7.2 | F-05 | Líder técnico | 3 | B-11 | P1 | Matriz de E1 actualizada, con cada requisito de §7 apuntando a su código y su evidencia |
| B-13 | Normalización de regiones y servicios en Silver: evidencia de valores distintos y reglas de mapeo, si hacen falta | §4.4 | F-02 | Batch y calidad | 2 | — | P2 | Conteo de valores distintos por columna en la evidencia; mapeo documentado si aparece alguna variante |
| B-14 | Presentación ejecutiva: contexto de negocio, propuesta, arquitectura, tecnologías, proceso, resultados y consultas | §7.5 | F-09 | Líder técnico · Todos | 6 | B-06 | P1 | Presentación revisada por todo el equipo |
| B-15 | Video explicativo: recorrido por la presentación y el pipeline (ETL, streaming, serving) | §7.5 | F-09 | Todos | 4 | B-14 | P1 | Video publicado y enlazado en el README |
| B-16 | Demo y defensa: guion, reparto de temas entre los 5 integrantes y ensayo completo | §7.2, §7.5 | F-09 | Todos | 5 | B-14 | P1 | Ensayo hecho; cada integrante explica su componente |
| B-17 | Correcciones del feedback de la entrega 2 | §1, F-01 | F-01 | Líder técnico | 6 | Devolución del docente | P1 | Plan de correcciones de E2 con todas las filas cerradas |
| | **Subtotal obligatorio** | | | | **72** | | | |

## 2. Deseable

Solo si los ítems P1 de su semana están cerrados. Ninguno se integra después del 04/12.

| ID | Ítem | Fuente | Rol | Horas | Prioridad | Criterio de aceptación |
|---|---|---|---|---:|---|---|
| BD-1 | Servir `cost_anomaly_mart` en Cassandra con una consulta de anomalías por organización | D-18 | Gold y serving | 3 | P2 | Tabla, CQL y captura |
| BD-2 | Medir la frescura del streaming (objetivo O4: Silver en ≤ 5 min desde `arrival_ts`) | E1 O4, gobierno §7 | Streaming | 2 | P2 | Percentiles de latencia en la evidencia |
| BD-3 | Token de solo lectura para el consumo y expiración de tokens en AstraDB | Gobierno §7 | Gold y serving | 1 | P2 | Consultas ejecutadas con el token de lectura; procedimiento documentado |
| BD-4 | Alertas: aviso si falla una etapa o aparece una anomalía de severidad alta | Gobierno §7 | Plataforma y gobierno | 3 | P3 | Mensaje generado en una prueba con un fallo simulado |
| BD-5 | Replay didáctico ordenado por `event_ts`, comparado con el orden original | D-04 | Streaming | 3 | P3 | Tardíos con ambos órdenes en la evidencia |
| BD-6 | Política de retención de fotos de Bronze y registros de `_runs/` | Gobierno §7 | Plataforma y gobierno | 1 | P3 | Política escrita y aplicada en `--clean` |
| BD-7 | Validar Google Colab como entorno alternativo | P-04, D-01 | Plataforma y gobierno | 3 | P3 | Pipeline corrido en Colab. **Si no está validado al 30/11, se declara fuera de alcance** en D-01: WSL2 ya es el entorno equivalente validado |
| BD-8 | Docker Compose con Python, Java y PySpark | D-01 | Plataforma y gobierno | 4 | P3 | `docker compose run` ejecuta las pruebas y el pipeline |
| | **Subtotal deseable** | | | **20** | | |

## 3. Fuera de alcance

| ID | Ítem | Por qué queda afuera | Decisión |
|---|---|---|---|
| BF-a | Orquestador externo (Airflow, Prefect) | Nueve etapas lineales: `run_pipeline.py` alcanza y agregaría un servicio más | D-19 |
| BF-b | Formato transaccional con `MERGE` (Delta Lake, Iceberg) | Con este volumen, recalcular desde Bronze es simple e idempotente; se menciona como evolución en las limitaciones | D-04, D-13 |
| BF-c | Historia de maestros (SCD tipo 2) | Hay una sola foto por maestro; la historia queda en Bronze | D-14 |
| BF-d | Modelos de ML entrenados (Isolation Forest, pronóstico) | Sin etiquetas no se pueden validar mejor que el método estadístico actual, que es explicable y está evaluado | D-18 |
| BF-e | Catálogo externo (DataHub, OpenMetadata) | Un servicio más para menos de 30 tablas; el catálogo como código cubre la necesidad | D-21 |
| BF-f | Clúster distribuido (YARN, HDFS) | Ningún requisito lo pide; se usa como marco conceptual | D-01 |
| BF-g | Enmascarar `email` en Bronze | Romper el "dato tal como llegó"; el riesgo se controla porque ningún proceso lo propaga | D-21 |

## 4. Re-estimación de la entrega final

Parte de lo que el plan asignaba a la final ya se hizo en la entrega 2:
- el comando único (`run_pipeline.py`);
- el diccionario preliminar y el linaje verificable;
- la analítica de anomalías, con métricas y limitaciones.

| Paquete | Plan (§4) | Hecho en E2 | Restante | Ítems |
|---|---:|---|---:|---|
| F-01 Correcciones del feedback de E2 | 6 | — | 6 | B-17 |
| F-02 Marts restantes | 14 | `cost_anomaly_mart` (E2-09) | 21 | B-01 a B-05, B-13 |
| F-03 Serving de Q3 a Q5 | 8 | Modelo y carga reutilizables (E2-08) | 8 | B-06 |
| F-04 Analítica o ML | 10 | Anomalías con método, umbrales y evaluación (E2-09) | 0 | — (mejoras en BD-1) |
| F-05 Gobierno y documentación | 8 | Catálogo, linaje, clasificación (E2-11) | 9 | B-11, B-12 |
| F-06 Pruebas ampliadas | 8 | 75 pruebas | 4 | B-08 |
| F-07 Un solo comando | 6 | `run_pipeline.py` (E2-12) | 9 | B-07, B-09, B-10 |
| F-08 Integración continua | 8 | Integración por ramas | 0 | Se absorbe en cada ítem |
| F-09 Presentación, video y defensa | 15 | — | 15 | B-14 a B-16 |
| **Total** | **83** | | **72 + 20 deseable** | |

F-02 crece de 14 a 21 horas. Se sumaron la resolución de FX y signos (B-01) y la normalización de regiones y
servicios (B-13), que en el plan no estaban estimadas. Con un 20 % de contingencia, lo obligatorio suma unas 86
horas para 5 personas en 3 semanas: unas **5,8 horas por persona por semana**.

## 5. Calendario

| Semana | Foco | Ítems |
|---|---|---|
| 16/11 al 22/11 | Cierre de E2, decisiones de facturación y marts | C-1 a C-5, B-01 a B-05, B-13 |
| 23/11 al 29/11 | Serving de las consultas 3 a 5, integración y pruebas | B-06, B-07, B-08, B-09, B-17 (según la fecha de la devolución) |
| 30/11 al 04/12 | Validación, documentación final y material de defensa | B-10, B-11, B-12, B-14, B-15; deseables solo si P1 está cerrado |
| **04/12** | **Congelamiento de alcance:** desde acá, solo correcciones | — |
| 05/12 al 07/12 | Ensayo, ajustes y entrega (lunes 07/12, 21:30) | B-16 |

## 6. Riesgos para la final

| Riesgo | Mitigación |
|---|---|
| La facturación sigue sin una regla clara de FX y signos | B-01 primero: la decisión se toma con la evidencia de D-08 y queda registrada; los datos originales se conservan, así que cambiarla después es una sola expresión |
| AstraDB hiberna o falla durante la demo | Resume el día anterior; plan alternativo con capturas y salidas guardadas (B-09) |
| La integración de los marts nuevos rompe la idempotencia | B-07 corre `check_idempotency.py` antes de integrar cada mart |
| Poca participación de algún integrante en la defensa | B-16 reparte los temas por componente y exige un ensayo completo |
