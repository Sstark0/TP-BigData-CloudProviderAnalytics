# Registro de decisiones técnicas

Formato: cada decisión tiene **contexto → decisión → alternativas descartadas → consecuencias**.
Estado: `Aceptada` · `Propuesta` (a validar en el feedback del 28/09) · `Abierta` (sin resolver todavía).

| ID | Decisión | Estado | Fecha |
|---|---|---|---|
| D-01 | Entorno de ejecución: PySpark 3.5 en Colab / local; Docker diferido | Aceptada | 2026-09-25 |
| D-02 | Landing inmutable; lectura con esquema explícito "defensivo" | Aceptada | 2026-09-25 |
| D-03 | Patrón arquitectónico híbrido | Propuesta | 2026-09-25 |
| D-04 | Tratamiento del desorden temporal de los eventos (watermark) | Abierta | 2026-09-25 |
| D-05 | Numéricos ambiguos leídos como texto y casteados con fallback | Aceptada | 2026-09-25 |
| D-06 | Deduplicación de eventos por `event_id` | Aceptada | 2026-09-25 |
| D-07 | Configuración de Spark para dataset chico | Aceptada | 2026-09-25 |
| D-08 | Normalización del revenue a USD (moneda y tasa de cambio) | Abierta · inconsistencia intencional (docente) | 2026-09-28 |
| D-09 | Subtotales negativos = notas de crédito; `credits` nulo = 0 (supuesto) | Propuesta | 2026-09-28 |
| D-10 | Dialecto CSV: `escape = '"'` y validación de encabezados | Aceptada | 2026-09-30 |

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
- **Decisión propuesta.** Híbrido: **eventos con un único camino Structured Streaming** (reprocesamiento por re-stream desde Landing, estilo Kappa) + **batch para maestros y facturación**.
- **Descartado.** *Lambda clásica* (batch layer + speed layer sobre los mismos eventos): duplica la lógica sin ganancia, porque Landing ya permite recalcular. *Kappa pura*: tratar CSV mensuales como streams es artificial.
- **Detalle.** Se desarrolla en el paso 4 (arquitectura v1).

## D-04 · Desorden temporal de los eventos (abierta)
- **Contexto.** Cada archivo JSONL cubre ~59,7 de los 60 días. Simulando un archivo por micro-lote (`maxFilesPerTrigger = 1`, en orden de fecha de modificación), un watermark de 24 h dejaría atrasado el 97,5 % de los eventos respecto del máximo `event_time` observado. Con la configuración por defecto (todos los archivos en el primer micro-lote) no habría late data, pero tampoco una simulación real de streaming. Evidencia: notebook, celda del watermark; es una simulación a validar en la entrega 2.
- **Opciones.** (a) Streaming solo *stateless* hacia Bronze (dedupe por `event_id` con watermark amplio o `foreachBatch` + merge) y agregados diarios en batch sobre Silver. (b) Watermark amplio (≥ 60 días) aceptando estado grande. (c) Documentar que en producción los eventos llegarían casi ordenados y usar un watermark operativo.
- **A decidir** en el paso 4, con la opción (a) como favorita.

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

## D-09 · Subtotales negativos y créditos nulos (propuesta)
- **Contexto.** 13 facturas (5,4 %) tienen subtotal negativo y 137 (57,1 %) tienen `credits` nulo.
- **Evidencia.** `taxes / subtotal` = 0,21 en las 240 facturas; en las 13 negativas el impuesto también es negativo (−21 %), como en una nota de crédito que revierte monto e IVA.
- **Decisión propuesta.** (a) Los subtotales negativos son **notas de crédito válidas**: se conservan y restan del revenue (no van a quarantine). (b) **Supuesto pendiente de validación:** `credits` nulo se normaliza a 0 ("sin créditos"). El dataset no permite distinguir entre "no hubo crédito", "dato faltante" o "no informado"; se registra en supuestos y riesgos y se marca con `flag_credits_imputado`.
- **Descartado.** Mandar los negativos a quarantine: subestimaría las devoluciones y sobrestimaría el revenue.

## D-10 · Dialecto CSV
- **Contexto.** Los CSV siguen RFC 4180: los campos con comas van entre comillas y las comillas internas se duplican (`""`). Spark usa por defecto la barra invertida como escape: con esa configuración `tags_json` se cortaba en la primera coma interna (45 recursos con `pii:true` detectados en lugar de 85).
- **Decisión.** Leer todos los CSV con `option("escape", '"')`. En Bronze, además, `enforceSchema = false` para que Spark valide que el encabezado coincida con el esquema explícito (con esquema explícito las columnas se asignan por posición).
- **Consecuencias.** Se corrigieron los conteos de PII en el perfil. Queda como regla del lector común de CSV.
