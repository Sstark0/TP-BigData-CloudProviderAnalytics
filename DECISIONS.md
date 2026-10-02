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
