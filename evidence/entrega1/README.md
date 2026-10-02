# Evidencia · Entrega 1 · Exploración y perfil de datos

| Archivo | Qué contiene |
|---|---|
| `../../notebooks/01_exploracion_fuentes.ipynb` | Notebook PySpark **ejecutado** (con salidas). Lee las 8 fuentes de Landing con esquema explícito. |
| `perfil_fuentes.json` | Todas las métricas del perfil, generadas por el notebook (regenerable). |

Ejecución de referencia: PySpark 3.5.3 · `local[*]` · 43.200 eventos + 7 CSV · ~2–3 min en Colab.

## Hallazgos principales y su impacto en el diseño

La tabla "hallazgo → evidencia → impacto" y la de implicancias para la arquitectura están al final del notebook.
Los chequeos sobre maestros se clasifican en **Calidad** (validez, completitud, consistencia), **Negocio** (condiciones válidas que se analizan) y **Gobierno** (PII).

### Volumen y forma de los datos
- **Landing total ≈ 13 MB**: 7 CSV (307.906 bytes ≈ 301 KiB) + 120 archivos JSONL de eventos (12.624 KiB ≈ 12,3 MiB; 1 KiB = 1.024 bytes).
- **43.200 eventos** · 60 días (2025-07-03 → 2025-08-31) · **exactamente 720 eventos/día** · 80 organizaciones · 400 recursos · 6 servicios · 7 regiones · 3 métricas.
- Archivos de eventos de **≈ 105 KB** cada uno: un bloque HDFS de 128 MB es ~1.250 veces más grande → caso típico de *small files*. **Impacto:** compactar al promover a Bronze/Silver y particionar grueso (por fecha, no por fecha × servicio × región).

### Evolución de esquema (v1 → v2)
- Corte limpio: **v1 = 10.800 eventos (03/07 – 17/07)**, **v2 = 32.400 eventos (desde 18/07 00:01 UTC)**.
- `carbon_kg` presente en el 100 % de v2 y 0 % de v1. `genai_tokens` solo en v2 **y solo** en `service = genai` (3.132 eventos).
- **Impacto:** un único esquema *superset* para leer ambas versiones; en Silver, `carbon_kg`/`genai_tokens` = NULL (no 0) para v1, porque "no medido" ≠ "cero".

### Calidad de los eventos
| Condición | Casos | % |
|---|---:|---:|
| `event_id` duplicado | 0 | 0 |
| Registros JSON corruptos / timestamp no parseable | 0 | 0 |
| `value` nulo | 877 | 2,0 |
| `value` llegó como **texto** (todos casteables) | 1.309 | 3,0 |
| `unit` nulo | 2.075 | 4,8 |
| `cost_usd_increment` < 0 | 216 | 0,5 |
| `cost_usd_increment` < −0,01 (fuera de tolerancia) | 211 | 0,5 |
| Costo > 10 × p99 (spike) | 34 | 0,08 |

- La relación `metric → unit` es **fija** (cpu_hours→hours, requests→count, storage_gb_hours→gb_hours) → el `unit` nulo es **imputable**.
- Costos: p50 = 1,00 · p99 = 16,69 · máx = 317,43 · mín = −154,46 USD. Distribución muy asimétrica → para anomalías conviene **MAD o percentiles por servicio** más que z-score global.
- **14 colisiones de clave contextual:** eventos que comparten (org, recurso, minuto, métrica) con **valores distintos**. No son duplicados → esa combinación no es clave de unicidad y la deduplicación se hace por `event_id`.
- Integridad: 0 huérfanos (todo `org_id` y `resource_id` existe en su maestro; org/servicio/región coinciden con el recurso). Pero **7.371 eventos (17 %) son anteriores a la `created_at` del recurso** → inconsistencia temporal: se propone **marcarlos como anomalía** y definir en Silver una política explícita (no eliminarlos).

### Orden temporal (hallazgo crítico para streaming)
- **Cada archivo cubre ~59,7 de los 60 días.** Los archivos no representan avance del tiempo: son muestras mezcladas del período completo.
- **Simulación** (notebook, celda del watermark). Supuesto: los archivos se procesan de a uno por micro-lote (`maxFilesPerTrigger = 1`) en orden de fecha de modificación, que en este dataset coincide con el orden de nombre. Con la configuración por defecto, el primer micro-lote tomaría los 120 archivos juntos y no habría late data (el watermark se recalcula al final de cada micro-lote). Bajo el supuesto de un archivo por micro-lote, quedarían atrasados respecto del máximo `event_time` observado:

| Watermark | Eventos atrasados |
|---|---:|
| 1 h | 42.801 (99,1 %) |
| 24 h | 42.118 (97,5 %) |
| 72 h | 40.683 (94,2 %) |
| 7 días | 37.831 (87,6 %) |

- Es una simulación sobre el dataset provisto; el comportamiento real se validará en la entrega 2 con Structured Streaming, checkpoint y watermark.

- **Impacto:** el watermark no puede usarse para agregaciones con estado sobre este replay sin perder casi todo. Decisión abierta para la arquitectura (ver `DECISIONS.md`, D-04).

### Maestros y fuentes batch
| Fuente | Filas | Clave | Nulos relevantes | Otros problemas |
|---|---:|---|---|---|
| customers_orgs | 80 | org_id | nps_score 13,8 % | NPS en escala −38…101 (1 fuera de [−100,100]) · 25 (31 %) con `is_enterprise` inconsistente con `plan_tier` |
| users | 800 | user_id | last_login 17,4 % | **29 %** con last_login < created_at · `email` = PII |
| resources | 400 | resource_id | tags_json 20,8 % | 21 % (85) con tag `pii:true` |
| support_tickets | 1.000 | ticket_id | resolved_at 24 % (abiertos) · csat 25,4 % | 40 csat fuera de [1,5] · 56 críticos · 95 SLA breach |
| marketing_touches | 1.500 | touch_id | — | 96 conversiones sin click |
| nps_surveys | 92 | org_id+survey_date | nps_score 20,7 % | 72 % fuera de [0,10] → escala ambigua |
| billing_monthly | 240 | invoice_id | credits 57 % | 3 monedas (USD/ARS/EUR) · **USD con FX ≠ 1** (0,85–1,12) · 13 subtotales negativos |

- Ninguna fuente tiene duplicados sobre su clave natural ni `org_id` huérfanos; `billing_monthly` cumple el grano 1 factura por (org, mes) y `users.email` es único (notebook, celda de integridad referencial de maestros).
- **Dialecto CSV:** los archivos usan comillas duplicadas (`""`) como escape (RFC 4180). Sin `option("escape", '"')`, Spark cortaba `tags_json` en la primera coma interna (se contaban 45 recursos `pii:true` en lugar de 85). Corregido en el lector.
- `billing_monthly` = 80 orgs × 3 meses (jun, jul, ago 2025). Los eventos cubren solo jul–ago → **junio no se puede conciliar** contra uso.
- **Impuestos ≈ 21 % de abs(subtotal)** en las 240 facturas, con redondeo a centavos. En las 13 con subtotal negativo el impuesto es positivo; no se confirma semántica de nota de crédito (D-09).

### Facturación y moneda (notebook, celdas de normalización a USD)
| Criterio para llevar a USD | Revenue jun–ago |
|---|---:|
| A · aplicar `exchange_rate_to_usd` tal cual | 164.185 |
| B · igual, pero USD forzado a 1 | 164.294 |
| C · no aplicar la tasa (montos ya en escala USD) | 211.294 |

- Los subtotales en ARS tienen la misma magnitud que los de USD (mediana 816 vs. 655). Con la tasa (~0,0015), las facturas en ARS pasan de 50.293 a **76 USD**.
- **50 de 80** organizaciones cambian de moneda entre meses.
- Subtotal / costo de uso medido (mediana, jul–ago): 0,73–1,08 en las tres monedas → los montos parecen estar ya en escala USD.
- **Impacto:** la elección cambia un 22 % el revenue de la consulta obligatoria n.º 4 → decisión abierta **D-08**. El docente confirmó que la inconsistencia es intencional y que en esta entrega corresponde documentar el problema y las posibles soluciones.