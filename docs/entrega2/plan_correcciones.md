# Plan de correcciones posterior a la entrega 1

> Los cambios hechos después del corte de la entrega 1 se identifican en los commits con el prefijo `fix(e1-feedback)`.

## 1. Feedback recibido

| Dimensión | Peso | Calificación | Observación del docente |
|---|---:|---:|---|
| Comprensión y justificación | 15 % | 10,0 | Objetivos O1–O7 medibles; 5V concreto y honesto |
| Arquitectura | 25 % | 9,5 | Buena justificación del patrón híbrido y de la convergencia batch/streaming |
| Data Lake | 20 % | 9,5 | Diseño sólido: particiones, metadata de ejecución, calidad y promoción |
| Datos | 15 % | 9,5 | Perfilado con conteos concretos, evolución de esquema y problemas temporales |
| Procesamiento batch | 10 % | 10,0 | Buena correspondencia MapReduce–Spark |
| **Ingeniería y documentación** | 10 % | **8,5** | Las 9 h estimadas cubren solo el cierre de E1, no el esfuerzo futuro; los roles son propuestos y no nominales |
| **Defensa / feedback** | 5 % | **8,0** | Varias decisiones organizativas quedan para una revisión posterior |

## 2. Correcciones derivadas del feedback

| ID | Observación | Corrección | Prioridad | Responsable | Fecha objetivo | Evidencia esperada | Estado |
|---|---|---|---|---|---|---|---|
| C-01 | Roles propuestos, no nominales | Asignar una persona a cada rol y un revisor a cada paquete | Alta | Líder técnico | 13/11 | Tabla de asignación en [`plan_trabajo.md`](plan_trabajo.md) §1, con nombres | Hecho |
| C-02 | El esfuerzo cubre solo el cierre de E1 | Estimar por paquete de trabajo todo lo que falta: entrega 2 y final, con contingencia y capacidad del equipo | Alta | Líder técnico | 13/11 | [`plan_trabajo.md`](plan_trabajo.md) §3 a §5 | Hecho |
| C-03 | Faltan criterios de cierre | Definir para cada paquete un criterio verificable y la evidencia que lo demuestra | Alta | Líder técnico | 13/11 | Columnas "Criterio de cierre" y "Evidencia" de [`plan_trabajo.md`](plan_trabajo.md) §3 y §4 | Hecho |
| C-04 | Decisiones organizativas pendientes | Cerrar: flujo de ramas y revisión, representante de la entrega, canal, y regla para cambios posteriores al corte | Alta | Todo el equipo | 13/11 | [`plan_trabajo.md`](plan_trabajo.md) §2, sin ítems "a definir" | Pendiente |
| C-05 | Gestión de la ejecución | Registrar el avance real de cada paquete (estado, horas, fecha de cierre) | Media | Líder técnico | 16/11 y cada semana | Tabla de seguimiento de [`plan_trabajo.md`](plan_trabajo.md) §6 actualizada | Pendiente |

## 3. Compromisos derivados de Entrega 1

Lo siguiente no proviene de directamente del feedback recibido, no obstante, responde a aquellos puntos que, de manera explícita, se marcaron en el diseño inicial como "a validar en Entrega 2"

| ID | Compromiso | Origen | Responsable | Fecha objetivo | Evidencia esperada | Estado |
|---|---|---|---|---|---|---|
| P-01 | Validar watermark, checkpoint, deduplicación, late data y recuperación con Structured Streaming real | D-04 | Streaming | 15/11 | Ejecución con conteos en `evidence/entrega2/` y D-04 en estado Aceptada | Pendiente |
| P-02 | Actualizar el diagrama para que refleje lo implementado | Consigna 6.2 | Líder técnico | 15/11 | Diagrama v2 con versión y fecha | Pendiente |
| P-03 | Registrar decisiones de particiones, claves de Cassandra y umbrales de anomalías | Consigna 6.3 | Gold y serving | 15/11 | Entradas nuevas en `DECISIONS.md` | Pendiente |
| P-04 | Validar la alternativa Colab o declararla fuera de alcance | README de E1 | Plataforma y gobierno | 15/11 | README actualizado | Pendiente |
| P-05 | Resolver o mantener abierta con fecha la normalización a USD y la semántica de signos | D-08, D-09 | Batch y calidad | Antes del mart de revenue (final) | `DECISIONS.md` | Abierta |


La verificación de las correcciones incorporadas no se limita a la tabla presente en el documento, para cada entrada en §2, la evidencia existe en la ruta indicada.
