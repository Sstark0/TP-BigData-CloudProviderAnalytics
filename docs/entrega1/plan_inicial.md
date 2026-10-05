# Plan inicial de primera entrega

Versión 1.1 · 05/10/2026. Roles y estimaciones propuestos, sin asignaciones nominales.

## 8 Plan inicial y riesgos

Los roles son propuestos; la asignación nominal se definirá en la revisión del equipo. La estimación corresponde al cierre de E1; el esfuerzo de implementación se ajustará con el feedback y el backlog de E2.

| Trabajo | Rol propuesto | Esfuerzo | Resultado |
| --- | --- | --- | --- |
| Revisar diseño, matriz y diagrama | Arquitectura y revisión | 2 h | Documento consistente |
| Cerrar contratos y documentar dudas | Datos | 2 h | Decisiones FX, signos y escalas |
| Reproducir exploración y conservar evidencia | Ingeniería | 2 h | Notebook, log y perfil |
| Preparar explicación de flujos y MapReduce | Procesamiento y defensa | 2 h | Recorrido técnico de E1 |
| Verificar corte, acceso y envío | Representante a definir | 1 h | Constancia de recepción |

Total preliminar: 9 horas-persona de revisión y cierre. Recursos: Python 3.11, Java 17, PySpark 3.5.3, editor y almacenamiento local para ZIP/Parquet. Colab es alternativa pendiente de validar. E1 no requiere cluster ni crear una cuenta de AstraDB.

| Riesgo | Probabilidad / impacto | Mitigación | Rol propuesto |
| --- | --- | --- | --- |
| Late data | Alta / alto | Bronze completo y reconciliación histórica | Procesamiento |
| FX, créditos e impuestos | Alta / alto | Conservar originales; comparar alternativas | Datos |
| PII en consumo | Media / alto | Acceso mínimo; excluir PII de marts | Datos y gobierno |
| CSV y schema drift | Media / alto | Escape, encabezados y campos desconocidos | Ingeniería |
| Small files | Alta / medio | Partición gruesa y compactación | Arquitectura |
| Atomicidad Parquet | Media / alto | Escritor único, staging y recuperación | Procesamiento |
| Entorno o demo | Media / alto | Entorno validado, logs y notebook con salidas | Ingeniería |

Próximos pasos: incorporar feedback con prioridad, responsable, fecha objetivo y evidencia; implementar batch de al menos tres maestros y streaming; conformar Silver, tres reglas y tres features; publicar el mart FinOps y dos consultas en Cassandra/AstraDB. Watermark, atomicidad y semántica contable requieren validación en E2.
