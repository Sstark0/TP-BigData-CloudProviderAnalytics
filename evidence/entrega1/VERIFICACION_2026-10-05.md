# Verificación de correcciones E1 · 05/10/2026

Base: `e04b00b3078ec5d26041b849890cfe3807cbb570`. Esta revisión integra el diseño v1.1, la matriz y el plan, y corrige detalles de reproducibilidad. No modifica los datos originales, los esquemas, las dependencias ni las salidas históricas del 02/10.

## Correcciones

- Documentación: rutas del notebook fuente y ejecutado, carpeta de Colab y distinción entre entorno validado y alternativa no probada.
- Evidencia externa: el notebook muestra una ruta absoluta cuando la salida queda fuera del repositorio; ya no falla después de escribir el perfil. El runner respeta `EVIDENCE_DIR` para su notebook ejecutado.
- Muestra de un JSONL: el esquema explícito permite representar el conjunto vacío de micro-lotes previos. La simulación informa cero tardíos; esto no valida una política de streaming real.
- Runner: la celda adicional de versiones respeta el formato original del notebook, que es anterior a nbformat 4.5.
- La tabla dinámica de hallazgos muestra la cantidad real de archivos y el promedio de eventos por día al usar la muestra.

## Pruebas realizadas

Entorno: Python 3.11.16, Temurin Java 17.0.20.1, PySpark 3.5.3, pandas 2.3.3 y pyarrow 18.1.0; `local[2]`, UTC.

| Comprobación | Resultado |
|---|---|
| Dataset completo, 19 celdas funcionales en orden | 19/19; 43.200 eventos y 120 JSONL |
| Muestra versionada, 19 celdas funcionales en orden | 19/19; 360 eventos y un JSONL |
| Perfil en `EVIDENCE_DIR` externo | Correcto en ambas ejecuciones |
| Regresiones automatizadas | 4/4 |
| Sintaxis de fuente, scripts, pruebas y celdas | Correcta |
| Inmutabilidad de Landing | Los 127 archivos conservaron SHA-256 |
| Preservación de muestras y evidencia histórica | Sin cambios |

Las dos ejecuciones completas usaron un solo proceso Python que ejecutó las celdas fuente secuencialmente, con evaluación de la expresión final para mostrar resultados. No se presentan como ejecuciones de un kernel Jupyter. Los resultados por celda, versiones y hashes del código están en [regresiones_2026-10-05.json](regresiones_2026-10-05.json).

Las cuatro pruebas versionadas cubren exportación de perfil a carpeta externa, ruta externa del runner, un archivo sin micro-lote previo y dos archivos con conteos esperados. La prueba del runner simula `NotebookClient.execute`; comprueba la ruta y el formato del archivo de salida, no la ejecución del kernel.

Con el dataset completo se conservaron los conteos básicos, nulos/tipos, integridad de eventos y simulación temporal de la evidencia histórica. Los tardíos simulados son 42.801 con 1 h; 42.118 con 24 h; 40.683 con 72 h y 37.831 con 168 h. Los percentiles aproximados pueden variar dentro del error de rango configurado.

## Límites

El comando literal `python scripts/run_exploration.py` se intentó en este entorno, pero ZeroMQ informó `Operation not permitted` y el kernel terminó antes de responder a `kernel_info`. No llegó a ejecutar celdas. El notebook ejecutado de referencia del 02/10 se conserva como antecedente, sin sobrescribirlo con la prueba actual.

La muestra no conserva integridad referencial y no reproduce los totales del dataset. Colab, los jobs productivos batch, Structured Streaming, Cassandra/AstraDB y los objetivos de latencia o idempotencia del diseño no se validaron en esta revisión. Publicar estos archivos no constituye una entrega enviada al docente.

## Reproducir las regresiones

Con el entorno del README activado y Java disponible:

```bash
python -m unittest discover -s tests -v
```

Para el notebook completo usar el runner o abrir la fuente en Jupyter. Mantener `EVIDENCE_DIR` separado al probar la muestra para no reemplazar el perfil completo con resultados reducidos. Si se invoca un Python por ruta absoluta en lugar de activar su entorno, configurar `PYSPARK_PYTHON` con ese mismo ejecutable evita mezclar versiones entre driver y workers.
