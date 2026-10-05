# Cierre de primera entrega · 02/10/2026

Nota de archivo histórico: las referencias a secciones y páginas de este cierre corresponden al diseño v1.0 del 02/10/2026. Para navegar la edición v1.1 del 05/10, consultar el diseño integrado actual y LEER_PRIMERO_ENTREGA.txt. La evidencia de ejecución de este cierre se conserva sin cambios.

Base revisada: `2a167696d23c95594dac297daf43a53eb7839bf0`. Cambios en copia aislada; no se modificó Landing. Entrega reprogramada al **05/10**, hora a confirmar. Preparado para revisión del equipo; no enviado al docente.

## Cambios

- Diseño integrado v1.0 en Markdown/PDF: arquitectura y flujos, contrato del lago, MapReduce, matriz y plan/riesgos/roles propuestos.
- Diagrama Mermaid fuente y SVG/PNG legibles, con versión y fecha. Render vectorial equivalente preparado para PDF.
- Corrección de D-09/impuestos, tamaño CSV, objetivo O4, balance de ingesta, política streaming e idempotencia.
- Quickstart con URL y rutas correctas, PROJECT_ROOT configurable, LANDING_PATH externo y config/.env.example sin secretos.
- Spark master configurable; notebook fuente sin salidas antiguas; runner guarda una copia ejecutada con versiones.
- Aportes anteriores conservados: caso/inventario/decisiones con cambios puntuales y perfil_fuentes_previo.json histórico.

## Verificación efectivamente corrida

Notebook completo con **Python3.11.17, Temurin Java17.0.20.1, PySpark3.5.3, pandas2.3.3, pyarrow18.1.0**, usando el dataset completo externo. Exit0; ninguna salida de error. Notebook ejecutado y perfil JSON regenerados en esta carpeta. La celda inicial del notebook ejecutado registra Python/Java; el perfil registra Spark/fecha UTC.

Confirmados por lectura Spark y verificación independiente: 43.200 eventos/120 JSONL, 10.800 v1/32.400 v2, claves únicas, ocho fuentes, nulos principales, 85 recursos pii:true, 7.371 eventos anteriores al recurso y simulación temporal 42.118 atrasados con watermark24h. Los campos completos y comprobaciones independientes están en verificacion_independiente.json. La simulación no es prueba de descarte de Structured Streaming.

Verificación de sintaxis de src/scripts y celdas Python; enlaces locales; muestras idénticas a los originales; hashes de los 127 archivos de Landing contra ZIP. Registro en controles_cierre.json. PDF de cinco páginas revisado visualmente: tablas, diagrama, encabezados y saltos legibles.

Las pruebas iniciales en Python3.14 fallaron por distutils y serialización; el Quickstart exige Python3.11. Java/Python y dependencias se instalaron sólo en carpetas de trabajo externas; no se aceptó licencia Xcode ni se modificó seguridad del Mac. Spark/Jupyter necesitaron permiso de ejecución local para puertos localhost.

No se ejecutaron jobs productivos, streaming, Cassandra, cargas cloud ni benchmark. No se creó recurso pago ni cuenta nueva. Son diseños para etapas posteriores.

## Checklist 9.1 de la consigna

| Criterio | Evidencia final |
|---|---|
| documento disponible | docs/entrega1/diseno_integrado.md y .pdf |
| repositorio versionado/accesible | base SHA indicada; PR de revisión se confirma aparte |
| caso y objetivos | diseño §1; 01_caso_y_5v.md |
| 5V | diseño §2; análisis original |
| inventario y perfil | diseño §2; 02_inventario_fuentes.md; perfil y notebook |
| arquitectura y patrón | diseño §3; arquitectura.mmd/.svg/.png |
| Landing/Bronze/Silver/Gold | diseño §4 |
| batch y streaming | diseño §3 |
| MapReduce | diseño §5 |
| matriz requisito-componente | diseño §6 |
| supuestos/riesgos/esfuerzo | diseño §7; DECISIONS.md |
| evidencia mínima | notebook ejecutado, perfil y controles |

## Aclaración del diagnóstico inicial

config/.env.example ya existía en el commit base; el primer listado omitía archivos ocultos. Se corrigió esa observación y se adaptó el ejemplo conservando placeholders sin secretos.

## Confirmaciones humanas pendientes

1. Hora/canal de corte del 05/10.
2. Asignación nominal y aprobación del equipo sobre roles/estimaciones y diseño propuesto.
3. Envío por representante. No se declara entrega realizada.

FX, semántica contable y watermark definitivo son decisiones abiertas documentadas para entrega2; no bloquean un diseño explícito de primera entrega. Después del feedback, versionar plan de correcciones con responsable, fecha y evidencia esperada.
