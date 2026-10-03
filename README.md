# TP-BigData-CloudProviderAnalytics

Proyecto integrador · **Big Data (72.80) · ITBA · 2.º cuatrimestre 2026** · Prof. Diego Mosquera

Pipeline de datos para el área de datos de un proveedor de nube: ingesta batch y streaming, Data Lake
en Parquet (Landing → Bronze → Silver → Gold) con PySpark, y publicación de marts analíticos para
**FinOps, Soporte y Producto/Usage** en Cassandra/AstraDB.

## Integrantes

- Stephanie Amairany Castillo Avendaño
- Gianluca Giannine Lizarraga
- Agustín Ezequiel Pagani
- Tomas Lautaro Reymundo
- Antonio Uriel Serrano Jaimes

## Estado

| Instancia | Foco | Estado |
|---|---|---|
| Entrega 1 | Diseño y fundación de datos | Diseño v1.0 completado; aprobación del equipo pendiente |
| Entrega 2 · 16/11/2026 18:30 | Implementación técnica end-to-end mínima | ⬜ |
| Final · 07/12/2026 21:30 | MVP integrado y defensa | ⬜ |

## Estructura del repositorio

```
.
├── README.md              ← este archivo
├── DECISIONS.md           ← decisiones técnicas, alternativas y trade-offs
├── requirements.txt       ← dependencias de Python
├── config/                ← configuración de ejemplo (.env.example), sin credenciales
├── data/                  ← cómo obtener el dataset + muestra chica versionada
├── datalake/              ← zonas del lago; los datos NO se versionan
│   ├── landing/           ←   datos originales, inmutables
│   └── bronze/ silver/ gold/ quarantine/ _checkpoints/
├── docs/                  ← documento de diseño y diagramas
├── notebooks/             ← exploración (01_exploracion_fuentes.ipynb)
├── src/common/            ← código compartido: rutas, sesión Spark, esquemas
├── tests/                 ← pruebas (entrega 2)
├── infra/                 ← entorno de ejecución (Docker diferido, ver D-01)
└── evidence/              ← resultados de ejecución por entrega
```

## Entrega 1 · 05/10/2026 (hora a confirmar)

Diseño integrado: [Markdown](docs/entrega1/diseno_integrado.md) y [PDF](docs/entrega1/diseno_integrado.pdf).
Arquitectura propuesta; no hay pipeline productivo, streaming ni Cassandra implementados.
Correcciones de esta preparación y comprobaciones: [cierre](evidence/entrega1/CIERRE.md).

## Quickstart (entrega 1: exploración de datos)

### Google Colab (alternativa por validar)
Esta preparación validó el entorno local Python3.11; no ejecutó Colab. Verificar una versión de Python compatible con Spark3.5 antes de usar esta alternativa.

1. Clonar o descargar este repositorio.
2. Copiar el dataset del docente dentro de `datalake/landing/` (ver [`data/README.md`](data/README.md)).
3. Subir la carpeta completa `TP-BigData-CloudProviderAnalytics/` a la raíz de Google Drive (*Mi unidad*). Si está en otro lugar, definir `PROJECT_ROOT` en una celda antes de la preparación.
4. En Drive, abrir `notebooks/01_exploracion_fuentes.ipynb` con **Google Colaboratory**.
5. *Entorno de ejecución → Ejecutar todas*. La primera celda pide permiso para conectar Drive e instala PySpark.

### Local (entorno validado)
Requisitos: **Python 3.11** (evitar Python3.14 con Spark3.5), **Java 11 o 17** (Spark corre sobre Java; no se instala con pip) y PySpark 3.5.x.
```bash
git clone https://github.com/Sstark0/TP-BigData-CloudProviderAnalytics.git
cd TP-BigData-CloudProviderAnalytics
python3.11 -m venv .venv
source .venv/bin/activate             # Windows: .venv\Scripts\activate
pip install -r requirements.txt
# Copiar el dataset dentro de datalake/landing/ (ver data/README.md)
python -m ipykernel install --user --name python3
python scripts/run_exploration.py
# Alternativa interactiva: jupyter notebook notebooks/01_exploracion_fuentes.ipynb
```

Si el dataset está fuera del repo, exportar `LANDING_PATH=/ruta/absoluta/datalake/landing` antes de ejecutar. El ejemplo de configuración documenta variables y no carga automáticamente un archivo .env. El runner guarda además un notebook ejecutado en evidence.

**Salida esperada:** tablas de perfil en el notebook y `evidence/entrega1/perfil_fuentes.json`
(43.200 eventos, 120 archivos, 0 `event_id` duplicados).

**Limpieza / reinicio:** el notebook solo lee Landing; para regenerar la evidencia basta con volver a ejecutarlo.

## Convenciones

| Tema | Convención |
|---|---|
| Idioma | Documentación en español; código, columnas y rutas en inglés `snake_case` |
| Zonas | `landing` (inmutable) → `bronze` → `silver` → `gold`; rechazados en `quarantine` |
| Tiempo | Todo en **UTC** |
| Columnas técnicas | `ingest_ts` y `source_file` desde Bronze |
| Configuración | Variables de entorno (`config/.env.example`); nunca credenciales en el repo |
| Commits | `tipo(ámbito): mensaje` — tipos: `feat`, `docs`, `chore`, `fix` |
| Flujo de trabajo | `git pull` antes de empezar; `git add` → `git commit` → `git push` al terminar |

## Documentación

- Documento de diseño · entrega 1: [`docs/`](docs/README.md)
- Registro de decisiones: [`DECISIONS.md`](DECISIONS.md)
- Evidencia de exploración: [`evidence/entrega1/`](evidence/entrega1/README.md)
