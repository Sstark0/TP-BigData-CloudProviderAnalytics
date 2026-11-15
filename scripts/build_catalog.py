"""Catálogo de datos (E2-11): genera docs/entrega2/catalogo_datos.md a partir del lago real.

Cruza los metadatos de src/common/catalog.py (grano, clave, origen, responsable, clasificación, descripción de
cada columna) con lo que hay en disco (esquema, filas, archivos, tamaño, partición). Si una columna real no tiene
descripción, aparece marcada y el script termina con código 1: el diccionario no puede quedar viejo sin avisar.

Uso (desde la raíz del repositorio, después de python scripts/run_pipeline.py):
    python scripts/build_catalog.py
Salidas: docs/entrega2/catalogo_datos.md y evidence/entrega2/e2_11_catalogo.json.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.catalog import COLUMN_FLAGS, ROLES, TABLES, describe, is_technical
from src.common.config import DATALAKE_ROOT, EVIDENCE_DIR, REPO_ROOT
from src.common.runlog import utc_now
from src.common.spark import get_spark

OUT = REPO_ROOT / "docs" / "entrega2" / "catalogo_datos.md"


def physical(spark, name: str) -> dict:
    path = DATALAKE_ROOT / name
    files = list(path.rglob("*.parquet"))
    df = spark.read.parquet(str(path))
    partitions = []                                  # en el orden de las carpetas, no alfabético
    for f in files:
        for part in f.relative_to(path).parts[:-1]:
            name = part.split("=")[0]
            if "=" in part and name not in partitions:
                partitions.append(name)
    return {"rows": df.count(), "files": len(files), "bytes": sum(f.stat().st_size for f in files),
            "partition": " / ".join(partitions) or "—",
            "columns": [(f.name, f.dataType.simpleString()) for f in df.schema]}


def size(n: int) -> str:
    return f"{n / 1_048_576:.1f} MB" if n >= 1_048_576 else f"{n / 1024:.0f} KB"


def render(entries: list, generated: str) -> str:
    lines = [
        "# Catálogo de datos (diccionario preliminar)", "",
        f"**Generado:** {generated} con `python scripts/build_catalog.py` · **No editar a mano:** las descripciones "
        "están en `src/common/catalog.py` y las cifras salen del lago.", "",
        "Clasificación: **interna** (operativo, sin datos personales ni financieros por cliente) · **confidencial** "
        "(información comercial o financiera de clientes) · **personal** (identifica a una persona). "
        "Responsables: roles de `docs/entrega2/plan_trabajo.md` §1. Gobierno: [`gobierno.md`](gobierno.md).", "",
        "## Resumen", "",
        "| Tabla | Capa | Grano | Filas | Partición | Clasificación | Responsable |",
        "|---|---|---|---:|---|---|---|",
    ]
    for e in entries:
        rows = f"{e['rows']:,}".replace(",", ".") if e.get("rows") is not None else "—"
        lines.append(f"| [`{e['name']}`](#{anchor(e['name'])}) | {e['layer']} | {e['grain']} | {rows} | "
                     f"{e.get('partition', '—')} | {e['classification']} | {e['owner']} |")
    for e in entries:
        lines += ["", f"## {e['name']}", "", e["description"] + ".", "",
                  f"- **Grano:** {e['grain']} · **Clave:** `{e['key']}` · **Decisión:** {e['decision']}",
                  f"- **Proceso:** {e['process']} · **Origen:** " + ", ".join(f"`{s}`" for s in e["sources"]),
                  f"- **Clasificación:** {e['classification']} · **Responsable:** {e['owner']} ({ROLES[e['owner']]})"]
        if e.get("rows") is None:
            lines += ["- **Columnas:** las mismas que la tabla de Gold de origen, más `loaded_at` "
                      "(esquema en `cql/01_schema.cql`)."]
            continue
        files = f"{e['files']} archivo" + ("s" if e["files"] != 1 else "")
        lines += [f"- **Física:** {e['rows']:,} filas · {files} · {size(e['bytes'])} · partición: "
                  f"{e['partition']}".replace(",", "."), "",
                  "| Columna | Tipo | Descripción | Nota |", "|---|---|---|---|"]
        business = [c for c in e["columns"] if not is_technical(c[0])]
        technical = [c for c in e["columns"] if is_technical(c[0])]
        for name, dtype in business + technical:
            note = COLUMN_FLAGS.get(name, "linaje" if is_technical(name) else "")
            lines.append(f"| `{name}` | {dtype} | {describe(name) or '**SIN DESCRIPCIÓN**'} | {note} |")
    return "\n".join(lines) + "\n"


def anchor(name: str) -> str:
    return "".join(ch for ch in name.lower().replace(" ", "-") if ch.isalnum() or ch in "-_")


def main() -> int:
    spark = get_spark("catalog")
    entries, missing = [], []
    for table in TABLES:
        entry = dict(table)
        if table.get("parquet", True):
            if not (DATALAKE_ROOT / table["name"]).exists():
                print(f"Falta {table['name']}: ejecutá antes python scripts/run_pipeline.py")
                return 1
            entry.update(physical(spark, table["name"]))
            missing += [f"{table['name']}.{c}" for c, _ in entry["columns"] if not describe(c)]
        entries.append(entry)
    spark.stop()

    generated = utc_now()
    OUT.write_text(render(entries, generated), encoding="utf-8")
    summary = {"generado_utc": generated, "tablas": len(entries),
               "columnas": sum(len(e.get("columns", [])) for e in entries),
               "columnas_sin_descripcion": missing,
               "por_clasificacion": {c: [e["name"] for e in entries if e["classification"] == c]
                                     for c in ("interna", "confidencial", "personal")},
               "tablas_detalle": [{k: e.get(k) for k in ("name", "layer", "grain", "rows", "files", "bytes",
                                                          "partition", "classification", "owner")} for e in entries]}
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / "e2_11_catalogo.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{summary['tablas']} tablas · {summary['columnas']} columnas · sin descripción: {len(missing)} · {OUT.relative_to(REPO_ROOT)}")
    for m in missing:
        print("  sin descripción:", m)
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
