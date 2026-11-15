"""Pipeline completo, de Landing a Cassandra, en el orden correcto (E2-12).

Cada etapa es uno de los scripts de scripts/, que se ejecuta como un proceso aparte (con su propia sesión de
Spark). Si una etapa falla, el pipeline se detiene: las siguientes leerían datos viejos o incompletos.

Uso (desde la raíz del repositorio, con el venv activo):
    python scripts/run_pipeline.py --tests --operador "Nombre"     # pruebas + pipeline completo
    python scripts/run_pipeline.py --clean                          # borra las zonas derivadas y reconstruye
    python scripts/run_pipeline.py --desde gold_finops              # retoma desde una etapa
    python scripts/run_pipeline.py --sin-serving                    # todo menos la carga en AstraDB

    Para la etapa de AstraDB, antes:  set -a; source ~/.astra/astra.env; set +a
    Sin esas variables, la etapa se omite (queda registrado) y el resto corre igual.

Limpieza y reinicio (--clean)
    Vacía bronze/, silver/, gold/, quarantine/ y _checkpoints/ del lago (conserva sus .gitkeep). NUNCA toca landing/ (los datos
    originales, inmutables) ni _runs/ (el historial de ejecuciones). Sin checkpoints, el streaming vuelve a leer
    todos los archivos desde cero. En Cassandra no hace falta borrar: la carga reemplaza las filas por clave y
    elimina las que ya no están en Gold (D-17).

Registro de la ejecución
    evidence/entrega2/e2_12_pipeline_run.json: quién ejecutó, en qué entorno, resultado de las pruebas y, por
    etapa, duración, estado y los conteos y balances del registro de cada proceso.
    datalake/_runs/pipeline/<run_id>/<etapa>.log: la salida completa de cada etapa (no se versiona).
    datalake/_runs/pipeline/<run_id>/evidence/: la evidencia que escribe cada etapa. Con --actualizar-evidencias
    se escribe en evidence/ del repositorio (solo cuando se quiere regenerar la evidencia de los paquetes).
"""
import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import BRONZE, CHECKPOINTS, EVIDENCE_DIR, GOLD, LANDING, QUARANTINE, REPO_ROOT, RUNS, SILVER
from src.common.runlog import latest_run, new_run_id, utc_now

# (etapa, script, argumentos, procesos cuyo registro se resume, ¿opcional?)
STAGES = [
    ("landing_check", "check_common.py", [], ["e2_01_check_common"], False),
    ("bronze_batch", "run_bronze_batch.py", [],
     [f"b1_bronze_{s}" for s in ("customers_orgs", "users", "resources", "support_tickets", "marketing_touches",
                                 "nps_surveys", "billing_monthly")], False),
    ("bronze_streaming", "run_bronze_streaming.py", [], ["s1_bronze_events"], False),
    ("silver_events", "run_silver_events.py", [], ["b2_silver_events"], False),
    ("silver_enrichment", "run_silver_enrichment.py", [], ["b3_silver_enrichment"], False),
    ("quality_report", "report_quality.py", [], [], False),
    ("gold_finops", "run_gold_finops.py", [], ["g1_gold_finops"], False),
    ("cost_anomalies", "run_cost_anomalies.py", [], ["g3_cost_anomalies"], False),
    ("serving_astra", "run_serving_astra.py", [], ["v1_serving_astra"], True),
]
DERIVED = [BRONZE, SILVER, GOLD, QUARANTINE, CHECKPOINTS]


def environment() -> dict:
    def version(cmd):
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            lines = (out.stderr + out.stdout).strip().splitlines()
            return next((l for l in lines if "version" in l), lines[0] if lines else "sin salida")
        except Exception as e:                       # noqa: BLE001 — solo informativo
            return f"no disponible ({type(e).__name__})"

    info = {"python": platform.python_version(), "sistema": platform.platform(), "java": version(["java", "-version"])}
    for module in ("pyspark", "cassandra"):
        try:
            info[module] = __import__(module).__version__
        except ImportError:
            info[module] = "no instalado"
    return info


def run_tests(log_dir: Path) -> dict:
    start = time.time()
    out = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests"], cwd=REPO_ROOT,
                         capture_output=True, text=True)
    (log_dir / "tests.log").write_text(out.stdout + out.stderr, encoding="utf-8")
    ran = re.search(r"Ran (\d+) tests? in", out.stderr)
    return {"comando": "python -m unittest discover -s tests", "pruebas": int(ran.group(1)) if ran else None,
            "resultado": "OK" if out.returncode == 0 else "FALLÓ", "duracion_s": round(time.time() - start, 1)}


def summarize(process: str, since: str):
    """Conteos y balances del registro de `process`, si corrió en esta ejecución del pipeline."""
    data = latest_run(process, status=None)
    if not data or data["run_id"] < since:
        return None
    return {"run_id": data["run_id"], "status": data["status"], "counts": data.get("counts", {}),
            "balances_ok": all(b["ok"] for b in data.get("balances", []))}


def clean() -> list:
    """Vacía las zonas derivadas. Conserva la carpeta y su .gitkeep (están versionados)."""
    removed = []
    for path in DERIVED:
        if path.resolve() == LANDING.resolve() or LANDING.resolve().is_relative_to(path.resolve()):
            raise RuntimeError(f"--clean no puede borrar {path}: contiene landing/")
        if not path.exists():
            continue
        for child in path.iterdir():
            if child.name == ".gitkeep":
                continue
            shutil.rmtree(child) if child.is_dir() else child.unlink()
            removed.append(f"{path.name}/{child.name}")
    return removed


def main() -> int:
    names = [s[0] for s in STAGES]
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tests", action="store_true", help="correr las pruebas antes del pipeline")
    parser.add_argument("--clean", action="store_true", help="borrar las zonas derivadas y reconstruir todo")
    parser.add_argument("--desde", choices=names, default=names[0], help="primera etapa a ejecutar")
    parser.add_argument("--hasta", choices=names, default=names[-1], help="última etapa a ejecutar")
    parser.add_argument("--sin-serving", action="store_true", help="omitir la carga en AstraDB")
    parser.add_argument("--operador", default=os.getenv("USER", "sin informar"), help="quién ejecuta (para el registro)")
    parser.add_argument("--evidencia", default="e2_12_pipeline_run.json", help="nombre del archivo de evidencia")
    parser.add_argument("--actualizar-evidencias", action="store_true",
                        help="que cada etapa reescriba su evidencia en evidence/entrega2 (por defecto va a _runs)")
    args = parser.parse_args()

    run_id = new_run_id()
    log_dir = RUNS / "pipeline" / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    record = {"pipeline_run_id": run_id, "inicio_utc": utc_now(), "operador": args.operador,
              "entorno": environment(), "opciones": {k: v for k, v in vars(args).items() if k != "evidencia"},
              "etapas": []}
    print(f"pipeline {run_id} · registro en {log_dir}")

    if args.tests:
        record["pruebas"] = run_tests(log_dir)
        print(f"pruebas: {record['pruebas']['pruebas']} · {record['pruebas']['resultado']}")
        if record["pruebas"]["resultado"] != "OK":
            return finish(record, args.evidencia, ok=False, reason="fallaron las pruebas (ver tests.log)")
    if args.clean:
        record["limpieza"] = clean()
        print("limpieza: " + (", ".join(record["limpieza"]) or "nada que borrar"))

    # Cada script escribe su propia evidencia (e2_01 ... e2_09). Por defecto se desvía a la carpeta de esta
    # ejecución, para no reescribir la evidencia ya versionada de cada paquete con cada corrida del pipeline.
    env = dict(os.environ)
    if not args.actualizar_evidencias:
        env["EVIDENCE_DIR"] = str(log_dir / "evidence")
    record["evidencias_de_etapas"] = "evidence/" if args.actualizar_evidencias else f"datalake/_runs/pipeline/{run_id}/evidence/"

    selected = STAGES[names.index(args.desde): names.index(args.hasta) + 1]
    for name, script, extra, processes, optional in selected:
        stage = {"etapa": name, "script": f"scripts/{script}"}
        if name == "serving_astra":
            missing = [v for v in ("ASTRA_DB_APPLICATION_TOKEN", "ASTRA_SECURE_BUNDLE_PATH") if not os.getenv(v)]
            if args.sin_serving or missing:
                stage["estado"] = "omitida"
                stage["motivo"] = "--sin-serving" if args.sin_serving else "faltan variables: " + ", ".join(missing)
                record["etapas"].append(stage)
                print(f"[{name}] omitida: {stage['motivo']}")
                continue
        start = time.time()
        print(f"[{name}] ejecutando scripts/{script} ...", flush=True)
        with open(log_dir / f"{name}.log", "w", encoding="utf-8") as log:
            code = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / script), *extra], cwd=REPO_ROOT,
                                  stdout=log, stderr=subprocess.STDOUT, env=env).returncode
        stage.update(duracion_s=round(time.time() - start, 1), codigo_salida=code,
                     estado="ok" if code == 0 else "error",
                     procesos={p: summarize(p, run_id) for p in processes})
        bad = [p for p, s in stage["procesos"].items() if s is None or s["status"] != "ok" or not s["balances_ok"]]
        if code == 0 and bad:
            stage["estado"] = "error"
            stage["detalle"] = "sin registro correcto o con balances que no cierran: " + ", ".join(bad)
        record["etapas"].append(stage)
        print(f"[{name}] {stage['estado']} en {stage['duracion_s']} s")
        if stage["estado"] != "ok":
            return finish(record, args.evidencia, ok=False, reason=f"falló la etapa {name} (ver {name}.log)")
    return finish(record, args.evidencia, ok=True)


def finish(record: dict, filename: str, ok: bool, reason: str = None) -> int:
    record["fin_utc"] = utc_now()
    record["estado"] = "ok" if ok else "error"
    if reason:
        record["motivo"] = reason
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / filename).write_text(json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"pipeline {record['estado']}" + (f": {reason}" if reason else "") + f" · evidencia: evidence/entrega2/{filename}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
