"""
Registro de ejecuciones.

Cada proceso del pipeline deja un JSON en <DATALAKE_ROOT>/_runs/<proceso>/<run_id>.json con
- Archivos leídos (tamaño y hash).
- Conteos de filas leídas, aceptadas, rechazadas o descartadas.
- Qué escribió, cuánto tardó y si terminó bien.

Esos conteos son los que permiten conciliar una zona contra la siguiente.

Uso:
    with RunLog("b1_bronze_users") as run:
        run.add_inputs([ruta_csv])
        ...
        run.count("leidas", 800)
        run.count("aceptadas", 800)
        run.add_output(table_path("bronze", "users"))
"""
import hashlib
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.common.config import RUNS


def get_logger(name: str) -> logging.Logger:
    """Logger con formato uniforme: hora UTC, nivel, proceso y mensaje."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)sZ %(levelname)s [%(name)s] %(message)s", "%Y-%m-%dT%H:%M:%S")
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_run_id() -> str:
    """Identificador ordenable por fecha: 20261113T073000Z-1a2b3c4d."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def describe_file(path) -> dict:
    """Nombre, tamaño y SHA-256 de un archivo de entrada. Prueba la versión exacta se procesó."""
    path = Path(path)
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return {"file": path.name, "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def describe_output(path) -> dict:
    """Cantidad de archivos Parquet y bytes de una tabla ya escrita."""
    path = Path(path)
    files = [f for f in path.rglob("*.parquet")] if path.exists() else []
    return {"path": str(path), "parquet_files": len(files), "bytes": sum(f.stat().st_size for f in files)}


class RunLog:
    """Acumula los datos de una ejecución y los escribe al salir del bloque `with`, también si hubo error."""

    def __init__(self, process: str, runs_dir=None):
        self.process = process
        self.run_id = new_run_id()
        self.runs_dir = Path(runs_dir) if runs_dir else RUNS
        self.log = get_logger(process)
        self.data = {"process": process, "run_id": self.run_id, "status": "running", "started_utc": None,
                     "finished_utc": None, "duration_s": None, "inputs": [], "counts": {}, "outputs": [],
                     "params": {}, "error": None}
        self._t0 = None

    @property
    def path(self) -> Path:
        return self.runs_dir / self.process / f"{self.run_id}.json"

    def __enter__(self):
        self._t0 = time.time()
        self.data["started_utc"] = utc_now()
        self.log.info("inicio run_id=%s", self.run_id)
        return self

    def add_inputs(self, paths) -> None:
        for p in paths:
            self.data["inputs"].append(describe_file(p))

    def param(self, name: str, value) -> None:
        self.data["params"][name] = value

    def count(self, name: str, value: int) -> None:
        self.data["counts"][name] = int(value)
        self.log.info("%s=%s", name, f"{int(value):,}".replace(",", "."))

    def add_output(self, path) -> None:
        self.data["outputs"].append(describe_output(path))

    def check_balance(self, total: str, parts) -> bool:
        """Conciliación: counts[total] debe ser igual a la suma de counts[parts]. Queda registrada en el JSON."""
        expected = self.data["counts"][total]
        got = sum(self.data["counts"][p] for p in parts)
        ok = expected == got
        self.data.setdefault("balances", []).append(
            {"check": f"{total} = " + " + ".join(parts), "left": expected, "right": got, "ok": ok})
        (self.log.info if ok else self.log.error)("balance %s = %s: %s vs %s -> %s",
                                                  total, " + ".join(parts), expected, got, "OK" if ok else "NO CIERRA")
        return ok

    def __exit__(self, exc_type, exc, tb):
        self.data["finished_utc"] = utc_now()
        self.data["duration_s"] = round(time.time() - self._t0, 2)
        if exc_type is None:
            self.data["status"] = "ok"
        else:
            self.data["status"] = "error"
            self.data["error"] = f"{exc_type.__name__}: {exc}"
            self.log.error("falló: %s", self.data["error"])
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")
        self.log.info("fin status=%s duración=%ss registro=%s", self.data["status"], self.data["duration_s"], self.path)
        return False          # no oculta la excepción: el proceso falla de forma visible


def latest_run(process: str, runs_dir=None, status: str = "ok"):
    """Última ejecución registrada de un proceso (por defecto, la última que terminó bien), o None."""
    folder = (Path(runs_dir) if runs_dir else RUNS) / process
    if not folder.exists():
        return None
    for f in sorted(folder.glob("*.json"), reverse=True):
        data = json.loads(f.read_text(encoding="utf-8"))
        if status is None or data.get("status") == status:
            return data
    return None
