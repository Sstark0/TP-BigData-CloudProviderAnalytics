"""
Rutas del Data Lake.

Todo proceso obtiene sus rutas de este módulo: ninguna ruta se escribe a mano en un job.
Los valores se pueden cambiar con variables de entorno (config/.env.example).

"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DATALAKE_ROOT = Path(os.getenv("DATALAKE_ROOT", REPO_ROOT / "datalake"))
LANDING = Path(os.getenv("LANDING_PATH", DATALAKE_ROOT / "landing"))
BRONZE = DATALAKE_ROOT / "bronze"
SILVER = DATALAKE_ROOT / "silver"
GOLD = DATALAKE_ROOT / "gold"
QUARANTINE = DATALAKE_ROOT / "quarantine"
CHECKPOINTS = DATALAKE_ROOT / "_checkpoints"

RUNS = DATALAKE_ROOT / "_runs"           # un JSON por ejecución de cada proceso

EVENTS_DIR = LANDING / "usage_events_stream"
EVIDENCE_DIR = Path(os.getenv("EVIDENCE_DIR", REPO_ROOT / "evidence"))

ZONES = {"landing": LANDING, "bronze": BRONZE, "silver": SILVER, "gold": GOLD, "quarantine": QUARANTINE}

# Parámetros de procesamiento (D-04). Se leen del entorno para no tocar código al cambiarlos.
EVENTS_WATERMARK = os.getenv("EVENTS_WATERMARK", "24 hours")
MAX_FILES_PER_TRIGGER = int(os.getenv("MAX_FILES_PER_TRIGGER", "1"))


def table_path(zone: str, table: str) -> Path:
    """Ruta de una tabla: <DATALAKE_ROOT>/<zona>/<tabla>."""
    if zone not in ZONES:
        raise ValueError(f"Zona desconocida: {zone!r}. Zonas válidas: {sorted(ZONES)}")
    return ZONES[zone] / table


def quarantine_path(source: str) -> Path:
    """Ruta de quarantine de una fuente: quarantine/source=<fuente>."""
    return QUARANTINE / f"source={source}"


def checkpoint_path(query_id: str) -> Path:
    """Checkpoint exclusivo de una consulta de streaming: _checkpoints/<query_id>."""
    return CHECKPOINTS / query_id
