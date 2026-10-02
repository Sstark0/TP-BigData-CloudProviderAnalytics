"""
Rutas del Data Lake.
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

EVENTS_DIR = LANDING / "usage_events_stream"
EVIDENCE_DIR = Path(os.getenv("EVIDENCE_DIR", REPO_ROOT / "evidence"))