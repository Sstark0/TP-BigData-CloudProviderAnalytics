"""Regresiones E1: rutas de evidencia y watermark con un solo archivo.

Ejecutar: python -m unittest discover -s tests -v
Las pruebas de watermark requieren Java y las dependencias de requirements.txt.
"""
import contextlib
from datetime import datetime
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import nbformat
import pandas as pd
from pyspark.sql import functions as F

from src.common.spark import get_spark

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = json.loads((ROOT / "notebooks/01_exploracion_fuentes.ipynb").read_text())


def cell_containing(fragment):
    cells = ["".join(c["source"]) for c in NOTEBOOK["cells"]
             if c["cell_type"] == "code" and fragment in "".join(c["source"])]
    if len(cells) != 1:
        raise AssertionError(f"Expected one cell containing {fragment!r}")
    return cells[0]


class EvidencePathTests(unittest.TestCase):
    def test_profile_writes_to_external_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "entrega1"
            out.mkdir()
            state = {"OUT": out, "REPO": ROOT, "json": json,
                     "resumen": {"regression": True},
                     "spark": type("SparkVersion", (), {"version": "test"})()}
            with contextlib.redirect_stdout(io.StringIO()) as printed:
                exec(cell_containing('ruta_salida = OUT / "perfil_fuentes.json"'), state)
            profile = json.loads((out / "perfil_fuentes.json").read_text())
            self.assertTrue(profile["regression"])
            self.assertIn(str(out.resolve()), printed.getvalue())

    def test_runner_respects_external_evidence_directory(self):
        spec = importlib.util.spec_from_file_location("run_exploration", ROOT / "scripts/run_exploration.py")
        runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runner)
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {"EVIDENCE_DIR": directory}):
                # Prueba de rutas; no afirma que Jupyter haya ejecutado el notebook.
                with patch.object(runner.NotebookClient, "execute") as execute:
                    with contextlib.redirect_stdout(io.StringIO()):
                        path = runner.main()
            execute.assert_called_once()
            self.assertEqual(path, Path(directory) / "entrega1/01_exploracion_ejecutada.ipynb")
            nbformat.validate(nbformat.read(path, as_version=4))


class WatermarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("entrega1-regression-tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def simulate(self, rows):
        events = self.spark.createDataFrame(rows, "source_file string, event_ts timestamp")
        state = {"spark": self.spark, "ev": events, "F": F, "pd": pd,
                 "resumen": {}, "total_ev": len(rows)}
        with contextlib.redirect_stdout(io.StringIO()):
            exec(cell_containing('por_archivo = (ev.groupBy("source_file")'), state)
        return state["resumen"]["orden_temporal"]["eventos_tardios_segun_watermark"]

    def test_single_file_has_no_previous_microbatch(self):
        result = self.simulate([("one.jsonl", datetime(2025, 1, 1)),
                                ("one.jsonl", datetime(2025, 1, 3))])
        self.assertEqual(result, {f"watermark {h} h": 0 for h in [1, 24, 72, 168]})

    def test_multiple_files_keep_previous_watermark_behavior(self):
        result = self.simulate([("a.jsonl", datetime(2025, 1, 3)),
                                ("b.jsonl", datetime(2025, 1, 1))])
        self.assertEqual(result, {"watermark 1 h": 1, "watermark 24 h": 1,
                                  "watermark 72 h": 0, "watermark 168 h": 0})


if __name__ == "__main__":
    unittest.main()
