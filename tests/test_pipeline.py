"""Pruebas del orquestador scripts/run_pipeline.py (E2-12): limpieza segura y lectura de registros.

Ejecutar: python -m unittest tests.test_pipeline -v
No ejecutan el pipeline: prueban las dos funciones que pueden hacer daño o mentir si fallan.
"""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.common.config import REPO_ROOT

spec = importlib.util.spec_from_file_location("run_pipeline", REPO_ROOT / "scripts" / "run_pipeline.py")
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.lake = Path(self._tmp.name) / "datalake"
        self.zones = [self.lake / z for z in ("bronze", "silver", "_checkpoints")]
        for z in self.zones:
            (z / "tabla" / "parte").mkdir(parents=True)
            (z / "tabla" / "parte" / "x.parquet").write_text("datos")
            (z / ".gitkeep").write_text("")
        self.landing = self.lake / "landing"
        self.landing.mkdir()
        (self.landing / "users.csv").write_text("a,b")

    def tearDown(self):
        self._tmp.cleanup()

    def test_clean_empties_derived_zones_but_keeps_gitkeep_and_landing(self):
        with mock.patch.object(pipeline, "DERIVED", self.zones), mock.patch.object(pipeline, "LANDING", self.landing):
            removed = pipeline.clean()
        self.assertEqual(sorted(removed), ["_checkpoints/tabla", "bronze/tabla", "silver/tabla"])
        for z in self.zones:
            self.assertEqual([p.name for p in z.iterdir()], [".gitkeep"])
        self.assertTrue((self.landing / "users.csv").exists())

    def test_clean_refuses_a_folder_that_contains_landing(self):
        with mock.patch.object(pipeline, "DERIVED", [self.lake]), mock.patch.object(pipeline, "LANDING", self.landing):
            with self.assertRaises(RuntimeError):
                pipeline.clean()
        self.assertTrue((self.landing / "users.csv").exists())

    def test_summarize_ignores_runs_older_than_this_pipeline(self):
        runs = Path(self._tmp.name) / "_runs" / "g1_gold_finops"
        runs.mkdir(parents=True)
        old = {"run_id": "20261101T000000Z-aaaa", "status": "ok", "counts": {"x": 1}, "balances": [{"ok": True}]}
        (runs / f"{old['run_id']}.json").write_text(json.dumps(old))
        with mock.patch("src.common.runlog.RUNS", runs.parent):
            self.assertIsNone(pipeline.summarize("g1_gold_finops", since="20261115T000000Z-bbbb"))
            new = dict(old, run_id="20261115T010000Z-cccc", balances=[{"ok": False}])
            (runs / f"{new['run_id']}.json").write_text(json.dumps(new))
            got = pipeline.summarize("g1_gold_finops", since="20261115T000000Z-bbbb")
        self.assertEqual((got["run_id"], got["balances_ok"]), ("20261115T010000Z-cccc", False))


if __name__ == "__main__":
    unittest.main()
