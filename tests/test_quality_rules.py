"""Pruebas de las reglas de calidad de Silver de eventos (E2-06).

Ejecutar: python -m unittest tests.test_quality_rules -v
Cada caso arma un Landing temporal y lo pasa por S1 (Bronze), S2 (stream) y B2 (Silver + registro de calidad).
NO usan el dataset real.
"""
import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from src.batch.silver_events import reconcile
from src.common.runlog import RunLog
from src.common.spark import get_spark
from src.streaming.bronze_events import Paths, read_bronze_events, run_query
from src.streaming.silver_events_live import run_live_query


def event(i, **changes):
    e = {"event_id": i, "timestamp": "2025-08-10T12:00:00Z", "org_id": "org_1", "resource_id": "res_1",
         "service": "compute", "region": "us-east-1", "metric": "requests", "value": 100.0, "unit": "count",
         "cost_usd_increment": 1.0, "schema_version": 1}
    e.update(changes)
    return json.dumps(e)


class QualityRulesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-quality-rules")

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.bronze = Paths(landing=self.tmp / "landing", root=self.tmp / "lake")
        self.bronze.landing.mkdir()
        self.silver = self.tmp / "lake" / "silver" / "usage_events"
        self.quarantine = self.tmp / "lake" / "quarantine" / "source=usage_events_silver"
        self.n_files = 0

    def tearDown(self):
        self._tmp.cleanup()

    def add_file(self, lines):
        path = self.bronze.landing / f"events_part_{self.n_files:04d}.jsonl"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        mtime = time.time() + self.n_files
        os.utime(path, (mtime, mtime))
        self.n_files += 1

    def process(self):
        run_query(self.spark, self.bronze, "t")
        lake = self.tmp / "lake"
        run_live_query(self.spark, self.bronze.bronze, lake / "silver" / "usage_events_live", lake / "_ck_s2")
        with RunLog("test_b2", runs_dir=self.tmp / "_runs") as run:
            counts = reconcile(self.spark, run, read_bronze_events(self.spark, self.bronze),
                               lake / "silver" / "usage_events_live", self.silver, self.quarantine)
        self.run_data = run.data
        silver = {r.event_id: r for r in self.spark.read.parquet(str(self.silver)).collect()}
        has_rows = any(self.quarantine.rglob("*.parquet"))
        log = [r.asDict() for r in self.spark.read.parquet(str(self.quarantine)).collect()] if has_rows else []
        return counts, silver, log

    def log_for(self, log, key):
        return sorted((r["rule_id"], r["action"]) for r in log if r["record_key"] == key)

    def test_missing_unit_is_inferred_and_logged(self):
        self.add_file([event("ok"), event("e1", unit=None), event("e2", unit=None, metric="cpu_hours", value=2.0)])
        counts, silver, log = self.process()
        self.assertEqual((silver["e1"].unit, silver["e1"].unit_inferred), ("count", True))
        self.assertEqual(silver["e2"].unit, "hours")
        self.assertFalse(silver["ok"].unit_inferred)
        self.assertEqual(self.log_for(log, "e1"), [("unit_missing", "corregido")])
        raw = json.loads(next(r for r in log if r["record_key"] == "e1")["raw_record"])
        self.assertNotIn("unit", raw)                       # el registro conserva el dato original (sin unidad)
        self.assertEqual(counts["corregidos"], 2)

    def test_missing_unit_without_value_is_not_a_problem(self):
        self.add_file([event("e1", unit=None, value=None)])
        counts, silver, log = self.process()
        self.assertEqual((silver["e1"].unit, silver["e1"].unit_inferred), (None, False))   # sin valor no se infiere
        self.assertEqual(log, [])

    def test_wrong_or_uninferable_unit_is_rejected(self):
        self.add_file([event("e1", unit="hours"),                              # requests medidos en horas
                       event("e2", unit=None, metric="gpu_hours"),             # métrica desconocida
                       event("e3")])
        counts, silver, log = self.process()
        self.assertEqual(set(silver), {"e3"})
        self.assertEqual(self.log_for(log, "e1"), [("unit_mismatch", "rechazado")])
        self.assertEqual(self.log_for(log, "e2"), [("unit_missing_uninferable", "rechazado")])
        self.assertEqual(counts["rechazados"], 2)

    def test_negative_cost_is_kept_and_flagged_beyond_tolerance(self):
        self.add_file([event("e1", cost_usd_increment=-5.0), event("e2", cost_usd_increment=-0.005)])
        counts, silver, log = self.process()
        self.assertEqual((silver["e1"].cost_negative, silver["e1"].cost_usd_increment), (True, -5.0))
        self.assertFalse(silver["e2"].cost_negative)        # dentro de la tolerancia de redondeo
        self.assertEqual(self.log_for(log, "e1"), [("cost_below_tolerance", "marcado")])
        self.assertEqual(counts["marcados"], 1)

    def test_one_event_can_be_corrected_and_flagged(self):
        self.add_file([event("e1", unit=None, cost_usd_increment=-3.0)])
        counts, silver, log = self.process()
        self.assertEqual(len(silver), 1)
        self.assertEqual(self.log_for(log, "e1"), [("cost_below_tolerance", "marcado"), ("unit_missing", "corregido")])
        self.assertEqual(counts["quarantine_registros"], 2)

    def test_duplicates_and_null_ids_leave_silver_and_balances_close(self):
        self.add_file([event("e1"), event(None)])
        self.add_file([event("e1")])
        counts, silver, log = self.process()
        self.assertEqual(set(silver), {"e1"})
        self.assertEqual(self.log_for(log, "e1"), [("event_id_duplicate", "descartado")])
        self.assertEqual(self.log_for(log, "∅"), [("event_id_null", "rechazado")])
        self.assertEqual((counts["bronze_eventos"], counts["rechazados"], counts["duplicados_event_id"]), (3, 1, 1))
        self.assertTrue(all(b["ok"] for b in self.run_data["balances"]))


if __name__ == "__main__":
    unittest.main()
