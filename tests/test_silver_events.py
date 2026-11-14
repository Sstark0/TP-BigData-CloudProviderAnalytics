"""Pruebas de Silver de eventos (E2-04): watermark, deduplicación, late data y reconciliación.

Ejecutar: python -m unittest tests.test_silver_events -v
Arman un Landing temporal, lo pasan por S1 (Bronze) y después por S2 + B2. NO usan el dataset real.
"""
import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from src.batch.silver_events import reconcile
from src.common.runlog import RunLog
from src.common.schemas import BRONZE_EVENTS_SCHEMA
from src.common.spark import get_spark
from src.streaming.bronze_events import Paths, read_bronze_events, run_query
from src.streaming.silver_events_live import run_live_query


def event(i, ts="2025-08-10T12:00:00Z", **changes):
    e = {"event_id": i, "timestamp": ts, "org_id": "org_1", "resource_id": "res_1", "service": "compute",
         "region": "us-east-1", "metric": "cpu_hours", "value": 1.0, "unit": "hours",
         "cost_usd_increment": 0.10, "schema_version": 1}
    e.update(changes)
    return json.dumps(e)


class SilverEventsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-silver-events")

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.bronze = Paths(landing=self.tmp / "landing", root=self.tmp / "lake")
        self.bronze.landing.mkdir()
        self.live = self.tmp / "lake" / "silver" / "usage_events_live"
        self.silver = self.tmp / "lake" / "silver" / "usage_events"
        self.quarantine = self.tmp / "lake" / "quarantine" / "source=usage_events_silver"
        self.n_files = 0

    def tearDown(self):
        self._tmp.cleanup()

    def add_file(self, lines):
        path = self.bronze.landing / f"events_part_{self.n_files:04d}.jsonl"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        mtime = time.time() + self.n_files            # orden de llegada = orden de creación
        os.utime(path, (mtime, mtime))
        self.n_files += 1

    def process(self, run_id="t"):
        """S1 (Landing → Bronze), S2 (stream con watermark) y B2 (reconciliación). Devuelve métricas y conteos."""
        run_query(self.spark, self.bronze, run_id)
        stream = run_live_query(self.spark, self.bronze.bronze, self.live, self.tmp / "lake" / "_ck_s2",
                                watermark="24 hours")
        with RunLog("test_b2", runs_dir=self.tmp / "_runs") as run:
            counts = reconcile(self.spark, run, read_bronze_events(self.spark, self.bronze),
                               self.live, self.silver, self.quarantine)
        return stream, counts

    def silver_rows(self):
        return {r.event_id: r for r in self.spark.read.parquet(str(self.silver)).collect()}

    def test_bronze_matches_the_declared_contract(self):
        self.add_file([event("e1")])
        run_query(self.spark, self.bronze, "t")
        written = [(f.name, f.dataType) for f in self.spark.read.parquet(str(self.bronze.bronze)).schema]
        self.assertEqual(written, [(f.name, f.dataType) for f in BRONZE_EVENTS_SCHEMA])

    def test_late_event_is_dropped_by_stream_and_recovered_by_reconciliation(self):
        self.add_file([event("e1", "2025-08-10T12:00:00Z")])
        self.add_file([event("e2", "2025-08-10T12:05:00Z")])
        self.add_file([event("e3", "2025-08-01T08:00:00Z")])         # 9 días antes del máximo visto: tardío
        stream, counts = self.process()
        self.assertEqual(stream["descartados_por_watermark"], 1)
        live = {r.event_id for r in self.spark.read.parquet(str(self.live)).collect()}
        self.assertEqual(live, {"e1", "e2"})
        rows = self.silver_rows()
        self.assertEqual(set(rows), {"e1", "e2", "e3"})                # el tardío no se pierde
        self.assertEqual(rows["e3"].arrival_path, "tardio")
        self.assertEqual(rows["e1"].arrival_path, "a_tiempo")
        self.assertEqual((counts["silver_a_tiempo"], counts["silver_tardios_recuperados"]), (2, 1))

    def test_duplicate_event_id_keeps_first_arrival(self):
        self.add_file([event("e1", value=1.0)])
        self.add_file([event("e1", value=1.0), event("e2")])           # reenvío de e1 (at-least-once)
        stream, counts = self.process()
        self.assertEqual(stream["duplicados_descartados"], 1)
        rows = self.silver_rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual((rows["e1"].bronze_batch_id, rows["e1"].copies_in_bronze), (0, 2))
        self.assertEqual(counts["duplicados_event_id"], 1)
        self.assertEqual(counts["bronze_eventos"], 3)                  # 3 = 2 en Silver + 0 quarantine + 1 duplicado

    def test_null_event_id_goes_to_quarantine(self):
        self.add_file([event("e1"), event(None, cost_usd_increment=9.99)])
        _, counts = self.process()
        self.assertEqual(counts["quarantine.event_id_null"], 1)
        q = self.spark.read.parquet(str(self.quarantine)).first()
        self.assertEqual((q.rule_id, q.record_key), ("event_id_null", "∅"))
        self.assertIn("9.99", q.raw_record)
        self.assertEqual(set(self.silver_rows()), {"e1"})

    def test_rerun_gives_the_same_silver(self):
        self.add_file([event("e1"), event("e2", "2025-08-11T00:00:00Z")])
        self.add_file([event("e3", "2025-07-20T00:00:00Z")])
        _, first = self.process("r1")
        before = sorted((r.event_id, r.arrival_path) for r in self.silver_rows().values())
        stream, second = self.process("r2")
        self.assertEqual(stream["leidos"], 0)                          # el stream no vuelve a leer nada
        self.assertEqual(sorted((r.event_id, r.arrival_path) for r in self.silver_rows().values()), before)
        for key in ("bronze_eventos", "silver_eventos", "silver_a_tiempo", "duplicados_event_id"):
            self.assertEqual(first[key], second[key], key)


if __name__ == "__main__":
    unittest.main()
