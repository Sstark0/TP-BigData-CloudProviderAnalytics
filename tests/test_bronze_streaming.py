"""Pruebas de la ingesta streaming de eventos a Bronze (E2-03).

Ejecutar: python -m unittest tests.test_bronze_streaming -v
Crean un Landing y un lago temporales; NO usan el dataset real.
"""
import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from pyspark.sql import functions as F

from src.common.schemas import EVENTS_RAW_SCHEMA
from src.common.spark import get_spark
from src.streaming.bronze_events import Paths, completed_batches, make_batch_writer, read_bronze_events, run_query


def event(i, **changes):
    e = {"event_id": f"evt_{i}", "timestamp": "2025-08-01T10:00:00Z", "org_id": "org_1", "resource_id": "res_1",
         "service": "compute", "region": "us-east-1", "metric": "cpu_hours", "value": 1.5, "unit": "hours",
         "cost_usd_increment": 0.12, "schema_version": 1}
    e.update(changes)
    return json.dumps(e)


class BronzeStreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-bronze-streaming")

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.paths = Paths(landing=tmp / "landing", root=tmp / "lake")
        self.paths.landing.mkdir()
        self.n_files = 0

    def tearDown(self):
        self._tmp.cleanup()

    def add_file(self, lines):
        """Deja un archivo nuevo en Landing, con fecha de modificación posterior a los anteriores."""
        path = self.paths.landing / f"events_part_{self.n_files:04d}.jsonl"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        mtime = time.time() + self.n_files
        os.utime(path, (mtime, mtime))
        self.n_files += 1

    def ingest(self, run_id="test"):
        run_query(self.spark, self.paths, run_id, max_files=1)
        return read_bronze_events(self.spark, self.paths)

    def test_events_are_typed_and_keep_lineage(self):
        self.add_file([event(1), event(2, schema_version=2, carbon_kg=0.3, genai_tokens=1200, value=None)])
        bronze = self.ingest()
        types = dict(bronze.dtypes)
        self.assertEqual(types["event_ts"], "timestamp")
        self.assertEqual(types["value"], "double")
        self.assertEqual(types["genai_tokens"], "bigint")
        row = bronze.where("event_id = 'evt_2'").first()
        self.assertIsNone(row["value"])                     # value nulo no es error: lo trata Silver
        self.assertEqual(row["genai_tokens"], 1200)
        self.assertEqual(row["source_file"], "events_part_0000.jsonl")
        self.assertEqual(str(row["event_date"]), "2025-08-01")

    def test_new_files_are_processed_incrementally_and_old_ones_not_again(self):
        self.add_file([event(1), event(2)])
        self.add_file([event(3)])
        self.assertEqual(self.ingest("r1").count(), 3)
        self.assertEqual(len(completed_batches(self.paths)), 2)       # 1 archivo por micro-lote

        self.assertEqual(self.ingest("r2").count(), 3)                # sin archivos nuevos: no hay lotes nuevos
        self.assertEqual(len(completed_batches(self.paths)), 2)

        self.add_file([event(4), event(5)])                           # llega un archivo después
        bronze = self.ingest("r3")
        self.assertEqual(bronze.count(), 5)
        last = completed_batches(self.paths)[-1]
        self.assertEqual((last["run_id"], last["files"], last["read"]), ("r3", ["events_part_0002.jsonl"], 2))

    def test_structural_errors_go_to_quarantine(self):
        self.add_file([event(1),
                       '{"event_id": "evt_2", "timestamp": ',                 # JSON cortado
                       event(3, timestamp="ayer a la tarde"),
                       event(4, timestamp=None),
                       event(5, value="mucho")])
        bronze = self.ingest()
        self.assertEqual([r.event_id for r in bronze.collect()], ["evt_1"])
        batch = completed_batches(self.paths)[0]
        self.assertEqual(batch["quarantine_by_rule"], {"malformed_json": 1, "timestamp_invalid": 2, "cast_error": 1})
        q = {r.record_key: r for r in self.spark.read.parquet(str(self.paths.quarantine)).collect()}
        self.assertEqual(q["evt_5"].rule_id, "cast_error")
        self.assertIn("value", q["evt_5"].reason)
        self.assertIn('"timestamp": ', q["∅"].raw_record)          # la línea original se conserva tal cual

    def test_replaying_a_batch_does_not_duplicate_rows(self):
        """Simula que Spark re-ejecuta el lote 0 (corte antes de confirmar el checkpoint)."""
        self.add_file([event(1), event(2)])
        # El mismo micro-lote que armaría Spark, construido a mano para poder entregarlo dos veces
        batch = (self.spark.read.schema(EVENTS_RAW_SCHEMA).json(str(self.paths.landing))
                 .withColumn("source_file", F.lit("events_part_0000.jsonl"))
                 .withColumn("arrival_ts", F.current_timestamp()))
        write = make_batch_writer(self.paths, "replay")
        write(batch, 0)
        write(batch, 0)
        self.assertEqual(read_bronze_events(self.spark, self.paths).count(), 2)

        # La re-ejecución puede ocurrir otro día: la copia vieja (otra ingest_date) también se reemplaza
        old_day = self.paths.bronze / "ingest_date=2000-01-01"
        (self.paths.bronze / next(self.paths.bronze.iterdir()).name).rename(old_day)
        write(batch, 0)
        self.assertFalse(old_day.exists())
        self.assertEqual(read_bronze_events(self.spark, self.paths).count(), 2)

    def test_batch_without_manifest_is_ignored(self):
        """Un lote que se cortó a mitad de escritura (sin manifiesto) no se lee."""
        self.add_file([event(1)])
        self.add_file([event(2)])
        self.ingest()
        (self.paths.manifest / "batch_id=1.json").unlink()
        self.assertEqual([r.event_id for r in read_bronze_events(self.spark, self.paths).collect()], ["evt_1"])


if __name__ == "__main__":
    unittest.main()
