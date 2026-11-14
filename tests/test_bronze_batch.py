"""Pruebas de la ingesta batch a Bronze (E2-02).

Ejecutar: python -m unittest tests.test_bronze_batch -v
Crean un Landing y un lago temporales con casos difíciles; NO usan el dataset real.
"""
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F

from src.batch.bronze import load_source
from src.common.runlog import RunLog
from src.common.schemas import CSV_COLUMNS
from src.common.spark import get_spark

HEADER = ",".join(CSV_COLUMNS["customers_orgs"])
GOOD_1 = "org_1,Nimbus,Education,sa-east,standard,True,2025-05-26,rep_c,active,partner,10.0"
GOOD_2 = "org_2,Nova,Retail,us-east,pro,False,2025-06-16,rep_d,lead,event,"


class BronzeBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-bronze-batch")

    def run_load(self, tmp: Path, lines):
        landing = tmp / "landing"
        landing.mkdir(exist_ok=True)
        (landing / "customers_orgs.csv").write_text("\n".join([HEADER, *lines]) + "\n", encoding="utf-8")
        with RunLog("test_b1", runs_dir=tmp / "_runs") as run:
            counts = load_source(self.spark, "customers_orgs", run, landing=landing,
                                 bronze_root=tmp / "bronze", quarantine_root=tmp / "quarantine")
        bronze = self.spark.read.parquet(str(tmp / "bronze" / "customers_orgs"))
        qdir = tmp / "quarantine" / "source=customers_orgs"
        quarantine = self.spark.read.parquet(str(qdir)) if any(qdir.rglob("*.parquet")) else None
        return counts, bronze, quarantine

    def test_valid_rows_are_typed_and_keep_lineage(self):
        with tempfile.TemporaryDirectory() as tmp:
            counts, bronze, quarantine = self.run_load(Path(tmp), [GOOD_1, GOOD_2])
            self.assertEqual(counts["aceptadas"], 2)
            self.assertIsNone(quarantine)
            types = dict(bronze.dtypes)
            self.assertEqual(types["is_enterprise"], "boolean")
            self.assertEqual(types["signup_date"], "date")
            self.assertEqual(types["nps_score"], "double")
            row = bronze.where("org_id = 'org_2'").first()
            self.assertIsNone(row["nps_score"])                   # vacío en el CSV = NULL, no error
            self.assertEqual(row["source_file"], "customers_orgs.csv")
            self.assertIn("ingest_date", bronze.columns)

    def test_bad_value_goes_to_quarantine_with_its_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad = GOOD_2.replace("org_2", "org_3").replace("2025-06-16", "16/06/2025")
            counts, bronze, quarantine = self.run_load(Path(tmp), [GOOD_1, bad])
            self.assertEqual(counts["quarantine.cast_error"], 1)
            q = quarantine.first()
            self.assertEqual(q["rule_id"], "cast_error")
            self.assertIn("signup_date", q["reason"])
            self.assertEqual(q["record_key"], "org_3")
            self.assertIn("16/06/2025", q["raw_record"])          # la fila original se conserva
            self.assertEqual(bronze.count(), 1)

    def test_missing_key_and_malformed_row_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            no_key = "," + GOOD_2.split(",", 1)[1]
            malformed = GOOD_1.replace("org_1", "org_9") + ",columna_de_mas"
            counts, _, quarantine = self.run_load(Path(tmp), [GOOD_1, no_key, malformed])
            rules = {r["rule_id"] for r in quarantine.collect()}
            self.assertEqual(rules, {"key_null", "malformed_row"})
            self.assertEqual(counts["aceptadas"], 1)

    def test_exact_duplicate_is_dropped_but_conflicting_key_is_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            conflict = GOOD_2.replace("pro", "enterprise")       # misma clave org_2, otro contenido
            counts, bronze, quarantine = self.run_load(Path(tmp), [GOOD_1, GOOD_1, GOOD_2, conflict])
            self.assertEqual(counts["duplicados_exactos"], 1)
            self.assertEqual(counts["quarantine.key_conflict"], 2)    # no se elige una versión al azar
            self.assertEqual(bronze.count(), 1)
            self.assertEqual(counts["leidas"], counts["aceptadas"] + counts["quarantine"] + counts["duplicados_exactos"])

    def test_rerun_same_day_does_not_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad_nps = GOOD_2.replace("org_2", "org_4").rstrip(",") + ",diez"     # nps_score no numérico
            lines = [GOOD_1, GOOD_2, bad_nps]
            first, bronze, quarantine = self.run_load(Path(tmp), lines)
            bronze_rows, quarantine_rows = bronze.count(), quarantine.count()   # se cuentan antes de re-ejecutar
            self.assertEqual((bronze_rows, quarantine_rows), (2, 1))
            second, bronze, quarantine = self.run_load(Path(tmp), lines)
            self.assertEqual(second, first)                                     # mismos conteos
            self.assertEqual((bronze.count(), quarantine.count()), (bronze_rows, quarantine_rows))
            self.assertEqual(bronze.select(F.countDistinct("run_id")).first()[0], 1)   # solo queda la última carga

if __name__ == "__main__":
    unittest.main()
