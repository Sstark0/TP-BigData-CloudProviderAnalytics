"""Pruebas de la huella de contenido (E2-10).

Ejecutar: python -m unittest tests.test_fingerprint -v
"""
import unittest

from src.common.fingerprint import content_fingerprint
from src.common.spark import get_spark

SCHEMA = "id string, cost double, gold_run_id string, gold_processed_ts string"


class FingerprintTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-fingerprint")

    def fp(self, rows, partitions=1):
        return content_fingerprint(self.spark.createDataFrame(rows, SCHEMA).repartition(partitions))

    def test_same_content_same_fingerprint_regardless_of_order_and_files(self):
        rows = [("a", 1.0, "r1", "t1"), ("b", 2.0, "r1", "t1"), ("c", 3.0, "r1", "t1")]
        self.assertEqual(self.fp(rows), self.fp(list(reversed(rows)), partitions=3))

    def test_volatile_columns_are_ignored(self):
        first = [("a", 1.0, "r1", "2026-11-15 08:00"), ("b", 2.0, "r1", "2026-11-15 08:00")]
        rerun = [("a", 1.0, "r2", "2026-11-15 09:30"), ("b", 2.0, "r2", "2026-11-15 09:30")]
        self.assertEqual(self.fp(first), self.fp(rerun))

    def test_changed_value_changes_the_fingerprint(self):
        base = [("a", 1.0, "r1", "t"), ("b", 2.0, "r1", "t")]
        changed = [("a", 1.0, "r1", "t"), ("b", 2.5, "r1", "t")]
        self.assertEqual(self.fp(base)["filas"], self.fp(changed)["filas"])     # mismas filas...
        self.assertNotEqual(self.fp(base)["huella"], self.fp(changed)["huella"])  # ...distinto contenido

    def test_duplicated_row_changes_rows_and_fingerprint(self):
        base = [("a", 1.0, "r1", "t"), ("b", 2.0, "r1", "t")]
        duplicated = base + [("b", 2.0, "r2", "t2")]
        self.assertNotEqual(self.fp(base), self.fp(duplicated))
        self.assertEqual(self.fp(duplicated)["filas"], 3)


if __name__ == "__main__":
    unittest.main()
