"""Pruebas del serving en Cassandra (E2-08) con una sesión simulada (tests/fake_cassandra.py).

Ejecutar: python -m unittest tests.test_serving -v
No se conectan a AstraDB: validan el esquema CQL, los parámetros de los INSERT, la carga desde los workers de
Spark, el upsert (re-cargar no duplica) y la limpieza de filas que ya no están en Gold.
"""
import datetime as dt
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from src.common.config import REPO_ROOT
from src.common.spark import get_spark
from src.serving.cassandra import TABLES, cql_statements, insert_cql, load_from_spark, partition_totals, \
    prune_stale, to_params, write_rows
from tests.fake_cassandra import FakeSession, FakeSessionFactory

D = dt.date(2025, 8, 31)
MART_SCHEMA = ("org_id string, usage_date date, service string, org_name string, plan_tier string, industry string, "
               "daily_cost_usd decimal(18,6), requests double, cpu_hours double, storage_gb_hours double, "
               "genai_tokens long, carbon_kg double, events long, resources_active long, negative_cost_events long, "
               "negative_cost_usd decimal(18,6), unit_inferred_events long, late_events long, carbon_coverage double, "
               "gold_run_id string")


def mart_row(org, day, service, cost="1.5"):
    row = {c: None for c in TABLES["org_daily_usage_by_service"]["columns"]}
    row.update(org_id=org, usage_date=day, service=service, daily_cost_usd=Decimal(cost), requests=10.0,
               events=3, resources_active=1, negative_cost_events=0, unit_inferred_events=0, late_events=0,
               negative_cost_usd=Decimal("0"), gold_run_id="r1")
    return row


class ServingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-serving")

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.log = Path(self._tmp.name) / "cassandra.jsonl"
        self.session = FakeSession(self.log)

    def tearDown(self):
        self._tmp.cleanup()

    def test_schema_file_defines_both_query_first_tables(self):
        statements = cql_statements(REPO_ROOT / "cql" / "01_schema.cql")
        self.assertEqual(len(statements), 2)
        mart, top = statements
        self.assertIn("PRIMARY KEY ((org_id), usage_date, service)", mart)
        self.assertIn("PRIMARY KEY ((org_id), window_end_date, cost_rank)", top)
        for table, cql in (("org_daily_usage_by_service", mart), ("top_services_by_org_14d", top)):
            for column in TABLES[table]["columns"] + ["loaded_at"]:
                self.assertRegex(cql, rf"\n\s+{column}\s+\w+", f"{table}.{column} no está en el CQL")

    def test_queries_file_has_the_two_queries(self):
        queries = cql_statements(REPO_ROOT / "cql" / "02_queries.cql")
        self.assertEqual(len(queries), 2)
        self.assertTrue(all(q.startswith("SELECT") and "org_id =" in q for q in queries))

    def test_insert_and_params_match(self):
        cql = insert_cql("org_daily_usage_by_service", "ks")
        params = to_params(mart_row("org_1", D, "compute"), "org_daily_usage_by_service", "ahora")
        self.assertEqual(cql.count("?"), len(params))
        self.assertIsInstance(params[TABLES["org_daily_usage_by_service"]["columns"].index("events")], int)
        self.assertEqual(params[-1], "ahora")                                   # loaded_at va último

    def test_write_rows_in_batches(self):
        rows = [mart_row("org_1", D - dt.timedelta(days=i), "compute") for i in range(1200)]
        self.assertEqual(write_rows(self.session, rows, "org_daily_usage_by_service", batch=500), 1200)
        self.assertEqual(len(self.session.state("org_daily_usage_by_service")), 1200)

    def test_load_from_spark_workers_and_reload_does_not_duplicate(self):
        rows = [mart_row(f"org_{o}", D - dt.timedelta(days=d), s) for o in range(3) for d in range(5)
                for s in ("compute", "storage")]
        df = self.spark.createDataFrame([tuple(r.values()) for r in rows], MART_SCHEMA)
        factory = FakeSessionFactory(self.log)
        self.assertEqual(load_from_spark(df, "org_daily_usage_by_service", factory, partitions=3), 30)
        self.assertEqual(load_from_spark(df, "org_daily_usage_by_service", factory, partitions=3), 30)
        self.assertEqual(len(self.session.state("org_daily_usage_by_service")), 30)   # upsert: sigue en 30
        totals = partition_totals(self.session, "org_daily_usage_by_service", ["org_0"], "daily_cost_usd")
        self.assertEqual(totals["org_0"], (10, Decimal("15.0")))

    def test_prune_removes_keys_that_left_gold(self):
        old = [mart_row("org_1", D, "compute"), mart_row("org_1", D, "storage"), mart_row("org_9", D, "genai")]
        write_rows(self.session, old, "org_daily_usage_by_service")
        gold = self.spark.createDataFrame([tuple(mart_row("org_1", D, "compute").values())], MART_SCHEMA)           # storage y org_9 ya no están
        self.assertEqual(prune_stale(self.session, gold, "org_daily_usage_by_service"), 2)
        self.assertEqual(list(self.session.state("org_daily_usage_by_service")), [("org_1", str(D), "compute")])
        self.assertEqual(prune_stale(self.session, gold, "org_daily_usage_by_service"), 0)   # nada más que borrar

    def test_keys_from_the_driver_match_gold_keys(self):
        """El driver devuelve fechas como cassandra.util.Date: igual a datetime.date pero con otro hash."""
        from cassandra.util import Date
        from src.serving.cassandra import _norm
        self.assertNotEqual(hash(Date(D)), hash(D))                      # el problema
        self.assertEqual(_norm(Date(D)), _norm(D))                       # la solución
        self.assertEqual({tuple(map(_norm, ("org_1", Date(D), "compute")))},
                         {tuple(map(_norm, ("org_1", D, "compute")))})


if __name__ == "__main__":
    unittest.main()
