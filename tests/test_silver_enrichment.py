"""Pruebas de Silver de maestros, enriquecimiento y features (E2-05).

    python -m unittest tests.test_silver_enrichment -v
    
Usan DataFrames y carpetas temporales construidos a mano; NO usan el dataset real.
"""
import datetime as dt
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from src.batch.silver_features import build_features, daily_features, enrich
from src.batch.silver_masters import build_masters, conform_resources
from src.common.runlog import RunLog
from src.common.spark import get_spark

D1, D2 = dt.date(2025, 8, 1), dt.date(2025, 8, 2)
EVENT_COLS = ("event_id string, event_ts timestamp, org_id string, resource_id string, service string, "
              "region string, metric string, value double, unit string, schema_version int, "
              "cost_usd_increment double, genai_tokens long, carbon_kg double, arrival_path string, "
              "source_file string, event_date date")


def ev(i, org, res, metric, value, cost, day=D1, v2=False):
    ts = dt.datetime.combine(day, dt.time(10, 0))
    return (i, ts, org, res, "compute", "us-east-1", metric, value, None, 2 if v2 else 1, cost,
            500 if v2 else None, 0.01 if v2 else None, "a_tiempo", "f.jsonl", day)


class SilverEnrichmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-silver-enrichment")

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def bronze_snapshot(self, table, schema, rows, day):
        df = self.spark.createDataFrame(rows, schema)
        df.write.mode("overwrite").parquet(str(self.tmp / "bronze" / table / f"ingest_date={day}"))

    def masters(self):
        org_schema = ("org_id string, org_name string, industry string, hq_region string, plan_tier string, "
                      "is_enterprise boolean, signup_date date, sales_rep string, lifecycle_stage string, "
                      "marketing_source string, nps_score double, source_file string")
        old = ("org_1", "Vieja SA", "Retail", "us-east", "free", False, D1, "rep_a", "lead", "event", None, "o.csv")
        new = ("org_1", " Nueva SA ", "Retail", "us-east", "pro", False, D1, "rep_a", "active", "event", 9.0, "o.csv")
        self.bronze_snapshot("customers_orgs", org_schema, [old], "2026-11-01")
        self.bronze_snapshot("customers_orgs", org_schema, [new], "2026-11-14")
        res_schema = ("resource_id string, org_id string, service string, region string, created_at date, "
                      "state string, tags_json string, source_file string")
        self.bronze_snapshot("resources", res_schema, [
            ("res_1", "org_1", "compute", "us-east-1", D1, "running", '["env:prod", "pii:true"]', "r.csv"),
            ("res_2", "org_1", "compute", "us-east-1", D1, "stopped", None, "r.csv")], "2026-11-14")
        with RunLog("test_b3", runs_dir=self.tmp / "_runs") as run:
            return build_masters(self.spark, run, self.tmp / "bronze", self.tmp / "silver")

    def test_masters_take_the_latest_snapshot(self):
        orgs = self.masters()["customers_orgs"].collect()
        self.assertEqual(len(orgs), 1)
        self.assertEqual((orgs[0].org_name, orgs[0].plan_tier), ("Nueva SA", "pro"))   # última foto, sin espacios
        self.assertEqual(str(orgs[0].bronze_ingest_date), "2026-11-14")

    def test_resource_tags_are_parsed(self):
        res = {r.resource_id: r for r in self.masters()["resources"].collect()}
        self.assertEqual((res["res_1"].env, res["res_1"].has_pii), ("prod", True))
        self.assertEqual((res["res_2"].tags, res["res_2"].env, res["res_2"].has_pii), ([], None, False))

    def test_enrichment_keeps_events_without_master(self):
        m = self.masters()
        events = self.spark.createDataFrame([ev("e1", "org_1", "res_1", "requests", 100.0, 1.0),
                                             ev("e2", "org_x", "res_x", "requests", 50.0, 2.0)], EVENT_COLS)
        rows = {r.event_id: r for r in enrich(events, m["customers_orgs"], m["resources"]).collect()}
        self.assertEqual(len(rows), 2)
        self.assertEqual((rows["e1"].org_name, rows["e1"].resource_env, rows["e1"].org_found), ("Nueva SA", "prod", True))
        self.assertEqual((rows["e2"].org_name, rows["e2"].org_found, rows["e2"].resource_found), (None, False, False))

    def test_daily_features_do_not_mix_metrics(self):
        m = self.masters()
        events = self.spark.createDataFrame([
            ev("e1", "org_1", "res_1", "requests", 100.0, 0.1, v2=True),
            ev("e2", "org_1", "res_1", "requests", None, 0.2),                 # value nulo: no suma requests
            ev("e3", "org_1", "res_1", "cpu_hours", 2.5, 0.3),
            ev("e4", "org_1", "res_1", "cpu_hours", 1.0, -0.05, day=D2),       # ese día no hay requests
        ], EVENT_COLS)
        f = {r.event_date: r for r in daily_features(enrich(events, m["customers_orgs"], m["resources"])).collect()}
        self.assertEqual(f[D1].daily_cost_usd, Decimal("0.6"))                   # 0.1 + 0.2 + 0.3, exacto
        self.assertEqual((f[D1].requests, f[D1].requests_events, f[D1].cpu_hours), (100.0, 1, 2.5))
        self.assertEqual((f[D1].genai_tokens, f[D1].carbon_coverage, f[D1].events), (500, 0.3333, 3))
        self.assertIsNone(f[D2].requests)                                       # NULL, no 0: no hubo requests
        self.assertEqual(f[D2].daily_cost_usd, Decimal("-0.05"))

    def test_build_features_reconciles_with_silver(self):
        m = self.masters()
        events = self.spark.createDataFrame([ev(f"e{i}", "org_1", "res_1", "requests", 10.0, 0.1, day=d)
                                             for i, d in enumerate([D1, D1, D2])], EVENT_COLS)
        with RunLog("test_b3", runs_dir=self.tmp / "_runs") as run:
            build_features(self.spark, run, events, m["customers_orgs"], m["resources"], self.tmp / "silver")
        self.assertTrue(all(b["ok"] for b in run.data["balances"]))
        self.assertEqual(run.data["counts"]["features_org_dia"], 2)
        self.assertEqual(run.data["counts"]["costo_features_micro_usd"], 300_000)


if __name__ == "__main__":
    unittest.main()
