"""Pruebas del mart FinOps org_daily_usage_by_service (E2-07).

Ejecutar: python -m unittest tests.test_gold_finops -v
Usan un Silver enriquecido armado a mano; NO usan el dataset real.
"""
import datetime as dt
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from src.common.runlog import RunLog
from src.common.spark import get_spark
from src.gold.finops import build_gold_finops, build_top_services, fingerprint, org_daily_usage_by_service, \
    reference_queries, top_services_by_org

ENRICHED = ("event_id string, org_id string, resource_id string, service string, metric string, "
            "cost_usd decimal(18,6), requests double, cpu_hours double, storage_gb_hours double, genai_tokens long, "
            "carbon_kg double, org_name string, plan_tier string, industry string, unit_inferred boolean, "
            "cost_negative boolean, arrival_path string, event_date date")
FEATURES = "org_id string, event_date date, daily_cost_usd decimal(28,6)"
D = dt.date(2025, 8, 31)


def ev(i, service="compute", day=D, cost="1.0", requests=10.0, org="org_1", res="res_1", tokens=None,
       carbon=None, negative=False, inferred=False, path="a_tiempo"):
    metric = "requests" if requests is not None else "cpu_hours"
    return (i, org, res, service, metric, Decimal(cost), requests, None if requests is not None else 1.0, None,
            tokens, carbon, "Org Uno", "pro", "Retail", inferred, negative, path, day)


class GoldFinopsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-gold-finops")

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def gold(self, rows):
        return {(r.org_id, r.usage_date, r.service): r for r in
                org_daily_usage_by_service(self.spark.createDataFrame(rows, ENRICHED), "t").collect()}

    def test_one_row_per_org_day_service(self):
        g = self.gold([ev("e1"), ev("e2", res="res_2"), ev("e3", service="storage"),
                       ev("e4", day=D - dt.timedelta(days=1))])
        self.assertEqual(len(g), 3)
        row = g[("org_1", D, "compute")]
        self.assertEqual((row.events, row.resources_active, row.daily_cost_usd, row.requests),
                         (2, 2, Decimal("2.0"), 20.0))
        self.assertEqual((row.org_name, row.plan_tier), ("Org Uno", "pro"))

    def test_quality_indicators(self):
        g = self.gold([ev("e1", cost="5.0", carbon=0.1, path="tardio"),
                       ev("e2", cost="-2.5", negative=True, inferred=True),
                       ev("e3", requests=None, cost="0.5")])
        row = g[("org_1", D, "compute")]
        self.assertEqual(row.daily_cost_usd, Decimal("3.0"))           # el negativo se suma: está marcado, no excluido
        self.assertEqual((row.negative_cost_events, row.negative_cost_usd), (1, Decimal("-2.5")))
        self.assertEqual((row.unit_inferred_events, row.late_events, row.carbon_coverage), (1, 1, 0.3333))
        self.assertEqual((row.requests, row.cpu_hours), (20.0, 1.0))   # e1 + e2 son requests; e3 es cpu_hours

    def test_build_reconciles_and_rerun_has_same_fingerprint(self):
        rows = [ev("e1"), ev("e2", service="genai", tokens=900, cost="2.25"), ev("e3", org="org_2")]
        enriched = self.spark.createDataFrame(rows, ENRICHED)
        features = self.spark.createDataFrame([("org_1", D, Decimal("3.25")), ("org_2", D, Decimal("1.0"))], FEATURES)
        prints = []
        for _ in range(2):
            with RunLog("test_g1", runs_dir=self.tmp / "_runs") as run:
                gold = build_gold_finops(self.spark, run, enriched, features, self.tmp / "gold")
            self.assertTrue(all(b["ok"] for b in run.data["balances"]))
            prints.append(fingerprint(gold))
            self.assertEqual(gold.count(), 3)                          # re-ejecutar no duplica
        self.assertEqual(prints[0], prints[1])

    def test_build_fails_if_features_disagree(self):
        enriched = self.spark.createDataFrame([ev("e1")], ENRICHED)
        features = self.spark.createDataFrame([("org_1", D, Decimal("9.99"))], FEATURES)
        with self.assertRaises(RuntimeError):
            with RunLog("test_g1", runs_dir=self.tmp / "_runs") as run:
                build_gold_finops(self.spark, run, enriched, features, self.tmp / "gold")

    def test_reference_queries_use_the_last_days_of_the_dataset(self):
        old = D - dt.timedelta(days=20)
        rows = [ev("e1", service="compute", cost="5.0"), ev("e2", service="storage", cost="7.0"),
                ev("e3", service="genai", cost="100.0", day=old)]        # fuera de los últimos 14 días
        gold = org_daily_usage_by_service(self.spark.createDataFrame(rows, ENRICHED), "t")
        q = reference_queries(gold, "org_1", days=14, top_n=5)
        top = q["q2_top5_servicios_ultimos_14_dias"]
        self.assertEqual([r["service"] for r in top], ["storage", "compute"])
        self.assertEqual(len(q["q1_costos_y_requests_ultimos_7_dias"]), 2)


    def test_top_services_rank_rolling_14_day_windows(self):
        day = lambda n: D - dt.timedelta(days=n)
        rows = [ev("e1", service="compute", cost="5.0", day=day(0)), ev("e2", service="storage", cost="3.0", day=day(0)),
                ev("e3", service="storage", cost="4.0", day=day(5)),       # dentro de la ventana que termina hoy
                ev("e4", service="genai", cost="50.0", day=day(20))]       # solo en ventanas viejas
        gold = org_daily_usage_by_service(self.spark.createDataFrame(rows, ENRICHED), "t")
        top = {(r.window_end_date, r.cost_rank): r for r in top_services_by_org(gold, "t").collect()}
        self.assertEqual((top[(D, 1)].service, top[(D, 1)].cost_usd), ("storage", Decimal("7.0")))   # 3 + 4
        self.assertEqual((top[(D, 2)].service, top[(D, 2)].days_with_usage), ("compute", 1))
        self.assertNotIn((D, 3), top)                                       # genai quedó fuera de los 14 días
        self.assertEqual(top[(day(20), 1)].service, "genai")
        self.assertFalse(top[(day(20), 1)].window_complete)                 # primer día del dataset: ventana parcial
        self.assertTrue(top[(D, 1)].window_complete)

    def test_build_top_services_reconciles_with_the_mart(self):
        rows = [ev("e1", service="compute", cost="2.0"), ev("e2", service="genai", cost="9.0"), ev("e3", org="org_2")]
        gold = org_daily_usage_by_service(self.spark.createDataFrame(rows, ENRICHED), "t")
        with RunLog("test_g2", runs_dir=self.tmp / "_runs") as run:
            top = build_top_services(self.spark, run, gold, self.tmp / "top")
        self.assertEqual(run.data["counts"]["top_ultima_ventana_diferencias"], 0)
        self.assertEqual([r.service for r in top.where("org_id = 'org_1'").orderBy("cost_rank").collect()],
                         ["genai", "compute"])


if __name__ == "__main__":
    unittest.main()
