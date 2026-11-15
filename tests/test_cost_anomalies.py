"""Pruebas de anomalías de costo (E2-09).

Ejecutar: python -m unittest tests.test_cost_anomalies -v
Usan series de costo diario armadas a mano; NO usan el dataset real.
"""
import datetime as dt
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from src.common.runlog import RunLog
from src.common.spark import get_spark
from src.gold.anomalies import build_anomalies, score

GOLD = ("org_id string, usage_date date, service string, org_name string, plan_tier string, "
        "daily_cost_usd decimal(18,6), requests double, events long, negative_cost_events long, "
        "negative_cost_usd decimal(18,6)")
START = dt.date(2025, 7, 1)
NORMAL = [10, 11, 9, 10, 12, 10, 9, 11, 10, 10, 11, 9, 10, 12, 10]       # 15 días "normales" alrededor de USD 10


def series(costs, org="org_1", service="compute", start=START, negative_on=()):
    return [(org, start + dt.timedelta(days=i), service, "Org", "pro", Decimal(str(c)), 1.0, 1,
             1 if i in negative_on else 0, Decimal("-1") if i in negative_on else Decimal("0"))
            for i, c in enumerate(costs)]


class CostAnomalyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-cost-anomalies")

    def scored(self, rows):
        df = score(self.spark.createDataFrame(rows, GOLD), "t")
        return {(r.org_id, r.service, (r.usage_date - START).days): r for r in df.collect()}

    def test_spike_is_flagged_with_context(self):
        r = self.scored(series(NORMAL + [60]))[("org_1", "compute", 15)]
        self.assertTrue(r.is_spike)
        self.assertEqual((r.detection, r.anomaly_type, r.score_status), ("mad", "spike", "puntuado"))
        self.assertEqual(r.baseline_median_usd, 10.0)
        self.assertEqual(r.excess_cost_usd, 50.0)
        self.assertGreater(r.robust_z, 3.5)

    def test_normal_days_are_not_flagged(self):
        rows = self.scored(series(NORMAL + [12]))
        self.assertFalse(any(r.is_spike for r in rows.values()))

    def test_small_excess_is_not_material(self):
        tiny = [0.05, 0.06, 0.05, 0.04, 0.05, 0.05, 0.06, 0.05, 0.04, 0.05, 0.5]
        r = self.scored(series(tiny))[("org_1", "compute", 10)]
        self.assertGreater(r.robust_z, 3.5)                              # estadísticamente raro...
        self.assertFalse(r.is_spike)                                     # ...pero USD 0,45 no es material

    def test_baseline_only_looks_at_the_past(self):
        without = self.scored(series(NORMAL))[("org_1", "compute", 10)]
        with_future_spike = self.scored(series(NORMAL[:12] + [500] + NORMAL[13:]))[("org_1", "compute", 10)]
        self.assertEqual(without.robust_z, with_future_spike.robust_z)  # un spike posterior no cambia el día 10
        self.assertEqual(without.history_days, 10)

    def test_robust_z_is_not_masked_by_a_previous_spike(self):
        rows = self.scored(series(NORMAL[:8] + [80] + NORMAL[8:] + [45]))
        second = rows[("org_1", "compute", 16)]
        self.assertTrue(second.is_spike)                                 # la MAD no se deja inflar por el 80
        self.assertLess(second.z_classic, 3)                             # el z clásico sí: el 80 infla el desvío

    def test_cold_start_uses_the_service_percentile(self):
        others = [row for o in range(5) for row in series(NORMAL, org=f"org_{o + 2}")]
        rows = self.scored(others + series([10, 12, 300]))
        r = rows[("org_1", "compute", 2)]
        self.assertEqual((r.score_status, r.detection, r.severity), ("historia_insuficiente", "percentil_servicio", "alta"))
        self.assertIsNone(r.robust_z)

    def test_zero_mad_falls_back_to_mean_absolute_deviation(self):
        flat = [10, 10, 10, 10, 10, 10, 10, 10, 12, 10]                  # MAD = 0, pero hay algo de variación
        r = self.scored(series(flat + [40]))[("org_1", "compute", 10)]
        self.assertEqual(r.baseline_mad_usd, 0.0)
        self.assertTrue(r.is_spike)

    def test_negative_cost_days_are_flagged_for_review(self):
        r = self.scored(series(NORMAL, negative_on=(12,)))[("org_1", "compute", 12)]
        self.assertEqual((r.is_anomaly, r.is_spike, r.anomaly_type, r.severity),
                         (True, False, "costo_negativo", "revisar"))

    def test_build_keeps_one_row_per_gold_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            gold = self.spark.createDataFrame(series(NORMAL + [60]) + series(NORMAL, service="storage"), GOLD)
            with RunLog("test_g3", runs_dir=Path(tmp) / "_runs") as run:
                build_anomalies(self.spark, run, gold, Path(tmp) / "mart")
            self.assertTrue(all(b["ok"] for b in run.data["balances"]))
            self.assertEqual((run.data["counts"]["anomaly_filas"], run.data["counts"]["flag_spike"]), (31, 1))


if __name__ == "__main__":
    unittest.main()
