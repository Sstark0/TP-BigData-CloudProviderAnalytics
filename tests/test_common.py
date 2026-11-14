"""Pruebas de la base común: rutas, registro de ejecuciones y lector CSV.

Ejecutar: python -m unittest tests.test_common -v
Requieren Java y las dependencias de requirements.txt. No usan el dataset: crean sus propios archivos.

Esencialmente, buscamos saber si el lector responde bien ante un caso específico
"""
import json
import tempfile
import unittest
from pathlib import Path

from src.common import config
from src.common.io import (CORRUPT, SchemaMismatchError, add_technical_columns, read_landing_csv,
                           validate_csv_header, write_parquet)
from src.common.runlog import RunLog, describe_file, latest_run
from src.common.schemas import CSV_COLUMNS
from src.common.spark import get_spark

RESOURCES_HEADER = ",".join(CSV_COLUMNS["resources"])


def write_csv(folder: Path, name: str, lines) -> Path:
    path = folder / f"{name}.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


class ConfigTests(unittest.TestCase):
    def test_table_path_follows_zone_layout(self):
        self.assertEqual(config.table_path("bronze", "users"), config.BRONZE / "users")
        self.assertEqual(config.quarantine_path("usage_events"), config.QUARANTINE / "source=usage_events")
        self.assertEqual(config.checkpoint_path("s1_events"), config.CHECKPOINTS / "s1_events")

    def test_unknown_zone_is_rejected(self):
        with self.assertRaises(ValueError):
            config.table_path("platinum", "users")


class RunLogTests(unittest.TestCase):
    def test_run_is_written_with_counts_and_balance(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "in.csv"
            source.write_text("a,b\n1,2\n", encoding="utf-8")
            with RunLog("demo", runs_dir=tmp) as run:
                run.add_inputs([source])
                run.count("leidas", 10)
                run.count("aceptadas", 8)
                run.count("rechazadas", 2)
                self.assertTrue(run.check_balance("leidas", ["aceptadas", "rechazadas"]))
            saved = json.loads(run.path.read_text(encoding="utf-8"))
            self.assertEqual(saved["status"], "ok")
            self.assertEqual(saved["counts"], {"leidas": 10, "aceptadas": 8, "rechazadas": 2})
            self.assertEqual(saved["inputs"][0], describe_file(source))
            self.assertTrue(saved["balances"][0]["ok"])
            self.assertEqual(latest_run("demo", runs_dir=tmp)["run_id"], run.run_id)

    def test_failed_run_is_recorded_and_error_is_not_hidden(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(RuntimeError):
                with RunLog("demo", runs_dir=tmp) as run:
                    raise RuntimeError("fallo simulado")
            saved = json.loads(run.path.read_text(encoding="utf-8"))
            self.assertEqual(saved["status"], "error")
            self.assertIn("fallo simulado", saved["error"])
            self.assertIsNone(latest_run("demo", runs_dir=tmp))            # no hay ninguna ejecución "ok"

    def test_balance_that_does_not_close_is_flagged(self):
        with tempfile.TemporaryDirectory() as tmp:
            with RunLog("demo", runs_dir=tmp) as run:
                run.count("leidas", 10)
                run.count("aceptadas", 7)
                self.assertFalse(run.check_balance("leidas", ["aceptadas"]))


class CsvReaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests-common")

    def test_quoted_json_with_commas_is_read_whole(self):
        """D-10: sin escape='"' el campo tags_json se cortaba en la primera coma interna."""
        with tempfile.TemporaryDirectory() as tmp:
            write_csv(Path(tmp), "resources", [
                RESOURCES_HEADER,
                'res_1,org_1,compute,sa-east,2025-08-14,running,"[""env:prod"",""pii:true""]"',
                "res_2,org_1,storage,us-east,2025-08-15,running,",
            ])
            rows = {r["resource_id"]: r for r in read_landing_csv(self.spark, "resources", landing=tmp).collect()}
            self.assertEqual(rows["res_1"]["tags_json"], '["env:prod","pii:true"]')
            self.assertIsNone(rows["res_2"]["tags_json"])
            self.assertIsNone(rows["res_1"][CORRUPT])

    def test_header_in_another_order_is_rejected(self):
        """Con esquema explícito las columnas se asignan por posición: un encabezado distinto debe fallar."""
        with tempfile.TemporaryDirectory() as tmp:
            swapped = RESOURCES_HEADER.replace("resource_id,org_id", "org_id,resource_id")
            path = write_csv(Path(tmp), "resources", [swapped, "org_1,res_1,compute,sa-east,2025-08-14,running,"])
            with self.assertRaises(SchemaMismatchError):
                validate_csv_header(path, CSV_COLUMNS["resources"])
            with self.assertRaises(SchemaMismatchError):
                read_landing_csv(self.spark, "resources", landing=tmp)

    def test_row_with_extra_columns_is_marked_not_lost(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_csv(Path(tmp), "resources", [
                RESOURCES_HEADER,
                "res_1,org_1,compute,sa-east,2025-08-14,running,,sobra,sobra",
                "res_2,org_1,storage,us-east,2025-08-15,running,",
            ])
            df = read_landing_csv(self.spark, "resources", landing=tmp).cache()   # cache: Spark lo exige para filtrar por _corrupt_record
            self.assertEqual(df.count(), 2)
            self.assertEqual(df.where(df[CORRUPT].isNotNull()).count(), 1)

    def test_technical_columns_and_single_file_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_csv(Path(tmp), "resources", [RESOURCES_HEADER, "res_1,org_1,compute,sa-east,2025-08-14,running,"])
            df = add_technical_columns(read_landing_csv(self.spark, "resources", landing=tmp), run_id="run-test")
            row = df.collect()[0]
            self.assertEqual(row["source_file"], "resources.csv")
            self.assertEqual(row["run_id"], "run-test")
            self.assertIsNotNone(row["arrival_ts"])
            self.assertIsNotNone(row["ingest_ts"])
            out = Path(tmp) / "out"
            write_parquet(df, out)
            self.assertEqual(len(list(out.glob("*.parquet"))), 1)

    def test_unknown_source_is_rejected(self):
        with self.assertRaises(ValueError):
            read_landing_csv(self.spark, "no_existe")


if __name__ == "__main__":
    unittest.main()
