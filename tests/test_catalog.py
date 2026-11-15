"""Pruebas del catálogo de datos (E2-11).

Ejecutar: python -m unittest tests.test_catalog -v
Verifican que el catálogo sea coherente consigo mismo y con los esquemas declarados en el código, sin leer el lago.
"""
import importlib.util
import unittest

from src.common.catalog import CLASSIFICATIONS, COLUMN_FLAGS, ROLES, TABLES, describe
from src.common.config import REPO_ROOT
from src.common.schemas import BRONZE_EVENTS_SCHEMA, CSV_COLUMNS

spec = importlib.util.spec_from_file_location("build_catalog", REPO_ROOT / "scripts" / "build_catalog.py")
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


class CatalogTests(unittest.TestCase):
    def test_every_table_has_valid_owner_classification_and_sources(self):
        names = {t["name"] for t in TABLES}
        for t in TABLES:
            self.assertIn(t["owner"], ROLES, t["name"])
            self.assertIn(t["classification"], CLASSIFICATIONS, t["name"])
            for source in t["sources"]:
                self.assertTrue(source.startswith("landing/") or source in names, f"{t['name']} ← {source}")

    def test_declared_schemas_are_fully_described(self):
        columns = {c for cols in CSV_COLUMNS.values() for c in cols} | {f.name for f in BRONZE_EVENTS_SCHEMA}
        self.assertEqual(sorted(c for c in columns if not describe(c)), [])

    def test_personal_data_is_classified_as_personal(self):
        self.assertEqual(COLUMN_FLAGS["email"], "personal")
        users = next(t for t in TABLES if t["name"] == "bronze/users")
        self.assertEqual(users["classification"], "personal")
        # el correo no se propaga: ninguna tabla deriva de bronze/users
        self.assertFalse([t["name"] for t in TABLES if "bronze/users" in t["sources"]])

    def test_render_marks_undescribed_columns(self):
        entry = {"name": "gold/x", "layer": "Gold", "grain": "g", "key": "k", "process": "p", "sources": ["landing/x"],
                 "owner": "Gold y serving", "classification": "interna", "decision": "D-0", "description": "d",
                 "rows": 1, "files": 1, "bytes": 10, "partition": "—",
                 "columns": [("org_id", "string"), ("columna_nueva", "int"), ("run_id", "string")]}
        text = build.render([entry], "hoy")
        self.assertIn("| `columna_nueva` | int | **SIN DESCRIPCIÓN** |", text)
        self.assertIn("| `run_id` | string | Ejecución que escribió la fila", text)
        self.assertIn("1 archivo ·", text)


if __name__ == "__main__":
    unittest.main()
