"""Sesión de Cassandra simulada para las pruebas de serving. NO forma parte del pipeline

Guarda cada escritura como una línea en un archivo (append), así funciona aunque escriban varios workers de
Spark a la vez, en procesos distintos. Las lecturas reconstruyen el estado reproduciendo el archivo: igual que
Cassandra, un INSERT con una clave existente reemplaza la fila (upsert) y un DELETE la borra.
Entiende solo las sentencias que genera src/serving/cassandra.py.
"""
import json
import re
from collections import namedtuple
from pathlib import Path

from src.serving.cassandra import TABLES


class FakeResult(list):
    def one(self):
        return self[0] if self else None


class FakeSession:
    def __init__(self, path):
        self.path = Path(path)
        self.executed = []

    # --- escritura ---
    def prepare(self, cql):
        return cql

    def execute_many(self, statement, params):
        table = re.search(r"(?:INTO|FROM) \w+\.(\w+)", statement).group(1)
        op = "insert" if statement.startswith("INSERT") else "delete"
        with open(self.path, "a", encoding="utf-8") as f:
            for p in params:
                f.write(json.dumps({"op": op, "table": table, "params": list(p)}, default=str) + "\n")
        return len(params)

    # --- lectura ---
    def state(self, table):
        rows = {}
        if self.path.exists():
            cols = TABLES[table]["columns"] + ["loaded_at"]
            nkey = len(TABLES[table]["key"])
            for line in self.path.read_text(encoding="utf-8").splitlines():
                e = json.loads(line)
                if e["table"] != table:
                    continue
                key = tuple(e["params"][:nkey])
                if e["op"] == "insert":
                    rows[key] = dict(zip(cols, e["params"]))
                else:
                    rows.pop(key, None)
        return rows

    def execute(self, cql, params=None):
        self.executed.append(cql)
        if cql.startswith("CREATE"):
            return FakeResult()
        table = re.search(r"FROM \w+\.(\w+)", cql).group(1)
        rows = self.state(table)
        if "DISTINCT org_id" in cql:
            Row = namedtuple("Row", ["org_id"])
            return FakeResult(Row(o) for o in sorted({r["org_id"] for r in rows.values()}))
        org = params[0]
        mine = [r for r in rows.values() if r["org_id"] == org]
        if "COUNT(*)" in cql:
            measure = re.search(r"SUM\((\w+)\)", cql).group(1)
            Row = namedtuple("Row", ["n", "total"])
            from decimal import Decimal
            return FakeResult([Row(len(mine), sum((Decimal(str(r[measure])) for r in mine), Decimal(0)))])
        key = TABLES[table]["key"]
        Row = namedtuple("Row", key)
        return FakeResult(Row(*[r[k] for k in key]) for r in mine)


class FakeSessionFactory:
    """Se envía a los workers de Spark como lo haría AstraSettings: cada llamada abre una 'sesión'."""

    def __init__(self, path):
        self.path = str(path)

    def __call__(self):
        return FakeSession(self.path)
