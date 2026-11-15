"""
S1 · Serving en Cassandra/AstraDB: esquema, carga desde Spark, limpieza y verificación (E2-08, D-17).

Conexión
    AstraDB con el driver de Python (cassandra-driver), el Secure Connect Bundle (SCB) y un token de aplicación.
    Usuario = la palabra "token", contraseña = el token. Todo se lee de variables de entorno;
    nada queda en el repositorio:
        ASTRA_DB_APPLICATION_TOKEN   token AstraCS:...
        ASTRA_SECURE_BUNDLE_PATH     ruta absoluta al zip del SCB (sin descomprimir)
        ASTRA_DB_KEYSPACE            keyspace (por defecto cloud_analytics)

Carga desde Spark
    df.foreachPartition: cada partición de Spark abre su propia sesión y escribe sus filas con sentencias
    preparadas en paralelo (execute_concurrent). No hace falta el conector de Spark ni JARs extra.

    En Cassandra un INSERT con la misma clave primaria reemplaza la fila (upsert): re-cargar no duplica.
    Una clave puede desaparecer de Gold (por ejemplo, tras corregir un dato). Esa fila quedaría huérfana en Cassandra. 
    `prune_stale` compara las claves de cada organización con las de Gold y borra las que sobran.
"""
import os
import re
from pathlib import Path

from pyspark.sql import DataFrame

KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE", "cloud_analytics")

# Tabla de Cassandra → (tabla de Gold, columnas de la clave primaria, columnas que se cargan)
TABLES = {
    "org_daily_usage_by_service": {
        "key": ["org_id", "usage_date", "service"],
        "columns": ["org_id", "usage_date", "service", "org_name", "plan_tier", "industry", "daily_cost_usd",
                    "requests", "cpu_hours", "storage_gb_hours", "genai_tokens", "carbon_kg", "events",
                    "resources_active", "negative_cost_events", "negative_cost_usd", "unit_inferred_events",
                    "late_events", "carbon_coverage", "gold_run_id"],
    },
    "top_services_by_org_14d": {
        "key": ["org_id", "window_end_date", "cost_rank"],
        "columns": ["org_id", "window_end_date", "cost_rank", "service", "cost_usd", "requests", "days_with_usage",
                    "window_days", "window_complete", "gold_run_id"],
    },
}
INT_COLUMNS = {"events", "resources_active", "negative_cost_events", "unit_inferred_events", "late_events",
               "cost_rank", "days_with_usage", "window_days"}


class AstraSettings:
    """Credenciales y parámetros de conexión, desde variables de entorno. Se puede enviar a los workers de Spark."""

    def __init__(self, token=None, bundle=None, keyspace=None, timeout=30.0):
        self.token = token or os.getenv("ASTRA_DB_APPLICATION_TOKEN")
        self.bundle = bundle or os.getenv("ASTRA_SECURE_BUNDLE_PATH")
        self.keyspace = keyspace or KEYSPACE
        self.timeout = timeout

    def check(self):
        missing = [n for n, v in (("ASTRA_DB_APPLICATION_TOKEN", self.token),
                                  ("ASTRA_SECURE_BUNDLE_PATH", self.bundle)) if not v]
        if missing:
            raise RuntimeError("Faltan variables de entorno: " + ", ".join(missing) +
                               ". Cargalas con: set -a; source ~/.astra/astra.env; set +a")
        if not Path(self.bundle).is_file():
            raise RuntimeError(f"No existe el Secure Connect Bundle en {self.bundle}")
        return self

    def __call__(self):
        """Abre una sesión. Así un AstraSettings sirve como `session_factory`."""
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster

        cluster = Cluster(cloud={"secure_connect_bundle": self.bundle},
                          auth_provider=PlainTextAuthProvider("token", self.token))
        session = cluster.connect()
        session.default_timeout = self.timeout
        return session


def cql_statements(path) -> list:
    """Sentencias de un archivo .cql, sin comentarios, separadas por ';'."""
    text = re.sub(r"--[^\n]*", "", Path(path).read_text(encoding="utf-8"))
    return [s.strip() for s in text.split(";") if s.strip()]


def apply_schema(session, path, keyspace: str = KEYSPACE) -> int:
    statements = [s.replace("cloud_analytics.", f"{keyspace}.") for s in cql_statements(path)]
    for statement in statements:
        session.execute(statement)
    return len(statements)


def insert_cql(table: str, keyspace: str = KEYSPACE) -> str:
    cols = TABLES[table]["columns"] + ["loaded_at"]
    return (f"INSERT INTO {keyspace}.{table} ({', '.join(cols)}) "
            f"VALUES ({', '.join('?' for _ in cols)})")


def to_params(row: dict, table: str, loaded_at) -> tuple:
    """Fila de Spark → parámetros del INSERT, con los tipos que espera Cassandra (int para las columnas int)."""
    values = []
    for c in TABLES[table]["columns"]:
        v = row[c]
        values.append(int(v) if (c in INT_COLUMNS and v is not None) else v)
    return tuple(values) + (loaded_at,)


def execute_many(session, statement, params: list, concurrency: int = 64) -> int:
    """Ejecuta un INSERT preparado para muchas filas en paralelo. Falla si alguna escritura falla."""
    if not params:
        return 0
    if hasattr(session, "execute_many"):              # sesiones de prueba (ver tests/test_serving.py)
        return session.execute_many(statement, params)
    from cassandra.concurrent import execute_concurrent_with_args
    execute_concurrent_with_args(session, statement, params, concurrency=concurrency, raise_on_first_error=True)
    return len(params)


def write_rows(session, rows, table: str, keyspace: str = KEYSPACE, batch: int = 500) -> int:
    """Escribe un iterable de filas (dicts o Rows de Spark) en `table`. Devuelve cuántas escribió."""
    import datetime as dt

    statement = session.prepare(insert_cql(table, keyspace))
    loaded_at = dt.datetime.now(dt.timezone.utc)
    written, pending = 0, []
    for row in rows:
        pending.append(to_params(row.asDict() if hasattr(row, "asDict") else row, table, loaded_at))
        if len(pending) >= batch:
            written += execute_many(session, statement, pending)
            pending = []
    return written + execute_many(session, statement, pending)


def load_from_spark(df: DataFrame, table: str, session_factory, keyspace: str = KEYSPACE, partitions: int = 4) -> int:
    """Carga un DataFrame de Gold en Cassandra desde los workers de Spark (foreachPartition).

    `session_factory` se envía a cada worker y abre allí su propia sesión (las sesiones no se pueden serializar).
    Devuelve las filas escritas, contadas con un acumulador de Spark.
    """
    written = df.sparkSession.sparkContext.accumulator(0)
    columns = TABLES[table]["columns"]

    def write_partition(rows):
        session = session_factory()
        try:
            written.add(write_rows(session, rows, table, keyspace))
        finally:
            shutdown = getattr(getattr(session, "cluster", None), "shutdown", None)
            if shutdown:
                shutdown()

    df.select(*columns).repartition(partitions).foreachPartition(write_partition)
    return written.value


def _norm(value):
    """Valor de clave comparable entre Spark y Cassandra.

    El driver devuelve las columnas `date` como cassandra.util.Date, que es igual (==) a un datetime.date pero
    con otro hash: dentro de un set nunca coincidirían y prune_stale borraría filas válidas. Se comparan como
    texto ISO (AAAA-MM-DD).
    """
    import datetime as dt
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if hasattr(value, "date") and callable(value.date):          # cassandra.util.Date
        return value.date().isoformat()
    return value


def keys_in_cassandra(session, table: str, org_id: str, keyspace: str = KEYSPACE) -> list:
    """Claves de una organización tal como las devuelve Cassandra (sirven para el DELETE)."""
    key = TABLES[table]["key"]
    rows = session.execute(f"SELECT {', '.join(key)} FROM {keyspace}.{table} WHERE org_id = %s", (org_id,))
    return [tuple(getattr(r, k) for k in key) for r in rows]


def prune_stale(session, gold: DataFrame, table: str, keyspace: str = KEYSPACE) -> int:
    """Borra de Cassandra las filas cuya clave ya no está en Gold. Recorre organización por organización."""
    key = TABLES[table]["key"]
    expected = {}
    for r in gold.select(*key).collect():
        expected.setdefault(r[0], set()).add(tuple(_norm(v) for v in r))
    orgs = set(expected) | _orgs_in_cassandra(session, table, keyspace)
    delete = session.prepare(f"DELETE FROM {keyspace}.{table} WHERE " + " AND ".join(f"{k} = ?" for k in key))
    stale = [k for org in sorted(orgs) for k in keys_in_cassandra(session, table, org, keyspace)
             if tuple(_norm(v) for v in k) not in expected.get(org, set())]
    return execute_many(session, delete, stale)


def _orgs_in_cassandra(session, table: str, keyspace: str) -> set:
    """Organizaciones (claves de partición) presentes en la tabla. SELECT DISTINCT sobre la clave de partición
    es la única forma eficiente de listar particiones en Cassandra."""
    return {r.org_id for r in session.execute(f"SELECT DISTINCT org_id FROM {keyspace}.{table}")}


def partition_totals(session, table: str, org_ids, measure: str, keyspace: str = KEYSPACE) -> dict:
    """COUNT(*) y SUM(measure) por organización (agregar dentro de una partición es barato en Cassandra)."""
    query = session.prepare(f"SELECT COUNT(*) AS n, SUM({measure}) AS total FROM {keyspace}.{table} WHERE org_id = ?")
    out = {}
    for org in org_ids:
        row = session.execute(query, (org,)).one()
        out[org] = (int(row.n), row.total)
    return out


def run_queries(session, path, keyspace: str = KEYSPACE) -> list:
    """Ejecuta las consultas de un .cql y devuelve sus filas (para la evidencia)."""
    results = []
    for statement in cql_statements(path):
        statement = statement.replace("cloud_analytics.", f"{keyspace}.")
        rows = [{k: (v if isinstance(v, (int, float, bool, type(None))) else str(v)) for k, v in r._asdict().items()}
                for r in session.execute(statement)]
        results.append({"cql": " ".join(statement.split()), "filas": rows})
    return results
