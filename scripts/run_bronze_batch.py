"""B1 · Ingesta batch de las fuentes CSV a Bronze (E2-02).

Uso (desde la raíz del repositorio, con el dataset en datalake/landing/):
    python scripts/run_bronze_batch.py                       # las 7 fuentes
    python scripts/run_bronze_batch.py customers_orgs users  # solo algunas

Re-ejecutarlo el mismo día reemplaza la partición del día, no duplica filas.
Deja un registro por fuente en datalake/_runs/ y un resumen en evidence/entrega2/e2_02_bronze_batch.json.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.batch.bronze import load_source
from src.common.config import BRONZE, EVIDENCE_DIR
from src.common.runlog import RunLog, describe_output, utc_now
from src.common.schemas import CSV_COLUMNS
from src.common.spark import get_spark


def main(sources) -> int:
    unknown = set(sources) - set(CSV_COLUMNS)
    if unknown:
        print(f"Fuentes desconocidas: {sorted(unknown)}. Válidas: {sorted(CSV_COLUMNS)}")
        return 2
    spark = get_spark("b1-bronze-batch")
    summary = {"generado_utc": utc_now(), "fuentes": {}}
    for source in sources:
        with RunLog(f"b1_bronze_{source}") as run:
            counts = load_source(spark, source, run)
        summary["fuentes"][source] = {"run_id": run.run_id, "ingest_date": run.data["params"]["ingest_date"],
                                      "counts": counts, "bronze": describe_output(BRONZE / source),
                                      "balances": run.data["balances"]}
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    (out / "e2_02_bronze_batch.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or list(CSV_COLUMNS)))
