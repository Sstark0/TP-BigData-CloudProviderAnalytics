"""Verificación de la base común sobre el dataset real.

Lee las 7 fuentes CSV con el lector común, cuenta filas y filas mal formadas, y deja el registro de la
ejecución. No escribe en ninguna zona del lago: solo lee Landing.

Uso (desde la raíz del repositorio, con el dataset en datalake/landing/):
    python scripts/check_common.py
"""
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.common.config import EVIDENCE_DIR, LANDING
from src.common.io import CORRUPT, add_technical_columns, read_landing_csv
from src.common.runlog import RunLog
from src.common.schemas import CSV_COLUMNS
from src.common.spark import get_spark

EXPECTED_ROWS = {"customers_orgs": 80, "users": 800, "resources": 400, "support_tickets": 1000,
                 "marketing_touches": 1500, "nps_surveys": 92, "billing_monthly": 240}


def main() -> int:
    spark = get_spark("check-common")
    with RunLog("e2_01_check_common") as run:
        run.add_inputs([LANDING / f"{s}.csv" for s in CSV_COLUMNS])
        for source in CSV_COLUMNS:
            df = add_technical_columns(read_landing_csv(spark, source), run.run_id).cache()
            run.count(f"{source}.filas", df.count())
            run.count(f"{source}.mal_formadas", df.where(F.col(CORRUPT).isNotNull()).count())
            run.count(f"{source}.columnas_tecnicas_nulas",
                      df.where(F.col("source_file").isNull() | F.col("arrival_ts").isNull()).count())
        pii = read_landing_csv(spark, "resources").where(F.col("tags_json").contains("pii:true")).count()
        run.count("resources.con_tag_pii", pii)
        ok = all(run.data["counts"][f"{s}.filas"] == n for s, n in EXPECTED_ROWS.items()) and pii == 85
        run.param("coincide_con_perfil_e1", ok)
    out = EVIDENCE_DIR / "entrega2"
    out.mkdir(parents=True, exist_ok=True)
    shutil.copy(run.path, out / "e2_01_base_comun.json")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
