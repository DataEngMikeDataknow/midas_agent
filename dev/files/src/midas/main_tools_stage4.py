import argparse
import logging
import os
import sys

try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from pyspark.sql import SparkSession
from midas.agents.variacion_significativa.tools import VariacionSignificativaToolBuilder

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def _parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "si", "sí"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Valor booleano inválido: {value!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Midas Stage 4 Tool Builder Runner")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--pipeline_sp", required=False, default=None)
    parser.add_argument("--grant_account_users", type=_parse_bool, default=False)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    builder = VariacionSignificativaToolBuilder(spark)
    builder.build_sql_functions(
        catalog=args.catalog,
        schema=args.schema,
        pipeline_sp=args.pipeline_sp,
        grant_account_users=args.grant_account_users,
    )
    log.info("SQL Functions Stage 4 creadas correctamente.")


if __name__ == "__main__":
    main()
