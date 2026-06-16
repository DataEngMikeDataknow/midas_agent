import os
import sys

try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

import argparse
import logging
from pyspark.sql import SparkSession
from midas.agent.cases.variacion_significativa.tools import VariacionSignificativaToolBuilder

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="MIDAS Stage 4 SQL Functions Builder")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--pipeline_sp", required=False, default=None)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    builder = VariacionSignificativaToolBuilder(spark)
    builder.build_sql_functions(args.catalog, args.schema, args.pipeline_sp)
    log.info("SQL Functions Etapa 4 creadas correctamente.")


if __name__ == "__main__":
    main()
