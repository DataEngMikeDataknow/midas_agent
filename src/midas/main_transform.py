import os
import sys

# Agrega src/ al path para que el paquete `midas` sea importable en Databricks
# cuando el job se ejecuta como spark_python_task.
# __file__ no está definido en contextos ipykernel de Databricks; sys.argv[0] es el fallback.
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

import logging
import argparse
from pyspark.sql import SparkSession
from midas.transformations import SilverTransformer
from midas.framework.control_cargas import ControlCargasClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Midas Transformations Runner")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    # El plano de control puede vivir en otro catálogo/schema que el destino; por
    # defecto son el mismo, igual que en main_ingestion.py.
    parser.add_argument("--control_catalog", default=None)
    parser.add_argument("--control_schema", default=None)
    parser.add_argument("--job_name", default="midas_silver")
    parser.add_argument("--run_id", default=None)

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    # current_user() puede fallar según el modo de acceso del cluster: no debe
    # tumbar la corrida por un dato de trazabilidad.
    try:
        usuario = spark.sql("SELECT current_user()").collect()[0][0]
    except Exception:  # noqa: BLE001
        usuario = None

    control = ControlCargasClient(
        spark=spark,
        catalog=args.control_catalog or args.catalog,
        schema=args.control_schema or args.schema,
        run_id=args.run_id,
        usuario_ejecutor=usuario,
        job_name=args.job_name,
    )
    log.info("Silver: run_id=%s job_name=%s destino=%s.%s",
             control.run_id, args.job_name, args.catalog, args.schema)

    transformer = SilverTransformer(spark, args.catalog, args.schema, control)
    transformer.transform_bronze_to_silver()

if __name__ == "__main__":
    main()
